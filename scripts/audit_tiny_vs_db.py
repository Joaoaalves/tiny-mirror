"""Audit script: compare Tiny v3 catalog vs the VPS mirror DB.

Workflow per entity (products, orders, stock):

1. Fetch the full payload from Tiny and persist it as JSONL under
   ``audit/<entity>.jsonl``. JSONL is git-ignored so subsequent runs can
   reuse the local cache and avoid re-paying the rate-limit budget.
2. Pull the matching ID set out of the VPS Postgres over SSH.
3. Diff the two sets and print a concise reconciliation report.

Run from the repo root::

    poetry run python scripts/audit_tiny_vs_db.py products
    poetry run python scripts/audit_tiny_vs_db.py orders --days 90
    poetry run python scripts/audit_tiny_vs_db.py stock
    poetry run python scripts/audit_tiny_vs_db.py all --days 90

Add ``--refetch`` to ignore the JSONL cache and pull from Tiny again.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import sys
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
AUDIT_DIR = REPO_ROOT / "audit"
ENV_FILE = REPO_ROOT / ".env"

TINY_BASE_URL = "https://api.tiny.com.br/public-api/v3"
TINY_TOKEN_URL = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token"

# VPS connection used for the "compare" half. Override via env if you ever
# move the host or rotate the deploy key.
DEFAULT_VPS_HOST = "212.85.1.135"
DEFAULT_VPS_USER = "root"
DEFAULT_VPS_DB = "tiny_mirror_db"
DEFAULT_VPS_KEY = "/tmp/root-offshop"

PAGE_SIZE = 100
ORDERS_DEFAULT_DAYS = 90
TRANSIENT = {400, 408, 425, 429, 500, 502, 503, 504}


# ---------------------------------------------------------------------------
# Tiny client (self-contained on purpose — no Redis/Postgres required)
# ---------------------------------------------------------------------------
class TinyClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        access_token: str,
    ) -> None:
        self._cid = client_id
        self._csec = client_secret
        self._refresh = refresh_token
        self._access = access_token
        self._http = httpx.AsyncClient(timeout=60)

    async def close(self) -> None:
        await self._http.aclose()

    async def _refresh_oauth(self) -> None:
        resp = await self._http.post(
            TINY_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "client_id": self._cid,
                "client_secret": self._csec,
                "refresh_token": self._refresh,
            },
        )
        resp.raise_for_status()
        body = resp.json()
        self._access = body["access_token"]
        self._refresh = body.get("refresh_token", self._refresh)
        print(
            f"  [auth] token rotated; expires in {body.get('expires_in')}s",
            file=sys.stderr,
        )

    async def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        url = TINY_BASE_URL + path
        token_already_refreshed = False
        for attempt in range(7):
            try:
                resp = await self._http.get(
                    url,
                    params=params,
                    headers={"Authorization": f"Bearer {self._access}"},
                )
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                wait = min(2**attempt, 30)
                print(
                    f"  [retry] {path} {type(exc).__name__}; sleeping {wait}s",
                    file=sys.stderr,
                )
                await asyncio.sleep(wait)
                continue

            if resp.status_code == 200:
                return resp.json()  # type: ignore[no-any-return]
            if resp.status_code == 404:
                return None
            if resp.status_code == 401 and not token_already_refreshed:
                await self._refresh_oauth()
                token_already_refreshed = True
                continue
            if resp.status_code in TRANSIENT:
                wait = min(2**attempt, 30)
                print(
                    f"  [retry] {path} HTTP {resp.status_code}; sleeping {wait}s",
                    file=sys.stderr,
                )
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
        raise RuntimeError(f"Exceeded retries for {path}")

    async def list_products(self, situacao: str) -> AsyncIterator[dict[str, Any]]:
        offset = 0
        while True:
            res = await self.get(
                "/produtos",
                {"situacao": situacao, "limit": PAGE_SIZE, "offset": offset},
            )
            assert res is not None
            items = res.get("itens") or []
            total = int((res.get("paginacao") or {}).get("total", 0))
            for it in items:
                yield it
            offset += PAGE_SIZE
            if offset >= total or len(items) < PAGE_SIZE:
                break

    async def list_orders(
        self,
        date_from: date,
        date_to: date,
    ) -> AsyncIterator[dict[str, Any]]:
        offset = 0
        while True:
            res = await self.get(
                "/pedidos",
                {
                    "dataInicial": date_from.isoformat(),
                    "dataFinal": date_to.isoformat(),
                    "limit": PAGE_SIZE,
                    "offset": offset,
                    "orderBy": "asc",
                },
            )
            assert res is not None
            items = res.get("itens") or []
            total = int((res.get("paginacao") or {}).get("total", 0))
            for it in items:
                yield it
            offset += PAGE_SIZE
            if offset >= total or len(items) < PAGE_SIZE:
                break

    async def get_stock(self, product_tiny_id: int) -> dict[str, Any] | None:
        return await self.get(f"/estoque/{product_tiny_id}")


# ---------------------------------------------------------------------------
# JSONL helpers
# ---------------------------------------------------------------------------
def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


# ---------------------------------------------------------------------------
# VPS DB query (over SSH, read-only — uses sudo psql)
# ---------------------------------------------------------------------------
def vps_query_ints(sql: str) -> list[int]:
    host = os.environ.get("VPS_HOST", DEFAULT_VPS_HOST)
    user = os.environ.get("VPS_USER", DEFAULT_VPS_USER)
    db = os.environ.get("VPS_DB", DEFAULT_VPS_DB)
    key = os.environ.get("VPS_SSH_KEY", DEFAULT_VPS_KEY)

    if not Path(key).exists():
        print(f"SSH key not found: {key}. Set VPS_SSH_KEY env var.", file=sys.stderr)
        sys.exit(2)

    remote_cmd = f"sudo -u postgres psql -d {db} -tAc {json.dumps(sql)}"
    result = subprocess.run(
        ["ssh", "-i", key, "-o", "StrictHostKeyChecking=accept-new", f"{user}@{host}", remote_cmd],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        print(
            f"VPS query failed (exit {result.returncode}): {result.stderr.strip()}",
            file=sys.stderr,
        )
        sys.exit(2)
    out: list[int] = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(int(line))
        except ValueError:
            continue
    return out


# ---------------------------------------------------------------------------
# Reconciliation reporting
# ---------------------------------------------------------------------------
def report_diff(label: str, tiny_ids: set[int], db_ids: set[int]) -> bool:
    missing = tiny_ids - db_ids  # in Tiny but not in DB → real loss
    extra = db_ids - tiny_ids  # in DB but not in Tiny → soft-deleted, not yet excluded

    print()
    print(f"=== {label} reconciliation ===")
    print(f"  Tiny total:     {len(tiny_ids)}")
    print(f"  DB total:       {len(db_ids)}")
    print(f"  Missing in DB:  {len(missing)}")
    print(f"  Extra in DB:    {len(extra)}")
    if missing:
        sample = sorted(missing)[:15]
        print(f"  Missing sample: {sample}")
    if extra:
        sample = sorted(extra)[:15]
        print(f"  Extra sample:   {sample}")
    return not missing  # OK iff nothing is missing on the DB side


# ---------------------------------------------------------------------------
# Per-entity orchestrators
# ---------------------------------------------------------------------------
async def audit_products(client: TinyClient, refetch: bool) -> bool:
    out_path = AUDIT_DIR / "products.jsonl"
    if refetch or not out_path.exists():
        print("[products] fetching from Tiny (situacao=A then situacao=I)…")
        records: list[dict[str, Any]] = []
        for situacao in ("A", "I"):
            count = 0
            async for it in client.list_products(situacao):
                records.append({**it, "_situacao_query": situacao})
                count += 1
            print(f"[products]   {situacao}: {count}")
        write_jsonl(out_path, records)
        print(f"[products] wrote {len(records)} → {out_path}")
    else:
        print(f"[products] using cached {out_path} (use --refetch to overwrite)")

    tiny_records = read_jsonl(out_path)
    tiny_ids = {int(r["id"]) for r in tiny_records}
    db_ids = set(vps_query_ints("SELECT tiny_id FROM products"))
    return report_diff("Products", tiny_ids, db_ids)


async def audit_orders(client: TinyClient, days: int, refetch: bool) -> bool:
    out_path = AUDIT_DIR / "orders.jsonl"
    today = datetime.now(UTC).date()
    date_from = today - timedelta(days=days)
    if refetch or not out_path.exists():
        print(f"[orders] fetching from Tiny ({date_from} → {today})…")
        records: list[dict[str, Any]] = []
        async for it in client.list_orders(date_from, today):
            records.append(it)
        write_jsonl(out_path, records)
        print(f"[orders] wrote {len(records)} → {out_path}")
    else:
        print(f"[orders] using cached {out_path} (use --refetch to overwrite)")

    tiny_records = read_jsonl(out_path)
    tiny_ids = {int(r["id"]) for r in tiny_records}
    db_ids = set(
        vps_query_ints(
            f"SELECT tiny_id FROM orders WHERE order_date >= '{date_from.isoformat()}'"
        )
    )
    return report_diff("Orders", tiny_ids, db_ids)


async def audit_stock(client: TinyClient, refetch: bool) -> bool:
    """Stock is keyed by product_tiny_id and only refreshes active products
    on the server side. Audit against the same population: fetch /estoque/
    for every active id we already cached in products.jsonl.
    """
    products_path = AUDIT_DIR / "products.jsonl"
    if not products_path.exists():
        print(
            "[stock] needs audit/products.jsonl first — run `products` once.",
            file=sys.stderr,
        )
        return False

    active_ids = sorted(
        {
            int(r["id"])
            for r in read_jsonl(products_path)
            if r.get("situacao") == "A"
        }
    )
    print(f"[stock] {len(active_ids)} active products to inspect")

    out_path = AUDIT_DIR / "stock.jsonl"
    if refetch or not out_path.exists():
        records: list[dict[str, Any]] = []
        for idx, pid in enumerate(active_ids, start=1):
            payload = await client.get_stock(pid)
            if payload is None:
                continue
            payload["_product_tiny_id"] = pid
            records.append(payload)
            if idx % 25 == 0:
                print(f"[stock]   {idx}/{len(active_ids)} fetched")
        write_jsonl(out_path, records)
        print(f"[stock] wrote {len(records)} → {out_path}")
    else:
        print(f"[stock] using cached {out_path} (use --refetch to overwrite)")

    tiny_records = read_jsonl(out_path)
    # Tiny returns the id under several possible keys; coalesce.
    def _stock_id(r: dict[str, Any]) -> int | None:
        for key in ("_product_tiny_id", "idProduto", "id"):
            v = r.get(key)
            if v is not None:
                try:
                    return int(v)
                except (TypeError, ValueError):
                    continue
        return None

    tiny_ids = {tid for r in tiny_records if (tid := _stock_id(r)) is not None}
    db_ids = set(vps_query_ints("SELECT product_tiny_id FROM stock"))
    return report_diff("Stock", tiny_ids, db_ids)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "entity",
        choices=("products", "orders", "stock", "all"),
    )
    parser.add_argument("--days", type=int, default=ORDERS_DEFAULT_DAYS)
    parser.add_argument("--refetch", action="store_true")
    args = parser.parse_args()

    if not ENV_FILE.exists():
        print(f"Missing {ENV_FILE}. Tiny credentials must be present.", file=sys.stderr)
        return 2
    load_dotenv(ENV_FILE)

    cid = os.environ.get("TINY_CLIENT_ID", "")
    csec = os.environ.get("TINY_CLIENT_SECRET", "")
    refresh = os.environ.get("TINY_REFRESH_TOKEN", "")
    access = os.environ.get("TINY_ACCESS_TOKEN", "")
    if not all([cid, csec, refresh, access]):
        print(
            "TINY_CLIENT_ID/SECRET/REFRESH_TOKEN/ACCESS_TOKEN must be set in .env",
            file=sys.stderr,
        )
        return 2

    if not shutil.which("ssh"):
        print("ssh not found in PATH.", file=sys.stderr)
        return 2

    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    client = TinyClient(cid, csec, refresh, access)
    try:
        ok = True
        if args.entity in ("products", "all"):
            ok &= await audit_products(client, args.refetch)
        if args.entity in ("orders", "all"):
            ok &= await audit_orders(client, args.days, args.refetch)
        if args.entity in ("stock", "all"):
            ok &= await audit_stock(client, args.refetch)
    finally:
        await client.close()

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
