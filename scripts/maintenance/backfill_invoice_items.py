"""Backfill ``invoice_items`` for the last N days of NFs.

The invoice sync up to now only persisted NF headers — the ``itens[]``
array on each NF (one row per actual product line, with the real SKU
and tiny_id that was decremented in stock) was never written. Without
those lines, kit-component sales are invisible: ``order_items`` stores
only the parent kit SKU, so a single SKU like ``CAMP-CNJ-FACPEG`` may
have *hundreds* of phantom duplicates in Tiny and zero detectable sales
activity in our DB.

This script paginates through ``GET /notas`` for a date window, fetches
``GET /notas/{id}`` for each invoice, and upserts the line items via
psql. Idempotent — each invoice's lines are replaced atomically.

Designed to be run on the VPS as root (it reads
``/root/.openclaw/.env`` for the Tiny token and shells out to
``sudo -u postgres psql``).

Uso:
    # Default: backfill últimos 90 dias
    python3 scripts/maintenance/backfill_invoice_items.py

    # Janela customizada
    python3 scripts/maintenance/backfill_invoice_items.py --days 30
    python3 scripts/maintenance/backfill_invoice_items.py --from 2026-01-01 --to 2026-05-27

    # Resume — pula NFs que já têm itens persistidos
    python3 scripts/maintenance/backfill_invoice_items.py --skip-existing
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, date, datetime, timedelta

PAGE_SIZE = 100
BASE_URL = "https://api.tiny.com.br/public-api/v3"


def env_var(env_path: str, key: str) -> str | None:
    try:
        with open(env_path) as f:
            for line in f:
                if line.startswith(key + "="):
                    return line[len(key) + 1 :].strip().strip('"').strip("'")
    except FileNotFoundError:
        return None
    return None


def http_get(url: str, token: str, *, retries: int = 5) -> tuple[int, dict | None]:
    """GET with bearer auth, returning (status, parsed_json or None).

    Retries on 429/5xx with exponential backoff up to ``retries`` attempts.
    """
    delay = 1.0
    for _ in range(retries):
        req = urllib.request.Request(
            url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status, json.load(r)
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504, 400) and _ < retries - 1:
                time.sleep(delay)
                delay = min(delay * 2, 30)
                continue
            try:
                return e.code, json.loads(e.read())
            except Exception:
                return e.code, None
        except (TimeoutError, urllib.error.URLError):
            time.sleep(delay)
            delay = min(delay * 2, 30)
    return 0, None


def list_invoice_ids(token: str, date_from: date, date_to: date) -> list[int]:
    """Paginate /notas in [date_from, date_to] returning every NF id."""
    ids: list[int] = []
    offset = 0
    while True:
        url = (
            f"{BASE_URL}/notas?dataInicial={date_from.isoformat()}"
            f"&dataFinal={date_to.isoformat()}&limit={PAGE_SIZE}&offset={offset}"
        )
        code, body = http_get(url, token)
        if code != 200 or not isinstance(body, dict):
            print(f"  list /notas offset={offset} HTTP {code} — abortando")
            break
        items = body.get("itens") or []
        if not items:
            break
        ids.extend(int(it["id"]) for it in items if it.get("id"))
        pagination = body.get("paginacao") or {}
        total = int(pagination.get("total", 0))
        offset += PAGE_SIZE
        if (total and offset >= total) or len(items) < PAGE_SIZE:
            break
    return ids


def already_has_items(invoice_tiny_id: int) -> bool:
    """Check via psql if invoice_items already has rows for this NF."""
    out = (
        subprocess.check_output(
            [
                "sudo",
                "-u",
                "postgres",
                "psql",
                "-d",
                "tiny_mirror_db",
                "-tA",
                "-c",
                f"SELECT 1 FROM invoice_items WHERE invoice_tiny_id = {invoice_tiny_id} LIMIT 1",
            ]
        )
        .decode()
        .strip()
    )
    return out == "1"


def _sql_str(s: str | None) -> str:
    if s is None:
        return "NULL"
    return "'" + s.replace("'", "''") + "'"


def _sql_num(s) -> str:
    if s is None or s == "":
        return "0"
    try:
        return str(float(s))
    except (TypeError, ValueError):
        return "0"


def _sql_int(s) -> str:
    if s is None or s == "":
        return "NULL"
    try:
        return str(int(s))
    except (TypeError, ValueError):
        return "NULL"


def replace_items(invoice_tiny_id: int, itens: list[dict]) -> None:
    """Atomic delete + insert for one invoice's lines via psql."""
    sql_parts = [f"BEGIN; DELETE FROM invoice_items WHERE invoice_tiny_id = {invoice_tiny_id};"]
    for line in itens:
        if not isinstance(line, dict):
            continue
        cols = (
            f"({invoice_tiny_id}, "
            f"{_sql_int(line.get('idItem'))}, "
            f"{_sql_int(line.get('idProduto'))}, "
            f"{_sql_str((line.get('codigo') or '').strip())}, "
            f"{_sql_str(line.get('descricao'))}, "
            f"{_sql_str(line.get('ncm'))}, "
            f"{_sql_str(line.get('unidade'))}, "
            f"{_sql_num(line.get('quantidade'))}, "
            f"{_sql_num(line.get('valorUnitario'))}, "
            f"{_sql_num(line.get('valorTotal'))}, "
            f"{_sql_str(line.get('cfop'))}, "
            f"{_sql_str(line.get('naturezaOperacao'))})"
        )
        sql_parts.append(
            "INSERT INTO invoice_items "
            "(invoice_tiny_id, tiny_item_id, product_tiny_id, product_sku, "
            "product_description, ncm, unit, quantity, unit_value, total_value, "
            "cfop, operation_nature) VALUES " + cols + ";"
        )
    sql_parts.append("COMMIT;")
    subprocess.run(
        [
            "sudo",
            "-u",
            "postgres",
            "psql",
            "-d",
            "tiny_mirror_db",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "\n".join(sql_parts),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--from", dest="date_from", help="Data inicial YYYY-MM-DD")
    ap.add_argument("--to", dest="date_to", help="Data final YYYY-MM-DD")
    ap.add_argument("--skip-existing", action="store_true")
    args = ap.parse_args()

    if args.date_from and args.date_to:
        date_from = date.fromisoformat(args.date_from)
        date_to = date.fromisoformat(args.date_to)
    else:
        date_to = datetime.now(UTC).date()
        date_from = date_to - timedelta(days=args.days)

    token = env_var("/root/.openclaw/.env", "TINY_V3_ACCESS_TOKEN")
    if not token:
        print(
            "ERROR: TINY_V3_ACCESS_TOKEN não encontrado em /root/.openclaw/.env",
            file=sys.stderr,
        )
        return 1

    print(
        f"Backfill invoice_items: {date_from.isoformat()} → {date_to.isoformat()}",
        flush=True,
    )

    invoice_ids = list_invoice_ids(token, date_from, date_to)
    print(f"  {len(invoice_ids)} NFs no período", flush=True)

    persisted = 0
    skipped = 0
    failed = 0
    for i, inv_id in enumerate(invoice_ids, 1):
        if args.skip_existing and already_has_items(inv_id):
            skipped += 1
            continue
        code, body = http_get(f"{BASE_URL}/notas/{inv_id}", token)
        if code != 200 or not isinstance(body, dict):
            failed += 1
            print(f"  NF {inv_id}: HTTP {code} — skipping", flush=True)
            continue
        try:
            replace_items(inv_id, body.get("itens") or [])
            persisted += 1
        except subprocess.CalledProcessError as exc:
            failed += 1
            print(
                f"  NF {inv_id}: psql falhou: {exc.stderr.decode()[:200]}",
                flush=True,
            )
        if i % 50 == 0:
            print(
                f"  ...{i}/{len(invoice_ids)} "
                f"(persisted={persisted} skipped={skipped} failed={failed})",
                flush=True,
            )

    print(
        f"\nDone. persisted={persisted} skipped={skipped} failed={failed} "
        f"total_seen={len(invoice_ids)}",
        flush=True,
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
