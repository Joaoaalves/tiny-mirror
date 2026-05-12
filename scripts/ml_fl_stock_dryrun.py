"""ML Fulfillment Stock Dry-Run Report.

Computes what each active product's "Full Mercado Livre" deposit balance
SHOULD be, based on the ML Inventory API, and compares it with the current
value stored in stock_deposits. Prints a full report. Makes no writes.

Computation rules per product type:

  Simple (type='S'):
      own FL inventory + sum(parent_kit FL inventory x component_qty_in_kit)
      where parent_kit iterates both quantity kits (XU-SKU) and combos.

  Quantity kit (type='K', SKU ~ ^\\d+U-):
      base_SKU own FL inventory ÷ X  (integer division)

  Combo (type='K', SKU not matching ^\\d+U-):
      own FL inventory only

Only fulfillment listings (logistic_type == 'fulfillment') are considered.
The Inventory API (GET /inventories/{id}/stock/fulfillment) is used for
accurate FL stock instead of item.available_quantity.

DB-backed path (default when ml_listings table exists):
    Loads all active ML listings from the ml_listings + ml_listing_variations
    DB tables (populated by the daily ML listings sync), then calls
    GET /inventories/{id}/stock/fulfillment once per unique inventory_id.
    ~20x fewer API calls than the per-SKU fallback.

Per-SKU fallback (when ml_listings table is absent):
    For each product SKU, calls GET /users/{id}/items/search?seller_sku={sku}
    then GET /items/{mlb_id} per result. ~540 API calls for a typical catalog.

Usage:
    poetry run python scripts/ml_fl_stock_dryrun.py [--limit N]

Env vars (from .env):
    ML_CLIENT_ID, ML_CLIENT_SECRET, ML_REFRESH_TOKEN, ML_ACCESS_TOKEN
    VPS_HOST, VPS_USER, VPS_DB, VPS_SSH_KEY  (optional, have defaults)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_ROOT / ".env"

DEFAULT_VPS_HOST = "212.85.1.135"
DEFAULT_VPS_USER = "root"
DEFAULT_VPS_DB = "tiny_mirror_db"
DEFAULT_VPS_KEY = "/tmp/root-offshop"

ML_BASE = "https://api.mercadolibre.com"
ML_TOKEN_URL = "https://api.mercadolibre.com/oauth/token"

QUANTITY_KIT_RE = re.compile(r"^(\d+)U-(.+)$")


# ---------------------------------------------------------------------------
# DB helpers (SSH-based, same approach as audit_tiny_vs_db.py)
# ---------------------------------------------------------------------------


def _vps_cfg() -> tuple[str, str, str, str]:
    host = os.environ.get("VPS_HOST", DEFAULT_VPS_HOST)
    user = os.environ.get("VPS_USER", DEFAULT_VPS_USER)
    db = os.environ.get("VPS_DB", DEFAULT_VPS_DB)
    key = os.environ.get("VPS_SSH_KEY", DEFAULT_VPS_KEY)
    if not Path(key).exists():
        print(f"SSH key not found: {key}. Set VPS_SSH_KEY.", file=sys.stderr)
        sys.exit(2)
    return host, user, db, key


def vps_query(sql: str, columns: list[str]) -> list[dict[str, str]]:
    """Run SQL on the VPS via SSH; parse pipe-delimited unaligned output."""
    host, user, db, key = _vps_cfg()
    remote_cmd = f"sudo -u postgres psql -d {db} -tAc {json.dumps(sql)}"
    result = subprocess.run(
        [
            "ssh",
            "-i",
            key,
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            "ConnectTimeout=10",
            f"{user}@{host}",
            remote_cmd,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        print(
            f"VPS query failed (exit {result.returncode}): {result.stderr.strip()[:300]}",
            file=sys.stderr,
        )
        sys.exit(2)
    rows: list[dict[str, str]] = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) != len(columns):
            continue
        rows.append(dict(zip(columns, [p.strip() for p in parts], strict=False)))
    return rows


# ---------------------------------------------------------------------------
# Self-contained ML API client
# ---------------------------------------------------------------------------


class SimpleMLClient:
    """Minimal ML API client for the dry-run script.

    Uses ML_ACCESS_TOKEN from env; refreshes once on 401 using
    ML_CLIENT_ID / ML_CLIENT_SECRET / ML_REFRESH_TOKEN.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        access_token: str,
        user_id: str,
    ) -> None:
        self._cid = client_id
        self._csec = client_secret
        self._refresh = refresh_token
        self._access = access_token
        self._user_id = user_id
        self._http = httpx.AsyncClient(timeout=30)

    async def close(self) -> None:
        await self._http.aclose()

    async def _refresh_token(self) -> None:
        resp = await self._http.post(
            ML_TOKEN_URL,
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
        print(f"  [auth] ML token refreshed; expires in {body.get('expires_in')}s", file=sys.stderr)

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
        url = ML_BASE + path
        refreshed = False
        for attempt in range(6):
            try:
                resp = await self._http.get(
                    url,
                    params=params,
                    headers={"Authorization": f"Bearer {self._access}"},
                )
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                wait = min(2**attempt, 30)
                print(f"  [retry] {path} {type(exc).__name__}; sleeping {wait}s", file=sys.stderr)
                await asyncio.sleep(wait)
                continue

            if resp.status_code == 200:
                return resp.json()  # type: ignore[no-any-return]
            if resp.status_code == 404:
                return None
            if resp.status_code == 401 and not refreshed:
                await self._refresh_token()
                refreshed = True
                continue
            if resp.status_code in {408, 425, 429, 500, 502, 503, 504}:
                wait = min(2**attempt, 30)
                print(
                    f"  [retry] {path} HTTP {resp.status_code}; sleeping {wait}s", file=sys.stderr
                )
                await asyncio.sleep(wait)
                continue
            print(f"  [warn] {path} HTTP {resp.status_code}: {resp.text[:200]}", file=sys.stderr)
            return None
        print(f"  [error] {path} exceeded retries", file=sys.stderr)
        return None

    async def list_items_by_sku(self, sku: str) -> list[str]:
        data = await self._get(f"/users/{self._user_id}/items/search", {"seller_sku": sku})
        if data is None:
            return []
        return data.get("results") or []

    async def get_item(self, mlb_id: str) -> dict[str, Any] | None:
        return await self._get(f"/items/{mlb_id}")

    async def get_inventory_stock(self, inventory_id: str) -> int:
        """Return available_quantity from the FL inventory endpoint, or 0."""
        data = await self._get(f"/inventories/{inventory_id}/stock/fulfillment")
        if data is None:
            return 0
        return int(data.get("available_quantity") or 0)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class Product:
    tiny_id: int
    sku: str
    ptype: str  # 'S', 'K', 'V'


@dataclass
class KitComponent:
    kit_tiny_id: int
    kit_sku: str
    component_sku: str
    component_qty: int


@dataclass
class MLListing:
    mlb_id: str
    logistic_type: str
    inventory_id: str | None
    fl_stock: int = 0


@dataclass
class ProductResult:
    sku: str
    ptype: str
    current_fl: float
    computed_fl: int
    listings: list[MLListing] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    kit_details: list[str] = field(default_factory=list)
    skipped: bool = False
    skip_reason: str = ""

    @property
    def delta(self) -> int:
        return self.computed_fl - int(self.current_fl)

    @property
    def needs_update(self) -> bool:
        return self.delta != 0


# ---------------------------------------------------------------------------
# ML data fetching — per-SKU API fallback
# ---------------------------------------------------------------------------


async def fetch_own_fl(
    ml: SimpleMLClient,
    sku: str,
    warnings: list[str],
) -> tuple[int, list[MLListing]]:
    """Fetch FL inventory for a SKU's own direct ML listings.

    Returns (total_fl_qty, listings).

    Handles two item structures:
    - Simple items: inventory tracked at item level via item.inventory_id.
    - Variation items: item.inventory_id is null; each variation carries its
      own inventory_id and available_quantity.

    Deduplicates by inventory_id so the same physical pool is counted once.
    When inventory_id is absent, falls back to available_quantity from the
    item/variation endpoint.
    """
    mlb_ids = await ml.list_items_by_sku(sku)
    if not mlb_ids:
        return 0, []

    listings: list[MLListing] = []
    # seen_inventory deduplicates all inventory pools (item-level + variation-level).
    # Pools without an inventory_id cannot be deduplicated; their qty is tracked
    # separately in no_id_total.
    seen_inventory: dict[str, int] = {}
    no_id_total: int = 0

    for mlb_id in mlb_ids:
        item = await ml.get_item(mlb_id)
        if item is None:
            warnings.append(f"  {mlb_id}: item fetch returned None")
            continue

        shipping = item.get("shipping") or {}
        logistic_type = shipping.get("logistic_type") or "unknown"
        item_inventory_id = item.get("inventory_id")
        variations = item.get("variations") or []

        listing = MLListing(
            mlb_id=mlb_id,
            logistic_type=logistic_type,
            inventory_id=item_inventory_id,
        )

        if logistic_type != "fulfillment":
            listings.append(listing)
            continue

        if variations:
            # Variation item: inventory tracked per-variation. Sum FL stock
            # across distinct variation inventory pools (deduplicated).
            var_total = 0
            var_inv_ids: list[str] = []
            for var in variations:
                var_inv_id = var.get("inventory_id")
                var_qty = int(var.get("available_quantity") or 0)
                if var_inv_id:
                    if var_inv_id not in seen_inventory:
                        seen_inventory[var_inv_id] = await ml.get_inventory_stock(var_inv_id)
                    var_total += seen_inventory[var_inv_id]
                    if var_inv_id not in var_inv_ids:
                        var_inv_ids.append(var_inv_id)
                else:
                    warnings.append(
                        f"  {mlb_id}/var{var.get('id')}: no inventory_id — using available_quantity={var_qty}"
                    )
                    var_total += var_qty
                    no_id_total += var_qty

            listing.inventory_id = ",".join(var_inv_ids) if var_inv_ids else None
            listing.fl_stock = var_total
        else:
            # Simple item: single inventory pool.
            fallback = int(item.get("available_quantity") or 0)
            if item_inventory_id:
                if item_inventory_id not in seen_inventory:
                    seen_inventory[item_inventory_id] = await ml.get_inventory_stock(
                        item_inventory_id
                    )
                listing.fl_stock = seen_inventory[item_inventory_id]
            else:
                warnings.append(
                    f"  {mlb_id}: no inventory_id — using available_quantity={fallback}"
                )
                listing.fl_stock = fallback
                no_id_total += fallback

        listings.append(listing)

    total = sum(seen_inventory.values()) + no_id_total
    return total, listings


async def _collect_own_fl_api(
    ml: SimpleMLClient,
    products: list[Product],
) -> tuple[dict[str, int], dict[str, list[MLListing]], dict[str, list[str]]]:
    """Per-SKU API fallback: list_items_by_sku + get_item per product (~540 calls)."""
    print(
        f"[ml] fetching FL inventory via per-SKU API for {len(products)} products…",
        file=sys.stderr,
    )
    own_fl: dict[str, int] = {}
    own_listings: dict[str, list[MLListing]] = {}
    all_warnings: dict[str, list[str]] = {}

    for idx, p in enumerate(products, start=1):
        if idx % 50 == 0 or idx == len(products):
            print(f"[ml]   {idx}/{len(products)} ({p.sku})", file=sys.stderr)
        warnings: list[str] = []
        qty, listings = await fetch_own_fl(ml, p.sku, warnings)
        own_fl[p.sku] = qty
        own_listings[p.sku] = listings
        if warnings:
            all_warnings[p.sku] = warnings

    return own_fl, own_listings, all_warnings


async def _collect_own_fl_from_db(
    ml: SimpleMLClient,
    products: list[Product],
    all_listings_by_sku: dict[str, list[dict]],
    variations_by_mlb: dict[str, list[str]],
) -> tuple[dict[str, int], dict[str, list[MLListing]], dict[str, list[str]]]:
    """DB-backed path: reads listings from ml_listings, calls Inventory API once
    per unique inventory_id instead of once per product SKU.

    all_listings_by_sku: sku → rows from ml_listings (all logistic_types)
    variations_by_mlb: mlb_id → list of non-null inventory_ids from ml_listing_variations
    """
    # Collect all unique inventory IDs from FL listings only
    all_inv_ids: set[str] = set()
    for rows in all_listings_by_sku.values():
        for row in rows:
            if row["logistic_type"] != "fulfillment":
                continue
            has_var = row["has_variations"] == "t"
            if has_var:
                for inv_id in variations_by_mlb.get(row["mlb_id"], []):
                    if inv_id:
                        all_inv_ids.add(inv_id)
            else:
                inv_id = row["inventory_id"] or None
                if inv_id:
                    all_inv_ids.add(inv_id)

    inv_ids_list = sorted(all_inv_ids)
    print(f"[ml] fetching {len(inv_ids_list)} unique inventory stocks…", file=sys.stderr)
    inventory_stocks: dict[str, int] = {}
    for idx, inv_id in enumerate(inv_ids_list, start=1):
        if idx % 50 == 0 or idx == len(inv_ids_list):
            print(f"[ml]   {idx}/{len(inv_ids_list)}", file=sys.stderr)
        inventory_stocks[inv_id] = await ml.get_inventory_stock(inv_id)

    own_fl: dict[str, int] = {}
    own_listings: dict[str, list[MLListing]] = {}
    all_warnings: dict[str, list[str]] = {}

    for p in products:
        warnings: list[str] = []
        db_rows = all_listings_by_sku.get(p.sku, [])
        listings: list[MLListing] = []
        seen_pool: dict[str, int] = {}  # per-SKU dedup within FL listings

        for row in db_rows:
            mlb_id = row["mlb_id"]
            logistic_type = row["logistic_type"]
            has_var = row["has_variations"] == "t"
            inventory_id = row["inventory_id"] or None

            listing = MLListing(
                mlb_id=mlb_id,
                logistic_type=logistic_type,
                inventory_id=inventory_id,
            )

            if logistic_type == "fulfillment":
                if has_var:
                    var_inv_ids: list[str] = []
                    for var_inv_id in variations_by_mlb.get(mlb_id, []):
                        if var_inv_id and var_inv_id not in seen_pool:
                            seen_pool[var_inv_id] = inventory_stocks.get(var_inv_id, 0)
                            var_inv_ids.append(var_inv_id)
                        elif not var_inv_id:
                            warnings.append(f"  {mlb_id}: variation has no inventory_id — skipped")
                    listing.inventory_id = ",".join(var_inv_ids) if var_inv_ids else None
                    listing.fl_stock = sum(seen_pool[iid] for iid in var_inv_ids)
                else:
                    if inventory_id:
                        if inventory_id not in seen_pool:
                            seen_pool[inventory_id] = inventory_stocks.get(inventory_id, 0)
                        listing.fl_stock = seen_pool[inventory_id]
                    else:
                        warnings.append(f"  {mlb_id}: no inventory_id in DB — stock treated as 0")

            listings.append(listing)

        own_fl[p.sku] = sum(seen_pool.values())
        own_listings[p.sku] = listings
        if warnings:
            all_warnings[p.sku] = warnings

    return own_fl, own_listings, all_warnings


# ---------------------------------------------------------------------------
# Main computation
# ---------------------------------------------------------------------------


async def run_dryrun(limit: int | None) -> None:
    print("ML Fulfillment Stock Dry-Run Report")
    print(f"Generated: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 70)
    print()

    # ------------------------------------------------------------------
    # 1. DB queries
    # ------------------------------------------------------------------
    print("[db] querying products…", file=sys.stderr)
    raw_products = vps_query(
        "SELECT tiny_id, sku, COALESCE(type, 'S') AS type "
        "FROM products WHERE situation = 'A' ORDER BY sku",
        ["tiny_id", "sku", "type"],
    )
    products: list[Product] = [
        Product(
            tiny_id=int(r["tiny_id"]),
            sku=r["sku"],
            ptype=r["type"],
        )
        for r in raw_products
        if r["sku"]
    ]
    if limit:
        products = products[:limit]
    print(f"[db] {len(products)} active products", file=sys.stderr)

    print("[db] querying kit components…", file=sys.stderr)
    raw_kits = vps_query(
        "SELECT pkc.kit_product_tiny_id, p.sku AS kit_sku, "
        "  pkc.component_sku, pkc.quantity "
        "FROM product_kit_components pkc "
        "JOIN products p ON p.tiny_id = pkc.kit_product_tiny_id",
        ["kit_tiny_id", "kit_sku", "component_sku", "component_qty"],
    )
    kit_components: list[KitComponent] = [
        KitComponent(
            kit_tiny_id=int(r["kit_tiny_id"]),
            kit_sku=r["kit_sku"],
            component_sku=r["component_sku"],
            component_qty=int(float(r["component_qty"])),
        )
        for r in raw_kits
    ]
    # parent_kits_by_component[component_sku] = [(kit_sku, component_qty), ...]
    parent_kits_by_component: dict[str, list[tuple[str, int]]] = {}
    for kc in kit_components:
        parent_kits_by_component.setdefault(kc.component_sku, []).append(
            (kc.kit_sku, kc.component_qty)
        )
    print(f"[db] {len(kit_components)} kit component rows", file=sys.stderr)

    print("[db] querying current FL deposits…", file=sys.stderr)
    raw_deposits = vps_query(
        "SELECT p.sku, sd.balance "
        "FROM stock_deposits sd "
        "JOIN products p ON p.tiny_id = sd.product_tiny_id "
        "WHERE sd.deposit_name = 'Full Mercado Livre'",
        ["sku", "balance"],
    )
    current_fl: dict[str, float] = {r["sku"]: float(r["balance"]) for r in raw_deposits}
    print(f"[db] {len(current_fl)} FL deposit rows", file=sys.stderr)

    # ------------------------------------------------------------------
    # 2. Check for ml_listings table (populated by daily ML listings sync)
    # ------------------------------------------------------------------
    table_check = vps_query(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = 'ml_listings'",
        ["count"],
    )
    use_db = bool(table_check) and table_check[0]["count"] == "1"

    all_listings_by_sku: dict[str, list[dict]] = {}
    variations_by_mlb: dict[str, list[str]] = {}

    if use_db:
        print("[db] loading ml_listings…", file=sys.stderr)
        raw_ml_all = vps_query(
            "SELECT mlb_id, sku, logistic_type, "
            "COALESCE(inventory_id, '') AS inventory_id, "
            "has_variations::text AS has_variations "
            "FROM ml_listings",
            ["mlb_id", "sku", "logistic_type", "inventory_id", "has_variations"],
        )
        for row in raw_ml_all:
            if row["sku"]:
                all_listings_by_sku.setdefault(row["sku"], []).append(row)

        raw_ml_vars = vps_query(
            "SELECT mlb_id, inventory_id FROM ml_listing_variations "
            "WHERE inventory_id IS NOT NULL",
            ["mlb_id", "inventory_id"],
        )
        for row in raw_ml_vars:
            variations_by_mlb.setdefault(row["mlb_id"], []).append(row["inventory_id"])

        total_listings = sum(len(v) for v in all_listings_by_sku.values())
        total_vars = sum(len(v) for v in variations_by_mlb.values())
        print(
            f"[db] {total_listings} ml_listings rows, {total_vars} variation inventory_ids",
            file=sys.stderr,
        )
    else:
        print(
            "[warn] ml_listings table not found — falling back to per-SKU API search",
            file=sys.stderr,
        )

    # ------------------------------------------------------------------
    # 3. Bootstrap ML client
    # ------------------------------------------------------------------
    cid = os.environ["ML_CLIENT_ID"]
    csec = os.environ["ML_CLIENT_SECRET"]
    refresh = os.environ["ML_REFRESH_TOKEN"]
    access = os.environ["ML_ACCESS_TOKEN"]
    user_id = os.environ["ML_USER_ID"]

    ml = SimpleMLClient(cid, csec, refresh, access, user_id)

    try:
        if use_db:
            own_fl, own_listings, all_warnings = await _collect_own_fl_from_db(
                ml, products, all_listings_by_sku, variations_by_mlb
            )
        else:
            own_fl, own_listings, all_warnings = await _collect_own_fl_api(ml, products)

        _run_report(
            products, parent_kits_by_component, current_fl, own_fl, own_listings, all_warnings
        )
    finally:
        await ml.close()


def _run_report(
    products: list[Product],
    parent_kits_by_component: dict[str, list[tuple[str, int]]],
    current_fl: dict[str, float],
    own_fl: dict[str, int],
    own_listings: dict[str, list[MLListing]],
    all_warnings: dict[str, list[str]],
) -> None:
    # ------------------------------------------------------------------
    # Phase 1: compute target FL per product type
    # ------------------------------------------------------------------
    results: list[ProductResult] = []
    products_by_sku = {p.sku: p for p in products}

    for p in products:
        warn = all_warnings.get(p.sku, [])
        cur = current_fl.get(p.sku, 0.0)
        listings = own_listings.get(p.sku, [])

        m = QUANTITY_KIT_RE.match(p.sku)

        if p.ptype == "K" and m:
            # --- Quantity kit: e.g. 2U-RON-COLL-PRE ---
            x = int(m.group(1))
            base_sku = m.group(2)
            base_own = own_fl.get(base_sku, 0)
            computed = base_own // x if x > 0 else 0
            res = ProductResult(
                sku=p.sku,
                ptype=f"K(qty kit ÷{x})",
                current_fl=cur,
                computed_fl=computed,
                listings=listings,
                warnings=warn,
            )
            if base_sku not in products_by_sku:
                res.warnings.append(f"  base SKU {base_sku!r} not in active products — using 0")

        elif p.ptype == "K":
            # --- Combo kit ---
            computed = own_fl.get(p.sku, 0)
            res = ProductResult(
                sku=p.sku,
                ptype="K(combo)",
                current_fl=cur,
                computed_fl=computed,
                listings=listings,
                warnings=warn,
            )

        else:
            # --- Simple product (type='S' or 'V') ---
            own = own_fl.get(p.sku, 0)
            kit_contrib = 0
            kit_details: list[str] = []
            for kit_sku, component_qty in parent_kits_by_component.get(p.sku, []):
                kit_fl = own_fl.get(kit_sku, 0)
                contribution = kit_fl * component_qty
                kit_contrib += contribution
                kit_details.append(
                    f"  parent kit {kit_sku}: FL={kit_fl} x {component_qty}u = +{contribution}"
                )
            computed = own + kit_contrib
            res = ProductResult(
                sku=p.sku,
                ptype="S" if p.ptype == "S" else p.ptype,
                current_fl=cur,
                computed_fl=computed,
                listings=listings,
                warnings=warn,
                kit_details=kit_details,
            )

        results.append(res)

    # ------------------------------------------------------------------
    # Phase 2: print report
    # ------------------------------------------------------------------
    needs_update = [r for r in results if r.needs_update]
    no_change = [r for r in results if not r.needs_update]
    fl_products = [r for r in results if any(e.logistic_type == "fulfillment" for e in r.listings)]
    no_ml_listing = [r for r in results if not r.listings]
    not_fl = [
        r
        for r in results
        if r.listings and not any(e.logistic_type == "fulfillment" for e in r.listings)
    ]

    increases = [r for r in needs_update if r.delta > 0]
    decreases = [r for r in needs_update if r.delta < 0]
    zeroes = [r for r in decreases if r.computed_fl == 0]

    print(f"Products analyzed:       {len(results)}")
    print(f"With FL listings in ML:  {len(fl_products)}")
    print(f"No ML listing at all:    {len(no_ml_listing)}")
    print(f"ML listings but not FL:  {len(not_fl)}")
    print(
        f"Need update:             {len(needs_update)}  "
        f"(↑ {len(increases)}  ↓ {len(decreases)}  zeroed {len(zeroes)})"
    )
    print(f"No change:               {len(no_change)}")
    print()

    # ---- Changes required ----
    if needs_update:
        print("━" * 70)
        print("CHANGES REQUIRED")
        print("━" * 70)
        _print_results_table(needs_update)

    # ---- No change ----
    if no_change:
        print()
        print("━" * 70)
        print("NO CHANGE")
        print("━" * 70)
        _print_results_table(no_change)

    # ---- Warnings ----
    warnings_with_content = {r.sku: r.warnings for r in results if r.warnings}
    if warnings_with_content:
        print()
        print("━" * 70)
        print("WARNINGS / DISCREPANCIES")
        print("━" * 70)
        for sku, warns in sorted(warnings_with_content.items()):
            print(f"  {sku}:")
            for w in warns:
                print(f"    {w}")

    # ---- Summary ----
    print()
    print("━" * 70)
    print("SUMMARY")
    print("━" * 70)
    total_current = sum(r.current_fl for r in results)
    total_computed = sum(r.computed_fl for r in results)
    print(f"  Total current FL (DB):   {int(total_current):>8}")
    print(f"  Total computed FL:       {int(total_computed):>8}")
    print(f"  Net delta:               {int(total_computed - total_current):>+8}")
    print()
    print(f"  Need update:             {len(needs_update)}")
    print(f"    Increase stock:        {len(increases)}")
    print(f"    Decrease stock:        {len(decreases)}")
    print(f"    Zero out:              {len(zeroes)}")
    print(f"  No change:               {len(no_change)}")
    print(f"  No FL listing in ML:     {len(no_ml_listing) + len(not_fl)}")


def _print_results_table(results: list[ProductResult]) -> None:
    header = f"  {'SKU':<30} {'TYPE':<14} {'CURRENT':>8} {'COMPUTED':>8} {'DELTA':>7}  MLBS"
    print(header)
    print("  " + "-" * 80)
    for r in sorted(results, key=lambda x: x.sku):
        fl_listings = [e for e in r.listings if e.logistic_type == "fulfillment"]
        mlbs_str = (
            ", ".join(
                f"{e.mlb_id}(inv={e.inventory_id or '?'},fl={e.fl_stock})" for e in fl_listings
            )
            or "(none)"
        )
        delta_str = f"{r.delta:+d}" if r.delta != 0 else "—"
        print(
            f"  {r.sku:<30} {r.ptype:<14} {int(r.current_fl):>8} {r.computed_fl:>8} {delta_str:>7}  {mlbs_str}"
        )
        if r.warnings:
            for w in r.warnings:
                print(f"    ⚠  {w}")
        if r.kit_details:
            for d in r.kit_details:
                print(f"    {d}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process only the first N products (for quick testing)",
    )
    args = parser.parse_args()

    if not ENV_FILE.exists():
        print(f"Missing {ENV_FILE}", file=sys.stderr)
        return 2
    load_dotenv(ENV_FILE)

    for var in (
        "ML_CLIENT_ID",
        "ML_CLIENT_SECRET",
        "ML_REFRESH_TOKEN",
        "ML_ACCESS_TOKEN",
        "ML_USER_ID",
    ):
        if not os.environ.get(var):
            print(f"Missing env var: {var}", file=sys.stderr)
            return 2

    await run_dryrun(limit=args.limit)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
