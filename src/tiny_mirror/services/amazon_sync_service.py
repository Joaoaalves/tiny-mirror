"""Amazon (SP-API, Brazil) → mp_orders / mp_listings. Read-only on Amazon.

- Orders (``/orders/2026-01-01/orders``): exact ``createdTime`` (Tiny orders
  have no time), fulfillment status, FBA vs merchant, grand total. The hourly
  run reads by ``lastUpdatedAfter`` (new orders and status changes); the
  backfill reads by ``createdAfter``. Tiny links by ``ecommerce_order_number``
  = Amazon order id.
- Listings (``/listings/2021-08-01/items/{sellerId}``): every seller SKU with
  ASIN, status (BUYABLE/DISCOVERABLE), price and fulfillable quantity. A full
  pass replaces the channel's snapshot (SKUs gone from Amazon are removed).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import structlog
from sqlalchemy import delete, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.external.amazon_spapi_client import AmazonSPAPIClient
from tiny_mirror.infrastructure.orm.models import MPListingORM, MPOrderORM

logger = structlog.get_logger(__name__)

CHANNEL = "amazon"
_ORDERS_PAGE = 100
_LISTINGS_PAGE = 20  # SP-API maximum for searchListingsItems
_MAX_PAGES = 500
_ORDER_DATA = "FULFILLMENT,PROCEEDS,CANCELLATION"


class AmazonSyncService:
    def __init__(self, client: AmazonSPAPIClient) -> None:
        self._client = client

    async def sync_orders(
        self, *, created_days: int | None = None, updated_hours: int = 3
    ) -> dict[str, Any]:
        creds = self._client.credentials()
        now = datetime.now(UTC)
        params: dict[str, Any] = {
            "marketplaceIds": creds["marketplace_id"],
            "includedData": _ORDER_DATA,
            "maxResultsPerPage": _ORDERS_PAGE,
        }
        if created_days is not None:
            params["createdAfter"] = _iso(now - timedelta(days=created_days))
        else:
            params["lastUpdatedAfter"] = _iso(now - timedelta(hours=updated_hours))

        rows: dict[str, dict[str, Any]] = {}
        base = dict(params)
        for _ in range(_MAX_PAGES):
            data = await self._client.get("/orders/2026-01-01/orders", params)
            for raw in data.get("orders") or []:
                row = parse_order(raw)
                if row:
                    rows[row["order_id"]] = row
            token = (data.get("pagination") or {}).get("nextToken")
            if not token:
                break
            # Same filters + includedData on every page, or later pages lose
            # fulfillment/proceeds.
            params = {**base, "paginationToken": token}
        await _upsert(MPOrderORM, list(rows.values()), ["channel", "order_id"])
        stats = {"orders": len(rows), "mode": "created" if created_days else "updated"}
        logger.info("amazon.orders_synced", **stats)
        return stats

    async def sync_listings(self) -> dict[str, Any]:
        creds = self._client.credentials()
        params: dict[str, Any] = {
            "marketplaceIds": creds["marketplace_id"],
            "includedData": "summaries,offers,fulfillmentAvailability",
            "pageSize": _LISTINGS_PAGE,
        }
        rows: list[dict[str, Any]] = []
        complete = False
        for _ in range(_MAX_PAGES):
            data = await self._client.get(
                f"/listings/2021-08-01/items/{creds['seller_id']}", params
            )
            rows.extend(row for row in map(parse_listing, data.get("items") or []) if row)
            token = (data.get("pagination") or {}).get("nextToken")
            if not token:
                complete = True
                break
            params = {**params, "pageToken": token}
        await _upsert(MPListingORM, rows, ["channel", "listing_id", "variation_id"])
        removed = 0
        if complete and rows:
            removed = await _prune_listings({(r["listing_id"], r["variation_id"]) for r in rows})
        stats = {
            "listings": len(rows),
            "active": sum(1 for r in rows if r["is_active"]),
            "removed": removed,
            "complete": complete,
        }
        logger.info("amazon.listings_synced", **stats)
        return stats


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
async def _upsert(model: Any, rows: list[dict[str, Any]], keys: list[str]) -> None:
    if not rows:
        return
    async with AsyncSessionLocal() as session:
        for i in range(0, len(rows), 500):
            chunk = rows[i : i + 500]
            stmt = pg_insert(model).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=keys,
                set_={c: stmt.excluded[c] for c in chunk[0] if c not in keys},
            )
            await session.execute(stmt)
        await session.commit()


async def _prune_listings(seen: set[tuple[str, str]]) -> int:
    """Drop this channel's listings that a complete pass no longer returned."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            delete(MPListingORM).where(
                MPListingORM.channel == CHANNEL,
                tuple_(MPListingORM.listing_id, MPListingORM.variation_id).not_in(list(seen)),
            )
        )
        await session.commit()
        return int(result.rowcount or 0)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Parsing (pure)
# ---------------------------------------------------------------------------
def parse_order(raw: dict[str, Any]) -> dict[str, Any] | None:
    order_id = raw.get("orderId")
    created = _ts(raw.get("createdTime"))
    if not order_id or created is None:
        return None
    fulfillment = raw.get("fulfillment") or {}
    total = (raw.get("proceeds") or {}).get("grandTotal") or {}
    return {
        "channel": CHANNEL,
        "order_id": str(order_id)[:40],
        "created_at": created,
        "last_updated": _ts(raw.get("lastUpdatedTime")),
        "status": fulfillment.get("fulfillmentStatus"),
        "fulfilled_by": fulfillment.get("fulfilledBy"),
        "total_amount": _dec(total.get("amount")),
        "currency": total.get("currencyCode"),
        "raw": raw,
        "synced_at": datetime.now(UTC),
    }


def parse_listing(raw: dict[str, Any]) -> dict[str, Any] | None:
    sku = raw.get("sku")
    if not sku:
        return None
    summary = (raw.get("summaries") or [{}])[0]
    statuses = [str(s) for s in summary.get("status") or []]
    offers = raw.get("offers") or []
    price = _dec(((offers[0] if offers else {}).get("price") or {}).get("amount"))
    qty = [
        int(f.get("quantity") or 0)
        for f in raw.get("fulfillmentAvailability") or []
        if f.get("quantity") is not None
    ]
    asin = summary.get("asin")
    return {
        "channel": CHANNEL,
        "listing_id": str(sku)[:80],
        "variation_id": "",
        "sku": str(sku)[:100],
        "external_id": asin,
        "title": summary.get("itemName"),
        "status": ",".join(statuses) or None,
        "is_active": "BUYABLE" in statuses,
        "price": price,
        "stock": sum(qty) if qty else None,
        "url": f"https://www.amazon.com.br/dp/{asin}" if asin else None,
        "raw": raw,
        "synced_at": datetime.now(UTC),
    }


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _dec(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


def _ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
