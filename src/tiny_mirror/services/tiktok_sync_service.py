"""TikTok Shop → mp_orders / mp_listings. Read-only on TikTok.

- Orders: ``orders/search`` by ``update_time`` hourly (new orders and status
  changes) or by ``create_time`` for the backfill, 100 per page with a page
  token. The search already returns the whole order, so there is no detail
  call. Buyer PII (address, e-mail, CPF, message) is dropped from ``raw``.
  Tiny links by ``ecommerce_order_number`` = order id ("TikTok Shop").
- Listings: ``products/search`` (every status; DELETED is skipped), one row
  per SKU (variation_id = sku id). A full pass replaces the channel's snapshot.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import structlog

from tiny_mirror.infrastructure.external.tiktok_shop_client import (
    ORDERS_SEARCH,
    PRODUCTS_SEARCH,
    TikTokShopClient,
)
from tiny_mirror.infrastructure.orm.models import MPListingORM, MPOrderORM
from tiny_mirror.services.marketplace_store import prune_listings, upsert

logger = structlog.get_logger(__name__)

CHANNEL = "tiktok"
_PAGE = 100
_MAX_PAGES = 500
_PII = frozenset(
    {"recipient_address", "buyer_email", "buyer_message", "cpf", "cpf_name", "user_id"}
)


class TikTokSyncService:
    def __init__(self, client: TikTokShopClient) -> None:
        self._client = client

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------
    async def sync_orders(
        self, *, created_days: int | None = None, updated_hours: int = 3
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        if created_days is not None:
            field, start = "create_time", now - timedelta(days=created_days)
        else:
            field, start = "update_time", now - timedelta(hours=updated_hours)
        body = {f"{field}_ge": int(start.timestamp()), f"{field}_lt": int(now.timestamp())}
        raws = await self._pages(ORDERS_SEARCH, "orders", body, {"sort_field": field})
        rows = list({r["order_id"]: r for r in map(parse_order, raws) if r}.values())
        await upsert(MPOrderORM, rows, ["channel", "order_id"])
        stats = {"orders": len(rows), "mode": field}
        logger.info("tiktok.orders_synced", **stats)
        return stats

    # ------------------------------------------------------------------
    # Listings
    # ------------------------------------------------------------------
    async def sync_listings(self) -> dict[str, Any]:
        products = await self._pages(PRODUCTS_SEARCH, "products", {}, {})
        live = [p for p in products if p.get("status") != "DELETED"]
        rows = list(
            {
                (r["listing_id"], r["variation_id"]): r for p in live for r in parse_product(p)
            }.values()
        )
        await upsert(MPListingORM, rows, ["channel", "listing_id", "variation_id"])
        removed = 0
        if rows:
            removed = await prune_listings(
                CHANNEL, {(r["listing_id"], r["variation_id"]) for r in rows}
            )
        stats = {
            "products": len(live),
            "listings": len(rows),
            "active": sum(1 for r in rows if r["is_active"]),
            "removed": removed,
        }
        logger.info("tiktok.listings_synced", **stats)
        return stats

    async def _pages(
        self, path: str, key: str, body: dict[str, Any], params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        token = ""
        for _ in range(_MAX_PAGES):
            query = {**params, "page_size": _PAGE, **({"page_token": token} if token else {})}
            page = await self._client.search(path, query, body)
            out.extend(page.get(key) or [])
            token = str(page.get("next_page_token") or "")
            if not token:
                break
        return out


# ---------------------------------------------------------------------------
# Parsing (pure)
# ---------------------------------------------------------------------------
def parse_order(raw: dict[str, Any]) -> dict[str, Any] | None:
    order_id = raw.get("id")
    created = _unix(raw.get("create_time"))
    if not order_id or created is None:
        return None
    payment = raw.get("payment") or {}
    return {
        "channel": CHANNEL,
        "order_id": str(order_id)[:40],
        "created_at": created,
        "last_updated": _unix(raw.get("update_time")),
        "status": (str(raw["status"])[:40] if raw.get("status") else None),
        "fulfilled_by": (
            str(raw["fulfillment_type"])[:40] if raw.get("fulfillment_type") else None
        ),
        "total_amount": _dec(payment.get("total_amount")),
        "currency": payment.get("currency"),
        "raw": {k: v for k, v in raw.items() if k not in _PII},
        "synced_at": datetime.now(UTC),
    }


def parse_product(product: dict[str, Any]) -> list[dict[str, Any]]:
    product_id = str(product.get("id"))
    status = product.get("status")
    rows = []
    for sku in product.get("skus") or []:
        stock = sum(int(i.get("quantity") or 0) for i in sku.get("inventory") or [])
        rows.append(
            {
                "channel": CHANNEL,
                "listing_id": product_id,
                "variation_id": str(sku.get("id") or ""),
                "sku": (str(sku["seller_sku"])[:100] if sku.get("seller_sku") else None),
                "external_id": product_id,
                "title": product.get("title"),
                "status": (str(status)[:80] if status else None),
                "is_active": status == "ACTIVATE",
                "price": _dec((sku.get("price") or {}).get("tax_exclusive_price")),
                "stock": stock if sku.get("inventory") is not None else None,
                "url": f"https://shop.tiktok.com/view/product/{product_id}",
                "raw": {"product": {k: v for k, v in product.items() if k != "skus"}, "sku": sku},
                "synced_at": datetime.now(UTC),
            }
        )
    return rows


def _unix(value: Any) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(value), tz=UTC) if value else None
    except (TypeError, ValueError, OverflowError):
        return None


def _dec(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None
