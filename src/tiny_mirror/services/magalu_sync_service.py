"""Magalu → mp_orders / mp_listings. Read-only on Magalu.

- Orders: ``GET /seller/v1/orders`` by ``updated_at`` hourly (new orders and
  status changes) or by ``purchased_at`` for the backfill, one day per window
  so the ``_offset`` ceiling is never reached. The list already returns the
  whole order (deliveries, items, amounts), so there is no detail call.
  Money is in cents with a ``normalizer``. Buyer PII (name, CPF, e-mail,
  phones, birth date, recipient and address) is dropped from ``raw``.
  Tiny keeps the order as ``"LU-" + code`` (the old Integracommerce prefix),
  so ``order_id`` is stored in that form to join ``ecommerce_order_number``;
  the API's own ``code`` and ``id`` (UUID) stay in ``raw``.
- Listings: ``GET /seller/v1/portfolios/skus`` (every status; DELETED and
  DELETING are skipped), one row per SKU (variation_id ''). Price and stock
  live in separate per-SKU endpoints, fetched ``_CONCURRENCY`` at a time; a
  SKU whose price/stock call fails keeps the values already in the mirror. A
  full pass replaces the channel's snapshot.
"""

from __future__ import annotations

import asyncio
import re
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

import httpx
import structlog
from sqlalchemy import select

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.external.magalu_client import (
    ORDERS,
    SKUS,
    MagaluAPIError,
    MagaluClient,
)
from tiny_mirror.infrastructure.orm.models import MPListingORM, MPOrderORM
from tiny_mirror.services.marketplace_store import prune_listings, upsert

logger = structlog.get_logger(__name__)

CHANNEL = "magalu"
MAGALU_CHANNEL_ID = "9fe0d853-732b-4e4a-a0b0-cff988ed043d"
TINY_PREFIX = "LU-"
_ORDERS_PAGE = 50
_SKUS_PAGE = 100
_CONCURRENCY = 4
_GONE = frozenset({"DELETED", "DELETING"})
_PII = frozenset(
    {"customer", "recipient", "email", "phones", "document_number", "birth_date", "cpf"}
)
_PRODUCT_ID = re.compile(r"/p/([A-Za-z0-9]+)/")


class MagaluSyncService:
    def __init__(self, client: MagaluClient) -> None:
        self._client = client

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------
    async def sync_orders(
        self, *, created_days: int | None = None, updated_hours: int = 3
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        if created_days is not None:
            field = "purchased_at"
            windows = [
                (now - timedelta(days=d + 1), now - timedelta(days=d)) for d in range(created_days)
            ]
        else:
            field = "updated_at"
            windows = [(now - timedelta(hours=updated_hours), now)]
        raws: list[dict[str, Any]] = []
        for start, end in windows:
            raws.extend(
                await self._client.pages(
                    ORDERS,
                    {
                        f"{field}__gte": start.isoformat(),
                        f"{field}__lte": end.isoformat(),
                        "_sort": "purchased_at:asc",
                    },
                    _ORDERS_PAGE,
                )
            )
        rows = list({r["order_id"]: r for r in map(parse_order, raws) if r}.values())
        await upsert(MPOrderORM, rows, ["channel", "order_id"])
        stats = {"orders": len(rows), "mode": field}
        logger.info("magalu.orders_synced", **stats)
        return stats

    # ------------------------------------------------------------------
    # Listings
    # ------------------------------------------------------------------
    async def sync_listings(self) -> dict[str, Any]:
        skus = await self._client.pages(SKUS, {"_sort": "sku:asc"}, _SKUS_PAGE)
        live = [s for s in skus if s.get("sku") and s.get("status") not in _GONE]
        stored = await stored_offers()
        offers, failed = await self._fetch_offers([str(s["sku"]) for s in live], stored)
        rows = list(
            {
                r["listing_id"]: r for s in live if (r := parse_sku(s, offers.get(str(s["sku"]))))
            }.values()
        )
        await upsert(MPListingORM, rows, ["channel", "listing_id", "variation_id"])
        removed = 0
        if rows:
            removed = await prune_listings(CHANNEL, {(r["listing_id"], "") for r in rows})
        stats = {
            "skus": len(live),
            "listings": len(rows),
            "active": sum(1 for r in rows if r["is_active"]),
            "removed": removed,
            "offers_failed": failed,
        }
        logger.info("magalu.listings_synced", **stats)
        return stats

    async def _fetch_offers(
        self, skus: list[str], stored: dict[str, dict[str, Any]]
    ) -> tuple[dict[str, dict[str, Any]], int]:
        """Prices + stocks of each SKU. A SKU whose call fails falls back to the
        offer already in the mirror (or none) until the next run."""
        sem = asyncio.Semaphore(_CONCURRENCY)

        async def one(sku: str) -> tuple[str, dict[str, Any] | None]:
            async with sem:
                try:
                    price = await self._client.get_price(sku)
                    stock = await self._client.get_stock(sku)
                except (MagaluAPIError, httpx.HTTPError, ValueError) as exc:
                    logger.warning("magalu.offer_failed", sku=sku, error=str(exc))
                    return sku, None
            return sku, {
                "prices": price.get("results") or [],
                "stocks": stock.get("results") or [],
                "synced_at": datetime.now(UTC).isoformat(),
            }

        results = await asyncio.gather(*(one(s) for s in skus))
        out: dict[str, dict[str, Any]] = {}
        failed = 0
        for sku, offer in results:
            if offer is None:
                failed += 1
                if sku in stored:
                    out[sku] = stored[sku]
            else:
                out[sku] = offer
        return out, failed


# ---------------------------------------------------------------------------
# Parsing (pure)
# ---------------------------------------------------------------------------
def strip_pii(value: Any) -> Any:
    if isinstance(value, list):
        return [strip_pii(v) for v in value]
    if isinstance(value, dict):
        return {k: strip_pii(v) for k, v in value.items() if k not in _PII}
    return value


def parse_order(raw: dict[str, Any]) -> dict[str, Any] | None:
    code = raw.get("code")
    created = _iso(raw.get("purchased_at")) or _iso(raw.get("created_at"))
    if not code or created is None:
        return None
    amounts = raw.get("amounts") or {}
    return {
        "channel": CHANNEL,
        "order_id": f"{TINY_PREFIX}{code}"[:40],
        "created_at": created,
        "last_updated": _iso(raw.get("updated_at")),
        "status": (str(raw["status"])[:40] if raw.get("status") else None),
        "fulfilled_by": None,
        "total_amount": cents(amounts.get("total"), amounts.get("normalizer")),
        "currency": (str(amounts.get("currency") or "BRL")[:5]),
        "raw": strip_pii(raw),
        "synced_at": datetime.now(UTC),
    }


async def stored_offers() -> dict[str, dict[str, Any]]:
    """Price/stock payloads already in the mirror, by SKU (fallback on failure)."""
    async with AsyncSessionLocal() as session:
        rows = (
            await session.execute(
                select(MPListingORM.listing_id, MPListingORM.raw).where(
                    MPListingORM.channel == CHANNEL
                )
            )
        ).all()
    return {
        listing_id: raw["offer"]
        for listing_id, raw in rows
        if isinstance(raw, dict) and isinstance(raw.get("offer"), dict)
    }


def parse_sku(sku: dict[str, Any], offer: dict[str, Any] | None) -> dict[str, Any] | None:
    code = str(sku.get("sku") or "")
    if not code:
        return None
    status = sku.get("status")
    url = _magalu_url(sku)
    match = _PRODUCT_ID.search(url or "")
    raw: dict[str, Any] = {"sku": sku}
    price: Decimal | None = None
    stock: int | None = None
    if offer is not None:
        raw["offer"] = offer
        prices = _for_channel(offer.get("prices") or [])
        if prices:
            price = cents(prices[0].get("price"), prices[0].get("normalizer"))
        stocks = _for_channel(offer.get("stocks") or [])
        if offer.get("stocks") is not None:
            stock = sum(int(s.get("quantity") or 0) for s in stocks)
    return {
        "channel": CHANNEL,
        "listing_id": code[:80],
        "variation_id": "",
        "sku": code[:100],
        "external_id": (match.group(1)[:40] if match else None),
        "title": sku.get("title"),
        "status": (str(status)[:80] if status else None),
        "is_active": status == "PUBLISHED" and sku.get("active") is not False,
        "price": price,
        "stock": stock,
        "url": url,
        "raw": raw,
        "synced_at": datetime.now(UTC),
    }


def _for_channel(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Entries of the Magalu sales channel (or channel-less ones)."""
    return [
        e
        for e in entries
        if isinstance(e, dict)
        and (e.get("channel") or {}).get("id") in (None, "", MAGALU_CHANNEL_ID)
    ]


def _magalu_url(sku: dict[str, Any]) -> str | None:
    urls = [u for u in sku.get("url_marketplace") or [] if isinstance(u, dict) and u.get("url")]
    for u in urls:
        if str(u.get("channel") or "").lower() in ("magalu", MAGALU_CHANNEL_ID):
            return str(u["url"])
    return str(urls[0]["url"]) if urls else None


def cents(value: Any, normalizer: Any = None) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        norm = Decimal(str(normalizer)) if normalizer else Decimal(100)
        return (Decimal(str(value)) / norm).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError, ZeroDivisionError):
        return None


def _iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
