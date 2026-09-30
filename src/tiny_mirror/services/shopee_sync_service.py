"""Shopee → mp_orders / mp_listings. Read-only on Shopee.

- Orders: ``get_order_list`` (by ``update_time`` hourly — new orders and
  status changes — or by ``create_time`` for the backfill; Shopee caps a
  window at 15 days, 100 per page with a cursor) then ``get_order_detail``
  (50 per call) for the exact ``create_time``, status, total and fulfilment.
  Tiny links by ``ecommerce_order_number`` = order_sn.
- Listings: ``get_item_list`` (NORMAL + UNLIST) → ``get_item_base_info``
  (50 per call) → ``get_model_list`` for items with variations; one row per
  model (variation_id = model_id) or per item. A full pass replaces the
  channel's snapshot.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import structlog

from tiny_mirror.infrastructure.external.shopee_client import ShopeeClient
from tiny_mirror.infrastructure.orm.models import MPListingORM, MPOrderORM
from tiny_mirror.services.marketplace_store import prune_listings, upsert

logger = structlog.get_logger(__name__)

CHANNEL = "shopee"
# Shopee caps get_order_list at 15 days; 14 keeps a margin. Windows share their
# edges (no gap); an order on an edge is listed twice and deduped.
_WINDOW_DAYS = 14
_PAGE = 100
_DETAIL_BATCH = 50
_MAX_PAGES = 500
_LIST_STATUSES = ("NORMAL", "UNLIST")
_DETAIL_FIELDS = "total_amount,fulfillment_flag,cancel_reason,pay_time"


class ShopeeSyncService:
    def __init__(self, client: ShopeeClient) -> None:
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
        order_sns: list[str] = []
        for w_from, w_to in _windows(start, now):
            order_sns.extend(await self._list_orders(field, w_from, w_to))
        unique = list(dict.fromkeys(order_sns))
        rows: list[dict[str, Any]] = []
        for i in range(0, len(unique), _DETAIL_BATCH):
            detail = await self._client.get(
                "/api/v2/order/get_order_detail",
                {
                    "order_sn_list": ",".join(unique[i : i + _DETAIL_BATCH]),
                    "response_optional_fields": _DETAIL_FIELDS,
                },
            )
            rows.extend(r for r in map(parse_order, detail.get("order_list") or []) if r)
        await upsert(MPOrderORM, rows, ["channel", "order_id"])
        stats = {"orders": len(rows), "mode": field}
        logger.info("shopee.orders_synced", **stats)
        return stats

    async def _list_orders(self, field: str, w_from: datetime, w_to: datetime) -> list[str]:
        params: dict[str, Any] = {
            "time_range_field": field,
            "time_from": int(w_from.timestamp()),
            "time_to": int(w_to.timestamp()),
            "page_size": _PAGE,
            "cursor": "",
        }
        sns: list[str] = []
        for _ in range(_MAX_PAGES):
            page = await self._client.get("/api/v2/order/get_order_list", params)
            sns.extend(o["order_sn"] for o in page.get("order_list") or [] if o.get("order_sn"))
            if not page.get("more"):
                break
            params = {**params, "cursor": page.get("next_cursor") or ""}
        return sns

    # ------------------------------------------------------------------
    # Listings
    # ------------------------------------------------------------------
    async def sync_listings(self) -> dict[str, Any]:
        shop_id = self._client.shop_id
        item_ids: list[int] = []
        for status in _LIST_STATUSES:
            offset = 0
            for _ in range(_MAX_PAGES):
                page = await self._client.get(
                    "/api/v2/product/get_item_list",
                    {"offset": offset, "page_size": _PAGE, "item_status": status},
                )
                item_ids.extend(int(i["item_id"]) for i in page.get("item") or [])
                if not page.get("has_next_page"):
                    break
                offset = int(page.get("next_offset") or offset + _PAGE)
        rows: list[dict[str, Any]] = []
        for i in range(0, len(item_ids), _DETAIL_BATCH):
            base = await self._client.get(
                "/api/v2/product/get_item_base_info",
                {"item_id_list": ",".join(map(str, item_ids[i : i + _DETAIL_BATCH]))},
            )
            for item in base.get("item_list") or []:
                if item.get("has_model"):
                    models = await self._client.get(
                        "/api/v2/product/get_model_list", {"item_id": item["item_id"]}
                    )
                    rows.extend(parse_models(item, models.get("model") or [], shop_id))
                else:
                    rows.append(parse_item(item, shop_id))
        await upsert(MPListingORM, rows, ["channel", "listing_id", "variation_id"])
        removed = 0
        if rows:
            removed = await prune_listings(
                CHANNEL, {(r["listing_id"], r["variation_id"]) for r in rows}
            )
        stats = {
            "items": len(item_ids),
            "listings": len(rows),
            "active": sum(1 for r in rows if r["is_active"]),
            "removed": removed,
        }
        logger.info("shopee.listings_synced", **stats)
        return stats


# ---------------------------------------------------------------------------
# Parsing (pure)
# ---------------------------------------------------------------------------
def _windows(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    out: list[tuple[datetime, datetime]] = []
    cursor = start
    while cursor < end:
        nxt = min(cursor + timedelta(days=_WINDOW_DAYS), end)
        out.append((cursor, nxt))
        cursor = nxt
    return out


def parse_order(raw: dict[str, Any]) -> dict[str, Any] | None:
    order_sn = raw.get("order_sn")
    created = _unix(raw.get("create_time"))
    if not order_sn or created is None:
        return None
    return {
        "channel": CHANNEL,
        "order_id": str(order_sn)[:40],
        "created_at": created,
        "last_updated": _unix(raw.get("update_time")),
        "status": raw.get("order_status"),
        "fulfilled_by": raw.get("fulfillment_flag") or None,
        "total_amount": _dec(raw.get("total_amount")),
        "currency": raw.get("currency"),
        "raw": raw,
        "synced_at": datetime.now(UTC),
    }


def parse_item(item: dict[str, Any], shop_id: int) -> dict[str, Any]:
    price = (item.get("price_info") or [{}])[0].get("current_price")
    stock = ((item.get("stock_info_v2") or {}).get("summary_info") or {}).get(
        "total_available_stock"
    )
    return _listing_row(
        item,
        "",
        item.get("item_sku"),
        item.get("item_name"),
        price,
        stock,
        item.get("item_status"),
        item.get("item_status") == "NORMAL",
        shop_id,
    )


def parse_models(
    item: dict[str, Any], models: list[dict[str, Any]], shop_id: int
) -> list[dict[str, Any]]:
    rows = []
    for m in models:
        price = (m.get("price_info") or [{}])[0].get("current_price")
        stock = ((m.get("stock_info_v2") or {}).get("summary_info") or {}).get(
            "total_available_stock"
        )
        name = " - ".join(x for x in (item.get("item_name"), m.get("model_name")) if x)
        active = (
            item.get("item_status") == "NORMAL"
            and m.get("model_status", "MODEL_NORMAL") == "MODEL_NORMAL"
        )
        rows.append(
            _listing_row(
                item,
                str(m.get("model_id")),
                m.get("model_sku"),
                name,
                price,
                stock,
                f"{item.get('item_status')}/{m.get('model_status')}",
                active,
                shop_id,
                model=m,
            )
        )
    return rows


def _listing_row(
    item: dict[str, Any],
    variation_id: str,
    sku: Any,
    title: Any,
    price: Any,
    stock: Any,
    status: Any,
    active: bool,
    shop_id: int,
    model: dict[str, Any] | None = None,
) -> dict[str, Any]:
    item_id = str(item.get("item_id"))
    return {
        "channel": CHANNEL,
        "listing_id": item_id,
        "variation_id": variation_id,
        "sku": (str(sku)[:100] if sku else None),
        "external_id": item_id,
        "title": title,
        "status": (str(status)[:80] if status else None),
        "is_active": bool(active),
        "price": _dec(price),
        "stock": int(stock) if stock is not None else None,
        "url": f"https://shopee.com.br/product/{shop_id}/{item_id}",
        "raw": {"item": {k: v for k, v in item.items() if k != "description"}, "model": model},
        "synced_at": datetime.now(UTC),
    }


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
