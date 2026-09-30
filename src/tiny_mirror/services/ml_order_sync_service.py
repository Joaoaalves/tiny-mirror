"""ML order sync — per-order Mercado Livre data into ml_orders / items / shipments.

The Tiny order carries only a date, and neither the ML commission nor the
seller's freight. The ML Orders API (``/orders/search``) has the exact
``date_created``, ``pack_id``, ``cancel_detail`` and per-item ``sale_fee``;
``/shipments/{id}/costs`` has the freight split. Read-only on ML (GET only).

Unlike ``ml_sales_daily`` this keeps EVERY status (cancelled orders carry the
cancellation reason) and one row per order, so the DASH can join Tiny orders
through ``tiny_ref`` (= ``orders.ecommerce_order_number``).

Shipments are fetched only when new or when their order changed since the
last fetch, so the hourly run costs ~2 GETs per new order.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
import structlog
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import MLOrderItemORM, MLOrderORM, MLShipmentORM

logger = structlog.get_logger(__name__)

_API = "https://api.mercadolibre.com"
_PAGE = 50
# ML caps /orders/search offsets at 10k; one day stays far below that.
_MAX_OFFSET = 10_000
_SHIP_CONCURRENCY = 8


class MLOrderSyncService:
    def __init__(self, token_service: Any, http_client: httpx.AsyncClient, ml_user_id: str) -> None:
        self._tok = token_service
        self._http = http_client
        self._uid = ml_user_id

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------
    async def _get(self, url: str, params: dict[str, Any] | None = None) -> httpx.Response:
        access = await self._tok.get_valid_access_token()
        r = await self._http.get(url, headers={"Authorization": f"Bearer {access}"}, params=params)
        if r.status_code == 401:
            access = await self._tok.handle_unauthorized()
            r = await self._http.get(
                url, headers={"Authorization": f"Bearer {access}"}, params=params
            )
        return r

    async def _fetch_day(self, day: date) -> list[dict[str, Any]]:
        """Every order created on ``day`` (UTC), any status."""
        params: dict[str, Any] = {
            "seller": self._uid,
            "order.date_created.from": f"{day.isoformat()}T00:00:00.000-00:00",
            "order.date_created.to": f"{day.isoformat()}T23:59:59.999-00:00",
            "sort": "date_asc",
            "limit": _PAGE,
            "offset": 0,
        }
        orders: list[dict[str, Any]] = []
        while True:
            r = await self._get(f"{_API}/orders/search", params)
            r.raise_for_status()
            data = r.json()
            results = data.get("results") or []
            orders.extend(results)
            if not results:
                break
            params["offset"] += _PAGE
            total = (data.get("paging") or {}).get("total")
            if (total is not None and int(total) > 0 and params["offset"] >= int(total)) or params[
                "offset"
            ] >= _MAX_OFFSET:
                break
        return orders

    async def _fetch_shipment(self, shipment_id: int) -> dict[str, Any] | None:
        """``ml_shipments`` row, or None when ML would not answer (retried next run)."""
        try:
            costs_r = await self._get(f"{_API}/shipments/{shipment_id}/costs")
            ship_r = await self._get(f"{_API}/shipments/{shipment_id}")
        except httpx.HTTPError as exc:
            logger.warning(
                "ml_orders.shipment_fetch_failed", shipment_id=shipment_id, error=str(exc)
            )
            return None
        if costs_r.status_code != 200:
            logger.warning(
                "ml_orders.shipment_costs_status",
                shipment_id=shipment_id,
                status=costs_r.status_code,
            )
            return None
        logistic = ship_r.json().get("logistic_type") if ship_r.status_code == 200 else None
        return parse_shipment_costs(shipment_id, costs_r.json(), self._uid, logistic)

    # ------------------------------------------------------------------
    # Sync
    # ------------------------------------------------------------------
    async def sync(self, days: int = 2) -> dict[str, Any]:
        """Upsert the orders created in the last ``days`` days (incl. today)."""
        today = datetime.now(UTC).date()
        start = today - timedelta(days=days - 1)
        orders: dict[int, dict[str, Any]] = {}
        items: dict[int, list[dict[str, Any]]] = {}
        days_failed = 0
        for i in range(days):
            day = start + timedelta(days=i)
            try:
                raw_orders = await self._fetch_day(day)
            except httpx.HTTPError as exc:
                days_failed += 1
                logger.warning("ml_orders.day_failed", day=day.isoformat(), error=str(exc))
                continue
            for raw in raw_orders:
                row = parse_order(raw)
                if row is None:
                    continue
                orders[row["order_id"]] = row
                items[row["order_id"]] = parse_order_items(raw)

        shipments = await self._shipments_to_refresh(orders)
        sem = asyncio.Semaphore(_SHIP_CONCURRENCY)

        async def one(sid: int) -> dict[str, Any] | None:
            async with sem:
                return await self._fetch_shipment(sid)

        fetched = [s for s in await asyncio.gather(*(one(s) for s in shipments)) if s]
        await self._persist(list(orders.values()), items, fetched)

        stats = {
            "days": days,
            "days_failed": days_failed,
            "orders": len(orders),
            "items": sum(len(v) for v in items.values()),
            "shipments_fetched": len(fetched),
            "shipments_failed": len(shipments) - len(fetched),
            "cancelled": sum(1 for o in orders.values() if o["status"] == "cancelled"),
        }
        logger.info("ml_orders.sync_done", **stats)
        return stats

    async def _shipments_to_refresh(self, orders: dict[int, dict[str, Any]]) -> list[int]:
        """Shipments never fetched, or fetched before their order last changed."""
        by_ship: dict[int, datetime | None] = {}
        for o in orders.values():
            sid = o.get("shipping_id")
            if sid is None:
                continue
            upd = o.get("last_updated")
            prev = by_ship.get(sid)
            by_ship[sid] = upd if prev is None or (upd is not None and upd > prev) else prev
        if not by_ship:
            return []
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(MLShipmentORM.shipment_id, MLShipmentORM.fetched_at).where(
                    MLShipmentORM.shipment_id.in_(list(by_ship))
                )
            )
            known = {int(sid): fetched for sid, fetched in result.all()}
        return [
            sid
            for sid, upd in by_ship.items()
            if sid not in known or (upd is not None and upd > known[sid])
        ]

    async def _persist(
        self,
        orders: list[dict[str, Any]],
        items: dict[int, list[dict[str, Any]]],
        shipments: list[dict[str, Any]],
    ) -> None:
        async with AsyncSessionLocal() as session:
            for chunk in _chunks(orders, 500):
                stmt = pg_insert(MLOrderORM).values(chunk)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["order_id"],
                    set_={
                        col: stmt.excluded[col]
                        for col in chunk[0]
                        if col not in {"order_id", "synced_at"}
                    }
                    | {"synced_at": datetime.now(UTC)},
                )
                await session.execute(stmt)
            if items:
                # Items are replaced per order (quantities/fees can change).
                for ids in _chunks(list(items), 1000):
                    await session.execute(
                        delete(MLOrderItemORM).where(MLOrderItemORM.order_id.in_(ids))
                    )
                rows = [row for order_items in items.values() for row in order_items]
                for chunk in _chunks(rows, 1000):
                    await session.execute(pg_insert(MLOrderItemORM).values(chunk))
            for chunk in _chunks(shipments, 500):
                stmt = pg_insert(MLShipmentORM).values(chunk)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["shipment_id"],
                    set_={col: stmt.excluded[col] for col in chunk[0] if col != "shipment_id"},
                )
                await session.execute(stmt)
            await session.commit()


# ---------------------------------------------------------------------------
# Parsing (pure)
# ---------------------------------------------------------------------------
def parse_order(raw: dict[str, Any]) -> dict[str, Any] | None:
    """``ml_orders`` row from an /orders/search result, or None if unusable."""
    order_id = _int(raw.get("id"))
    created = _ts(raw.get("date_created"))
    if order_id is None or created is None:
        return None
    pack_id = _int(raw.get("pack_id"))
    cancel = raw.get("cancel_detail")
    tags = raw.get("tags")
    return {
        "order_id": order_id,
        "pack_id": pack_id,
        "tiny_ref": str(pack_id or order_id),
        "status": str(raw.get("status") or "unknown")[:30],
        "date_created": created,
        "date_closed": _ts(raw.get("date_closed")),
        "last_updated": _ts(raw.get("last_updated") or raw.get("date_last_updated")),
        "buyer_id": _int((raw.get("buyer") or {}).get("id")),
        "total_amount": _dec(raw.get("total_amount")),
        "paid_amount": _dec(raw.get("paid_amount")),
        "shipping_id": _int((raw.get("shipping") or {}).get("id")),
        "cancel_detail": cancel if isinstance(cancel, dict) and cancel else None,
        "tags": tags if isinstance(tags, list) else None,
        "synced_at": datetime.now(UTC),
    }


def parse_order_items(raw: dict[str, Any]) -> list[dict[str, Any]]:
    order_id = _int(raw.get("id"))
    rows: list[dict[str, Any]] = []
    for it in raw.get("order_items") or []:
        item = it.get("item") or {}
        mlb = item.get("id")
        if not mlb or order_id is None:
            continue
        rows.append(
            {
                "order_id": order_id,
                "mlb_id": str(mlb)[:20],
                "variation_id": _int(item.get("variation_id")),
                "seller_sku": (str(item["seller_sku"])[:100] if item.get("seller_sku") else None),
                "quantity": _int(it.get("quantity")) or 0,
                "unit_price": _dec(it.get("unit_price")),
                "sale_fee": _dec(it.get("sale_fee")),
                "listing_type_id": it.get("listing_type_id"),
            }
        )
    return rows


def parse_shipment_costs(
    shipment_id: int, costs: dict[str, Any], seller_id: str, logistic_type: str | None
) -> dict[str, Any]:
    """``ml_shipments`` row from /shipments/{id}/costs.

    ``senders`` may list more than one party; the seller's entry is the one
    with our user id (falls back to the first entry when ids are absent).
    """
    senders = costs.get("senders") or []
    mine = next((s for s in senders if str(s.get("user_id")) == str(seller_id)), None)
    if mine is None and senders:
        mine = senders[0]
    receiver = costs.get("receiver") or {}
    return {
        "shipment_id": shipment_id,
        "logistic_type": logistic_type,
        "seller_cost": _dec((mine or {}).get("cost")),
        "seller_save": _dec((mine or {}).get("save")),
        "buyer_cost": _dec(receiver.get("cost")),
        "list_cost": _dec(costs.get("gross_amount")),
        "fetched_at": datetime.now(UTC),
    }


def _int(value: Any) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


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


def _chunks(seq: list[Any], size: int) -> list[list[Any]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]
