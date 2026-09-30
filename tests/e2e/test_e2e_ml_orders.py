"""End-to-end coverage for ml_orders / ml_order_items / ml_shipments.

- persistence + the ``v_order_datetime`` view run against live Postgres with
  synthetic ids (cleaned up afterwards);
- the ML read path runs against the live ML Orders API (GET only) and is
  skipped when ``ML_CLIENT_ID`` is not configured.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import delete, func, select, text

from tiny_mirror.config import settings
from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import (
    MLOrderItemORM,
    MLOrderORM,
    MLShipmentORM,
    OrderORM,
)
from tiny_mirror.infrastructure.repositories.order_repository import (
    PostgreSQLOrderRepository,
)
from tiny_mirror.redis_client import get_redis
from tiny_mirror.services.mercadolivre_token_service import MercadoLivreTokenService
from tiny_mirror.services.ml_order_sync_service import (
    MLOrderSyncService,
    parse_order,
    parse_order_items,
)

pytestmark = pytest.mark.e2e

_TINY_ID = 9_990_000_101
_PACK = 9_990_000_000_101
_ORDERS = (9_990_000_000_201, 9_990_000_000_202)
_SHIP = 9_990_000_000_301


def _raw(order_id: int, created: str) -> dict[str, object]:
    return {
        "id": order_id,
        "pack_id": _PACK,
        "status": "paid",
        "date_created": created,
        "last_updated": created,
        "buyer": {"id": 1},
        "total_amount": 10,
        "shipping": {"id": _SHIP},
        "order_items": [
            {
                "item": {"id": "MLB9990000001", "seller_sku": "E2E-SKU"},
                "quantity": 2,
                "unit_price": 5,
                "sale_fee": 1.1,
                "listing_type_id": "gold_special",
            }
        ],
    }


async def _cleanup() -> None:
    async with AsyncSessionLocal() as session:
        await session.execute(delete(MLOrderORM).where(MLOrderORM.order_id.in_(_ORDERS)))
        await session.execute(delete(MLShipmentORM).where(MLShipmentORM.shipment_id == _SHIP))
        await session.execute(delete(OrderORM).where(OrderORM.tiny_id == _TINY_ID))
        await session.commit()


async def test_persist_and_order_datetime_view(live_db: None) -> None:
    """A pack of two ML orders maps to ONE Tiny order whose exact time is the
    earliest ML order; items are replaced, not duplicated, on re-sync."""
    await _cleanup()
    service = MLOrderSyncService(token_service=None, http_client=None, ml_user_id="1")  # type: ignore[arg-type]
    raws = [
        _raw(_ORDERS[0], "2026-09-30T11:38:37.000-04:00"),
        _raw(_ORDERS[1], "2026-09-30T11:40:00.000-04:00"),
    ]
    orders = [parse_order(r) for r in raws]
    items = {o["order_id"]: parse_order_items(r) for o, r in zip(orders, raws, strict=True) if o}
    ship = {
        "shipment_id": _SHIP,
        "logistic_type": "fulfillment",
        "seller_cost": Decimal("6.95"),
        "seller_save": Decimal("2.98"),
        "buyer_cost": Decimal("0"),
        "list_cost": Decimal("52.72"),
        "fetched_at": datetime.now(UTC),
    }
    try:
        async with AsyncSessionLocal() as session:
            await PostgreSQLOrderRepository(session).upsert(
                {
                    "tiny_id": _TINY_ID,
                    "order_number": 990_000_101,
                    "ecommerce_order_number": str(_PACK),
                    "customer": {},
                    "situation": 3,
                    "order_date": datetime(2026, 9, 30).date(),
                    "synced_at": datetime.now(UTC),
                }
            )
        valid = [o for o in orders if o]
        await service._persist(valid, items, [ship])
        await service._persist(valid, items, [ship])  # idempotent re-sync

        async with AsyncSessionLocal() as session:
            view = (
                await session.execute(
                    text("SELECT order_datetime FROM v_order_datetime WHERE tiny_id = :t"),
                    {"t": _TINY_ID},
                )
            ).scalar_one()
            item_count = (
                await session.execute(
                    select(func.count(MLOrderItemORM.id)).where(
                        MLOrderItemORM.order_id.in_(_ORDERS)
                    )
                )
            ).scalar_one()
            cost = (
                await session.execute(
                    select(MLShipmentORM.seller_cost).where(MLShipmentORM.shipment_id == _SHIP)
                )
            ).scalar_one()
        assert view == datetime(2026, 9, 30, 15, 38, 37, tzinfo=UTC)
        assert item_count == 2
        assert cost == Decimal("6.95")
    finally:
        await _cleanup()


@pytest_asyncio.fixture
async def live_ml_service(live_db: None, live_redis: None) -> AsyncIterator[MLOrderSyncService]:
    if not settings.ml_client_id:
        pytest.skip("ML_CLIENT_ID not configured")
    async with httpx.AsyncClient(timeout=30.0) as http:
        tokens = MercadoLivreTokenService(
            session_factory=AsyncSessionLocal,
            redis_client=get_redis(),
            http_client=http,
            ml_client_id=settings.ml_client_id,
            ml_client_secret=settings.ml_client_secret,
            ml_initial_refresh_token=settings.ml_refresh_token,
        )
        await tokens.validate_on_startup()
        yield MLOrderSyncService(
            token_service=tokens, http_client=http, ml_user_id=settings.ml_user_id
        )


async def test_live_orders_parse_with_exact_time(live_ml_service: MLOrderSyncService) -> None:
    """Read-only: yesterday's ML orders all parse, with a timezone-aware time."""
    day = datetime.now(UTC).date() - timedelta(days=1)
    raw_orders = await live_ml_service._fetch_day(day)

    parsed = [parse_order(r) for r in raw_orders]
    assert all(p is not None for p in parsed)
    for p in parsed:
        assert p is not None and p["date_created"].tzinfo is not None
        assert p["tiny_ref"] == str(p["pack_id"] or p["order_id"])
