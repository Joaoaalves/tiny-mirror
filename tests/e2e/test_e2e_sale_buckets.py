"""End-to-end coverage for stage 09 — sale-bucket aggregation.

Bucket math is precise (kit expansion multiplies the line quantity by the
component quantity, channel is normalized to 'Direct' when missing, etc.),
so we drive the tests with **synthetic** rows inserted directly into
Postgres. This keeps assertions deterministic — running them against the
live Tiny dataset would couple us to whatever orders happen to exist.

Each test picks a unique sentinel ``bucket_date`` (well in the future) and
unique product / order tiny ids so concurrent or repeat runs don't collide.
Every test cleans up after itself.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import delete, select

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import (
    OrderItemORM,
    OrderORM,
    ProductKitComponentORM,
    ProductORM,
    SaleBucketORM,
)
from tiny_mirror.services.sale_bucket_service import SaleBucketService

pytestmark = pytest.mark.e2e

SENTINEL_DATE = date(2099, 1, 15)
DIRECT_PRODUCT_TINY_ID = 99000001
KIT_PRODUCT_TINY_ID = 99000010
COMPONENT_A_TINY_ID = 99000020
COMPONENT_B_TINY_ID = 99000021
ORPHAN_KIT_TINY_ID = 99000030  # kit with no components in DB
ORDER_BASE_ID = 99000100


@pytest_asyncio.fixture
async def clean_sentinel(live_db: None) -> AsyncIterator[None]:
    """Make sure every sentinel row from previous runs is gone before /
    after each test, so assertions are deterministic.
    """
    await _drop_sentinel_rows()
    yield
    await _drop_sentinel_rows()


async def _drop_sentinel_rows() -> None:
    async with AsyncSessionLocal() as session:
        # Delete buckets first (no FKs into them).
        await session.execute(
            delete(SaleBucketORM).where(SaleBucketORM.bucket_date == SENTINEL_DATE)
        )
        # Delete orders — CASCADE cleans order_items.
        await session.execute(delete(OrderORM).where(OrderORM.order_date == SENTINEL_DATE))
        # Delete kit components and products by tiny_id range.
        await session.execute(
            delete(ProductKitComponentORM).where(
                ProductKitComponentORM.kit_product_tiny_id.in_(
                    [KIT_PRODUCT_TINY_ID, ORPHAN_KIT_TINY_ID]
                )
            )
        )
        await session.execute(
            delete(ProductORM).where(
                ProductORM.tiny_id.in_(
                    [
                        DIRECT_PRODUCT_TINY_ID,
                        KIT_PRODUCT_TINY_ID,
                        COMPONENT_A_TINY_ID,
                        COMPONENT_B_TINY_ID,
                        ORPHAN_KIT_TINY_ID,
                    ]
                )
            )
        )
        await session.commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
async def _insert_product(session, *, tiny_id: int, sku: str, type_: str = "P") -> None:
    session.add(
        ProductORM(
            tiny_id=tiny_id,
            sku=sku,
            description=f"Test product {sku}",
            type=type_,
            situation="A",
            synced_at=datetime.now(UTC),
            prices={},
        )
    )


async def _insert_kit_component(
    session,
    *,
    kit_tiny_id: int,
    component_sku: str,
    component_tiny_id: int | None,
    quantity: float,
) -> None:
    session.add(
        ProductKitComponentORM(
            kit_product_tiny_id=kit_tiny_id,
            component_product_tiny_id=component_tiny_id,
            component_sku=component_sku,
            component_description=f"Component {component_sku}",
            component_type="P",
            quantity=Decimal(str(quantity)),
        )
    )


async def _insert_order_with_items(
    session,
    *,
    order_tiny_id: int,
    order_number: int,
    ecommerce_name: str | None,
    items: list[dict[str, Any]],
) -> None:
    session.add(
        OrderORM(
            tiny_id=order_tiny_id,
            order_number=order_number,
            customer={"name": "Test"},
            situation=3,
            order_date=SENTINEL_DATE,
            ecommerce_name=ecommerce_name,
            synced_at=datetime.now(UTC),
        )
    )
    await session.flush()
    for item in items:
        session.add(
            OrderItemORM(
                order_tiny_id=order_tiny_id,
                product_tiny_id=item.get("product_tiny_id"),
                product_sku=item["product_sku"],
                product_type=item["product_type"],
                product_description=item.get("product_description") or item["product_sku"],
                quantity=Decimal(str(item["quantity"])),
                unit_value=Decimal(str(item["unit_value"])),
            )
        )


async def _get_buckets_for_sentinel() -> list[SaleBucketORM]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(SaleBucketORM)
            .where(SaleBucketORM.bucket_date == SENTINEL_DATE)
            .order_by(SaleBucketORM.is_kit_expansion, SaleBucketORM.sku)
        )
        return list(result.scalars().all())


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
async def test_direct_bucket_for_simple_product(clean_sentinel: None) -> None:
    async with AsyncSessionLocal() as session:
        await _insert_product(session, tiny_id=DIRECT_PRODUCT_TINY_ID, sku="DIRECT-1")
        await _insert_order_with_items(
            session,
            order_tiny_id=ORDER_BASE_ID,
            order_number=ORDER_BASE_ID,
            ecommerce_name="Shopify",
            items=[
                {
                    "product_tiny_id": DIRECT_PRODUCT_TINY_ID,
                    "product_sku": "DIRECT-1",
                    "product_type": "P",
                    "quantity": 3,
                    "unit_value": 25.50,
                }
            ],
        )
        await session.commit()

    await SaleBucketService().refresh_buckets(SENTINEL_DATE, SENTINEL_DATE)

    buckets = await _get_buckets_for_sentinel()
    assert len(buckets) == 1
    b = buckets[0]
    assert b.sku == "DIRECT-1"
    assert b.ecommerce_name == "Shopify"
    assert b.is_kit_expansion is False
    assert b.source_kit_sku is None
    assert b.quantity_sold == Decimal("3.00")
    assert b.total_revenue == Decimal("76.50")  # 3 * 25.50
    assert b.order_count == 1


async def test_kit_with_single_component_emits_one_expansion(
    clean_sentinel: None,
) -> None:
    async with AsyncSessionLocal() as session:
        await _insert_product(session, tiny_id=KIT_PRODUCT_TINY_ID, sku="10U-MAST", type_="K")
        await _insert_product(session, tiny_id=COMPONENT_A_TINY_ID, sku="MAST-FIT")
        # Flush so the FK on product_kit_components.component_product_tiny_id
        # finds the row inserted above.
        await session.flush()
        await _insert_kit_component(
            session,
            kit_tiny_id=KIT_PRODUCT_TINY_ID,
            component_sku="MAST-FIT",
            component_tiny_id=COMPONENT_A_TINY_ID,
            quantity=10,
        )
        await _insert_order_with_items(
            session,
            order_tiny_id=ORDER_BASE_ID,
            order_number=ORDER_BASE_ID,
            ecommerce_name="Shopify",
            items=[
                {
                    "product_tiny_id": KIT_PRODUCT_TINY_ID,
                    "product_sku": "10U-MAST",
                    "product_type": "K",
                    "quantity": 2,
                    "unit_value": 50,
                }
            ],
        )
        await session.commit()

    await SaleBucketService().refresh_buckets(SENTINEL_DATE, SENTINEL_DATE)

    buckets = await _get_buckets_for_sentinel()
    direct = [b for b in buckets if not b.is_kit_expansion]
    expansion = [b for b in buckets if b.is_kit_expansion]
    assert len(direct) == 1 and len(expansion) == 1

    d = direct[0]
    assert d.sku == "10U-MAST"
    assert d.quantity_sold == Decimal("2.00")
    assert d.total_revenue == Decimal("100.00")  # 2 * 50

    e = expansion[0]
    assert e.sku == "MAST-FIT"
    assert e.source_kit_sku == "10U-MAST"
    assert e.quantity_sold == Decimal("20.00")  # 2 * 10
    assert e.total_revenue == Decimal("0.00")  # always zero
    assert e.order_count == 1


async def test_kit_with_two_components_emits_two_expansions(
    clean_sentinel: None,
) -> None:
    async with AsyncSessionLocal() as session:
        await _insert_product(session, tiny_id=KIT_PRODUCT_TINY_ID, sku="COMBO-ABC", type_="K")
        await _insert_product(session, tiny_id=COMPONENT_A_TINY_ID, sku="PROD-A")
        await _insert_product(session, tiny_id=COMPONENT_B_TINY_ID, sku="PROD-B")
        await session.flush()
        await _insert_kit_component(
            session,
            kit_tiny_id=KIT_PRODUCT_TINY_ID,
            component_sku="PROD-A",
            component_tiny_id=COMPONENT_A_TINY_ID,
            quantity=1,
        )
        await _insert_kit_component(
            session,
            kit_tiny_id=KIT_PRODUCT_TINY_ID,
            component_sku="PROD-B",
            component_tiny_id=COMPONENT_B_TINY_ID,
            quantity=2,
        )
        await _insert_order_with_items(
            session,
            order_tiny_id=ORDER_BASE_ID,
            order_number=ORDER_BASE_ID,
            ecommerce_name="Mercado Livre",
            items=[
                {
                    "product_tiny_id": KIT_PRODUCT_TINY_ID,
                    "product_sku": "COMBO-ABC",
                    "product_type": "K",
                    "quantity": 3,
                    "unit_value": 80,
                }
            ],
        )
        await session.commit()

    await SaleBucketService().refresh_buckets(SENTINEL_DATE, SENTINEL_DATE)

    buckets = await _get_buckets_for_sentinel()
    direct = [b for b in buckets if not b.is_kit_expansion]
    expansion = sorted([b for b in buckets if b.is_kit_expansion], key=lambda b: b.sku)

    assert len(direct) == 1
    assert direct[0].quantity_sold == Decimal("3.00")
    assert direct[0].total_revenue == Decimal("240.00")  # 3 * 80

    assert len(expansion) == 2
    assert expansion[0].sku == "PROD-A"
    assert expansion[0].quantity_sold == Decimal("3.00")  # 3 * 1
    assert expansion[0].total_revenue == Decimal("0.00")
    assert expansion[1].sku == "PROD-B"
    assert expansion[1].quantity_sold == Decimal("6.00")  # 3 * 2
    assert expansion[1].total_revenue == Decimal("0.00")


async def test_two_orders_same_day_aggregate_into_one_bucket(
    clean_sentinel: None,
) -> None:
    async with AsyncSessionLocal() as session:
        await _insert_product(session, tiny_id=DIRECT_PRODUCT_TINY_ID, sku="DIRECT-1")
        # Two orders, same product / channel / day.
        for offset, qty in enumerate([2, 5]):
            await _insert_order_with_items(
                session,
                order_tiny_id=ORDER_BASE_ID + offset,
                order_number=ORDER_BASE_ID + offset,
                ecommerce_name="Shopify",
                items=[
                    {
                        "product_tiny_id": DIRECT_PRODUCT_TINY_ID,
                        "product_sku": "DIRECT-1",
                        "product_type": "P",
                        "quantity": qty,
                        "unit_value": 10,
                    }
                ],
            )
        await session.commit()

    await SaleBucketService().refresh_buckets(SENTINEL_DATE, SENTINEL_DATE)

    buckets = await _get_buckets_for_sentinel()
    assert len(buckets) == 1
    b = buckets[0]
    assert b.quantity_sold == Decimal("7.00")  # 2 + 5
    assert b.total_revenue == Decimal("70.00")  # 2*10 + 5*10
    assert b.order_count == 2


async def test_order_without_ecommerce_name_uses_direct(
    clean_sentinel: None,
) -> None:
    async with AsyncSessionLocal() as session:
        await _insert_product(session, tiny_id=DIRECT_PRODUCT_TINY_ID, sku="DIRECT-1")
        await _insert_order_with_items(
            session,
            order_tiny_id=ORDER_BASE_ID,
            order_number=ORDER_BASE_ID,
            ecommerce_name=None,
            items=[
                {
                    "product_tiny_id": DIRECT_PRODUCT_TINY_ID,
                    "product_sku": "DIRECT-1",
                    "product_type": "P",
                    "quantity": 1,
                    "unit_value": 10,
                }
            ],
        )
        await session.commit()

    await SaleBucketService().refresh_buckets(SENTINEL_DATE, SENTINEL_DATE)

    buckets = await _get_buckets_for_sentinel()
    assert len(buckets) == 1
    assert buckets[0].ecommerce_name == "Direct"


async def test_kit_without_components_in_db_emits_only_direct_bucket(
    clean_sentinel: None,
) -> None:
    async with AsyncSessionLocal() as session:
        # The kit row exists but has zero rows in product_kit_components.
        await _insert_product(session, tiny_id=ORPHAN_KIT_TINY_ID, sku="ORPHAN-KIT", type_="K")
        await _insert_order_with_items(
            session,
            order_tiny_id=ORDER_BASE_ID,
            order_number=ORDER_BASE_ID,
            ecommerce_name="Shopify",
            items=[
                {
                    "product_tiny_id": ORPHAN_KIT_TINY_ID,
                    "product_sku": "ORPHAN-KIT",
                    "product_type": "K",
                    "quantity": 1,
                    "unit_value": 100,
                }
            ],
        )
        await session.commit()

    await SaleBucketService().refresh_buckets(SENTINEL_DATE, SENTINEL_DATE)

    buckets = await _get_buckets_for_sentinel()
    assert len(buckets) == 1
    assert buckets[0].is_kit_expansion is False
    assert buckets[0].sku == "ORPHAN-KIT"


async def test_refresh_is_idempotent(clean_sentinel: None) -> None:
    async with AsyncSessionLocal() as session:
        await _insert_product(session, tiny_id=DIRECT_PRODUCT_TINY_ID, sku="DIRECT-1")
        await _insert_order_with_items(
            session,
            order_tiny_id=ORDER_BASE_ID,
            order_number=ORDER_BASE_ID,
            ecommerce_name="Shopify",
            items=[
                {
                    "product_tiny_id": DIRECT_PRODUCT_TINY_ID,
                    "product_sku": "DIRECT-1",
                    "product_type": "P",
                    "quantity": 4,
                    "unit_value": 12.50,
                }
            ],
        )
        await session.commit()

    svc = SaleBucketService()
    await svc.refresh_buckets(SENTINEL_DATE, SENTINEL_DATE)
    first = [
        (b.sku, b.quantity_sold, b.total_revenue, b.order_count)
        for b in await _get_buckets_for_sentinel()
    ]

    await svc.refresh_buckets(SENTINEL_DATE, SENTINEL_DATE)
    second = [
        (b.sku, b.quantity_sold, b.total_revenue, b.order_count)
        for b in await _get_buckets_for_sentinel()
    ]

    assert first == second


async def test_empty_period_produces_no_buckets(clean_sentinel: None) -> None:
    # No orders inserted for the sentinel date.
    await SaleBucketService().refresh_buckets(SENTINEL_DATE, SENTINEL_DATE)

    buckets = await _get_buckets_for_sentinel()
    assert buckets == []


async def test_get_buckets_for_sku_returns_in_descending_date(
    clean_sentinel: None,
) -> None:
    other_date = date(2099, 1, 17)
    async with AsyncSessionLocal() as session:
        await _insert_product(session, tiny_id=DIRECT_PRODUCT_TINY_ID, sku="DIRECT-1")
        # First sentinel date.
        await _insert_order_with_items(
            session,
            order_tiny_id=ORDER_BASE_ID,
            order_number=ORDER_BASE_ID,
            ecommerce_name="Shopify",
            items=[
                {
                    "product_tiny_id": DIRECT_PRODUCT_TINY_ID,
                    "product_sku": "DIRECT-1",
                    "product_type": "P",
                    "quantity": 1,
                    "unit_value": 10,
                }
            ],
        )
        # Second date — insert order directly with the other date.
        session.add(
            OrderORM(
                tiny_id=ORDER_BASE_ID + 1,
                order_number=ORDER_BASE_ID + 1,
                customer={"name": "Test"},
                situation=3,
                order_date=other_date,
                ecommerce_name="Shopify",
                synced_at=datetime.now(UTC),
            )
        )
        await session.flush()
        session.add(
            OrderItemORM(
                order_tiny_id=ORDER_BASE_ID + 1,
                product_tiny_id=DIRECT_PRODUCT_TINY_ID,
                product_sku="DIRECT-1",
                product_type="P",
                product_description="Test",
                quantity=Decimal("2"),
                unit_value=Decimal("10"),
            )
        )
        await session.commit()

    try:
        await SaleBucketService().refresh_buckets(SENTINEL_DATE, other_date)

        from tiny_mirror.infrastructure.repositories.sale_bucket_repository import (
            PostgreSQLSaleBucketRepository,
        )

        async with AsyncSessionLocal() as session:
            buckets = await PostgreSQLSaleBucketRepository(session).get_buckets_for_sku(
                "DIRECT-1", days=365 * 100
            )

        # Should have two buckets for DIRECT-1, ordered DESC by bucket_date.
        sku_buckets = [b for b in buckets if b["sku"] == "DIRECT-1"]
        assert len(sku_buckets) == 2
        assert sku_buckets[0]["bucket_date"] == other_date
        assert sku_buckets[1]["bucket_date"] == SENTINEL_DATE
    finally:
        # Manual cleanup for the second date row that the fixture won't
        # remove (it only handles the SENTINEL_DATE).
        async with AsyncSessionLocal() as session:
            await session.execute(
                delete(SaleBucketORM).where(SaleBucketORM.bucket_date == other_date)
            )
            await session.execute(delete(OrderORM).where(OrderORM.order_date == other_date))
            await session.commit()
