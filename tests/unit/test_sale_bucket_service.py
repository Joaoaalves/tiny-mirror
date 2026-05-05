"""Unit tests for :class:`SaleBucketService`.

The kit-expansion logic is the most fragile part: it has to identify
kit line items by their MASTER product type (``products.type``), not by
``order_items.product_type`` which mirrors Tiny's order payload and
reports ``'P'`` even for kit lines.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from tiny_mirror.services.sale_bucket_service import SaleBucketService

pytestmark = pytest.mark.unit


def _order(
    tiny_id: int,
    order_date: date,
    items: list[dict],
    ecommerce_name: str = "Mercado Livre",
) -> dict:
    return {
        "tiny_id": tiny_id,
        "order_date": order_date,
        "ecommerce_name": ecommerce_name,
        "items": items,
    }


def _item(
    sku: str,
    product_tiny_id: int | None,
    quantity: float,
    unit_value: float,
    product_type: str = "P",
) -> dict:
    return {
        "product_sku": sku,
        "product_tiny_id": product_tiny_id,
        "quantity": Decimal(str(quantity)),
        "unit_value": Decimal(str(unit_value)),
        "product_type": product_type,  # mirrors Tiny payload — UNRELIABLE for kits
    }


@pytest.fixture
def fake_session():
    s = AsyncMock()
    s.__aenter__ = AsyncMock(return_value=s)
    s.__aexit__ = AsyncMock(return_value=False)
    return s


@pytest.fixture
def fake_buckets_repo():
    repo = AsyncMock()
    repo.delete_buckets_for_period = AsyncMock(return_value=0)
    repo.upsert_buckets_batch = AsyncMock(return_value=None)
    return repo


@pytest.fixture
def fake_orders_repo():
    return AsyncMock()


@pytest.fixture
def fake_products_repo():
    repo = AsyncMock()
    repo.get_types_for_ids = AsyncMock(return_value={})
    repo.get_kit_components_for_ids = AsyncMock(return_value={})
    return repo


@pytest.fixture
def patched_service(fake_session, fake_buckets_repo, fake_orders_repo, fake_products_repo):
    """Yield SaleBucketService with all repositories + AsyncSessionLocal patched."""
    with (
        patch(
            "tiny_mirror.services.sale_bucket_service.AsyncSessionLocal",
            return_value=fake_session,
        ),
        patch(
            "tiny_mirror.services.sale_bucket_service.PostgreSQLSaleBucketRepository",
            return_value=fake_buckets_repo,
        ),
        patch(
            "tiny_mirror.services.sale_bucket_service.PostgreSQLOrderRepository",
            return_value=fake_orders_repo,
        ),
        patch(
            "tiny_mirror.services.sale_bucket_service.PostgreSQLProductRepository",
            return_value=fake_products_repo,
        ),
    ):
        yield SaleBucketService(), fake_buckets_repo, fake_orders_repo, fake_products_repo


# ---------------------------------------------------------------------------
# THE BUG: order_items.product_type='P' but products.type='K' must still expand
# ---------------------------------------------------------------------------
async def test_kit_expansion_uses_product_master_type_not_order_line_type(
    patched_service,
) -> None:
    """Regression: SLF-PDISLIQUI-PR was showing 0 sales because kit
    expansion was gated on ``order_items.product_type=='K'``, which
    Tiny never returns. Now the lookup is ``products.type``.
    """
    service, buckets_repo, orders_repo, products_repo = patched_service

    kit_id = 970398531
    kit_sku = "SLF-KITDISPLPDEN-PR"

    orders_repo.get_orders_in_period = AsyncMock(
        return_value=[
            _order(
                tiny_id=971865167,
                order_date=date(2026, 5, 2),
                items=[
                    # IMPORTANT: product_type='P' here matches the real
                    # Tiny payload — kit lines are reported as 'P'.
                    _item(kit_sku, kit_id, quantity=2, unit_value=55.57, product_type="P"),
                ],
            ),
        ]
    )
    products_repo.get_types_for_ids = AsyncMock(return_value={kit_id: "K"})
    products_repo.get_kit_components_for_ids = AsyncMock(
        return_value={
            kit_id: [
                {"component_sku": "SLF-PESCDENT-PR", "quantity": Decimal("1")},
                {"component_sku": "SLF-PDISLIQUI-PR", "quantity": Decimal("1")},
            ]
        }
    )

    await service.refresh_buckets(date(2026, 5, 1), date(2026, 5, 3))

    buckets_repo.upsert_buckets_batch.assert_awaited_once()
    written = buckets_repo.upsert_buckets_batch.call_args.args[0]

    by_sku = {(b["sku"], b["is_kit_expansion"]): b for b in written}

    # Direct bucket for the kit
    kit_direct = by_sku[(kit_sku, False)]
    assert kit_direct["quantity_sold"] == Decimal("2")
    assert kit_direct["total_revenue"] == Decimal("111.14")  # 2 * 55.57
    assert kit_direct["source_kit_sku"] is None

    # Expansion buckets for both components
    pdis = by_sku[("SLF-PDISLIQUI-PR", True)]
    assert pdis["quantity_sold"] == Decimal("2")  # 2 kits * 1 per kit
    assert pdis["total_revenue"] == Decimal("0")  # expansion rows always zero
    assert pdis["source_kit_sku"] == kit_sku

    pesc = by_sku[("SLF-PESCDENT-PR", True)]
    assert pesc["quantity_sold"] == Decimal("2")
    assert pesc["source_kit_sku"] == kit_sku


async def test_non_kit_items_do_not_create_expansion_rows(patched_service) -> None:
    service, buckets_repo, orders_repo, products_repo = patched_service

    simple_id = 111
    orders_repo.get_orders_in_period = AsyncMock(
        return_value=[
            _order(
                tiny_id=1,
                order_date=date(2026, 5, 2),
                items=[_item("SKU-SIMPLES", simple_id, quantity=3, unit_value=10)],
            ),
        ]
    )
    products_repo.get_types_for_ids = AsyncMock(return_value={simple_id: "S"})

    await service.refresh_buckets(date(2026, 5, 1), date(2026, 5, 3))

    written = buckets_repo.upsert_buckets_batch.call_args.args[0]
    assert len(written) == 1
    assert written[0]["sku"] == "SKU-SIMPLES"
    assert written[0]["is_kit_expansion"] is False
    products_repo.get_kit_components_for_ids.assert_awaited_once_with([])


async def test_kit_with_no_components_still_writes_direct_bucket(patched_service) -> None:
    """A kit with no components_map entry warns + skips expansion but still
    writes the direct bucket so the kit's own revenue is tracked."""
    service, buckets_repo, orders_repo, products_repo = patched_service

    kit_id = 999
    orders_repo.get_orders_in_period = AsyncMock(
        return_value=[
            _order(
                tiny_id=2,
                order_date=date(2026, 5, 2),
                items=[_item("KIT-NOCOMP", kit_id, quantity=1, unit_value=20)],
            ),
        ]
    )
    products_repo.get_types_for_ids = AsyncMock(return_value={kit_id: "K"})
    products_repo.get_kit_components_for_ids = AsyncMock(return_value={kit_id: []})

    await service.refresh_buckets(date(2026, 5, 1), date(2026, 5, 3))

    written = buckets_repo.upsert_buckets_batch.call_args.args[0]
    assert len(written) == 1
    assert written[0]["sku"] == "KIT-NOCOMP"
    assert written[0]["is_kit_expansion"] is False


async def test_component_quantity_multiplies_by_kit_qty(patched_service) -> None:
    """Kit with component qty=3, sold 2 kits → expansion qty = 6."""
    service, buckets_repo, orders_repo, products_repo = patched_service

    kit_id = 555
    orders_repo.get_orders_in_period = AsyncMock(
        return_value=[
            _order(
                tiny_id=3,
                order_date=date(2026, 5, 2),
                items=[_item("KIT-3X", kit_id, quantity=2, unit_value=99)],
            )
        ]
    )
    products_repo.get_types_for_ids = AsyncMock(return_value={kit_id: "K"})
    products_repo.get_kit_components_for_ids = AsyncMock(
        return_value={kit_id: [{"component_sku": "COMP-A", "quantity": Decimal("3")}]}
    )

    await service.refresh_buckets(date(2026, 5, 1), date(2026, 5, 3))

    written = buckets_repo.upsert_buckets_batch.call_args.args[0]
    comp = next(b for b in written if b["sku"] == "COMP-A" and b["is_kit_expansion"])
    assert comp["quantity_sold"] == Decimal("6")  # 2 kits * 3 per kit
    assert comp["total_revenue"] == Decimal("0")


async def test_empty_period_writes_no_buckets(patched_service) -> None:
    service, buckets_repo, orders_repo, products_repo = patched_service
    orders_repo.get_orders_in_period = AsyncMock(return_value=[])

    await service.refresh_buckets(date(2026, 5, 1), date(2026, 5, 3))

    buckets_repo.upsert_buckets_batch.assert_awaited_once_with([])
