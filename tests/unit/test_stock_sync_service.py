"""Unit tests for :class:`tiny_mirror.services.stock_sync_service.StockSyncService`.

Focus areas:
- _overlay_ml_full_deposit (pure function)
- _fetch_ml_full_qty — FL computation logic (mocks _fl_for_sku)
- _fl_for_sku — DB + Inventory API integration (mocks session + ML client)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tiny_mirror.services.stock_sync_service import (
    ML_FULL_DEPOSIT_NAME,
    ML_FULL_DEPOSIT_SENTINEL_ID,
    StockSyncService,
    _overlay_ml_full_deposit,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# _overlay_ml_full_deposit (pure function)
# ---------------------------------------------------------------------------
def test_overlay_overwrites_existing_full_ml_row() -> None:
    deposits = [
        {
            "deposit_tiny_id": 851264346,
            "deposit_name": "Galpão",
            "ignore": False,
            "balance": 100.0,
            "reserved": 0.0,
            "available": 100.0,
            "company": None,
        },
        {
            "deposit_tiny_id": 912048995,
            "deposit_name": "Full Mercado Livre",
            "ignore": True,
            "balance": -3.0,
            "reserved": 0.0,
            "available": -3.0,
            "company": None,
        },
    ]

    _overlay_ml_full_deposit(deposits, ml_qty=12)

    assert len(deposits) == 2
    full_row = next(d for d in deposits if d["deposit_name"] == "Full Mercado Livre")
    assert full_row["balance"] == 12.0
    assert full_row["available"] == 12.0
    assert full_row["reserved"] == 0.0
    assert full_row["ignore"] is False
    assert full_row["deposit_tiny_id"] == 912048995


def test_overlay_appends_synthetic_row_when_tiny_has_none() -> None:
    deposits = [
        {
            "deposit_tiny_id": 851264346,
            "deposit_name": "Galpão",
            "ignore": False,
            "balance": 100.0,
            "reserved": 0.0,
            "available": 100.0,
            "company": None,
        },
    ]

    _overlay_ml_full_deposit(deposits, ml_qty=7)

    assert len(deposits) == 2
    full_row = next(d for d in deposits if d["deposit_name"] == ML_FULL_DEPOSIT_NAME)
    assert full_row["deposit_tiny_id"] == ML_FULL_DEPOSIT_SENTINEL_ID
    assert full_row["balance"] == 7.0
    assert full_row["available"] == 7.0
    assert full_row["ignore"] is False


def test_overlay_with_zero_qty_writes_zero_and_unignores() -> None:
    deposits = [
        {
            "deposit_tiny_id": 912048995,
            "deposit_name": "Full Mercado Livre",
            "ignore": True,
            "balance": 5.0,
            "reserved": 0.0,
            "available": 5.0,
            "company": None,
        },
    ]

    _overlay_ml_full_deposit(deposits, ml_qty=0)

    full_row = deposits[0]
    assert full_row["balance"] == 0.0
    assert full_row["available"] == 0.0
    assert full_row["ignore"] is False


# ---------------------------------------------------------------------------
# _fetch_ml_full_qty — logic tests (mock _fl_for_sku to avoid DB)
# ---------------------------------------------------------------------------
@pytest.fixture
def ml_client() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def stock_service(ml_client: AsyncMock) -> StockSyncService:
    return StockSyncService(
        tiny_client=AsyncMock(),
        queue_publisher=AsyncMock(),
        ml_client=ml_client,
    )


async def test_fetch_ml_full_qty_returns_none_when_ml_disabled() -> None:
    service = StockSyncService(
        tiny_client=AsyncMock(),
        queue_publisher=AsyncMock(),
        ml_client=None,
    )

    result = await service._fetch_ml_full_qty("ANY-SKU")

    assert result is None


async def test_fetch_ml_full_qty_simple_sku_no_listings_returns_none(
    stock_service: StockSyncService,
) -> None:
    stock_service._fl_for_sku = AsyncMock(return_value=None)  # type: ignore[method-assign]

    result = await stock_service._fetch_ml_full_qty("SKU-MISSING")

    assert result is None
    stock_service._fl_for_sku.assert_awaited_once_with("SKU-MISSING")


async def test_fetch_ml_full_qty_simple_sku_returns_inventory_stock(
    stock_service: StockSyncService,
) -> None:
    stock_service._fl_for_sku = AsyncMock(return_value=10)  # type: ignore[method-assign]

    result = await stock_service._fetch_ml_full_qty("SKU-A")

    assert result == 10


async def test_fetch_ml_full_qty_simple_sku_zero_returns_zero(
    stock_service: StockSyncService,
) -> None:
    stock_service._fl_for_sku = AsyncMock(return_value=0)  # type: ignore[method-assign]

    result = await stock_service._fetch_ml_full_qty("SKU-OOS")

    assert result == 0


async def test_fetch_ml_full_qty_adds_parent_kit_contributions(
    stock_service: StockSyncService,
) -> None:
    """own=5 + kit_fl=8 x component_qty=2 = 5 + 16 = 21."""
    stock_service._fl_for_sku = AsyncMock(  # type: ignore[method-assign]
        side_effect={"SKU-A": 5, "KIT-SKU": 8}.get
    )
    stock_service._fl_for_sku = AsyncMock(side_effect=[5, 8])  # type: ignore[method-assign]

    result = await stock_service._fetch_ml_full_qty("SKU-A", parent_kits=[("KIT-SKU", 2)])

    assert result == 21  # 5 + 8*2


async def test_fetch_ml_full_qty_own_none_but_kit_has_fl_returns_kit_contribution(
    stock_service: StockSyncService,
) -> None:
    """Own SKU has no FL listing (None), but parent kit has FL → sum counts."""
    stock_service._fl_for_sku = AsyncMock(side_effect=[None, 6])  # type: ignore[method-assign]

    result = await stock_service._fetch_ml_full_qty("SKU-B", parent_kits=[("KIT-SKU", 3)])

    assert result == 18  # 0 + 6*3


async def test_fetch_ml_full_qty_all_none_returns_none(
    stock_service: StockSyncService,
) -> None:
    """Own and all kits return None → we don't know, return None."""
    stock_service._fl_for_sku = AsyncMock(side_effect=[None, None])  # type: ignore[method-assign]

    result = await stock_service._fetch_ml_full_qty("SKU-C", parent_kits=[("KIT-SKU", 1)])

    assert result is None


async def test_fetch_ml_full_qty_quantity_kit_divides_base(
    stock_service: StockSyncService,
) -> None:
    """3U-BASE-SKU: base_fl=9, x=3 → 9//3 = 3."""
    stock_service._fl_for_sku = AsyncMock(return_value=9)  # type: ignore[method-assign]

    result = await stock_service._fetch_ml_full_qty("3U-BASE-SKU", ptype="K")

    assert result == 3
    stock_service._fl_for_sku.assert_awaited_once_with("BASE-SKU")


async def test_fetch_ml_full_qty_quantity_kit_base_fl_none_returns_none(
    stock_service: StockSyncService,
) -> None:
    stock_service._fl_for_sku = AsyncMock(return_value=None)  # type: ignore[method-assign]

    result = await stock_service._fetch_ml_full_qty("2U-BASE-SKU", ptype="K")

    assert result is None


async def test_fetch_ml_full_qty_quantity_kit_integer_division(
    stock_service: StockSyncService,
) -> None:
    """5U-BASE: base_fl=11 → 11//5 = 2 (floor)."""
    stock_service._fl_for_sku = AsyncMock(return_value=11)  # type: ignore[method-assign]

    result = await stock_service._fetch_ml_full_qty("5U-BASE", ptype="K")

    assert result == 2


async def test_fetch_ml_full_qty_combo_kit_not_qty_pattern_uses_own_fl(
    stock_service: StockSyncService,
) -> None:
    """COM- kit SKU doesn't match qty pattern → treated like simple, own FL only."""
    stock_service._fl_for_sku = AsyncMock(return_value=4)  # type: ignore[method-assign]

    result = await stock_service._fetch_ml_full_qty("COM-KIT2PRTV", ptype="K")

    assert result == 4
    stock_service._fl_for_sku.assert_awaited_once_with("COM-KIT2PRTV")


async def test_fetch_ml_full_qty_exception_returns_none(
    stock_service: StockSyncService,
) -> None:
    stock_service._fl_for_sku = AsyncMock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]

    result = await stock_service._fetch_ml_full_qty("SKU-FAIL")

    assert result is None


# ---------------------------------------------------------------------------
# _fl_for_sku — DB + Inventory API tests
# ---------------------------------------------------------------------------
def _make_listing_row(mlb_id: str, inventory_id: str | None, has_variations: bool) -> tuple:
    row = MagicMock()
    row.mlb_id = mlb_id
    row.inventory_id = inventory_id
    row.has_variations = has_variations
    # Make tuple-unpack work: mlb_id, inventory_id, has_variations
    row.__iter__ = lambda self: iter([self.mlb_id, self.inventory_id, self.has_variations])
    return row


@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_fl_for_sku_no_listings_returns_none(
    mock_session_local: MagicMock, stock_service: StockSyncService
) -> None:
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    result_mock = MagicMock()
    result_mock.all.return_value = []
    mock_session.execute = AsyncMock(return_value=result_mock)

    result = await stock_service._fl_for_sku("SKU-MISSING")

    assert result is None


@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_fl_for_sku_simple_listing_sums_inventory_stock(
    mock_session_local: MagicMock,
    stock_service: StockSyncService,
    ml_client: AsyncMock,
) -> None:
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    listings_result = MagicMock()
    listings_result.all.return_value = [("MLB1", "INV-AAA", False)]
    mock_session.execute = AsyncMock(return_value=listings_result)

    ml_client.get_inventory_stock = AsyncMock(return_value={"available_quantity": 15})

    result = await stock_service._fl_for_sku("SKU-A")

    assert result == 15
    ml_client.get_inventory_stock.assert_awaited_once_with("INV-AAA")


@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_fl_for_sku_deduplicates_shared_inventory_id(
    mock_session_local: MagicMock,
    stock_service: StockSyncService,
    ml_client: AsyncMock,
) -> None:
    """Two listings with the same inventory_id → API called once, not twice."""
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    listings_result = MagicMock()
    listings_result.all.return_value = [
        ("MLB_A", "INV-SHARED", False),
        ("MLB_B", "INV-SHARED", False),
    ]
    mock_session.execute = AsyncMock(return_value=listings_result)

    ml_client.get_inventory_stock = AsyncMock(return_value={"available_quantity": 30})

    result = await stock_service._fl_for_sku("SKU-SHARED")

    assert result == 30
    ml_client.get_inventory_stock.assert_awaited_once_with("INV-SHARED")


@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_fl_for_sku_sums_distinct_inventory_ids(
    mock_session_local: MagicMock,
    stock_service: StockSyncService,
    ml_client: AsyncMock,
) -> None:
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    listings_result = MagicMock()
    listings_result.all.return_value = [
        ("MLB_X", "INV-AAA", False),
        ("MLB_Y", "INV-BBB", False),
    ]
    mock_session.execute = AsyncMock(return_value=listings_result)

    ml_client.get_inventory_stock = AsyncMock(
        side_effect=[{"available_quantity": 20}, {"available_quantity": 15}]
    )

    result = await stock_service._fl_for_sku("SKU-MULTI")

    assert result == 35


@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_fl_for_sku_variation_listing_uses_variation_inventory_ids(
    mock_session_local: MagicMock,
    stock_service: StockSyncService,
    ml_client: AsyncMock,
) -> None:
    """Listing with has_variations=True: fetch inventory_id from ml_listing_variations."""
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    listings_result = MagicMock()
    listings_result.all.return_value = [("MLB_V", None, True)]

    var_scalars = MagicMock()
    var_scalars.scalars.return_value.all.return_value = ["INV-VAR"]
    var_scalars.all = MagicMock()

    mock_session.execute = AsyncMock(side_effect=[listings_result, var_scalars])

    ml_client.get_inventory_stock = AsyncMock(return_value={"available_quantity": 8})

    result = await stock_service._fl_for_sku("SKU-VAR")

    assert result == 8
    ml_client.get_inventory_stock.assert_awaited_once_with("INV-VAR")


@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_fl_for_sku_no_inventory_ids_returns_zero(
    mock_session_local: MagicMock,
    stock_service: StockSyncService,
    ml_client: AsyncMock,
) -> None:
    """Fulfillment listing exists but has no inventory_id → return 0 (not None)."""
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    listings_result = MagicMock()
    listings_result.all.return_value = [("MLB_NO_INV", None, False)]
    mock_session.execute = AsyncMock(return_value=listings_result)

    result = await stock_service._fl_for_sku("SKU-NO-INV")

    assert result == 0
    ml_client.get_inventory_stock.assert_not_awaited()
