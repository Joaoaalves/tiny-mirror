"""Unit tests for :class:`tiny_mirror.services.stock_sync_service.StockSyncService`.

Focus areas:
- _overlay_ml_full_deposit (pure function)
- _sum_tiny_fl_available + _extract_cost_price (pure helpers)
- _maybe_record_webhook_transfer — webhook delta detection
- _fetch_ml_full_qty — FL computation logic (mocks _fl_for_sku)
- _fl_for_sku — DB + Inventory API integration (mocks session + ML client)
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tiny_mirror.services.stock_sync_service import (
    ML_FULL_DEPOSIT_NAME,
    ML_FULL_DEPOSIT_SENTINEL_ID,
    StockSyncService,
    _extract_cost_price,
    _overlay_ml_full_deposit,
    _sum_tiny_fl_available,
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

    _overlay_ml_full_deposit(deposits, available_qty=12)

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

    _overlay_ml_full_deposit(deposits, available_qty=7)

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

    _overlay_ml_full_deposit(deposits, available_qty=0)

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


# ---------------------------------------------------------------------------
# _sum_tiny_fl_available (pure helper)
# ---------------------------------------------------------------------------
def test_sum_tiny_fl_available_matches_by_name_case_insensitive() -> None:
    deposits = [
        {"deposit_name": "Galpão", "available": 100.0},
        {"deposit_name": "Full Mercado Livre", "available": 12.0},
    ]
    assert _sum_tiny_fl_available(deposits) == 12


def test_sum_tiny_fl_available_floors_negatives_at_zero() -> None:
    """Tiny occasionally returns negative balances on the FL row (desync)."""
    deposits = [
        {"deposit_name": "Full Mercado Livre", "available": -5.0},
    ]
    assert _sum_tiny_fl_available(deposits) == 0


def test_sum_tiny_fl_available_returns_zero_when_no_fl_row() -> None:
    deposits = [
        {"deposit_name": "Galpão", "available": 7.0},
    ]
    assert _sum_tiny_fl_available(deposits) == 0


def test_sum_tiny_fl_available_handles_missing_available_key() -> None:
    deposits = [
        {"deposit_name": "Full Mercado Livre"},
    ]
    assert _sum_tiny_fl_available(deposits) == 0


# ---------------------------------------------------------------------------
# _extract_cost_price (pure helper)
# ---------------------------------------------------------------------------
def test_extract_cost_price_reads_cost_price_from_prices_jsonb() -> None:
    product = {"prices": {"cost_price": "4.20", "price": "9.90"}}
    assert _extract_cost_price(product) == Decimal("4.20")


def test_extract_cost_price_falls_back_to_price_when_cost_missing() -> None:
    product = {"prices": {"price": "9.90"}}
    assert _extract_cost_price(product) == Decimal("9.90")


def test_extract_cost_price_returns_zero_when_product_is_none() -> None:
    assert _extract_cost_price(None) == Decimal("0")


def test_extract_cost_price_returns_zero_when_prices_missing() -> None:
    assert _extract_cost_price({}) == Decimal("0")


# ---------------------------------------------------------------------------
# _maybe_record_webhook_transfer — webhook delta path
# ---------------------------------------------------------------------------
def _snap(fl: int, galpao: int):
    """Build a TinyFLSnapshot mock-friendly value (frozen dataclass)."""
    from tiny_mirror.infrastructure.repositories.tiny_fl_stock_snapshot_repository import (
        TinyFLSnapshot,
    )

    return TinyFLSnapshot(tiny_fl_qty=fl, stock_galpao_qty=galpao)


@patch("tiny_mirror.services.stock_sync_service.FulfillmentTransferRepository")
@patch("tiny_mirror.services.stock_sync_service.TinyFLStockSnapshotRepository")
@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_webhook_transfer_seeds_snapshot_on_first_observation(
    mock_session_local: MagicMock,
    mock_snapshot_repo_cls: MagicMock,
    mock_transfer_repo_cls: MagicMock,
    stock_service: StockSyncService,
) -> None:
    """No previous snapshot → record it and skip transfer creation."""
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    snapshot_repo = MagicMock()
    snapshot_repo.get = AsyncMock(return_value=None)
    snapshot_repo.upsert = AsyncMock()
    mock_snapshot_repo_cls.return_value = snapshot_repo

    transfer_repo = MagicMock()
    transfer_repo.has_recent_pending = AsyncMock()
    transfer_repo.create = AsyncMock()
    mock_transfer_repo_cls.return_value = transfer_repo

    await stock_service._maybe_record_webhook_transfer(
        product_tiny_id=1234,
        sku="FAKE-SKU",
        new_tiny_fl_qty=20,
        new_stock_galpao_qty=100,
        product_data={"prices": {"cost_price": "5.00"}},
    )

    snapshot_repo.upsert.assert_awaited_once_with(1234, tiny_fl_qty=20, stock_galpao_qty=100)
    transfer_repo.create.assert_not_awaited()
    transfer_repo.has_recent_pending.assert_not_awaited()


@patch("tiny_mirror.services.stock_sync_service.MLListingRepository")
@patch("tiny_mirror.services.stock_sync_service.FulfillmentTransferRepository")
@patch("tiny_mirror.services.stock_sync_service.TinyFLStockSnapshotRepository")
@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_webhook_transfer_creates_pending_on_positive_delta_with_galpao_drop(
    mock_session_local: MagicMock,
    mock_snapshot_repo_cls: MagicMock,
    mock_transfer_repo_cls: MagicMock,
    mock_ml_listing_repo_cls: MagicMock,
    stock_service: StockSyncService,
) -> None:
    """FL +20 corroborated by galpão -20 → insert pending transfer."""
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    snapshot_repo = MagicMock()
    snapshot_repo.get = AsyncMock(return_value=_snap(fl=3, galpao=100))
    snapshot_repo.upsert = AsyncMock()
    mock_snapshot_repo_cls.return_value = snapshot_repo

    transfer_repo = MagicMock()
    transfer_repo.has_recent_pending = AsyncMock(return_value=False)
    transfer_repo.create = AsyncMock()
    mock_transfer_repo_cls.return_value = transfer_repo

    # SKU has a fulfillment listing on ML — guard passes, transfer is created.
    ml_listing_repo = MagicMock()
    ml_listing_repo.sku_logistic_status = AsyncMock(return_value=(1, 1))
    mock_ml_listing_repo_cls.return_value = ml_listing_repo

    await stock_service._maybe_record_webhook_transfer(
        product_tiny_id=42,
        sku="CAMP-FITADUPLA-12-2",
        new_tiny_fl_qty=23,
        new_stock_galpao_qty=80,
        product_data={"prices": {"cost_price": "12.32"}},
    )

    transfer_repo.create.assert_awaited_once()
    kwargs = transfer_repo.create.call_args.kwargs
    assert kwargs["product_tiny_id"] == 42
    assert kwargs["product_sku"] == "CAMP-FITADUPLA-12-2"
    assert kwargs["quantity"] == 20
    assert kwargs["cost_per_unit"] == Decimal("12.32")
    assert kwargs["source"] == "tiny_webhook"


# Bug 1 fix (2026-06-05): on a hot SKU, ML sales can fire on FL between the
# operator's T entry and the next stock webhook. FL then decrements via N
# entries while galpão stays put, so fl_delta under-counts the real
# transfer. Pin quantity = max(fl_delta, galpao_drop) so the galpão side
# (which only changes via T) sets the lower bound on the transfer size.
# Real case: BUB-ASPR-NAS-ESTJ 2026-06-01: T +100 fired in Tiny, 11 sales
# landed before the webhook arrived → fl_delta=89, galpao_drop=100.
@patch("tiny_mirror.services.stock_sync_service.MLListingRepository")
@patch("tiny_mirror.services.stock_sync_service.FulfillmentTransferRepository")
@patch("tiny_mirror.services.stock_sync_service.TinyFLStockSnapshotRepository")
@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_webhook_transfer_quantity_uses_galpao_drop_when_sales_in_window(
    mock_session_local: MagicMock,
    mock_snapshot_repo_cls: MagicMock,
    mock_transfer_repo_cls: MagicMock,
    mock_ml_listing_repo_cls: MagicMock,
    stock_service: StockSyncService,
) -> None:
    """fl_delta=89, galpao_drop=100 → quantity persisted = 100."""
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    snapshot_repo = MagicMock()
    snapshot_repo.get = AsyncMock(return_value=_snap(fl=0, galpao=119))
    snapshot_repo.upsert = AsyncMock()
    mock_snapshot_repo_cls.return_value = snapshot_repo

    transfer_repo = MagicMock()
    transfer_repo.has_recent_pending = AsyncMock(return_value=False)
    transfer_repo.create = AsyncMock()
    mock_transfer_repo_cls.return_value = transfer_repo

    ml_listing_repo = MagicMock()
    ml_listing_repo.sku_logistic_status = AsyncMock(return_value=(1, 1))
    mock_ml_listing_repo_cls.return_value = ml_listing_repo

    await stock_service._maybe_record_webhook_transfer(
        product_tiny_id=955038884,
        sku="BUB-ASPR-NAS-ESTJ",
        new_tiny_fl_qty=89,  # only +89 because of 11 sales in the window
        new_stock_galpao_qty=19,  # full -100 from the T entry
        product_data={"prices": {"cost_price": "13.37"}},
    )

    transfer_repo.create.assert_awaited_once()
    kwargs = transfer_repo.create.call_args.kwargs
    assert (
        kwargs["quantity"] == 100
    ), f"expected max(fl_delta=89, galpao_drop=100)=100, got {kwargs['quantity']}"
    assert "max(fl_delta=89, galpao_drop=100)" in kwargs["notes"]


@patch("tiny_mirror.services.stock_sync_service.FulfillmentTransferRepository")
@patch("tiny_mirror.services.stock_sync_service.TinyFLStockSnapshotRepository")
@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_webhook_transfer_skips_when_galpao_did_not_drop(
    mock_session_local: MagicMock,
    mock_snapshot_repo_cls: MagicMock,
    mock_transfer_repo_cls: MagicMock,
    stock_service: StockSyncService,
) -> None:
    """FL +6 but galpão untouched → likely sale cancellation, skip."""
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    snapshot_repo = MagicMock()
    snapshot_repo.get = AsyncMock(return_value=_snap(fl=450, galpao=200))
    snapshot_repo.upsert = AsyncMock()
    mock_snapshot_repo_cls.return_value = snapshot_repo

    transfer_repo = MagicMock()
    transfer_repo.has_recent_pending = AsyncMock()
    transfer_repo.create = AsyncMock()
    mock_transfer_repo_cls.return_value = transfer_repo

    await stock_service._maybe_record_webhook_transfer(
        product_tiny_id=967523943,
        sku="RTA-GAV6-P",
        new_tiny_fl_qty=456,
        new_stock_galpao_qty=200,  # unchanged
        product_data={"prices": {"cost_price": "11.26"}},
    )

    transfer_repo.create.assert_not_awaited()
    # Snapshot is still updated so the next webhook has fresh baselines.
    snapshot_repo.upsert.assert_awaited_once_with(967523943, tiny_fl_qty=456, stock_galpao_qty=200)


@patch("tiny_mirror.services.stock_sync_service.MLListingRepository")
@patch("tiny_mirror.services.stock_sync_service.FulfillmentTransferRepository")
@patch("tiny_mirror.services.stock_sync_service.TinyFLStockSnapshotRepository")
@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_webhook_transfer_skips_when_sku_not_fulfillment_on_ml(
    mock_session_local: MagicMock,
    mock_snapshot_repo_cls: MagicMock,
    mock_transfer_repo_cls: MagicMock,
    mock_ml_listing_repo_cls: MagicMock,
    stock_service: StockSyncService,
) -> None:
    """Galpão drop corroborates the FL +20, but the SKU has no fulfillment
    listing on ML (only xd_drop_off). The transfer would never reconcile
    via INBOUND_RECEPTION, so the webhook skips creation. Snapshot still
    updates so we don't get stuck on the same delta forever."""
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    snapshot_repo = MagicMock()
    snapshot_repo.get = AsyncMock(return_value=_snap(fl=3, galpao=100))
    snapshot_repo.upsert = AsyncMock()
    mock_snapshot_repo_cls.return_value = snapshot_repo

    transfer_repo = MagicMock()
    transfer_repo.has_recent_pending = AsyncMock(return_value=False)
    transfer_repo.create = AsyncMock()
    mock_transfer_repo_cls.return_value = transfer_repo

    # Listing exists but logistic_type != fulfillment.
    ml_listing_repo = MagicMock()
    ml_listing_repo.sku_logistic_status = AsyncMock(return_value=(0, 2))
    mock_ml_listing_repo_cls.return_value = ml_listing_repo

    await stock_service._maybe_record_webhook_transfer(
        product_tiny_id=42,
        sku="SKU-XD-DROP-OFF",
        new_tiny_fl_qty=23,
        new_stock_galpao_qty=80,
        product_data={"prices": {"cost_price": "12.32"}},
    )

    transfer_repo.create.assert_not_awaited()
    snapshot_repo.upsert.assert_awaited_once()


@patch("tiny_mirror.services.stock_sync_service.MLListingRepository")
@patch("tiny_mirror.services.stock_sync_service.FulfillmentTransferRepository")
@patch("tiny_mirror.services.stock_sync_service.TinyFLStockSnapshotRepository")
@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_webhook_transfer_skips_when_sku_absent_from_ml_listings(
    mock_session_local: MagicMock,
    mock_snapshot_repo_cls: MagicMock,
    mock_transfer_repo_cls: MagicMock,
    mock_ml_listing_repo_cls: MagicMock,
    stock_service: StockSyncService,
) -> None:
    """Bug 5 fix (2026-06-05): SKU with zero ml_listings rows is a
    component-only SKU (never sold standalone). Tiny explodes a kit's T
    entry into per-component deltas, so the same physical shipment fires
    a webhook for the kit AND each component. The kit's webhook is the
    canonical record; recording the component's webhook double-counts.

    Before 2026-06-05 this branch RECORDED the transfer ("we don't know,
    let operator decide later"); audit found 66 phantom units across the
    SLF-KITDISPLPDEN-PR components alone. The component webhook is now
    skipped; the kit's webhook remains the source of truth.
    """
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    snapshot_repo = MagicMock()
    snapshot_repo.get = AsyncMock(return_value=_snap(fl=3, galpao=100))
    snapshot_repo.upsert = AsyncMock()
    mock_snapshot_repo_cls.return_value = snapshot_repo

    transfer_repo = MagicMock()
    transfer_repo.has_recent_pending = AsyncMock(return_value=False)
    transfer_repo.create = AsyncMock()
    mock_transfer_repo_cls.return_value = transfer_repo

    ml_listing_repo = MagicMock()
    ml_listing_repo.sku_logistic_status = AsyncMock(return_value=(0, 0))  # absent
    mock_ml_listing_repo_cls.return_value = ml_listing_repo

    await stock_service._maybe_record_webhook_transfer(
        product_tiny_id=42,
        sku="SLF-PESCDENT-PR",
        new_tiny_fl_qty=23,
        new_stock_galpao_qty=80,
        product_data={"prices": {"cost_price": "12.32"}},
    )

    transfer_repo.create.assert_not_awaited()
    # Snapshot still updates so we don't replay the same delta forever
    snapshot_repo.upsert.assert_awaited_once()


@patch("tiny_mirror.services.stock_sync_service.FulfillmentTransferRepository")
@patch("tiny_mirror.services.stock_sync_service.TinyFLStockSnapshotRepository")
@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_webhook_transfer_skips_on_zero_fl_delta(
    mock_session_local: MagicMock,
    mock_snapshot_repo_cls: MagicMock,
    mock_transfer_repo_cls: MagicMock,
    stock_service: StockSyncService,
) -> None:
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    snapshot_repo = MagicMock()
    snapshot_repo.get = AsyncMock(return_value=_snap(fl=10, galpao=50))
    snapshot_repo.upsert = AsyncMock()
    mock_snapshot_repo_cls.return_value = snapshot_repo

    transfer_repo = MagicMock()
    transfer_repo.has_recent_pending = AsyncMock()
    transfer_repo.create = AsyncMock()
    mock_transfer_repo_cls.return_value = transfer_repo

    await stock_service._maybe_record_webhook_transfer(
        product_tiny_id=1,
        sku="X",
        new_tiny_fl_qty=10,
        new_stock_galpao_qty=45,
        product_data={},
    )

    transfer_repo.create.assert_not_awaited()


@patch("tiny_mirror.services.stock_sync_service.FulfillmentTransferRepository")
@patch("tiny_mirror.services.stock_sync_service.TinyFLStockSnapshotRepository")
@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_webhook_transfer_skips_on_negative_fl_delta(
    mock_session_local: MagicMock,
    mock_snapshot_repo_cls: MagicMock,
    mock_transfer_repo_cls: MagicMock,
    stock_service: StockSyncService,
) -> None:
    """Negative FL delta = sale shipped from FL; do not create transfer."""
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    snapshot_repo = MagicMock()
    snapshot_repo.get = AsyncMock(return_value=_snap(fl=15, galpao=10))
    snapshot_repo.upsert = AsyncMock()
    mock_snapshot_repo_cls.return_value = snapshot_repo

    transfer_repo = MagicMock()
    transfer_repo.has_recent_pending = AsyncMock()
    transfer_repo.create = AsyncMock()
    mock_transfer_repo_cls.return_value = transfer_repo

    await stock_service._maybe_record_webhook_transfer(
        product_tiny_id=1,
        sku="X",
        new_tiny_fl_qty=12,
        new_stock_galpao_qty=10,
        product_data={},
    )

    transfer_repo.create.assert_not_awaited()
    snapshot_repo.upsert.assert_awaited_once_with(1, tiny_fl_qty=12, stock_galpao_qty=10)


@patch("tiny_mirror.services.stock_sync_service.FulfillmentTransferRepository")
@patch("tiny_mirror.services.stock_sync_service.TinyFLStockSnapshotRepository")
@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_webhook_transfer_idempotent_when_recent_pending_exists(
    mock_session_local: MagicMock,
    mock_snapshot_repo_cls: MagicMock,
    mock_transfer_repo_cls: MagicMock,
    stock_service: StockSyncService,
) -> None:
    """Positive corroborated delta but recent pending exists → skip duplicate."""
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    snapshot_repo = MagicMock()
    snapshot_repo.get = AsyncMock(return_value=_snap(fl=2, galpao=100))
    snapshot_repo.upsert = AsyncMock()
    mock_snapshot_repo_cls.return_value = snapshot_repo

    transfer_repo = MagicMock()
    transfer_repo.has_recent_pending = AsyncMock(return_value=True)
    transfer_repo.create = AsyncMock()
    mock_transfer_repo_cls.return_value = transfer_repo

    await stock_service._maybe_record_webhook_transfer(
        product_tiny_id=1,
        sku="X",
        new_tiny_fl_qty=22,
        new_stock_galpao_qty=80,  # dropped 20, matches FL +20
        product_data={},
    )

    transfer_repo.has_recent_pending.assert_awaited_once()
    transfer_repo.create.assert_not_awaited()
    snapshot_repo.upsert.assert_awaited_once_with(1, tiny_fl_qty=22, stock_galpao_qty=80)


# ---------------------------------------------------------------------------
# run_ml_fl_only_sync — high-frequency ML-only refresh
# ---------------------------------------------------------------------------
@patch("tiny_mirror.services.stock_sync_service.SyncLogRepository")
@patch("tiny_mirror.services.stock_sync_service.PostgreSQLStockRepository")
@patch("tiny_mirror.services.stock_sync_service.PostgreSQLProductRepository")
@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_run_ml_fl_only_sync_writes_per_product(
    mock_session_local: MagicMock,
    mock_product_repo_cls: MagicMock,
    mock_stock_repo_cls: MagicMock,
    mock_sync_log_cls: MagicMock,
    stock_service: StockSyncService,
) -> None:
    """Happy path: 2 FL-exposed products, each yields a ML qty,
    upsert_ml_full_deposit fires per product with the right qty."""
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    product_repo = MagicMock()
    product_repo.list_fl_exposed_active = AsyncMock(
        return_value=[(100, "SKU-A", "S"), (200, "SKU-B", "S")]
    )
    product_repo.get_parent_kits_for_sku = AsyncMock(return_value=[])
    mock_product_repo_cls.return_value = product_repo

    stock_repo = MagicMock()
    stock_repo.upsert_ml_full_deposit = AsyncMock()
    mock_stock_repo_cls.return_value = stock_repo

    sync_logs = MagicMock()
    sync_logs.update_sync_log_complete = AsyncMock()
    mock_sync_log_cls.return_value = sync_logs

    stock_service._fetch_ml_full_breakdown = AsyncMock(side_effect=[(12, 0), (7, 0)])  # type: ignore[method-assign]

    await stock_service.run_ml_fl_only_sync(sync_log_id=42)

    assert stock_repo.upsert_ml_full_deposit.await_count == 2
    calls = stock_repo.upsert_ml_full_deposit.await_args_list
    assert calls[0].args[0] == 100 and calls[0].args[1] == 12
    assert calls[1].args[0] == 200 and calls[1].args[1] == 7
    sync_logs.update_sync_log_complete.assert_awaited_once_with(
        42, items_processed=2, items_failed=0
    )


@patch("tiny_mirror.services.stock_sync_service.SyncLogRepository")
@patch("tiny_mirror.services.stock_sync_service.PostgreSQLStockRepository")
@patch("tiny_mirror.services.stock_sync_service.PostgreSQLProductRepository")
@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_run_ml_fl_only_sync_skips_when_fetch_returns_none(
    mock_session_local: MagicMock,
    mock_product_repo_cls: MagicMock,
    mock_stock_repo_cls: MagicMock,
    mock_sync_log_cls: MagicMock,
    stock_service: StockSyncService,
) -> None:
    """ML transient failure (None) → don't zero the existing row, just skip."""
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    product_repo = MagicMock()
    product_repo.list_fl_exposed_active = AsyncMock(return_value=[(1, "SKU-X", "S")])
    product_repo.get_parent_kits_for_sku = AsyncMock(return_value=[])
    mock_product_repo_cls.return_value = product_repo

    stock_repo = MagicMock()
    stock_repo.upsert_ml_full_deposit = AsyncMock()
    mock_stock_repo_cls.return_value = stock_repo

    sync_logs = MagicMock()
    sync_logs.update_sync_log_complete = AsyncMock()
    mock_sync_log_cls.return_value = sync_logs

    stock_service._fetch_ml_full_breakdown = AsyncMock(return_value=None)  # type: ignore[method-assign]

    await stock_service.run_ml_fl_only_sync(sync_log_id=99)

    stock_repo.upsert_ml_full_deposit.assert_not_awaited()
    sync_logs.update_sync_log_complete.assert_awaited_once_with(
        99, items_processed=0, items_failed=0
    )


@patch("tiny_mirror.services.stock_sync_service.SyncLogRepository")
@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_run_ml_fl_only_sync_noop_when_ml_disabled(
    mock_session_local: MagicMock,
    mock_sync_log_cls: MagicMock,
) -> None:
    """No ML client wired → finalize the sync_log immediately, do no work."""
    service = StockSyncService(
        tiny_client=AsyncMock(),
        queue_publisher=AsyncMock(),
        ml_client=None,
    )
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    sync_logs = MagicMock()
    sync_logs.update_sync_log_complete = AsyncMock()
    mock_sync_log_cls.return_value = sync_logs

    await service.run_ml_fl_only_sync(sync_log_id=7)

    sync_logs.update_sync_log_complete.assert_awaited_once_with(
        7, items_processed=0, items_failed=0
    )


@patch("tiny_mirror.services.stock_sync_service.SyncLogRepository")
@patch("tiny_mirror.services.stock_sync_service.PostgreSQLStockRepository")
@patch("tiny_mirror.services.stock_sync_service.PostgreSQLProductRepository")
@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_run_ml_fl_only_sync_counts_failures(
    mock_session_local: MagicMock,
    mock_product_repo_cls: MagicMock,
    mock_stock_repo_cls: MagicMock,
    mock_sync_log_cls: MagicMock,
    stock_service: StockSyncService,
) -> None:
    """Per-product exception is swallowed: failed count advances, loop continues."""
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    product_repo = MagicMock()
    product_repo.list_fl_exposed_active = AsyncMock(
        return_value=[(1, "BAD", "S"), (2, "GOOD", "S")]
    )
    product_repo.get_parent_kits_for_sku = AsyncMock(return_value=[])
    mock_product_repo_cls.return_value = product_repo

    stock_repo = MagicMock()
    stock_repo.upsert_ml_full_deposit = AsyncMock()
    mock_stock_repo_cls.return_value = stock_repo

    sync_logs = MagicMock()
    sync_logs.update_sync_log_complete = AsyncMock()
    mock_sync_log_cls.return_value = sync_logs

    # First product raises, second succeeds with (5, 0) breakdown.
    stock_service._fetch_ml_full_breakdown = AsyncMock(  # type: ignore[method-assign]
        side_effect=[RuntimeError("ML down"), (5, 0)]
    )

    await stock_service.run_ml_fl_only_sync(sync_log_id=1)

    assert stock_repo.upsert_ml_full_deposit.await_count == 1
    sync_logs.update_sync_log_complete.assert_awaited_once_with(
        1, items_processed=1, items_failed=1
    )


@patch("tiny_mirror.services.stock_sync_service.FulfillmentTransferRepository")
@patch("tiny_mirror.services.stock_sync_service.TinyFLStockSnapshotRepository")
@patch("tiny_mirror.services.stock_sync_service.AsyncSessionLocal")
async def test_webhook_transfer_commits_snapshot_on_early_return_paths(
    mock_session_local: MagicMock,
    mock_snapshot_repo_cls: MagicMock,
    mock_transfer_repo_cls: MagicMock,
    stock_service: StockSyncService,
) -> None:
    """The snapshot repo no longer commits internally; every early-return
    path of the webhook delta handler must commit the snapshot advance
    itself (seed and negative-delta paths here)."""
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)

    snapshot_repo = MagicMock()
    snapshot_repo.get = AsyncMock(return_value=None)
    snapshot_repo.upsert = AsyncMock()
    mock_snapshot_repo_cls.return_value = snapshot_repo
    mock_transfer_repo_cls.return_value = MagicMock()

    # Seed path (no previous snapshot).
    await stock_service._maybe_record_webhook_transfer(
        product_tiny_id=1,
        sku="SKU-A",
        new_tiny_fl_qty=5,
        new_stock_galpao_qty=50,
        product_data=None,
    )
    mock_session.commit.assert_awaited()

    # Negative-delta path (previous snapshot above the new value).
    mock_session.commit.reset_mock()
    snapshot_repo.get = AsyncMock(return_value=_snap(fl=10, galpao=50))
    await stock_service._maybe_record_webhook_transfer(
        product_tiny_id=1,
        sku="SKU-A",
        new_tiny_fl_qty=5,
        new_stock_galpao_qty=50,
        product_data=None,
    )
    mock_session.commit.assert_awaited()
