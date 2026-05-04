"""Unit tests for :class:`tiny_mirror.services.stock_sync_service.StockSyncService`.

Focus is on the ML overlay: the per-product stock sync pulls Full ML
available_quantity from the ML API and rewrites the (unreliable) Tiny
"Full Mercado Livre" deposit row with that quantity.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

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
            "ignore": True,  # Tiny marks this ignore=true
            "balance": -3.0,  # ...and the saldo is unreliable
            "reserved": 0.0,
            "available": -3.0,
            "company": None,
        },
    ]

    _overlay_ml_full_deposit(deposits, ml_qty=12)

    assert len(deposits) == 2  # no new row appended
    full_row = next(d for d in deposits if d["deposit_name"] == "Full Mercado Livre")
    assert full_row["balance"] == 12.0
    assert full_row["available"] == 12.0
    assert full_row["reserved"] == 0.0
    assert full_row["ignore"] is False  # flipped: now counts in coverage
    # The original Tiny deposit_tiny_id is preserved so the unique
    # constraint (product_tiny_id, deposit_tiny_id) keeps holding.
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
            "balance": 5.0,  # stale Tiny value
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
# _fetch_ml_full_qty
# ---------------------------------------------------------------------------
@pytest.fixture
def ml_client() -> AsyncMock:
    client = AsyncMock()
    client.list_items_by_sku = AsyncMock(return_value=[])
    client.get_item = AsyncMock(return_value={})
    return client


@pytest.fixture
def stock_service(ml_client: AsyncMock) -> StockSyncService:
    return StockSyncService(
        tiny_client=AsyncMock(),
        queue_publisher=AsyncMock(),
        ml_client=ml_client,
    )


async def test_fetch_ml_full_qty_no_listings_returns_none(
    stock_service: StockSyncService, ml_client: AsyncMock
) -> None:
    ml_client.list_items_by_sku = AsyncMock(return_value=[])

    result = await stock_service._fetch_ml_full_qty("SKU-NOT-LISTED")

    assert result is None


async def test_fetch_ml_full_qty_sums_fulfillment_only(
    stock_service: StockSyncService, ml_client: AsyncMock
) -> None:
    ml_client.list_items_by_sku = AsyncMock(return_value=["MLB1", "MLB2", "MLB3"])
    ml_client.get_item = AsyncMock(
        side_effect=[
            {
                "id": "MLB1",
                "available_quantity": 10,
                "shipping": {"logistic_type": "fulfillment"},
            },
            {
                "id": "MLB2",
                "available_quantity": 5,
                "shipping": {"logistic_type": "me2"},  # NOT fulfillment — skipped
            },
            {
                "id": "MLB3",
                "available_quantity": 3,
                "shipping": {"logistic_type": "fulfillment"},
            },
        ]
    )

    result = await stock_service._fetch_ml_full_qty("SKU-MIXED")

    assert result == 13  # 10 + 3, MLB2 (me2) excluded


async def test_fetch_ml_full_qty_listed_but_no_fulfillment_returns_none(
    stock_service: StockSyncService, ml_client: AsyncMock
) -> None:
    """If MLBs exist but none are fulfillment, return None — caller leaves
    the existing Tiny row alone (which is ignore=true anyway, so harmless)."""
    ml_client.list_items_by_sku = AsyncMock(return_value=["MLB_ME2"])
    ml_client.get_item = AsyncMock(
        return_value={
            "id": "MLB_ME2",
            "available_quantity": 4,
            "shipping": {"logistic_type": "me2"},
        }
    )

    result = await stock_service._fetch_ml_full_qty("SKU-NO-FULL")

    assert result is None


async def test_fetch_ml_full_qty_search_failure_returns_none(
    stock_service: StockSyncService, ml_client: AsyncMock
) -> None:
    ml_client.list_items_by_sku = AsyncMock(side_effect=RuntimeError("boom"))

    result = await stock_service._fetch_ml_full_qty("SKU-FAIL")

    assert result is None


async def test_fetch_ml_full_qty_one_item_fails_others_still_summed(
    stock_service: StockSyncService, ml_client: AsyncMock
) -> None:
    ml_client.list_items_by_sku = AsyncMock(return_value=["MLB_BAD", "MLB_OK"])
    ml_client.get_item = AsyncMock(
        side_effect=[
            RuntimeError("get_item failed"),
            {
                "id": "MLB_OK",
                "available_quantity": 9,
                "shipping": {"logistic_type": "fulfillment"},
            },
        ]
    )

    result = await stock_service._fetch_ml_full_qty("SKU-PARTIAL")

    assert result == 9


async def test_fetch_ml_full_qty_zero_when_fulfillment_with_zero_stock(
    stock_service: StockSyncService, ml_client: AsyncMock
) -> None:
    """Paused Full listing with available_quantity=0 → return 0
    (we know it's on Full and out of stock)."""
    ml_client.list_items_by_sku = AsyncMock(return_value=["MLB_OOS"])
    ml_client.get_item = AsyncMock(
        return_value={
            "id": "MLB_OOS",
            "available_quantity": 0,
            "status": "paused",
            "shipping": {"logistic_type": "fulfillment"},
        }
    )

    result = await stock_service._fetch_ml_full_qty("SKU-OOS")

    assert result == 0


# ---------------------------------------------------------------------------
# StockSyncService(ml_client=None) — overlay disabled path
# ---------------------------------------------------------------------------
async def test_fetch_ml_full_qty_returns_none_when_ml_disabled() -> None:
    service = StockSyncService(
        tiny_client=AsyncMock(),
        queue_publisher=AsyncMock(),
        ml_client=None,
    )

    result = await service._fetch_ml_full_qty("ANY-SKU")

    assert result is None
