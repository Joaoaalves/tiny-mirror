"""Unit tests for :class:`MLListingSyncService`.

All external collaborators are mocked: MercadoLivreAPIClient,
MLListingRepository, SyncLogRepository, and AsyncSessionLocal.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from tiny_mirror.services.ml_listing_sync_service import MLListingSyncService, _extract_seller_sku

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_ml_client() -> AsyncMock:
    client = AsyncMock()
    client.list_all_item_ids = AsyncMock(return_value=([], 0))
    client.batch_get_items = AsyncMock(return_value=[])
    return client


@pytest.fixture
def service(mock_ml_client: AsyncMock) -> MLListingSyncService:
    return MLListingSyncService(ml_client=mock_ml_client)


def _make_item(
    mlb_id: str,
    sku: str | None = "SKU-001",
    logistic_type: str = "fulfillment",
    inventory_id: str | None = "INV-001",
    status: str = "active",
    variations: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "id": mlb_id,
        "status": status,
        "shipping": {"logistic_type": logistic_type},
        "inventory_id": inventory_id if not variations else None,
        "attributes": [{"id": "SELLER_SKU", "value_name": sku}] if sku else [],
        "title": f"Product {mlb_id}",
    }
    if variations is not None:
        item["variations"] = variations
    return item


@pytest.fixture
def mock_session_factory():
    """Returns a context-manager mock wrapping a session with repo methods."""
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__ = AsyncMock(return_value=session)
    session_cm.__aexit__ = AsyncMock(return_value=False)
    return session, session_cm


# ---------------------------------------------------------------------------
# _extract_seller_sku helper
# ---------------------------------------------------------------------------


def test_extract_seller_sku_found() -> None:
    item = {"attributes": [{"id": "SELLER_SKU", "value_name": "ABC-123"}]}
    assert _extract_seller_sku(item) == "ABC-123"


def test_extract_seller_sku_missing_attribute() -> None:
    item = {"attributes": [{"id": "BRAND", "value_name": "Acme"}]}
    assert _extract_seller_sku(item) is None


def test_extract_seller_sku_no_attributes_key() -> None:
    assert _extract_seller_sku({}) is None


def test_extract_seller_sku_empty_value_returns_none() -> None:
    item = {"attributes": [{"id": "SELLER_SKU", "value_name": ""}]}
    assert _extract_seller_sku(item) is None


def test_extract_seller_sku_null_value_name() -> None:
    item = {"attributes": [{"id": "SELLER_SKU", "value_name": None}]}
    assert _extract_seller_sku(item) is None


# ---------------------------------------------------------------------------
# Happy path: single page, single batch
# ---------------------------------------------------------------------------


async def test_run_sync_simple_item(
    service: MLListingSyncService,
    mock_ml_client: AsyncMock,
) -> None:
    """One active listing, simple item (no variations), FL logistic type."""
    mock_ml_client.list_all_item_ids = AsyncMock(return_value=(["MLB111"], 1))
    mock_ml_client.batch_get_items = AsyncMock(
        return_value=[_make_item("MLB111", sku="PROD-A", inventory_id="INV-A")]
    )

    mock_repo = AsyncMock()
    mock_sync_log_repo = AsyncMock()
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.ml_listing_sync_service.AsyncSessionLocal",
            return_value=mock_session,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.MLListingRepository",
            return_value=mock_repo,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.SyncLogRepository",
            return_value=mock_sync_log_repo,
        ),
    ):
        await service.run_sync(sync_log_id=42)

    mock_repo.replace_all.assert_awaited_once()
    listings_arg, variations_arg = mock_repo.replace_all.call_args.args

    assert len(listings_arg) == 1
    row = listings_arg[0]
    assert row["mlb_id"] == "MLB111"
    assert row["sku"] == "PROD-A"
    assert row["logistic_type"] == "fulfillment"
    assert row["inventory_id"] == "INV-A"
    assert row["has_variations"] is False
    assert variations_arg == []

    mock_sync_log_repo.update_sync_log_complete.assert_awaited_once_with(
        42, items_processed=1, items_failed=0
    )


# ---------------------------------------------------------------------------
# Variation item: variations stored in separate table
# ---------------------------------------------------------------------------


async def test_run_sync_variation_item(
    service: MLListingSyncService,
    mock_ml_client: AsyncMock,
) -> None:
    """Variation item: item-level inventory_id is null; variations have their own."""
    variations = [
        {"id": "111", "inventory_id": "VAR-INV-1"},
        {"id": "222", "inventory_id": "VAR-INV-2"},
    ]
    mock_ml_client.list_all_item_ids = AsyncMock(return_value=(["MLB222"], 1))
    mock_ml_client.batch_get_items = AsyncMock(
        return_value=[_make_item("MLB222", sku="PROD-B", inventory_id=None, variations=variations)]
    )

    mock_repo = AsyncMock()
    mock_sync_log_repo = AsyncMock()
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.ml_listing_sync_service.AsyncSessionLocal",
            return_value=mock_session,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.MLListingRepository",
            return_value=mock_repo,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.SyncLogRepository",
            return_value=mock_sync_log_repo,
        ),
    ):
        await service.run_sync(sync_log_id=7)

    listings_arg, variations_arg = mock_repo.replace_all.call_args.args

    assert len(listings_arg) == 1
    row = listings_arg[0]
    assert row["mlb_id"] == "MLB222"
    assert row["has_variations"] is True
    assert row["inventory_id"] is None  # null at item level for variation items

    assert len(variations_arg) == 2
    assert {"mlb_id": "MLB222", "variation_id": 111, "inventory_id": "VAR-INV-1"} in variations_arg
    assert {"mlb_id": "MLB222", "variation_id": 222, "inventory_id": "VAR-INV-2"} in variations_arg


# ---------------------------------------------------------------------------
# Non-fulfillment listing: stored but logistic_type preserved
# ---------------------------------------------------------------------------


async def test_run_sync_non_fl_listing_stored(
    service: MLListingSyncService,
    mock_ml_client: AsyncMock,
) -> None:
    mock_ml_client.list_all_item_ids = AsyncMock(return_value=(["MLB333"], 1))
    mock_ml_client.batch_get_items = AsyncMock(
        return_value=[_make_item("MLB333", sku="PROD-C", logistic_type="me2", inventory_id=None)]
    )

    mock_repo = AsyncMock()
    mock_sync_log_repo = AsyncMock()
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.ml_listing_sync_service.AsyncSessionLocal",
            return_value=mock_session,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.MLListingRepository",
            return_value=mock_repo,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.SyncLogRepository",
            return_value=mock_sync_log_repo,
        ),
    ):
        await service.run_sync(sync_log_id=1)

    listings_arg, _ = mock_repo.replace_all.call_args.args
    assert listings_arg[0]["logistic_type"] == "me2"


# ---------------------------------------------------------------------------
# Pagination: multiple pages of active item IDs
# ---------------------------------------------------------------------------


async def test_run_sync_pagination(
    service: MLListingSyncService,
    mock_ml_client: AsyncMock,
) -> None:
    """list_all_item_ids called repeatedly until offset >= total.

    _PAGE_SIZE is patched to 2 so we can test multi-page behaviour with few items.
    """
    mock_ml_client.list_all_item_ids = AsyncMock(
        side_effect=[
            (["MLB001", "MLB002"], 3),
            (["MLB003"], 3),
            ([], 3),
        ]
    )
    mock_ml_client.batch_get_items = AsyncMock(return_value=[])

    mock_repo = AsyncMock()
    mock_sync_log_repo = AsyncMock()
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch("tiny_mirror.services.ml_listing_sync_service._PAGE_SIZE", 2),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.AsyncSessionLocal",
            return_value=mock_session,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.MLListingRepository",
            return_value=mock_repo,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.SyncLogRepository",
            return_value=mock_sync_log_repo,
        ),
    ):
        await service.run_sync(sync_log_id=1)

    # batch_get_items called once with all 3 IDs (< _BATCH_SIZE=20)
    assert mock_ml_client.batch_get_items.await_count == 1
    call_ids = mock_ml_client.batch_get_items.call_args.args[0]
    assert sorted(call_ids) == ["MLB001", "MLB002", "MLB003"]


# ---------------------------------------------------------------------------
# Batch failure: failed batch increments items_failed, sync still completes
# ---------------------------------------------------------------------------


async def test_run_sync_batch_failure_increments_failed_count(
    service: MLListingSyncService,
    mock_ml_client: AsyncMock,
) -> None:
    mock_ml_client.list_all_item_ids = AsyncMock(return_value=(["MLB111", "MLB222"], 2))
    # First batch raises; service logs and continues
    mock_ml_client.batch_get_items = AsyncMock(side_effect=Exception("network error"))

    mock_repo = AsyncMock()
    mock_sync_log_repo = AsyncMock()
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.ml_listing_sync_service.AsyncSessionLocal",
            return_value=mock_session,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.MLListingRepository",
            return_value=mock_repo,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.SyncLogRepository",
            return_value=mock_sync_log_repo,
        ),
    ):
        await service.run_sync(sync_log_id=99)

    mock_sync_log_repo.update_sync_log_complete.assert_awaited_once_with(
        99, items_processed=0, items_failed=2
    )
    # replace_all still called (with empty data)
    mock_repo.replace_all.assert_awaited_once_with([], [])


# ---------------------------------------------------------------------------
# Empty catalog: no active listings
# ---------------------------------------------------------------------------


async def test_run_sync_empty_catalog(
    service: MLListingSyncService,
    mock_ml_client: AsyncMock,
) -> None:
    mock_ml_client.list_all_item_ids = AsyncMock(return_value=([], 0))

    mock_repo = AsyncMock()
    mock_sync_log_repo = AsyncMock()
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.ml_listing_sync_service.AsyncSessionLocal",
            return_value=mock_session,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.MLListingRepository",
            return_value=mock_repo,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.SyncLogRepository",
            return_value=mock_sync_log_repo,
        ),
    ):
        await service.run_sync(sync_log_id=5)

    mock_ml_client.batch_get_items.assert_not_awaited()
    mock_repo.replace_all.assert_awaited_once_with([], [])
    mock_sync_log_repo.update_sync_log_complete.assert_awaited_once_with(
        5, items_processed=0, items_failed=0
    )


# ---------------------------------------------------------------------------
# Item without SELLER_SKU attribute: sku stored as None
# ---------------------------------------------------------------------------


async def test_run_sync_item_without_sku_stored_as_none(
    service: MLListingSyncService,
    mock_ml_client: AsyncMock,
) -> None:
    mock_ml_client.list_all_item_ids = AsyncMock(return_value=(["MLB444"], 1))
    mock_ml_client.batch_get_items = AsyncMock(
        return_value=[_make_item("MLB444", sku=None, inventory_id="INV-X")]
    )

    mock_repo = AsyncMock()
    mock_sync_log_repo = AsyncMock()
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.ml_listing_sync_service.AsyncSessionLocal",
            return_value=mock_session,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.MLListingRepository",
            return_value=mock_repo,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.SyncLogRepository",
            return_value=mock_sync_log_repo,
        ),
    ):
        await service.run_sync(sync_log_id=3)

    listings_arg, _ = mock_repo.replace_all.call_args.args
    assert listings_arg[0]["sku"] is None


# ---------------------------------------------------------------------------
# Batching: more than 20 items → multiple batch_get_items calls
# ---------------------------------------------------------------------------


async def test_run_sync_large_catalog_batches_correctly(
    service: MLListingSyncService,
    mock_ml_client: AsyncMock,
) -> None:
    """25 items should result in 2 batch calls (20 + 5)."""
    mlb_ids = [f"MLB{i:03d}" for i in range(25)]
    mock_ml_client.list_all_item_ids = AsyncMock(
        side_effect=[
            (mlb_ids, 25),
            ([], 25),
        ]
    )
    mock_ml_client.batch_get_items = AsyncMock(return_value=[])

    mock_repo = AsyncMock()
    mock_sync_log_repo = AsyncMock()
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.ml_listing_sync_service.AsyncSessionLocal",
            return_value=mock_session,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.MLListingRepository",
            return_value=mock_repo,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.SyncLogRepository",
            return_value=mock_sync_log_repo,
        ),
    ):
        await service.run_sync(sync_log_id=1)

    assert mock_ml_client.batch_get_items.await_count == 2
    first_call = mock_ml_client.batch_get_items.call_args_list[0].args[0]
    second_call = mock_ml_client.batch_get_items.call_args_list[1].args[0]
    assert len(first_call) == 20
    assert len(second_call) == 5


# ---------------------------------------------------------------------------
# Item with missing id field is skipped
# ---------------------------------------------------------------------------


async def test_run_sync_item_without_id_is_skipped(
    service: MLListingSyncService,
    mock_ml_client: AsyncMock,
) -> None:
    mock_ml_client.list_all_item_ids = AsyncMock(return_value=(["MLB555"], 1))
    bad_item: dict[str, Any] = {"id": "", "status": "active", "shipping": {}}
    mock_ml_client.batch_get_items = AsyncMock(return_value=[bad_item])

    mock_repo = AsyncMock()
    mock_sync_log_repo = AsyncMock()
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.ml_listing_sync_service.AsyncSessionLocal",
            return_value=mock_session,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.MLListingRepository",
            return_value=mock_repo,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.SyncLogRepository",
            return_value=mock_sync_log_repo,
        ),
    ):
        await service.run_sync(sync_log_id=1)

    listings_arg, _ = mock_repo.replace_all.call_args.args
    assert listings_arg == []
    mock_sync_log_repo.update_sync_log_complete.assert_awaited_once_with(
        1, items_processed=0, items_failed=0
    )


# ---------------------------------------------------------------------------
# Title truncation to 500 chars
# ---------------------------------------------------------------------------


async def test_run_sync_title_truncated_to_500_chars(
    service: MLListingSyncService,
    mock_ml_client: AsyncMock,
) -> None:
    long_title = "X" * 600
    item = _make_item("MLB666", sku="LONG-TITLE")
    item["title"] = long_title

    mock_ml_client.list_all_item_ids = AsyncMock(return_value=(["MLB666"], 1))
    mock_ml_client.batch_get_items = AsyncMock(return_value=[item])

    mock_repo = AsyncMock()
    mock_sync_log_repo = AsyncMock()
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.ml_listing_sync_service.AsyncSessionLocal",
            return_value=mock_session,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.MLListingRepository",
            return_value=mock_repo,
        ),
        patch(
            "tiny_mirror.services.ml_listing_sync_service.SyncLogRepository",
            return_value=mock_sync_log_repo,
        ),
    ):
        await service.run_sync(sync_log_id=1)

    listings_arg, _ = mock_repo.replace_all.call_args.args
    assert len(listings_arg[0]["title"]) == 500
