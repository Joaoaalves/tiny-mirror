"""Unit tests for :class:`MercadoLivreStockService`."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tiny_mirror.exceptions import TinyAPIException
from tiny_mirror.services.mercadolivre_stock_service import MercadoLivreStockService

pytestmark = pytest.mark.unit


@pytest.fixture
def fake_ml_client() -> AsyncMock:
    client = AsyncMock()
    client.list_items_by_sku = AsyncMock(return_value=[])
    client.get_item = AsyncMock(return_value={})
    return client


@pytest.fixture
def service(fake_ml_client: AsyncMock) -> MercadoLivreStockService:
    return MercadoLivreStockService(ml_client=fake_ml_client)


def _item(
    mlb_id: str,
    available_quantity: int = 10,
    logistic_type: str = "fulfillment",
    status: str = "active",
) -> dict:
    return {
        "id": mlb_id,
        "available_quantity": available_quantity,
        "status": status,
        "shipping": {"logistic_type": logistic_type},
    }


# ---------------------------------------------------------------------------
# _sync_sku
# ---------------------------------------------------------------------------
async def test_sync_sku_no_mlbs_calls_replace_with_empty_list(
    service: MercadoLivreStockService,
    fake_ml_client: AsyncMock,
) -> None:
    fake_ml_client.list_items_by_sku = AsyncMock(return_value=[])

    fake_repo = AsyncMock()
    fake_session = MagicMock()
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.MercadoLivreStockRepository",
            return_value=fake_repo,
        ),
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.AsyncSessionLocal",
            return_value=fake_session,
        ),
    ):
        await service._sync_sku("SKU-NONE")

    fake_repo.replace_for_sku.assert_awaited_once_with("SKU-NONE", [])


async def test_sync_sku_fulfillment_listing_is_persisted(
    service: MercadoLivreStockService,
    fake_ml_client: AsyncMock,
) -> None:
    fake_ml_client.list_items_by_sku = AsyncMock(return_value=["MLB1"])
    fake_ml_client.get_item = AsyncMock(
        return_value=_item("MLB1", available_quantity=5, logistic_type="fulfillment")
    )

    fake_repo = AsyncMock()
    fake_session = MagicMock()
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.MercadoLivreStockRepository",
            return_value=fake_repo,
        ),
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.AsyncSessionLocal",
            return_value=fake_session,
        ),
    ):
        await service._sync_sku("SKU-001")

    assert fake_repo.replace_for_sku.call_count == 1
    _sku, listings = fake_repo.replace_for_sku.call_args.args
    assert _sku == "SKU-001"
    assert len(listings) == 1
    assert listings[0]["mlb_id"] == "MLB1"
    assert listings[0]["available_quantity"] == 5
    assert listings[0]["logistic_type"] == "fulfillment"


async def test_sync_sku_non_fulfillment_listing_is_also_persisted(
    service: MercadoLivreStockService,
    fake_ml_client: AsyncMock,
) -> None:
    """Non-fulfillment listings are persisted; the coverage query filters them."""
    fake_ml_client.list_items_by_sku = AsyncMock(return_value=["MLB_ME2"])
    fake_ml_client.get_item = AsyncMock(
        return_value=_item("MLB_ME2", available_quantity=3, logistic_type="me2")
    )

    fake_repo = AsyncMock()
    fake_session = MagicMock()
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.MercadoLivreStockRepository",
            return_value=fake_repo,
        ),
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.AsyncSessionLocal",
            return_value=fake_session,
        ),
    ):
        await service._sync_sku("SKU-ME2")

    _sku, listings = fake_repo.replace_for_sku.call_args.args
    assert listings[0]["logistic_type"] == "me2"


async def test_sync_sku_multiple_mlbs_all_persisted(
    service: MercadoLivreStockService,
    fake_ml_client: AsyncMock,
) -> None:
    fake_ml_client.list_items_by_sku = AsyncMock(return_value=["MLB1", "MLB2"])
    fake_ml_client.get_item = AsyncMock(
        side_effect=[
            _item("MLB1", available_quantity=8, logistic_type="fulfillment"),
            _item("MLB2", available_quantity=2, logistic_type="fulfillment"),
        ]
    )

    fake_repo = AsyncMock()
    fake_session = MagicMock()
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.MercadoLivreStockRepository",
            return_value=fake_repo,
        ),
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.AsyncSessionLocal",
            return_value=fake_session,
        ),
    ):
        await service._sync_sku("SKU-MULTI")

    _sku, listings = fake_repo.replace_for_sku.call_args.args
    assert len(listings) == 2
    assert {ln["mlb_id"] for ln in listings} == {"MLB1", "MLB2"}


async def test_sync_sku_paused_zero_qty_is_persisted_as_zero(
    service: MercadoLivreStockService,
    fake_ml_client: AsyncMock,
) -> None:
    fake_ml_client.list_items_by_sku = AsyncMock(return_value=["MLB_OOS"])
    fake_ml_client.get_item = AsyncMock(
        return_value=_item("MLB_OOS", available_quantity=0, status="paused")
    )

    fake_repo = AsyncMock()
    fake_session = MagicMock()
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.MercadoLivreStockRepository",
            return_value=fake_repo,
        ),
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.AsyncSessionLocal",
            return_value=fake_session,
        ),
    ):
        await service._sync_sku("SKU-OOS")

    _sku, listings = fake_repo.replace_for_sku.call_args.args
    assert listings[0]["available_quantity"] == 0
    assert listings[0]["status"] == "paused"


async def test_sync_sku_get_item_api_error_skips_that_mlb(
    service: MercadoLivreStockService,
    fake_ml_client: AsyncMock,
) -> None:
    """If get_item raises for one MLB, skip it but persist the others."""
    fake_ml_client.list_items_by_sku = AsyncMock(return_value=["MLB_BAD", "MLB_OK"])
    fake_ml_client.get_item = AsyncMock(
        side_effect=[
            TinyAPIException("API error", status_code=500),
            _item("MLB_OK", available_quantity=7, logistic_type="fulfillment"),
        ]
    )

    fake_repo = AsyncMock()
    fake_session = MagicMock()
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.MercadoLivreStockRepository",
            return_value=fake_repo,
        ),
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.AsyncSessionLocal",
            return_value=fake_session,
        ),
    ):
        await service._sync_sku("SKU-PARTIAL")

    _sku, listings = fake_repo.replace_for_sku.call_args.args
    assert len(listings) == 1
    assert listings[0]["mlb_id"] == "MLB_OK"


# ---------------------------------------------------------------------------
# run_full_sync
# ---------------------------------------------------------------------------
async def test_run_full_sync_calls_sync_for_each_sku(
    service: MercadoLivreStockService,
    fake_ml_client: AsyncMock,
) -> None:
    fake_ml_client.list_items_by_sku = AsyncMock(return_value=[])

    fake_product_repo = AsyncMock()
    fake_product_repo.list_active_skus = AsyncMock(return_value=["SKU-A", "SKU-B", "SKU-C"])

    fake_sync_repo = AsyncMock()

    fake_session = MagicMock()
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.PostgreSQLProductRepository",
            return_value=fake_product_repo,
        ),
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.MercadoLivreStockRepository",
            return_value=AsyncMock(),
        ),
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.SyncLogRepository",
            return_value=fake_sync_repo,
        ),
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.AsyncSessionLocal",
            return_value=fake_session,
        ),
    ):
        await service.run_full_sync(sync_log_id=42)

    assert fake_ml_client.list_items_by_sku.await_count == 3
    fake_sync_repo.update_sync_log_complete.assert_awaited_once_with(
        42, items_processed=3, items_failed=0
    )


async def test_run_full_sync_partial_failure_marks_failed(
    service: MercadoLivreStockService,
    fake_ml_client: AsyncMock,
) -> None:
    fake_ml_client.list_items_by_sku = AsyncMock(
        side_effect=[
            [],  # SKU-A: no MLBs, succeeds
            Exception("unexpected API error"),  # SKU-B: fails
        ]
    )

    fake_product_repo = AsyncMock()
    fake_product_repo.list_active_skus = AsyncMock(return_value=["SKU-A", "SKU-B"])

    fake_sync_repo = AsyncMock()
    fake_session = MagicMock()
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.PostgreSQLProductRepository",
            return_value=fake_product_repo,
        ),
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.MercadoLivreStockRepository",
            return_value=AsyncMock(),
        ),
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.SyncLogRepository",
            return_value=fake_sync_repo,
        ),
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.AsyncSessionLocal",
            return_value=fake_session,
        ),
    ):
        await service.run_full_sync(sync_log_id=99)

    fake_sync_repo.update_sync_log_failed.assert_awaited_once()
    call_kwargs = fake_sync_repo.update_sync_log_failed.call_args
    assert call_kwargs.kwargs["items_processed"] == 1
    assert call_kwargs.kwargs["items_failed"] == 1


async def test_run_full_sync_empty_catalog_marks_complete(
    service: MercadoLivreStockService,
    fake_ml_client: AsyncMock,
) -> None:
    fake_product_repo = AsyncMock()
    fake_product_repo.list_active_skus = AsyncMock(return_value=[])

    fake_sync_repo = AsyncMock()
    fake_session = MagicMock()
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)

    with (
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.PostgreSQLProductRepository",
            return_value=fake_product_repo,
        ),
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.SyncLogRepository",
            return_value=fake_sync_repo,
        ),
        patch(
            "tiny_mirror.services.mercadolivre_stock_service.AsyncSessionLocal",
            return_value=fake_session,
        ),
    ):
        await service.run_full_sync(sync_log_id=7)

    fake_ml_client.list_items_by_sku.assert_not_awaited()
    fake_sync_repo.update_sync_log_complete.assert_awaited_once_with(
        7, items_processed=0, items_failed=0
    )
