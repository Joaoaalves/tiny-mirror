"""Unit tests for :class:`tiny_mirror.services.product_sync_service.ProductSyncService`.

Covers the sync-log counter isolation fix: even when the product upsert
session rolls back (aborted transaction), increment_failed must still be
committed via the separate log session.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tiny_mirror.exceptions import TinyAPIException, TinyNotFoundException
from tiny_mirror.services.product_sync_service import ProductSyncService

pytestmark = pytest.mark.unit

_RAW_PRODUCT = {
    "id": 42,
    "sku": "TEST-SKU",
    "descricao": "Test Product",
    "tipo": "P",
    "situacao": "A",
}


def _session_cm(session: AsyncMock) -> MagicMock:
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=session)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


@pytest.fixture
def tiny_client() -> AsyncMock:
    client = AsyncMock()
    client.get_product = AsyncMock(return_value=_RAW_PRODUCT)
    return client


@pytest.fixture
def service(tiny_client: AsyncMock) -> ProductSyncService:
    return ProductSyncService(tiny_client=tiny_client, queue_publisher=AsyncMock())


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------
async def test_increments_processed_on_success(service: ProductSyncService) -> None:
    product_session = AsyncMock()
    log_session = AsyncMock()
    mock_product_repo = AsyncMock()
    mock_product_repo.upsert = AsyncMock(return_value="created")
    mock_sync_log_repo = AsyncMock()

    with (
        patch(
            "tiny_mirror.services.product_sync_service.AsyncSessionLocal",
            side_effect=[_session_cm(product_session), _session_cm(log_session)],
        ),
        patch(
            "tiny_mirror.services.product_sync_service.PostgreSQLProductRepository",
            return_value=mock_product_repo,
        ),
        patch(
            "tiny_mirror.services.product_sync_service.SyncLogRepository",
            return_value=mock_sync_log_repo,
        ),
    ):
        await service.process_product_item(42, sync_log_id=7)

    mock_sync_log_repo.increment_processed.assert_awaited_once_with(7)
    mock_sync_log_repo.increment_failed.assert_not_awaited()
    mock_sync_log_repo.try_finalize.assert_awaited_once_with(7)


# ---------------------------------------------------------------------------
# Error paths — sync-log must be updated even when the product session fails
# ---------------------------------------------------------------------------
async def test_increments_failed_and_reraises_on_db_error(
    service: ProductSyncService,
) -> None:
    product_session = AsyncMock()
    log_session = AsyncMock()
    mock_product_repo = AsyncMock()
    mock_product_repo.upsert = AsyncMock(side_effect=RuntimeError("constraint violation"))
    mock_sync_log_repo = AsyncMock()

    with (
        patch(
            "tiny_mirror.services.product_sync_service.AsyncSessionLocal",
            side_effect=[_session_cm(product_session), _session_cm(log_session)],
        ),
        patch(
            "tiny_mirror.services.product_sync_service.PostgreSQLProductRepository",
            return_value=mock_product_repo,
        ),
        patch(
            "tiny_mirror.services.product_sync_service.SyncLogRepository",
            return_value=mock_sync_log_repo,
        ),
    ):
        with pytest.raises(RuntimeError, match="constraint violation"):
            await service.process_product_item(42, sync_log_id=7)

    mock_sync_log_repo.increment_failed.assert_awaited_once_with(7)
    mock_sync_log_repo.increment_processed.assert_not_awaited()
    mock_sync_log_repo.try_finalize.assert_awaited_once_with(7)


async def test_increments_failed_and_reraises_on_tiny_api_error(
    service: ProductSyncService,
) -> None:
    product_session = AsyncMock()
    log_session = AsyncMock()
    mock_product_repo = AsyncMock()
    mock_product_repo.upsert = AsyncMock(
        side_effect=TinyAPIException("API error", status_code=500)
    )
    mock_sync_log_repo = AsyncMock()

    with (
        patch(
            "tiny_mirror.services.product_sync_service.AsyncSessionLocal",
            side_effect=[_session_cm(product_session), _session_cm(log_session)],
        ),
        patch(
            "tiny_mirror.services.product_sync_service.PostgreSQLProductRepository",
            return_value=mock_product_repo,
        ),
        patch(
            "tiny_mirror.services.product_sync_service.SyncLogRepository",
            return_value=mock_sync_log_repo,
        ),
    ):
        with pytest.raises(TinyAPIException):
            await service.process_product_item(42, sync_log_id=7)

    mock_sync_log_repo.increment_failed.assert_awaited_once_with(7)
    mock_sync_log_repo.try_finalize.assert_awaited_once_with(7)


# ---------------------------------------------------------------------------
# TinyNotFoundException — skip silently, no session opened
# ---------------------------------------------------------------------------
async def test_skips_silently_when_product_not_found(service: ProductSyncService) -> None:
    service._tiny.get_product = AsyncMock(
        side_effect=TinyNotFoundException(
            "not found", resource_type="produto", resource_id=42
        )
    )

    with patch(
        "tiny_mirror.services.product_sync_service.AsyncSessionLocal"
    ) as mock_sl:
        await service.process_product_item(42, sync_log_id=7)

    mock_sl.assert_not_called()
