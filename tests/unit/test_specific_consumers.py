"""Unit tests for the per-queue consumers.

Each consumer is a thin adapter: pull args out of the message body and
delegate to the matching service method. We mock the service and verify
the right method was called with the right kwargs.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest

from tiny_mirror.queue.consumers.bucket_consumer import BucketRefreshConsumer
from tiny_mirror.queue.consumers.order_consumers import (
    OrderFullSyncConsumer,
    OrderItemConsumer,
)
from tiny_mirror.queue.consumers.product_consumers import (
    ProductFullSyncConsumer,
    ProductItemConsumer,
)
from tiny_mirror.queue.consumers.stock_consumers import (
    StockFullSyncConsumer,
    StockItemConsumer,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------
async def test_product_full_sync_consumer_delegates_to_run_full_sync() -> None:
    service = AsyncMock()
    consumer = ProductFullSyncConsumer(
        channel=MagicMock(), queue_publisher=MagicMock(), product_sync_service=service
    )

    await consumer.handle({"sync_log_id": 7})

    service.run_full_sync.assert_awaited_once_with(7)


async def test_product_item_consumer_delegates_to_process_product_item() -> None:
    service = AsyncMock()
    consumer = ProductItemConsumer(
        channel=MagicMock(), queue_publisher=MagicMock(), product_sync_service=service
    )

    await consumer.handle({"product_tiny_id": 100, "sync_log_id": 1})

    service.process_product_item.assert_awaited_once_with(product_tiny_id=100, sync_log_id=1)


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------
async def test_order_full_sync_consumer_routes_incremental() -> None:
    service = AsyncMock()
    consumer = OrderFullSyncConsumer(
        channel=MagicMock(), queue_publisher=MagicMock(), order_sync_service=service
    )

    await consumer.handle({"is_historical": False, "sync_log_id": 5, "lookback_hours": 2})

    service.run_incremental_sync.assert_awaited_once_with(5)
    service.run_date_range_sync.assert_not_awaited()


async def test_order_full_sync_consumer_routes_historical() -> None:
    service = AsyncMock()
    consumer = OrderFullSyncConsumer(
        channel=MagicMock(), queue_publisher=MagicMock(), order_sync_service=service
    )

    await consumer.handle(
        {
            "is_historical": True,
            "date_from": "2025-01-01",
            "date_to": "2025-01-07",
            "sync_log_id": 9,
        }
    )

    service.run_date_range_sync.assert_awaited_once_with(date(2025, 1, 1), date(2025, 1, 7), 9)
    service.run_incremental_sync.assert_not_awaited()


async def test_order_item_consumer_delegates() -> None:
    service = AsyncMock()
    consumer = OrderItemConsumer(
        channel=MagicMock(), queue_publisher=MagicMock(), order_sync_service=service
    )

    await consumer.handle({"order_tiny_id": 999, "sync_log_id": 2})

    service.process_order_item.assert_awaited_once_with(order_tiny_id=999, sync_log_id=2)


# ---------------------------------------------------------------------------
# Stock
# ---------------------------------------------------------------------------
async def test_stock_full_sync_consumer_delegates() -> None:
    service = AsyncMock()
    consumer = StockFullSyncConsumer(
        channel=MagicMock(), queue_publisher=MagicMock(), stock_sync_service=service
    )

    await consumer.handle({"sync_log_id": 3})

    service.run_full_sync.assert_awaited_once_with(3)


async def test_stock_item_consumer_delegates() -> None:
    service = AsyncMock()
    consumer = StockItemConsumer(
        channel=MagicMock(), queue_publisher=MagicMock(), stock_sync_service=service
    )

    await consumer.handle({"product_tiny_id": 50, "sync_log_id": 4})

    service.process_stock_item.assert_awaited_once_with(product_tiny_id=50, sync_log_id=4)


# ---------------------------------------------------------------------------
# Buckets
# ---------------------------------------------------------------------------
async def test_bucket_refresh_consumer_parses_dates_and_delegates() -> None:
    service = AsyncMock()
    consumer = BucketRefreshConsumer(
        channel=MagicMock(), queue_publisher=MagicMock(), sale_bucket_service=service
    )

    await consumer.handle({"date_from": "2025-01-01", "date_to": "2025-01-05"})

    service.refresh_buckets.assert_awaited_once_with(date(2025, 1, 1), date(2025, 1, 5))


# ---------------------------------------------------------------------------
# Invoices
# ---------------------------------------------------------------------------
async def test_invoice_cold_start_without_sync_log_id_raises_for_dlq() -> None:
    """A malformed cold-start message must raise (→ BaseConsumer nacks to the
    DLQ) instead of being silently acked and lost."""
    from tiny_mirror.queue.consumers.invoice_consumers import InvoiceFullSyncConsumer

    service = MagicMock()
    service.run_cold_start = AsyncMock()
    consumer = InvoiceFullSyncConsumer(MagicMock(), MagicMock(), service)

    with pytest.raises(ValueError, match="sync_log_id"):
        await consumer.handle({"is_cold_start": True, "sync_log_id": None})

    service.run_cold_start.assert_not_awaited()


async def test_invoice_cold_start_with_sync_log_id_delegates() -> None:
    from tiny_mirror.queue.consumers.invoice_consumers import InvoiceFullSyncConsumer

    service = MagicMock()
    service.run_cold_start = AsyncMock()
    consumer = InvoiceFullSyncConsumer(MagicMock(), MagicMock(), service)

    await consumer.handle({"is_cold_start": True, "sync_log_id": 7})

    service.run_cold_start.assert_awaited_once_with(7)
