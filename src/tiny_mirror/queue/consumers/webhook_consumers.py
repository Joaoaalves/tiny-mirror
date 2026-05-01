"""Webhook payload consumers. The detailed handlers land in stage 11."""

from __future__ import annotations

from typing import Any

import structlog
from aio_pika.abc import AbstractChannel

from tiny_mirror.queue.base_consumer import BaseConsumer
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.services.order_sync_service import OrderSyncService
from tiny_mirror.services.sale_bucket_service import SaleBucketService
from tiny_mirror.services.stock_sync_service import StockSyncService

logger = structlog.get_logger(__name__)


class OrderWebhookConsumer(BaseConsumer):
    QUEUE_NAME = "tiny.webhooks.orders"

    def __init__(
        self,
        channel: AbstractChannel,
        queue_publisher: QueuePublisher,
        order_sync_service: OrderSyncService,
        stock_sync_service: StockSyncService,
        sale_bucket_service: SaleBucketService,
    ) -> None:
        super().__init__(channel, queue_publisher)
        self._orders = order_sync_service
        self._stock = stock_sync_service
        self._buckets = sale_bucket_service

    async def handle(self, message_body: dict[str, Any]) -> None:
        # Detailed flow (sync the order, refresh stock for its products, then
        # recompute the affected sale buckets) is wired in stage 11.
        raise NotImplementedError("Order webhook handler implemented in stage 11")


class StockWebhookConsumer(BaseConsumer):
    QUEUE_NAME = "tiny.webhooks.stock"

    def __init__(
        self,
        channel: AbstractChannel,
        queue_publisher: QueuePublisher,
        stock_sync_service: StockSyncService,
    ) -> None:
        super().__init__(channel, queue_publisher)
        self._stock = stock_sync_service

    async def handle(self, message_body: dict[str, Any]) -> None:
        raise NotImplementedError("Stock webhook handler implemented in stage 11")
