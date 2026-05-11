"""Consumers for stock history and purchase order sync queues."""

from __future__ import annotations

from typing import Any

from aio_pika.abc import AbstractChannel

from tiny_mirror.queue.base_consumer import BaseConsumer
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.services.purchase_order_sync_service import PurchaseOrderSyncService
from tiny_mirror.services.stock_history_sync_service import StockHistorySyncService


class StockHistoryFullSyncConsumer(BaseConsumer):
    QUEUE_NAME = "tiny.sync.stock_history.full"

    def __init__(
        self,
        channel: AbstractChannel,
        queue_publisher: QueuePublisher,
        stock_history_sync: StockHistorySyncService,
    ) -> None:
        super().__init__(channel, queue_publisher)
        self._service = stock_history_sync

    async def handle(self, message_body: dict[str, Any]) -> None:
        await self._service.run_sync(int(message_body["sync_log_id"]))


class PurchaseOrderFullSyncConsumer(BaseConsumer):
    QUEUE_NAME = "tiny.sync.purchase_orders.full"

    def __init__(
        self,
        channel: AbstractChannel,
        queue_publisher: QueuePublisher,
        purchase_order_sync: PurchaseOrderSyncService,
    ) -> None:
        super().__init__(channel, queue_publisher)
        self._service = purchase_order_sync

    async def handle(self, message_body: dict[str, Any]) -> None:
        await self._service.run_sync(int(message_body["sync_log_id"]))
