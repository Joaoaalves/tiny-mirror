"""Stock sync queue consumers."""

from __future__ import annotations

from typing import Any

from aio_pika.abc import AbstractChannel

from tiny_mirror.queue.base_consumer import BaseConsumer
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.services.stock_sync_service import StockSyncService


class StockFullSyncConsumer(BaseConsumer):
    QUEUE_NAME = "tiny.sync.stock.full"

    def __init__(
        self,
        channel: AbstractChannel,
        queue_publisher: QueuePublisher,
        stock_sync_service: StockSyncService,
    ) -> None:
        super().__init__(channel, queue_publisher)
        self._service = stock_sync_service

    async def handle(self, message_body: dict[str, Any]) -> None:
        await self._service.run_full_sync(int(message_body["sync_log_id"]))


class StockItemConsumer(BaseConsumer):
    QUEUE_NAME = "tiny.sync.stock.item"

    def __init__(
        self,
        channel: AbstractChannel,
        queue_publisher: QueuePublisher,
        stock_sync_service: StockSyncService,
    ) -> None:
        super().__init__(channel, queue_publisher)
        self._service = stock_sync_service

    async def handle(self, message_body: dict[str, Any]) -> None:
        await self._service.process_stock_item(
            product_tiny_id=int(message_body["product_tiny_id"]),
            sync_log_id=int(message_body["sync_log_id"]),
        )
