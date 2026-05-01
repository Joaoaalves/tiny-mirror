"""Order sync queue consumers."""

from __future__ import annotations

from datetime import date
from typing import Any

from aio_pika.abc import AbstractChannel

from tiny_mirror.queue.base_consumer import BaseConsumer
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.services.order_sync_service import OrderSyncService


class OrderFullSyncConsumer(BaseConsumer):
    QUEUE_NAME = "tiny.sync.orders.full"

    def __init__(
        self,
        channel: AbstractChannel,
        queue_publisher: QueuePublisher,
        order_sync_service: OrderSyncService,
    ) -> None:
        super().__init__(channel, queue_publisher)
        self._service = order_sync_service

    async def handle(self, message_body: dict[str, Any]) -> None:
        sync_log_id = int(message_body["sync_log_id"])
        is_historical = bool(message_body.get("is_historical", False))
        if is_historical:
            date_from = date.fromisoformat(message_body["date_from"])
            date_to = date.fromisoformat(message_body["date_to"])
            await self._service.run_date_range_sync(date_from, date_to, sync_log_id)
        else:
            await self._service.run_incremental_sync(sync_log_id)


class OrderItemConsumer(BaseConsumer):
    QUEUE_NAME = "tiny.sync.orders.item"

    def __init__(
        self,
        channel: AbstractChannel,
        queue_publisher: QueuePublisher,
        order_sync_service: OrderSyncService,
    ) -> None:
        super().__init__(channel, queue_publisher)
        self._service = order_sync_service

    async def handle(self, message_body: dict[str, Any]) -> None:
        await self._service.process_order_item(
            order_tiny_id=int(message_body["order_tiny_id"]),
            sync_log_id=int(message_body["sync_log_id"]),
        )
