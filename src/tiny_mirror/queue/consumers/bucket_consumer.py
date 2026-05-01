"""Sale-bucket recompute consumer."""

from __future__ import annotations

from datetime import date
from typing import Any

from aio_pika.abc import AbstractChannel

from tiny_mirror.queue.base_consumer import BaseConsumer
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.services.sale_bucket_service import SaleBucketService


class BucketRefreshConsumer(BaseConsumer):
    QUEUE_NAME = "tiny.sync.buckets.refresh"

    def __init__(
        self,
        channel: AbstractChannel,
        queue_publisher: QueuePublisher,
        sale_bucket_service: SaleBucketService,
    ) -> None:
        super().__init__(channel, queue_publisher)
        self._service = sale_bucket_service

    async def handle(self, message_body: dict[str, Any]) -> None:
        date_from = date.fromisoformat(message_body["date_from"])
        date_to = date.fromisoformat(message_body["date_to"])
        await self._service.refresh_buckets(date_from, date_to)
