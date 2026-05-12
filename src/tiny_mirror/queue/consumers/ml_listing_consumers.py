"""Consumer for the ml_listings sync queue."""

from __future__ import annotations

from typing import Any

from aio_pika.abc import AbstractChannel

from tiny_mirror.queue.base_consumer import BaseConsumer
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.services.ml_listing_sync_service import MLListingSyncService


class MLListingFullSyncConsumer(BaseConsumer):
    QUEUE_NAME = "tiny.sync.ml_listings.full"

    def __init__(
        self,
        channel: AbstractChannel,
        queue_publisher: QueuePublisher,
        ml_listing_sync: MLListingSyncService,
    ) -> None:
        super().__init__(channel, queue_publisher)
        self._service = ml_listing_sync

    async def handle(self, message_body: dict[str, Any]) -> None:
        await self._service.run_sync(int(message_body["sync_log_id"]))
