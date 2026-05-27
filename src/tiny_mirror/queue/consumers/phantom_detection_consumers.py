"""Consumer for the daily phantom detection cron."""

from __future__ import annotations

from typing import Any

from aio_pika.abc import AbstractChannel

from tiny_mirror.queue.base_consumer import BaseConsumer
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.services.phantom_detection_service import PhantomDetectionService


class PhantomDetectionConsumer(BaseConsumer):
    """One message per cron tick. Single-pass over phantom candidates."""

    QUEUE_NAME = "tiny.sync.phantom_detection.full"

    def __init__(
        self,
        channel: AbstractChannel,
        queue_publisher: QueuePublisher,
        service: PhantomDetectionService,
    ) -> None:
        super().__init__(channel, queue_publisher)
        self._service = service

    async def handle(self, message_body: dict[str, Any]) -> None:
        await self._service.run_detection(int(message_body["sync_log_id"]))
