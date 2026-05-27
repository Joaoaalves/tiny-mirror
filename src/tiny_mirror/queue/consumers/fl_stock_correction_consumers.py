"""Consumer for the hourly FL stock correction cron."""

from __future__ import annotations

from typing import Any

from aio_pika.abc import AbstractChannel

from tiny_mirror.queue.base_consumer import BaseConsumer
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.services.fl_stock_correction_service import FLStockCorrectionService


class FLStockCorrectionConsumer(BaseConsumer):
    """One message per cron tick. Single-pass over all eligible base SKUs.

    Doesn't fan out per SKU — the in-process loop in
    :meth:`FLStockCorrectionService.run_correction` handles ~100 products in
    <60s including the per-SKU forensic capture.
    """

    QUEUE_NAME = "tiny.sync.fl_stock_correction.full"

    def __init__(
        self,
        channel: AbstractChannel,
        queue_publisher: QueuePublisher,
        service: FLStockCorrectionService,
    ) -> None:
        super().__init__(channel, queue_publisher)
        self._service = service

    async def handle(self, message_body: dict[str, Any]) -> None:
        await self._service.run_correction(int(message_body["sync_log_id"]))
