"""Invoice sync queue consumers."""

from __future__ import annotations

from datetime import date
from typing import Any

from aio_pika.abc import AbstractChannel

from tiny_mirror.queue.base_consumer import BaseConsumer
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.services.invoice_sync_service import InvoiceSyncService


class InvoiceFullSyncConsumer(BaseConsumer):
    """Handles both cold-start windows and incremental invoice syncs.

    Message shapes:
    - Cold-start window: ``{is_cold_start_window: true, date_from, date_to, sync_log_id}``
    - Incremental (scheduled): ``{sync_log_id}`` (no date range → default lookback)
    - Order-triggered: ``{date_from, date_to}`` (sync_log_id absent or null)
    """

    QUEUE_NAME = "tiny.sync.invoices.full"

    def __init__(
        self,
        channel: AbstractChannel,
        queue_publisher: QueuePublisher,
        invoice_sync_service: InvoiceSyncService,
    ) -> None:
        super().__init__(channel, queue_publisher)
        self._service = invoice_sync_service

    async def handle(self, message_body: dict[str, Any]) -> None:
        sync_log_id: int | None = (
            int(message_body["sync_log_id"])
            if message_body.get("sync_log_id") is not None
            else None
        )

        if message_body.get("is_cold_start"):
            if sync_log_id is None:
                return
            await self._service.run_cold_start(sync_log_id)
            return

        date_from_raw = message_body.get("date_from")
        date_to_raw = message_body.get("date_to")

        if date_from_raw and date_to_raw:
            date_from = date.fromisoformat(date_from_raw)
            date_to = date.fromisoformat(date_to_raw)

            if message_body.get("is_cold_start_window") and sync_log_id is not None:
                # Cold-start window: run without per-invoice tracking, then
                # increment the window counter (total_enqueued = num windows).
                # Skip detail/items fetch — historical backfills are run by a
                # focused script (90-day window) to keep the cold-start cheap.
                await self._service.run_date_range_sync(
                    date_from, date_to, sync_log_id=None, fetch_items=False
                )
                await self._service.finalize_cold_start_window(sync_log_id)
            else:
                # Incremental range (order-triggered or scheduler) — fetch
                # items so phantom detection / sales views stay current.
                await self._service.run_date_range_sync(date_from, date_to, sync_log_id)
        else:
            # No date range → incremental with default lookback.
            await self._service.run_incremental_sync(sync_log_id)
