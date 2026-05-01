"""Order sync orchestrator. Logic lands in stage 07."""

from __future__ import annotations

from datetime import date


class OrderSyncService:
    async def run_incremental_sync(self, sync_log_id: int) -> None:
        raise NotImplementedError("Implemented in stage 07")

    async def run_date_range_sync(
        self, date_from: date, date_to: date, sync_log_id: int
    ) -> None:
        raise NotImplementedError("Implemented in stage 07")

    async def process_order_item(self, order_tiny_id: int, sync_log_id: int) -> None:
        raise NotImplementedError("Implemented in stage 07")
