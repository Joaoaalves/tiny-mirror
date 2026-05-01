"""Stock sync orchestrator. Logic lands in stage 08."""

from __future__ import annotations


class StockSyncService:
    async def run_full_sync(self, sync_log_id: int) -> None:
        raise NotImplementedError("Implemented in stage 08")

    async def process_stock_item(
        self, product_tiny_id: int, sync_log_id: int
    ) -> None:
        raise NotImplementedError("Implemented in stage 08")
