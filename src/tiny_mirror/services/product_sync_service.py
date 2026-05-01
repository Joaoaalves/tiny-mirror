"""Product sync orchestrator. Logic lands in stage 06."""

from __future__ import annotations


class ProductSyncService:
    async def run_full_sync(self, sync_log_id: int) -> None:
        raise NotImplementedError("Implemented in stage 06")

    async def process_product_item(
        self, product_tiny_id: int, sync_log_id: int
    ) -> None:
        raise NotImplementedError("Implemented in stage 06")
