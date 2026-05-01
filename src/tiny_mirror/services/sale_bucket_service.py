"""Sale buckets recompute service. Logic lands in stage 09."""

from __future__ import annotations

from datetime import date


class SaleBucketService:
    async def refresh_buckets(self, date_from: date, date_to: date) -> None:
        raise NotImplementedError("Implemented in stage 09")
