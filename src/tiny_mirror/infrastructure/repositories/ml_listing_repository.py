"""Repository for ml_listings and ml_listing_variations tables."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import delete, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.orm.models import MLListingORM, MLListingVariationORM


class MLListingRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def replace_all(
        self,
        listings: list[dict[str, Any]],
        variations: list[dict[str, Any]],
    ) -> None:
        """Replace the entire ml_listings + ml_listing_variations dataset atomically.

        Truncates both tables and re-inserts all rows in one transaction so
        the tables are never partially updated.
        """
        now = datetime.now(UTC)

        await self._session.execute(delete(MLListingVariationORM))
        await self._session.execute(delete(MLListingORM))

        if listings:
            for row in listings:
                row["synced_at"] = now
            await self._session.execute(insert(MLListingORM).values(listings))

        if variations:
            await self._session.execute(insert(MLListingVariationORM).values(variations))

        await self._session.commit()

    async def get_fl_listings_by_sku(self, sku: str) -> list[MLListingORM]:
        """Return all fulfillment listings for a given seller SKU."""
        from sqlalchemy import select

        result = await self._session.execute(
            select(MLListingORM).where(
                MLListingORM.sku == sku,
                MLListingORM.logistic_type == "fulfillment",
            )
        )
        return list(result.scalars().all())

    async def get_variations(self, mlb_id: str) -> list[MLListingVariationORM]:
        """Return all variations for a given listing."""
        from sqlalchemy import select

        result = await self._session.execute(
            select(MLListingVariationORM).where(MLListingVariationORM.mlb_id == mlb_id)
        )
        return list(result.scalars().all())

    async def count(self) -> int:
        result = await self._session.execute(text("SELECT COUNT(*) FROM ml_listings"))
        return int(result.scalar_one())
