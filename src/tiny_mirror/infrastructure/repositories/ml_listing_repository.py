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

    async def get_active_mlb_ids_for_sku(self, sku: str) -> list[str]:
        """Return all active MLB IDs (any logistic type) for a given seller SKU."""
        from sqlalchemy import select

        result = await self._session.execute(
            select(MLListingORM.mlb_id).where(
                MLListingORM.sku == sku,
                MLListingORM.status == "active",
            )
        )
        return [str(r) for r in result.scalars().all()]

    async def get_all_active_mlb_ids(self) -> list[tuple[str, str]]:
        """Return (mlb_id, sku) pairs for every active listing in the catalog."""
        from sqlalchemy import select

        result = await self._session.execute(
            select(MLListingORM.mlb_id, MLListingORM.sku).where(
                MLListingORM.status == "active",
            )
        )
        return [(str(m), str(s)) for m, s in result.all()]

    async def sku_logistic_status(self, sku: str) -> tuple[int, int]:
        """Return ``(fulfillment_rows, any_rows)`` for ``sku`` in ml_listings.

        ``fulfillment_rows`` counts listings with logistic_type='fulfillment';
        ``any_rows`` counts all listings regardless of type. Used by the
        webhook to decide whether a positive FL-delta should produce a
        pending transfer:

        - fulfillment_rows > 0 → SKU still on FL, transfer reconciles via ML.
        - any_rows > 0 and fulfillment_rows == 0 → SKU was on FL but
          migrated to xd_drop_off / self_service; transfer would never
          reconcile, so skip it.
        - any_rows == 0 → SKU absent from ml_listings (likely a kit
          component). Caller may still record the transfer; reception scan
          is the appropriate place to decide.
        """
        from sqlalchemy import func, select

        result = await self._session.execute(
            select(
                func.count().filter(MLListingORM.logistic_type == "fulfillment"),
                func.count(),
            ).where(MLListingORM.sku == sku)
        )
        row = result.first()
        if row is None:
            return (0, 0)
        return (int(row[0] or 0), int(row[1] or 0))
