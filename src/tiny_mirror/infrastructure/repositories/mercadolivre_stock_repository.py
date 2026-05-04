"""PostgreSQL repository for Mercado Livre per-listing stock data."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.orm.models import MercadoLivreStockORM


class MercadoLivreStockRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def replace_for_sku(self, sku: str, listings: list[dict[str, Any]]) -> None:
        """Atomically replace all ML listings for a given SKU.

        Deletes existing rows for the SKU then bulk-inserts the new ones.
        Pass an empty ``listings`` list to clear a SKU that no longer has
        any ML listings (coverage query treats absence as ml_full_stock=0).
        """
        await self._session.execute(
            delete(MercadoLivreStockORM).where(MercadoLivreStockORM.sku == sku)
        )

        if not listings:
            await self._session.commit()
            return

        now = datetime.now(UTC)
        rows = [
            {
                "sku": sku,
                "mlb_id": item["mlb_id"],
                "available_quantity": item.get("available_quantity", 0),
                "logistic_type": item["logistic_type"],
                "status": item.get("status", "active"),
                "last_synced_at": item.get("last_synced_at", now),
            }
            for item in listings
        ]
        await self._session.execute(pg_insert(MercadoLivreStockORM).values(rows))
        await self._session.commit()
