"""Repository for tiny_fl_stock_snapshots — per-product Tiny RAW FL qty memory.

Only purpose: let the stock-sync path detect positive deltas in Tiny's
'Full Mercado Livre' deposit value across runs. The ML overlay rewrites
``stock_deposits`` in place, so we cannot diff against that table.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.orm.models import TinyFLStockSnapshotORM


class TinyFLStockSnapshotRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, product_tiny_id: int) -> int | None:
        result = await self._session.execute(
            select(TinyFLStockSnapshotORM.tiny_fl_qty).where(
                TinyFLStockSnapshotORM.product_tiny_id == product_tiny_id
            )
        )
        row = result.scalar_one_or_none()
        return None if row is None else int(row)

    async def upsert(self, product_tiny_id: int, tiny_fl_qty: int) -> None:
        from sqlalchemy import func

        stmt = pg_insert(TinyFLStockSnapshotORM).values(
            product_tiny_id=product_tiny_id,
            tiny_fl_qty=int(tiny_fl_qty),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["product_tiny_id"],
            set_={
                "tiny_fl_qty": stmt.excluded.tiny_fl_qty,
                "updated_at": func.now(),
            },
        )
        await self._session.execute(stmt)
        await self._session.commit()
