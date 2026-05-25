"""Repository for tiny_fl_stock_snapshots — per-product Tiny RAW FL + galpão memory.

Two purposes:
  - Detect positive deltas in Tiny's 'Full Mercado Livre' deposit across
    runs (the ML overlay rewrites stock_deposits in place, so we cannot
    diff against that table).
  - Remember the previous galpão balance so the webhook can corroborate
    that a positive FL delta is matched by a galpão drop (= real
    transfer) rather than a sale cancellation (= galpão untouched).
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.orm.models import TinyFLStockSnapshotORM


@dataclass(frozen=True)
class TinyFLSnapshot:
    tiny_fl_qty: int
    stock_galpao_qty: int


class TinyFLStockSnapshotRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, product_tiny_id: int) -> TinyFLSnapshot | None:
        result = await self._session.execute(
            select(
                TinyFLStockSnapshotORM.tiny_fl_qty,
                TinyFLStockSnapshotORM.stock_galpao_qty,
            ).where(TinyFLStockSnapshotORM.product_tiny_id == product_tiny_id)
        )
        row = result.one_or_none()
        if row is None:
            return None
        return TinyFLSnapshot(tiny_fl_qty=int(row[0]), stock_galpao_qty=int(row[1]))

    async def upsert(
        self,
        product_tiny_id: int,
        tiny_fl_qty: int,
        stock_galpao_qty: int,
    ) -> None:
        from sqlalchemy import func

        stmt = pg_insert(TinyFLStockSnapshotORM).values(
            product_tiny_id=product_tiny_id,
            tiny_fl_qty=int(tiny_fl_qty),
            stock_galpao_qty=int(stock_galpao_qty),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["product_tiny_id"],
            set_={
                "tiny_fl_qty": stmt.excluded.tiny_fl_qty,
                "stock_galpao_qty": stmt.excluded.stock_galpao_qty,
                "updated_at": func.now(),
            },
        )
        await self._session.execute(stmt)
        await self._session.commit()
