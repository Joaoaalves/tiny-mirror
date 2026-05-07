"""PostgreSQL implementation of the invoice repository."""

from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.orm.models import InvoiceORM


class PostgreSQLInvoiceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert_batch(self, invoices: list[dict[str, Any]]) -> int:
        """Upsert a page of invoices and return the count inserted/updated."""
        if not invoices:
            return 0

        stmt = pg_insert(InvoiceORM).values(invoices)
        first = invoices[0]
        update_cols = {
            col: stmt.excluded[col]
            for col in first
            if col not in {"tiny_id", "created_at"}
        }
        update_cols["updated_at"] = func.now()  # type: ignore[assignment]

        stmt = stmt.on_conflict_do_update(  # type: ignore[assignment]
            index_elements=["tiny_id"],
            set_=update_cols,
        )
        await self._session.execute(stmt)
        await self._session.commit()
        return len(invoices)

    async def count(self) -> int:
        result = await self._session.execute(select(func.count(InvoiceORM.tiny_id)))
        return int(result.scalar_one())

    async def get_by_tiny_id(self, tiny_id: int) -> dict[str, Any] | None:
        result = await self._session.execute(
            select(InvoiceORM).where(InvoiceORM.tiny_id == tiny_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return {col.name: getattr(row, col.name) for col in row.__table__.columns}

    async def get_by_issue_date_range(
        self, date_from: date, date_to: date
    ) -> list[dict[str, Any]]:
        result = await self._session.execute(
            select(InvoiceORM)
            .where(InvoiceORM.issue_date >= date_from)
            .where(InvoiceORM.issue_date <= date_to)
            .order_by(InvoiceORM.issue_date, InvoiceORM.tiny_id)
        )
        return [
            {col.name: getattr(row, col.name) for col in row.__table__.columns}
            for row in result.scalars().all()
        ]
