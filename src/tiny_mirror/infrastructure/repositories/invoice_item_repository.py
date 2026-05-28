"""PostgreSQL repository for invoice_items.

Each row mirrors one ``itens[]`` entry from ``GET /notas/{id}`` on Tiny.
The natural key is ``(invoice_tiny_id, tiny_item_id)``; if Tiny rotates a
line item (unlikely), the upsert replaces the previous row. Bulk loads
use ``replace_for_invoice`` so a re-sync of one NF is atomic.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.orm.models import InvoiceItemORM


class PostgreSQLInvoiceItemRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def replace_for_invoice(self, invoice_tiny_id: int, items: list[dict[str, Any]]) -> int:
        """Atomically delete + insert the lines for one invoice.

        Idempotent: re-syncing the same NF yields the same final state. We
        delete first because Tiny may renumber idItem on revisions; doing an
        upsert keyed on ``(invoice_tiny_id, tiny_item_id)`` would leave orphan
        rows behind if the line was dropped on the Tiny side.
        """
        await self._session.execute(
            delete(InvoiceItemORM).where(InvoiceItemORM.invoice_tiny_id == invoice_tiny_id)
        )
        if items:
            await self._session.execute(pg_insert(InvoiceItemORM).values(items))
        await self._session.commit()
        return len(items)

    async def count_for_invoice(self, invoice_tiny_id: int) -> int:
        result = await self._session.execute(
            select(InvoiceItemORM).where(InvoiceItemORM.invoice_tiny_id == invoice_tiny_id)
        )
        return len(result.scalars().all())
