"""Repository for the FL stock correction audit log.

Append-only table — every detected mismatch (corrected or not) becomes a row
with enough investigation context to later trace what caused the drift.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.orm.models import FLStockCorrectionLogORM


class FLStockCorrectionLogRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def record(
        self,
        *,
        product_tiny_id: int,
        sku: str,
        tiny_saldo_before: int,
        ml_qty: int,
        delta: int,
        correction_applied: bool,
        tiny_id_lancamento: int | None = None,
        tiny_saldo_after: int | None = None,
        http_status: int | None = None,
        error_message: str | None = None,
        investigation_payload: dict[str, Any] | None = None,
    ) -> int:
        """Insert a single audit row. Returns the inserted id."""
        result = await self._session.execute(
            pg_insert(FLStockCorrectionLogORM)
            .values(
                product_tiny_id=product_tiny_id,
                sku=sku,
                tiny_saldo_before=tiny_saldo_before,
                ml_qty=ml_qty,
                delta=delta,
                correction_applied=correction_applied,
                tiny_id_lancamento=tiny_id_lancamento,
                tiny_saldo_after=tiny_saldo_after,
                http_status=http_status,
                error_message=error_message,
                investigation_payload=investigation_payload,
            )
            .returning(FLStockCorrectionLogORM.id)
        )
        row_id = int(result.scalar_one())
        await self._session.commit()
        return row_id
