"""Repository for the phantom products detection log."""

from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.orm.models import PhantomProductsLogORM


class PhantomProductsLogRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def record(
        self,
        *,
        detection_run_id: int,
        sku: str,
        product_active_tiny_id: int | None,
        excluded_tiny_ids: list[int],
        orders_ml_count: int,
        units_ml: int,
        first_sale_date: date | None,
        last_sale_date: date | None,
        investigation_payload: dict[str, Any] | None = None,
    ) -> int:
        result = await self._session.execute(
            pg_insert(PhantomProductsLogORM)
            .values(
                detection_run_id=detection_run_id,
                sku=sku,
                product_active_tiny_id=product_active_tiny_id,
                num_excluded=len(excluded_tiny_ids),
                excluded_tiny_ids=excluded_tiny_ids,
                orders_ml_count=orders_ml_count,
                units_ml=units_ml,
                first_sale_date=first_sale_date,
                last_sale_date=last_sale_date,
                investigation_payload=investigation_payload,
            )
            .returning(PhantomProductsLogORM.id)
        )
        row_id = int(result.scalar_one())
        await self._session.commit()
        return row_id
