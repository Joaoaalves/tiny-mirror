"""Repository for fulfillment_transfers table."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.orm.models import FulfillmentTransferORM


class FulfillmentTransferRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        product_tiny_id: int,
        product_sku: str,
        quantity: int,
        cost_per_unit: Decimal,
        transferred_at: datetime,
        notes: str | None = None,
    ) -> FulfillmentTransferORM:
        row = FulfillmentTransferORM(
            product_tiny_id=product_tiny_id,
            product_sku=product_sku,
            quantity=quantity,
            cost_per_unit=cost_per_unit,
            transferred_at=transferred_at,
            status="pending",
            notes=notes,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return row

    async def list_pending_by_sku(self, sku: str) -> list[FulfillmentTransferORM]:
        result = await self._session.execute(
            select(FulfillmentTransferORM)
            .where(
                FulfillmentTransferORM.product_sku == sku,
                FulfillmentTransferORM.status == "pending",
            )
            .order_by(FulfillmentTransferORM.transferred_at.desc())
        )
        return list(result.scalars().all())

    async def list_all(
        self,
        sku: str | None = None,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[FulfillmentTransferORM], int]:
        from sqlalchemy import func

        q = select(FulfillmentTransferORM)
        count_q = select(func.count(FulfillmentTransferORM.id))
        if sku is not None:
            q = q.where(FulfillmentTransferORM.product_sku == sku)
            count_q = count_q.where(FulfillmentTransferORM.product_sku == sku)
        if status is not None:
            q = q.where(FulfillmentTransferORM.status == status)
            count_q = count_q.where(FulfillmentTransferORM.status == status)
        q = q.order_by(FulfillmentTransferORM.transferred_at.desc()).limit(limit).offset(offset)
        rows = list((await self._session.execute(q)).scalars().all())
        total = int((await self._session.execute(count_q)).scalar_one())
        return rows, total

    async def get_by_id(self, transfer_id: int) -> FulfillmentTransferORM | None:
        result = await self._session.execute(
            select(FulfillmentTransferORM).where(FulfillmentTransferORM.id == transfer_id)
        )
        return result.scalar_one_or_none()

    async def mark_received(
        self, transfer_id: int, received_at: datetime
    ) -> FulfillmentTransferORM | None:
        row = await self.get_by_id(transfer_id)
        if row is None:
            return None
        row.status = "received"
        row.received_at = received_at
        await self._session.flush()
        return row
