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
        source: str = "api",
    ) -> FulfillmentTransferORM:
        row = FulfillmentTransferORM(
            product_tiny_id=product_tiny_id,
            product_sku=product_sku,
            quantity=quantity,
            cost_per_unit=cost_per_unit,
            transferred_at=transferred_at,
            status="pending",
            source=source,
            notes=notes,
        )
        self._session.add(row)
        await self._session.flush()
        await self._session.refresh(row)
        return row

    async def has_recent_pending(self, product_sku: str, since: datetime) -> bool:
        """Return True if there is a pending transfer for ``product_sku``
        created at or after ``since``. Idempotency guard for the webhook
        delta path — Tiny retries on 5xx and the stock cron may race the
        webhook.
        """
        from sqlalchemy import func

        result = await self._session.execute(
            select(func.count(FulfillmentTransferORM.id)).where(
                FulfillmentTransferORM.product_sku == product_sku,
                FulfillmentTransferORM.status == "pending",
                FulfillmentTransferORM.created_at >= since,
            )
        )
        count = int(result.scalar_one() or 0)
        return count > 0

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
        """Fully receive a transfer: status='received', qty_received=qty."""
        row = await self.get_by_id(transfer_id)
        if row is None:
            return None
        row.status = "received"
        row.received_at = received_at
        row.quantity_received = row.quantity
        row.last_event_at = received_at
        await self._session.flush()
        return row

    async def apply_partial_reception(
        self,
        transfer_id: int,
        delta_quantity: int,
        last_event_at: datetime,
    ) -> FulfillmentTransferORM | None:
        """Increment ``quantity_received`` by ``delta_quantity`` and stamp
        ``last_event_at``. Transitions status to 'received' (with
        received_at = last_event_at) when the cumulative equals the
        ordered quantity; otherwise stays pending. Caller is responsible
        for ensuring ``delta_quantity`` does not over-shoot.

        The increment runs as a single SQL UPDATE so concurrent scans (6h
        cron racing the manual /reception/scan trigger) accumulate instead
        of overwriting each other's read-modify-write.
        """
        from sqlalchemy import case, func, update

        orm = FulfillmentTransferORM
        new_received = func.least(orm.quantity, orm.quantity_received + delta_quantity)
        is_complete = orm.quantity_received + delta_quantity >= orm.quantity
        stmt = (
            update(orm)
            .where(orm.id == transfer_id)
            .values(
                quantity_received=new_received,
                last_event_at=last_event_at,
                status=case((is_complete, "received"), else_=orm.status),
                received_at=case((is_complete, last_event_at), else_=orm.received_at),
            )
            .returning(orm)
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def mark_cancelled(self, transfer_id: int, reason: str) -> FulfillmentTransferORM | None:
        """Cancel a pending transfer (e.g. SKU is not fulfillment on ML).
        Cancellation does NOT touch Tiny stock — it only marks our internal
        ledger so coverage math stops subtracting this row.
        """
        row = await self.get_by_id(transfer_id)
        if row is None:
            return None
        row.status = "cancelled"
        existing = row.notes or ""
        suffix = f" [cancelled: {reason}]"
        row.notes = (existing + suffix)[:5000] if existing else suffix.strip()
        await self._session.flush()
        return row
