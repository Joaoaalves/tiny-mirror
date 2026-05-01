"""Persistence helpers for the ``sync_logs`` audit table.

This is intentionally lighter than the other repositories — there is no
abstract interface and no domain model. Sync logs are an operational
artifact, not a domain concept; services interact with them via the
methods below.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from tiny_mirror.infrastructure.orm.models import SyncLogORM


class SyncLogRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_sync_log(self, sync_type: str, metadata: dict[str, Any] | None = None) -> int:
        stmt = (
            pg_insert(SyncLogORM)
            .values(
                sync_type=sync_type,
                status="running",
                sync_metadata=metadata,
            )
            .returning(SyncLogORM.id)
        )
        result = await self._session.execute(stmt)
        sync_log_id = int(result.scalar_one())
        await self._session.commit()
        return sync_log_id

    async def update_sync_log_complete(
        self, sync_log_id: int, items_processed: int, items_failed: int
    ) -> None:
        await self._session.execute(
            update(SyncLogORM)
            .where(SyncLogORM.id == sync_log_id)
            .values(
                status="completed",
                completed_at=func.now(),
                items_processed=items_processed,
                items_failed=items_failed,
            )
        )
        await self._session.commit()

    async def update_sync_log_failed(
        self,
        sync_log_id: int,
        error_message: str,
        items_processed: int,
        items_failed: int,
    ) -> None:
        await self._session.execute(
            update(SyncLogORM)
            .where(SyncLogORM.id == sync_log_id)
            .values(
                status="failed",
                completed_at=func.now(),
                error_message=error_message,
                items_processed=items_processed,
                items_failed=items_failed,
            )
        )
        await self._session.commit()

    async def increment_processed(self, sync_log_id: int) -> None:
        # Atomic increment so concurrent item handlers don't race.
        await self._session.execute(
            update(SyncLogORM)
            .where(SyncLogORM.id == sync_log_id)
            .values(items_processed=SyncLogORM.items_processed + 1)
        )
        await self._session.commit()

    async def increment_failed(self, sync_log_id: int) -> None:
        await self._session.execute(
            update(SyncLogORM)
            .where(SyncLogORM.id == sync_log_id)
            .values(items_failed=SyncLogORM.items_failed + 1)
        )
        await self._session.commit()
