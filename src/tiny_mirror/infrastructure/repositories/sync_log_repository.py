"""Persistence helpers for the ``sync_logs`` audit table.

This is intentionally lighter than the other repositories — there is no
abstract interface and no domain model. Sync logs are an operational
artifact, not a domain concept; services interact with them via the
methods below.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text, update
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

    async def try_finalize(self, sync_log_id: int) -> bool:
        """Mark a running sync_log as 'completed' once every fanned-out item
        has been processed or failed.

        The fan-out persists ``metadata.total_enqueued``; every consumer
        bumps ``items_processed`` or ``items_failed``. When the two
        counters reach the enqueued total, this method flips the status
        to ``completed``. It is safe to call after every per-item update —
        the WHERE clause makes the UPDATE a no-op until the threshold is
        met, and the second hit (after the row is already completed) does
        nothing because the status filter excludes it.

        Returns True iff this call performed the transition.
        """
        # Use raw SQL so the comparison can read total_enqueued out of the
        # JSONB metadata column atomically with the status check.
        result = await self._session.execute(
            text(
                """
                UPDATE sync_logs
                SET status = 'completed',
                    completed_at = now()
                WHERE id = :id
                  AND status = 'running'
                  AND (sync_metadata ->> 'total_enqueued') IS NOT NULL
                  AND (items_processed + items_failed)
                      >= ((sync_metadata ->> 'total_enqueued')::int)
                """
            ),
            {"id": sync_log_id},
        )
        await self._session.commit()
        return bool(result.rowcount or 0)  # type: ignore[attr-defined]

    async def mark_stalled_as_failed(self, max_minutes: int) -> int:
        """Watchdog helper: close every sync_log stuck in ``running`` for
        longer than ``max_minutes`` so dashboards reflect reality.

        Items dropped to a DLQ never increment processed or failed, so
        items_processed + items_failed < total_enqueued can hold forever.
        Operators triage DLQ separately; the sync_log row should not stay
        running forever just because of that.

        Returns the number of rows the watchdog touched.
        """
        result = await self._session.execute(
            text(
                """
                UPDATE sync_logs
                SET status = 'failed',
                    completed_at = now(),
                    error_message = COALESCE(
                        error_message,
                        'auto-closed by watchdog: running > '
                        || :max_minutes || ' minutes'
                    )
                WHERE status = 'running'
                  AND started_at < now() - make_interval(mins => :max_minutes)
                """
            ),
            {"max_minutes": max_minutes},
        )
        await self._session.commit()
        return int(result.rowcount or 0)  # type: ignore[attr-defined]
