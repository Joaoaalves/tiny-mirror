"""PostgreSQL implementation of :class:`SaleBucketRepository`."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import and_, delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.domain.interfaces import SaleBucketRepository
from tiny_mirror.infrastructure.orm.models import SaleBucketORM

_BATCH_SIZE = 500


class PostgreSQLSaleBucketRepository(SaleBucketRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert_bucket(self, data: dict[str, Any]) -> None:
        await self.upsert_buckets_batch([data])

    async def upsert_buckets_batch(self, buckets: list[dict[str, Any]]) -> None:
        if not buckets:
            return
        # Always stamp computed_at on the way in.
        now = datetime.now(UTC)
        for b in buckets:
            b.setdefault("computed_at", now)

        # Insert in batches. The service's refresh flow always DELETEs the
        # period before inserting, so duplicates within a single batch are
        # impossible (the in-memory accumulator dedupes by natural key).
        # We use ON CONFLICT DO NOTHING as a defensive safety net for
        # accidental retries — it preserves the first-write-wins behavior
        # without raising, and the rest of the batch still lands.
        for start in range(0, len(buckets), _BATCH_SIZE):
            chunk = buckets[start : start + _BATCH_SIZE]
            stmt = pg_insert(SaleBucketORM).values(chunk)
            stmt = stmt.on_conflict_do_nothing()
            await self._session.execute(stmt)
        await self._session.commit()

    async def delete_buckets_for_period(
        self, date_from: date, date_to: date
    ) -> int:
        result = await self._session.execute(
            delete(SaleBucketORM).where(
                and_(
                    SaleBucketORM.bucket_date >= date_from,
                    SaleBucketORM.bucket_date <= date_to,
                )
            )
        )
        await self._session.commit()
        return int(result.rowcount or 0)

    async def get_buckets_for_sku(
        self, sku: str, days: int = 90
    ) -> list[dict[str, Any]]:
        cutoff = (datetime.now(UTC).date()) - timedelta(days=days)
        result = await self._session.execute(
            select(SaleBucketORM)
            .where(SaleBucketORM.sku == sku)
            .where(SaleBucketORM.bucket_date >= cutoff)
            .order_by(SaleBucketORM.bucket_date.desc())
        )
        return [_row_to_dict(row) for row in result.scalars().all()]

    async def get_buckets_for_period(
        self, date_from: date, date_to: date
    ) -> list[dict[str, Any]]:
        result = await self._session.execute(
            select(SaleBucketORM)
            .where(SaleBucketORM.bucket_date >= date_from)
            .where(SaleBucketORM.bucket_date <= date_to)
            .order_by(SaleBucketORM.bucket_date, SaleBucketORM.sku)
        )
        return [_row_to_dict(row) for row in result.scalars().all()]


def _row_to_dict(row: Any) -> dict[str, Any]:
    return {col.name: getattr(row, col.name) for col in row.__table__.columns}
