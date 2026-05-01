"""PostgreSQL implementation of :class:`TokenRepository`."""

from __future__ import annotations

from datetime import UTC

import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.domain.interfaces import TokenRepository
from tiny_mirror.domain.models import OAuthToken
from tiny_mirror.infrastructure.orm.models import OAuthTokenORM

logger = structlog.get_logger(__name__)

# Fixed primary key for the singleton row. INSERT ... ON CONFLICT (id) DO UPDATE
# uses this id so that subsequent saves replace the existing row in place.
_SINGLETON_ID = 1


class PostgreSQLTokenRepository(TokenRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_current_token(self) -> OAuthToken | None:
        result = await self._session.execute(
            select(OAuthTokenORM).order_by(OAuthTokenORM.id).limit(1)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return OAuthToken(
            access_token=row.access_token,
            refresh_token=row.refresh_token,
            expires_at=_ensure_utc(row.expires_at),
            refresh_expires_at=_ensure_utc(row.refresh_expires_at),
        )

    async def save_token(self, token: OAuthToken) -> None:
        stmt = pg_insert(OAuthTokenORM).values(
            id=_SINGLETON_ID,
            access_token=token.access_token,
            refresh_token=token.refresh_token,
            expires_at=token.expires_at,
            refresh_expires_at=token.refresh_expires_at,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={
                "access_token": stmt.excluded.access_token,
                "refresh_token": stmt.excluded.refresh_token,
                "expires_at": stmt.excluded.expires_at,
                "refresh_expires_at": stmt.excluded.refresh_expires_at,
            },
        )
        await self._session.execute(stmt)
        await self._session.commit()


def _ensure_utc(value: object) -> object:
    """Datetimes round-tripped through asyncpg are tz-aware, but be defensive.

    SQLAlchemy + asyncpg already returns timezone-aware datetimes for
    ``TIMESTAMPTZ`` columns; this helper only kicks in if a future driver
    change strips the tzinfo.
    """
    from datetime import datetime  # local import to avoid module-level dep

    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value
