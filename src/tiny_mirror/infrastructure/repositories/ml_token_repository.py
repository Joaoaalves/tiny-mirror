"""PostgreSQL implementation of :class:`TokenRepository` for Mercado Livre tokens."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.domain.interfaces import TokenRepository
from tiny_mirror.domain.models import OAuthToken
from tiny_mirror.infrastructure.orm.models import MLOAuthTokenORM

logger = structlog.get_logger(__name__)

_SINGLETON_ID = 1


class MLTokenRepository(TokenRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_current_token(self) -> OAuthToken | None:
        result = await self._session.execute(
            select(MLOAuthTokenORM).order_by(MLOAuthTokenORM.id).limit(1)
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
        stmt = pg_insert(MLOAuthTokenORM).values(
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


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value
