"""Reusable FastAPI dependencies."""

from __future__ import annotations

from collections.abc import AsyncGenerator

import httpx
import redis.asyncio as redis
from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.config import settings
from tiny_mirror.database import get_async_session
from tiny_mirror.infrastructure.repositories.token_repository import (
    PostgreSQLTokenRepository,
)
from tiny_mirror.redis_client import get_redis
from tiny_mirror.services.token_service import TokenService


async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Yield an :class:`AsyncSession` per request."""
    async for session in get_async_session():
        yield session


def get_http_client(request: Request) -> httpx.AsyncClient:
    """Return the shared ``httpx.AsyncClient`` created in the lifespan."""
    return request.app.state.http_client  # type: ignore[no-any-return]


def get_redis_client() -> redis.Redis:
    return get_redis()


def get_token_service(
    session: AsyncSession = Depends(db_session),
    redis_client: redis.Redis = Depends(get_redis_client),
    http_client: httpx.AsyncClient = Depends(get_http_client),
) -> TokenService:
    return TokenService(
        token_repository=PostgreSQLTokenRepository(session),
        redis_client=redis_client,
        http_client=http_client,
        tiny_client_id=settings.tiny_client_id,
        tiny_client_secret=settings.tiny_client_secret,
        tiny_initial_refresh_token=settings.tiny_refresh_token,
    )
