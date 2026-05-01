"""End-to-end coverage for stage 03 — OAuth token lifecycle.

Exercises the live ``TokenService`` against the real Tiny refresh
endpoint (``accounts.tiny.com.br``) and Postgres + Redis. Verifies that:

- ``validate_on_startup`` either bootstraps from ``.env`` or accepts the
  existing DB row, never the OAuth authorization-code flow;
- ``get_valid_access_token`` returns a non-empty JWT and populates the
  Redis cache with a sane TTL;
- a Redis cache miss falls back to the DB row without raising.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import OAuthTokenORM
from tiny_mirror.redis_client import get_redis
from tiny_mirror.services.token_service import TokenService

pytestmark = pytest.mark.e2e


async def test_validate_on_startup_persists_token(
    live_token_service: TokenService,
) -> None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(OAuthTokenORM))
        rows = result.scalars().all()

    assert len(rows) == 1, "oauth_tokens must hold exactly one row after bootstrap"
    row = rows[0]
    assert row.access_token, "access_token must be persisted"
    assert row.refresh_token, "refresh_token must be persisted"
    assert row.expires_at is not None
    assert row.refresh_expires_at is not None


async def test_get_valid_access_token_returns_jwt(
    live_token_service: TokenService,
) -> None:
    token = await live_token_service.get_valid_access_token()

    assert isinstance(token, str)
    assert len(token) > 100, "access token should be a JWT, not a placeholder"
    assert token.count(".") == 2, "access token should look like a JWT"


async def test_cache_is_populated_after_first_fetch(
    live_token_service: TokenService,
) -> None:
    redis = get_redis()
    # Force a cache miss + repopulate via the service.
    await redis.delete(TokenService.REDIS_KEY_ACCESS_TOKEN)
    await live_token_service.get_valid_access_token()

    cached = await redis.get(TokenService.REDIS_KEY_ACCESS_TOKEN)
    ttl = await redis.ttl(TokenService.REDIS_KEY_ACCESS_TOKEN)

    assert cached is not None and len(cached) > 100
    assert ttl >= TokenService.REDIS_TTL_MIN_SECONDS
    # Tiny issues 4-hour access tokens; with the 10-minute buffer the TTL
    # should land well below 4h * 3600s.
    assert ttl <= 4 * 3600


async def test_cache_miss_falls_back_to_db(
    live_token_service: TokenService,
) -> None:
    redis = get_redis()
    await redis.delete(TokenService.REDIS_KEY_ACCESS_TOKEN)

    token = await live_token_service.get_valid_access_token()

    assert token, "service must recover from cache miss"
