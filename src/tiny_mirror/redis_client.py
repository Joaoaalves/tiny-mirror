"""Async Redis client lifecycle helpers."""

from __future__ import annotations

import redis.asyncio as redis
import structlog

from tiny_mirror.config import settings
from tiny_mirror.exceptions import DatabaseException

logger = structlog.get_logger(__name__)

redis_client: redis.Redis | None = None


async def initialize_redis() -> None:
    """Create the global Redis client and verify connectivity with ``PING``."""
    global redis_client
    redis_client = redis.from_url(
        settings.redis_url,
        encoding="utf-8",
        decode_responses=True,
    )
    try:
        await redis_client.ping()
    except Exception as exc:
        logger.error("Redis connectivity check failed", error=str(exc))
        raise DatabaseException(f"Failed to connect to Redis: {exc}") from exc


async def close_redis() -> None:
    """Close the global Redis client if it was initialized."""
    global redis_client
    if redis_client is not None:
        await redis_client.aclose()
        redis_client = None


def get_redis() -> redis.Redis:
    """Return the initialized Redis client.

    Raises :class:`DatabaseException` if :func:`initialize_redis` was not called.
    """
    if redis_client is None:
        raise DatabaseException("Redis not initialized")
    return redis_client
