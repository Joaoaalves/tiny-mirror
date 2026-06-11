"""Redis-backed cooperative rate limiter for the Tiny REST API.

The Tiny API responds with ``X-RateLimit-Remaining`` and ``X-RateLimit-Reset``
headers. We mirror those values into Redis so every worker / async task in
the deployment shares the same view of the budget and pauses cooperatively
when the window is almost exhausted.
"""

from __future__ import annotations

import asyncio
import random
import time

import redis.asyncio as redis
import structlog

logger = structlog.get_logger(__name__)


class RateLimiter:
    REDIS_KEY_REMAINING = "tiny:rate_limit:remaining"
    REDIS_KEY_RESET_AT = "tiny:rate_limit:reset_at"
    RATE_LIMIT_SAFE_THRESHOLD = 5
    REDIS_TTL_SECONDS = 120

    def __init__(self, redis_client: redis.Redis) -> None:
        self._redis = redis_client

    async def wait_if_needed(self) -> None:
        remaining_raw = await self._redis.get(self.REDIS_KEY_REMAINING)
        if remaining_raw is None:
            return
        try:
            remaining = int(remaining_raw)
        except (TypeError, ValueError):
            return
        if remaining > self.RATE_LIMIT_SAFE_THRESHOLD:
            return

        reset_at_raw = await self._redis.get(self.REDIS_KEY_RESET_AT)
        if reset_at_raw is None:
            return
        try:
            reset_at = float(reset_at_raw)
        except (TypeError, ValueError):
            return

        seconds_to_wait = reset_at - time.time()
        if seconds_to_wait <= 0:
            return

        logger.warning(
            "Rate limit low, waiting before next request",
            remaining=remaining,
            seconds_to_wait=round(seconds_to_wait, 2),
        )
        # Jitter spreads concurrent waiters across the first second after the
        # window resets, instead of all firing at the same instant and
        # re-exhausting the fresh budget immediately.
        await asyncio.sleep(seconds_to_wait + 0.1 + random.uniform(0, 1.0))
        logger.debug("Rate limit window reset, proceeding")

    async def update_from_headers(self, headers: dict[str, str]) -> None:
        # Header names from the Tiny API are case-insensitive in HTTP, but
        # httpx already lowercases keys when iterated as a dict. Be defensive.
        remaining = headers.get("X-RateLimit-Remaining") or headers.get("x-ratelimit-remaining")
        reset_in = headers.get("X-RateLimit-Reset") or headers.get("x-ratelimit-reset")
        if remaining is None or reset_in is None:
            return
        try:
            reset_at = time.time() + int(reset_in)
        except (TypeError, ValueError):
            return
        await self._redis.set(self.REDIS_KEY_REMAINING, str(remaining), ex=self.REDIS_TTL_SECONDS)
        await self._redis.set(self.REDIS_KEY_RESET_AT, f"{reset_at:.3f}", ex=self.REDIS_TTL_SECONDS)
