"""OAuth token lifecycle: cache, rotation, startup bootstrap.

This is the only module allowed to talk directly to ``oauth_tokens`` or to
the ``tiny:access_token`` Redis key. Every caller (Tiny API client, scheduler
job, request handler) must go through :class:`TokenService` to obtain an
access token.

Initial-token strategy: this service does NOT implement the OAuth2
authorization-code flow. The operator obtains the first ``refresh_token``
manually and stores it in ``.env`` as ``TINY_REFRESH_TOKEN``. On the very
first startup, when ``oauth_tokens`` is empty, the service performs a
``grant_type=refresh_token`` call against accounts.tiny.com.br using the
env-provided refresh token. From then on the database row is the source of
truth; the env values are ignored.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime, timedelta

import httpx
import redis.asyncio as redis
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.domain.interfaces import TokenRepository
from tiny_mirror.domain.models import OAuthToken
from tiny_mirror.exceptions import (
    ConfigurationException,
    TokenExpiredException,
)
from tiny_mirror.infrastructure.repositories.token_repository import (
    PostgreSQLTokenRepository,
)

SessionFactory = Callable[[], AbstractAsyncContextManager[AsyncSession]]
RepositoryFactory = Callable[[AsyncSession], TokenRepository]

logger = structlog.get_logger(__name__)


class TokenService:
    REDIS_KEY_ACCESS_TOKEN = "tiny:access_token"
    REDIS_KEY_REFRESH_LOCK = "tiny:token:refresh_lock"
    REDIS_TTL_BUFFER_SECONDS = 600  # 10-minute safety margin
    REDIS_TTL_MIN_SECONDS = 60
    REFRESH_LOCK_TTL_SECONDS = 30
    TOKEN_EXPIRY_WARNING_MINUTES = 30
    TINY_TOKEN_URL = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token"

    def __init__(
        self,
        session_factory: SessionFactory,
        redis_client: redis.Redis,
        http_client: httpx.AsyncClient,
        tiny_client_id: str,
        tiny_client_secret: str,
        tiny_initial_refresh_token: str,
        repository_factory: RepositoryFactory = PostgreSQLTokenRepository,
    ) -> None:
        self._session_factory = session_factory
        self._repository_factory = repository_factory
        self._redis = redis_client
        self._http = http_client
        self._client_id = tiny_client_id
        self._client_secret = tiny_client_secret
        self._initial_refresh_token = tiny_initial_refresh_token

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def get_valid_access_token(self) -> str:
        cached = await self._redis.get(self.REDIS_KEY_ACCESS_TOKEN)
        if cached is not None:
            logger.debug("OAuth token cache hit")
            return cached if isinstance(cached, str) else cached.decode("utf-8")

        logger.debug("OAuth token cache miss, loading from database")
        async with self._session_factory() as session:
            token = await self._repository_factory(session).get_current_token()
        if token is None:
            raise ConfigurationException(
                "No OAuth token found in database. Bootstrap from .env failed; check "
                "TINY_REFRESH_TOKEN."
            )

        if token.is_refresh_expired():
            logger.critical(
                "OAuth refresh token expired",
                refresh_expires_at=token.refresh_expires_at.isoformat(),
            )
            raise TokenExpiredException(
                "Refresh token has expired. Manual re-authentication required."
            )

        if token.is_expired():
            logger.info(
                "Access token expired, refreshing reactively",
                expires_at=token.expires_at.isoformat(),
            )
            token = await self._refresh_with_lock(token.refresh_token)
        elif token.is_expiring_soon(self.TOKEN_EXPIRY_WARNING_MINUTES):
            minutes_remaining = max(0, token.seconds_until_expiry() // 60)
            logger.info(
                "Access token expiring soon, refreshing proactively",
                minutes_remaining=minutes_remaining,
                expires_at=token.expires_at.isoformat(),
            )
            try:
                token = await self._refresh_with_lock(token.refresh_token)
            except TokenExpiredException as exc:
                # Token still valid for up to 30min; warn but keep going.
                logger.warning(
                    "Proactive refresh failed, continuing with current token",
                    error=str(exc),
                )

        await self._cache_access_token(token)
        return token.access_token

    async def refresh_tokens(self) -> OAuthToken:
        """Force a refresh using the current DB row as the source of truth."""
        async with self._session_factory() as session:
            current = await self._repository_factory(session).get_current_token()
        if current is None:
            raise ConfigurationException(
                "Cannot refresh: no OAuth token in database. Run bootstrap first."
            )
        if current.is_refresh_expired():
            logger.critical(
                "OAuth refresh token expired",
                refresh_expires_at=current.refresh_expires_at.isoformat(),
            )
            raise TokenExpiredException(
                "Refresh token has expired. Manual re-authentication required."
            )
        return await self._refresh_with_lock(current.refresh_token)

    async def handle_unauthorized(self) -> str:
        """Invalidate the cache and force a refresh in response to a 401."""
        logger.warning("Received 401 from Tiny API, attempting token refresh")
        await self._redis.delete(self.REDIS_KEY_ACCESS_TOKEN)
        new_token = await self.refresh_tokens()
        return new_token.access_token

    def is_token_expiring_soon(self, token: OAuthToken) -> bool:
        return token.is_expiring_soon(self.TOKEN_EXPIRY_WARNING_MINUTES)

    async def validate_on_startup(self) -> None:
        async with self._session_factory() as session:
            token = await self._repository_factory(session).get_current_token()

        if token is None:
            logger.info("oauth_tokens empty, bootstrapping from .env")
            if not self._initial_refresh_token:
                raise ConfigurationException(
                    "No OAuth token in database and TINY_REFRESH_TOKEN is not set."
                )
            try:
                token = await self._refresh_with_token(self._initial_refresh_token)
            except TokenExpiredException as exc:
                logger.critical("Bootstrap from .env failed", error=str(exc))
                raise ConfigurationException(
                    "Bootstrap failed: env TINY_REFRESH_TOKEN is invalid or expired. "
                    "Generate fresh tokens via the Tiny console and update .env."
                ) from exc

        if token.is_refresh_expired():
            logger.critical(
                "OAuth refresh token expired at startup",
                refresh_expires_at=token.refresh_expires_at.isoformat(),
            )
            raise ConfigurationException(
                "OAuth refresh token has expired. Manual re-authentication with Tiny "
                "is required."
            )

        if token.is_expired():
            logger.warning("Access token is expired, refreshing on startup")
            token = await self._refresh_with_lock(token.refresh_token)
        elif token.is_expiring_soon(self.TOKEN_EXPIRY_WARNING_MINUTES):
            logger.info("Access token expires soon, refreshing proactively on startup")
            token = await self._refresh_with_lock(token.refresh_token)

        await self._cache_access_token(token)
        logger.info(
            "OAuth token validated successfully",
            expires_at=token.expires_at.isoformat(),
            refresh_expires_at=token.refresh_expires_at.isoformat(),
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    async def _refresh_with_lock(self, refresh_token: str) -> OAuthToken:
        """Refresh under a Redis lock so concurrent workers don't double-rotate.

        If the lock is held by another worker, wait briefly and re-read the
        cache populated by that worker. If the cache is still empty after the
        wait, fall through and refresh anyway — staleness is preferable to a
        deadlock.
        """
        acquired = await self._redis.set(
            self.REDIS_KEY_REFRESH_LOCK,
            "1",
            nx=True,
            ex=self.REFRESH_LOCK_TTL_SECONDS,
        )
        if acquired:
            try:
                return await self._refresh_with_token(refresh_token)
            finally:
                await self._redis.delete(self.REDIS_KEY_REFRESH_LOCK)

        # Another worker is refreshing — give it a moment, then retry the DB.
        await asyncio.sleep(1.0)
        async with self._session_factory() as session:
            latest = await self._repository_factory(session).get_current_token()
        if latest is not None and not latest.is_expired():
            return latest
        return await self._refresh_with_token(refresh_token)

    async def _refresh_with_token(self, refresh_token: str) -> OAuthToken:
        """Hit the refresh endpoint with the given token and persist the result."""
        try:
            response = await self._http.post(
                self.TINY_TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30.0,
            )
        except httpx.HTTPError as exc:
            logger.error("HTTP error during token refresh", error=str(exc))
            raise TokenExpiredException(
                f"Failed to refresh OAuth token (HTTP error): {exc}"
            ) from exc

        if response.status_code >= 400:
            logger.error(
                "Token refresh rejected by Tiny",
                status_code=response.status_code,
                error_detail=response.text[:500],
            )
            raise TokenExpiredException(
                f"Failed to refresh OAuth token: HTTP {response.status_code}"
            )

        payload = response.json()
        now = datetime.now(UTC)
        new_token = OAuthToken(
            access_token=payload["access_token"],
            refresh_token=payload["refresh_token"],
            expires_at=now + timedelta(seconds=int(payload["expires_in"])),
            refresh_expires_at=now + timedelta(seconds=int(payload["refresh_expires_in"])),
        )

        async with self._session_factory() as session:
            await self._repository_factory(session).save_token(new_token)
        await self._cache_access_token(new_token)

        logger.info(
            "OAuth token refreshed successfully",
            new_expires_at=new_token.expires_at.isoformat(),
            new_refresh_expires_at=new_token.refresh_expires_at.isoformat(),
        )
        return new_token

    async def _cache_access_token(self, token: OAuthToken) -> None:
        ttl = max(
            self.REDIS_TTL_MIN_SECONDS,
            token.seconds_until_expiry() - self.REDIS_TTL_BUFFER_SECONDS,
        )
        await self._redis.set(self.REDIS_KEY_ACCESS_TOKEN, token.access_token, ex=ttl)
