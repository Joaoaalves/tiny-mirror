"""OAuth token lifecycle for Mercado Livre.

Mirrors :class:`tiny_mirror.services.token_service.TokenService` with two
differences:

1. Token endpoint is ``https://api.mercadolibre.com/oauth/token``.
2. ML does not return ``refresh_expires_in`` — we set ``refresh_expires_at``
   to ``expires_at + 365 days`` so the shared :class:`OAuthToken` model stays
   compatible and the "refresh token is expired" guard never fires spuriously.

The operator seeds the first token via ``ML_REFRESH_TOKEN`` in ``.env``.
After bootstrap the ``ml_oauth_tokens`` singleton row is the source of truth.
"""

from __future__ import annotations

import asyncio
import json
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
from tiny_mirror.infrastructure.repositories.ml_token_repository import MLTokenRepository

SessionFactory = Callable[[], AbstractAsyncContextManager[AsyncSession]]
RepositoryFactory = Callable[[AsyncSession], TokenRepository]

logger = structlog.get_logger(__name__)


class MercadoLivreTokenService:
    REDIS_KEY_ACCESS_TOKEN = "ml:access_token"
    REDIS_KEY_REFRESH_LOCK = "ml:token:refresh_lock"
    REDIS_KEY_PENDING_ROTATION = "ml:token:pending_rotation"
    REDIS_TTL_BUFFER_SECONDS = 600
    REDIS_TTL_MIN_SECONDS = 60
    REFRESH_LOCK_TTL_SECONDS = 30
    PENDING_ROTATION_TTL_SECONDS = 7 * 24 * 3600
    TOKEN_EXPIRY_WARNING_MINUTES = 30
    # ML access_token TTL is 6h (21600s); refresh_token has no documented expiry.
    ML_TOKEN_URL = "https://api.mercadolibre.com/oauth/token"
    # Sentinel used to fill in the absent refresh_expires_in from ML.
    REFRESH_TOKEN_LIFETIME_DAYS = 365

    def __init__(
        self,
        session_factory: SessionFactory,
        redis_client: redis.Redis,
        http_client: httpx.AsyncClient,
        ml_client_id: str,
        ml_client_secret: str,
        ml_initial_refresh_token: str,
        repository_factory: RepositoryFactory = MLTokenRepository,
    ) -> None:
        self._session_factory = session_factory
        self._repository_factory = repository_factory
        self._redis = redis_client
        self._http = http_client
        self._client_id = ml_client_id
        self._client_secret = ml_client_secret
        self._initial_refresh_token = ml_initial_refresh_token

    # ------------------------------------------------------------------
    # Public API (same surface as TokenService)
    # ------------------------------------------------------------------
    async def get_valid_access_token(self) -> str:
        cached = await self._redis.get(self.REDIS_KEY_ACCESS_TOKEN)
        if cached is not None:
            logger.debug("ML OAuth token cache hit")
            return cached if isinstance(cached, str) else cached.decode("utf-8")

        logger.debug("ML OAuth token cache miss, loading from database")
        async with self._session_factory() as session:
            token = await self._repository_factory(session).get_current_token()
        if token is None:
            raise ConfigurationException(
                "No ML OAuth token in database. Bootstrap from .env failed; "
                "check ML_REFRESH_TOKEN."
            )

        if token.is_expired():
            logger.info("ML access token expired, refreshing reactively")
            token = await self._refresh_with_lock(token.refresh_token)
        elif token.is_expiring_soon(self.TOKEN_EXPIRY_WARNING_MINUTES):
            minutes_remaining = max(0, token.seconds_until_expiry() // 60)
            logger.info(
                "ML access token expiring soon, refreshing proactively",
                minutes_remaining=minutes_remaining,
            )
            try:
                token = await self._refresh_with_lock(token.refresh_token)
            except TokenExpiredException as exc:
                logger.warning(
                    "ML proactive refresh failed, continuing with current token",
                    error=str(exc),
                )

        await self._cache_access_token(token)
        return token.access_token

    async def refresh_tokens(self) -> OAuthToken:
        async with self._session_factory() as session:
            current = await self._repository_factory(session).get_current_token()
        if current is None:
            raise ConfigurationException(
                "Cannot refresh ML token: no row in database. Run bootstrap first."
            )
        return await self._refresh_with_lock(current.refresh_token)

    async def handle_unauthorized(self) -> str:
        logger.warning("Received 401 from ML API, attempting token refresh")
        await self._redis.delete(self.REDIS_KEY_ACCESS_TOKEN)
        new_token = await self.refresh_tokens()
        return new_token.access_token

    async def validate_on_startup(self) -> None:
        async with self._session_factory() as session:
            token = await self._repository_factory(session).get_current_token()

        if token is None:
            logger.info("ml_oauth_tokens empty, bootstrapping from .env")
            if not self._initial_refresh_token:
                raise ConfigurationException(
                    "No ML token in database and ML_REFRESH_TOKEN is not set."
                )
            try:
                token = await self._refresh_with_token(self._initial_refresh_token)
            except TokenExpiredException as exc:
                logger.critical("ML bootstrap from .env failed", error=str(exc))
                raise ConfigurationException(
                    "ML bootstrap failed: ML_REFRESH_TOKEN is invalid or expired."
                ) from exc

        if token.is_expired():
            logger.warning("ML access token is expired, refreshing on startup")
            token = await self._refresh_with_lock(token.refresh_token)
        elif token.is_expiring_soon(self.TOKEN_EXPIRY_WARNING_MINUTES):
            logger.info("ML access token expires soon, refreshing proactively on startup")
            token = await self._refresh_with_lock(token.refresh_token)

        await self._cache_access_token(token)
        logger.info(
            "ML OAuth token validated successfully",
            expires_at=token.expires_at.isoformat(),
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    async def _refresh_with_lock(self, refresh_token: str) -> OAuthToken:
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

        await asyncio.sleep(1.0)
        async with self._session_factory() as session:
            latest = await self._repository_factory(session).get_current_token()
        if latest is not None and not latest.is_expired():
            return latest
        return await self._refresh_with_token(refresh_token)

    async def _refresh_with_token(self, refresh_token: str) -> OAuthToken:
        # Crash recovery: if a previous rotation died between the HTTP
        # response and the DB save, the stash holds tokens newer than the DB
        # row (ML rotates the refresh token on every call, so the DB token is
        # already burned). Complete that rotation instead of retrying with the
        # stale token.
        stashed = await self._load_pending_rotation()
        if stashed is not None and stashed.refresh_token != refresh_token:
            logger.warning(
                "Recovering ML token rotation interrupted before persistence; "
                "using stashed tokens instead of the stale DB row"
            )
            await self._persist_token(stashed)
            if not stashed.is_expired():
                return stashed
            refresh_token = stashed.refresh_token

        try:
            response = await self._http.post(
                self.ML_TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "refresh_token": refresh_token,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30.0,
            )
        except httpx.HTTPError as exc:
            logger.error("HTTP error during ML token refresh", error=str(exc))
            raise TokenExpiredException(
                f"Failed to refresh ML OAuth token (HTTP error): {exc}"
            ) from exc

        if response.status_code >= 400:
            logger.error(
                "ML token refresh rejected",
                status_code=response.status_code,
                error_detail=response.text[:500],
            )
            raise TokenExpiredException(
                f"Failed to refresh ML OAuth token: HTTP {response.status_code}"
            )

        payload = response.json()
        now = datetime.now(UTC)
        expires_at = now + timedelta(seconds=int(payload["expires_in"]))
        # ML does not return refresh_expires_in; use a generous sentinel so the
        # "refresh token expired" guard never fires for long-lived ML tokens.
        refresh_expires_at = expires_at + timedelta(days=self.REFRESH_TOKEN_LIFETIME_DAYS)

        new_token = OAuthToken(
            access_token=payload["access_token"],
            refresh_token=payload["refresh_token"],
            expires_at=expires_at,
            refresh_expires_at=refresh_expires_at,
        )

        # Stash BEFORE the DB save: ML already burned the old refresh token,
        # so losing new_token here would require manual re-authentication.
        await self._stash_pending_rotation(new_token)
        await self._persist_token(new_token)

        logger.info(
            "ML OAuth token refreshed successfully",
            new_expires_at=new_token.expires_at.isoformat(),
        )
        return new_token

    async def _persist_token(self, token: OAuthToken) -> None:
        async with self._session_factory() as session:
            await self._repository_factory(session).save_token(token)
        await self._cache_access_token(token)
        await self._redis.delete(self.REDIS_KEY_PENDING_ROTATION)

    async def _stash_pending_rotation(self, token: OAuthToken) -> None:
        payload = json.dumps(
            {
                "access_token": token.access_token,
                "refresh_token": token.refresh_token,
                "expires_at": token.expires_at.isoformat(),
                "refresh_expires_at": token.refresh_expires_at.isoformat(),
            }
        )
        await self._redis.set(
            self.REDIS_KEY_PENDING_ROTATION, payload, ex=self.PENDING_ROTATION_TTL_SECONDS
        )

    async def _load_pending_rotation(self) -> OAuthToken | None:
        raw = await self._redis.get(self.REDIS_KEY_PENDING_ROTATION)
        if raw is None:
            return None
        try:
            data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
            return OAuthToken(
                access_token=data["access_token"],
                refresh_token=data["refresh_token"],
                expires_at=datetime.fromisoformat(data["expires_at"]),
                refresh_expires_at=datetime.fromisoformat(data["refresh_expires_at"]),
            )
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            logger.warning("Discarding unreadable ML pending token rotation stash", error=str(exc))
            await self._redis.delete(self.REDIS_KEY_PENDING_ROTATION)
            return None

    async def _cache_access_token(self, token: OAuthToken) -> None:
        ttl = max(
            self.REDIS_TTL_MIN_SECONDS,
            token.seconds_until_expiry() - self.REDIS_TTL_BUFFER_SECONDS,
        )
        await self._redis.set(self.REDIS_KEY_ACCESS_TOKEN, token.access_token, ex=ttl)
