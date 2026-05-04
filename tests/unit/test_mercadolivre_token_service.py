"""Unit tests for :class:`MercadoLivreTokenService`."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from tiny_mirror.domain.models import OAuthToken
from tiny_mirror.exceptions import ConfigurationException, TokenExpiredException
from tiny_mirror.services.mercadolivre_token_service import MercadoLivreTokenService

pytestmark = pytest.mark.unit

_NOW = datetime.now(UTC)
_ML_TOKEN_PAYLOAD = {
    "access_token": "new.ml.access",
    "refresh_token": "new.ml.refresh",
    "expires_in": 21600,
}


@pytest.fixture
def fake_repo() -> MagicMock:
    repo = MagicMock()
    repo.get_current_token = AsyncMock(return_value=None)
    repo.save_token = AsyncMock(return_value=None)
    return repo


@pytest.fixture
def session_factory() -> MagicMock:
    @asynccontextmanager
    async def _ctx():
        yield "fake-session"

    return MagicMock(side_effect=lambda: _ctx())


@pytest.fixture
def repository_factory(fake_repo: MagicMock) -> MagicMock:
    return MagicMock(return_value=fake_repo)


@pytest.fixture
def service(
    session_factory: MagicMock,
    repository_factory: MagicMock,
    mock_redis: AsyncMock,
    mock_http_client: AsyncMock,
) -> MercadoLivreTokenService:
    return MercadoLivreTokenService(
        session_factory=session_factory,
        redis_client=mock_redis,
        http_client=mock_http_client,
        ml_client_id="ml_cid",
        ml_client_secret="ml_csec",
        ml_initial_refresh_token="init.refresh",
        repository_factory=repository_factory,
    )


def _make_fresh_token(extra_hours: int = 4) -> OAuthToken:
    now = datetime.now(UTC)
    expires = now + timedelta(hours=extra_hours)
    return OAuthToken(
        access_token="ml.access",
        refresh_token="ml.refresh",
        expires_at=expires,
        refresh_expires_at=expires + timedelta(days=365),
    )


def _make_expired_token() -> OAuthToken:
    now = datetime.now(UTC)
    expires = now - timedelta(minutes=10)
    return OAuthToken(
        access_token="old.ml.access",
        refresh_token="old.ml.refresh",
        expires_at=expires,
        refresh_expires_at=expires + timedelta(days=365),
    )


# ---------------------------------------------------------------------------
# get_valid_access_token — cache hit
# ---------------------------------------------------------------------------
async def test_cache_hit_returns_cached_token(
    service: MercadoLivreTokenService,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.get = AsyncMock(return_value="cached.ml.token")

    token = await service.get_valid_access_token()

    assert token == "cached.ml.token"
    mock_redis.get.assert_awaited_once_with(MercadoLivreTokenService.REDIS_KEY_ACCESS_TOKEN)


# ---------------------------------------------------------------------------
# get_valid_access_token — cache miss, valid DB token
# ---------------------------------------------------------------------------
async def test_cache_miss_valid_db_token_caches_and_returns(
    service: MercadoLivreTokenService,
    mock_redis: AsyncMock,
    fake_repo: MagicMock,
) -> None:
    mock_redis.get = AsyncMock(return_value=None)
    fresh = _make_fresh_token()
    fake_repo.get_current_token = AsyncMock(return_value=fresh)

    token = await service.get_valid_access_token()

    assert token == fresh.access_token
    mock_redis.set.assert_awaited_once()


# ---------------------------------------------------------------------------
# get_valid_access_token — cache miss, expired DB token → refresh
# ---------------------------------------------------------------------------
async def test_expired_token_triggers_refresh(
    service: MercadoLivreTokenService,
    mock_redis: AsyncMock,
    mock_http_client: AsyncMock,
    fake_repo: MagicMock,
    make_response,
) -> None:
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.set = AsyncMock(return_value=True)
    mock_redis.delete = AsyncMock(return_value=1)
    expired = _make_expired_token()
    fake_repo.get_current_token = AsyncMock(return_value=expired)
    mock_http_client.post = AsyncMock(return_value=make_response(200, json_body=_ML_TOKEN_PAYLOAD))

    token = await service.get_valid_access_token()

    assert token == "new.ml.access"
    mock_http_client.post.assert_awaited_once()


# ---------------------------------------------------------------------------
# get_valid_access_token — no DB row raises ConfigurationException
# ---------------------------------------------------------------------------
async def test_no_db_token_raises_configuration_exception(
    service: MercadoLivreTokenService,
    mock_redis: AsyncMock,
    fake_repo: MagicMock,
) -> None:
    mock_redis.get = AsyncMock(return_value=None)
    fake_repo.get_current_token = AsyncMock(return_value=None)

    with pytest.raises(ConfigurationException, match="ML OAuth token"):
        await service.get_valid_access_token()


# ---------------------------------------------------------------------------
# validate_on_startup — empty DB, bootstrap from .env
# ---------------------------------------------------------------------------
async def test_validate_on_startup_bootstraps_from_env(
    service: MercadoLivreTokenService,
    mock_redis: AsyncMock,
    mock_http_client: AsyncMock,
    fake_repo: MagicMock,
    make_response,
) -> None:
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.set = AsyncMock(return_value=True)
    mock_redis.delete = AsyncMock(return_value=1)
    fake_repo.get_current_token = AsyncMock(return_value=None)
    mock_http_client.post = AsyncMock(return_value=make_response(200, json_body=_ML_TOKEN_PAYLOAD))

    await service.validate_on_startup()

    mock_http_client.post.assert_awaited_once()
    fake_repo.save_token.assert_awaited_once()


async def test_validate_on_startup_no_env_token_raises(
    session_factory: MagicMock,
    repository_factory: MagicMock,
    mock_redis: AsyncMock,
    mock_http_client: AsyncMock,
    fake_repo: MagicMock,
) -> None:
    mock_redis.get = AsyncMock(return_value=None)
    fake_repo.get_current_token = AsyncMock(return_value=None)
    service = MercadoLivreTokenService(
        session_factory=session_factory,
        redis_client=mock_redis,
        http_client=mock_http_client,
        ml_client_id="cid",
        ml_client_secret="csec",
        ml_initial_refresh_token="",  # empty — no bootstrap possible
        repository_factory=repository_factory,
    )

    with pytest.raises(ConfigurationException, match="ML_REFRESH_TOKEN"):
        await service.validate_on_startup()


async def test_validate_on_startup_fresh_token_no_refresh(
    service: MercadoLivreTokenService,
    mock_redis: AsyncMock,
    mock_http_client: AsyncMock,
    fake_repo: MagicMock,
) -> None:
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.set = AsyncMock(return_value=True)
    fresh = _make_fresh_token()
    fake_repo.get_current_token = AsyncMock(return_value=fresh)

    await service.validate_on_startup()

    mock_http_client.post.assert_not_awaited()


# ---------------------------------------------------------------------------
# handle_unauthorized — invalidates cache and refreshes
# ---------------------------------------------------------------------------
async def test_handle_unauthorized_invalidates_cache_and_refreshes(
    service: MercadoLivreTokenService,
    mock_redis: AsyncMock,
    mock_http_client: AsyncMock,
    fake_repo: MagicMock,
    make_response,
) -> None:
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.set = AsyncMock(return_value=True)
    mock_redis.delete = AsyncMock(return_value=1)
    fresh = _make_fresh_token()
    fake_repo.get_current_token = AsyncMock(return_value=fresh)
    mock_http_client.post = AsyncMock(return_value=make_response(200, json_body=_ML_TOKEN_PAYLOAD))

    new_token = await service.handle_unauthorized()

    assert new_token == "new.ml.access"
    mock_redis.delete.assert_any_await(MercadoLivreTokenService.REDIS_KEY_ACCESS_TOKEN)


# ---------------------------------------------------------------------------
# _refresh_with_token — parses ML token response
# ---------------------------------------------------------------------------
async def test_refresh_with_token_sets_refresh_expires_at_to_365_days(
    service: MercadoLivreTokenService,
    mock_redis: AsyncMock,
    mock_http_client: AsyncMock,
    fake_repo: MagicMock,
    make_response,
) -> None:
    mock_redis.set = AsyncMock(return_value=True)
    mock_redis.delete = AsyncMock(return_value=1)
    mock_http_client.post = AsyncMock(return_value=make_response(200, json_body=_ML_TOKEN_PAYLOAD))

    token = await service._refresh_with_token("old.refresh")

    assert token.access_token == "new.ml.access"
    assert token.refresh_token == "new.ml.refresh"
    delta = token.refresh_expires_at - token.expires_at
    assert delta.days >= 364


async def test_refresh_rejected_raises_token_expired(
    service: MercadoLivreTokenService,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.post = AsyncMock(return_value=make_response(401, text="invalid refresh token"))

    with pytest.raises(TokenExpiredException):
        await service._refresh_with_token("bad.refresh")
