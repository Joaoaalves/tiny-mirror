"""Unit tests for :class:`tiny_mirror.services.token_service.TokenService`."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from tiny_mirror.domain.models import OAuthToken
from tiny_mirror.exceptions import (
    ConfigurationException,
    TokenExpiredException,
)
from tiny_mirror.services.token_service import TokenService

pytestmark = pytest.mark.unit


@pytest.fixture
def fake_repo() -> MagicMock:
    """Fake :class:`TokenRepository`. Override ``get_current_token`` per test."""
    repo = MagicMock()
    repo.get_current_token = AsyncMock(return_value=None)
    repo.save_token = AsyncMock(return_value=None)
    return repo


@pytest.fixture
def session_factory() -> MagicMock:
    """Returns a callable that yields an async context manager whose
    ``__aenter__`` resolves to a sentinel session. The TokenService never
    inspects the session — it just hands it to the repo factory.
    """

    @asynccontextmanager
    async def _ctx():
        yield "fake-session"

    return MagicMock(side_effect=lambda: _ctx())


@pytest.fixture
def repository_factory(fake_repo: MagicMock) -> MagicMock:
    """Returns a callable that always returns the same fake repository,
    ignoring the session parameter."""
    factory = MagicMock(return_value=fake_repo)
    return factory


@pytest.fixture
def service(
    session_factory: MagicMock,
    repository_factory: MagicMock,
    mock_redis: AsyncMock,
    mock_http_client: AsyncMock,
) -> TokenService:
    return TokenService(
        session_factory=session_factory,
        redis_client=mock_redis,
        http_client=mock_http_client,
        tiny_client_id="cid",
        tiny_client_secret="csec",
        tiny_initial_refresh_token="bootstrap-refresh",
        repository_factory=repository_factory,
    )


# ---------------------------------------------------------------------------
# get_valid_access_token
# ---------------------------------------------------------------------------
async def test_get_valid_access_token_cache_hit_returns_cached(
    service: TokenService,
    mock_redis: AsyncMock,
    fake_repo: MagicMock,
) -> None:
    mock_redis.get = AsyncMock(return_value="cached.jwt")

    token = await service.get_valid_access_token()

    assert token == "cached.jwt"
    fake_repo.get_current_token.assert_not_awaited()


async def test_get_valid_access_token_decodes_bytes_from_redis(
    service: TokenService,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.get = AsyncMock(return_value=b"cached.bytes.jwt")

    token = await service.get_valid_access_token()

    assert token == "cached.bytes.jwt"


async def test_get_valid_access_token_no_cache_returns_db_token(
    service: TokenService,
    mock_redis: AsyncMock,
    fake_repo: MagicMock,
    fresh_token: OAuthToken,
) -> None:
    mock_redis.get = AsyncMock(return_value=None)
    fake_repo.get_current_token = AsyncMock(return_value=fresh_token)

    token = await service.get_valid_access_token()

    assert token == fresh_token.access_token
    # The token gets stored back to Redis with a calculated TTL.
    mock_redis.set.assert_awaited()


async def test_get_valid_access_token_no_token_anywhere_raises_configuration(
    service: TokenService,
    mock_redis: AsyncMock,
    fake_repo: MagicMock,
) -> None:
    mock_redis.get = AsyncMock(return_value=None)
    fake_repo.get_current_token = AsyncMock(return_value=None)

    with pytest.raises(ConfigurationException):
        await service.get_valid_access_token()


async def test_get_valid_access_token_refresh_when_expired(
    service: TokenService,
    mock_redis: AsyncMock,
    fake_repo: MagicMock,
    expired_token: OAuthToken,
    fresh_token: OAuthToken,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_redis.get = AsyncMock(return_value=None)
    # Lock acquired so refresh runs in-process.
    mock_redis.set = AsyncMock(return_value=True)
    fake_repo.get_current_token = AsyncMock(return_value=expired_token)
    mock_http_client.post = AsyncMock(
        return_value=make_response(
            200,
            json_body={
                "access_token": fresh_token.access_token,
                "refresh_token": fresh_token.refresh_token,
                "expires_in": 14400,
                "refresh_expires_in": 86400,
            },
        )
    )

    token = await service.get_valid_access_token()

    assert token == fresh_token.access_token
    mock_http_client.post.assert_awaited_once()
    fake_repo.save_token.assert_awaited()


async def test_get_valid_access_token_proactive_refresh_when_expiring_soon(
    service: TokenService,
    mock_redis: AsyncMock,
    fake_repo: MagicMock,
    expiring_soon_token: OAuthToken,
    fresh_token: OAuthToken,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.set = AsyncMock(return_value=True)
    fake_repo.get_current_token = AsyncMock(return_value=expiring_soon_token)
    mock_http_client.post = AsyncMock(
        return_value=make_response(
            200,
            json_body={
                "access_token": fresh_token.access_token,
                "refresh_token": fresh_token.refresh_token,
                "expires_in": 14400,
                "refresh_expires_in": 86400,
            },
        )
    )

    token = await service.get_valid_access_token()

    assert token == fresh_token.access_token
    mock_http_client.post.assert_awaited_once()


async def test_get_valid_access_token_proactive_refresh_failure_falls_back_to_current(
    service: TokenService,
    mock_redis: AsyncMock,
    fake_repo: MagicMock,
    expiring_soon_token: OAuthToken,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    """When proactive refresh fails the service still has up to 30
    minutes left on the current token — return that and continue."""
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.set = AsyncMock(return_value=True)
    fake_repo.get_current_token = AsyncMock(return_value=expiring_soon_token)
    mock_http_client.post = AsyncMock(return_value=make_response(500, text="err"))

    token = await service.get_valid_access_token()

    assert token == expiring_soon_token.access_token


async def test_get_valid_access_token_refresh_token_expired_raises(
    service: TokenService,
    mock_redis: AsyncMock,
    fake_repo: MagicMock,
    fully_expired_token: OAuthToken,
) -> None:
    mock_redis.get = AsyncMock(return_value=None)
    fake_repo.get_current_token = AsyncMock(return_value=fully_expired_token)

    with pytest.raises(TokenExpiredException):
        await service.get_valid_access_token()


# ---------------------------------------------------------------------------
# refresh_tokens
# ---------------------------------------------------------------------------
async def test_refresh_tokens_calls_correct_endpoint_with_form_body(
    service: TokenService,
    fake_repo: MagicMock,
    fresh_token: OAuthToken,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    fake_repo.get_current_token = AsyncMock(return_value=fresh_token)
    mock_http_client.post = AsyncMock(
        return_value=make_response(
            200,
            json_body={
                "access_token": "new.access",
                "refresh_token": "new.refresh",
                "expires_in": 14400,
                "refresh_expires_in": 86400,
            },
        )
    )

    await service.refresh_tokens()

    call = mock_http_client.post.await_args
    assert call.args[0] == TokenService.TINY_TOKEN_URL
    body = call.kwargs["data"]
    assert body == {
        "grant_type": "refresh_token",
        "refresh_token": fresh_token.refresh_token,
        "client_id": "cid",
        "client_secret": "csec",
    }


async def test_refresh_tokens_persists_new_pair_to_db(
    service: TokenService,
    fake_repo: MagicMock,
    fresh_token: OAuthToken,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    fake_repo.get_current_token = AsyncMock(return_value=fresh_token)
    mock_http_client.post = AsyncMock(
        return_value=make_response(
            200,
            json_body={
                "access_token": "new.access",
                "refresh_token": "new.refresh",
                "expires_in": 14400,
                "refresh_expires_in": 86400,
            },
        )
    )

    new_token = await service.refresh_tokens()

    assert new_token.access_token == "new.access"
    assert new_token.refresh_token == "new.refresh"
    fake_repo.save_token.assert_awaited()


async def test_refresh_tokens_http_error_raises_token_expired(
    service: TokenService,
    fake_repo: MagicMock,
    fresh_token: OAuthToken,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    fake_repo.get_current_token = AsyncMock(return_value=fresh_token)
    mock_http_client.post = AsyncMock(
        return_value=make_response(401, text='{"error": "invalid_grant"}')
    )

    with pytest.raises(TokenExpiredException):
        await service.refresh_tokens()


async def test_refresh_tokens_when_no_current_raises_configuration(
    service: TokenService,
    fake_repo: MagicMock,
) -> None:
    fake_repo.get_current_token = AsyncMock(return_value=None)

    with pytest.raises(ConfigurationException):
        await service.refresh_tokens()


async def test_refresh_tokens_when_refresh_expired_raises_token_expired(
    service: TokenService,
    fake_repo: MagicMock,
    fully_expired_token: OAuthToken,
) -> None:
    fake_repo.get_current_token = AsyncMock(return_value=fully_expired_token)

    with pytest.raises(TokenExpiredException):
        await service.refresh_tokens()


# ---------------------------------------------------------------------------
# handle_unauthorized
# ---------------------------------------------------------------------------
async def test_handle_unauthorized_invalidates_cache_and_refreshes(
    service: TokenService,
    mock_redis: AsyncMock,
    fake_repo: MagicMock,
    fresh_token: OAuthToken,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    fake_repo.get_current_token = AsyncMock(return_value=fresh_token)
    mock_http_client.post = AsyncMock(
        return_value=make_response(
            200,
            json_body={
                "access_token": "rotated.jwt",
                "refresh_token": "rotated.refresh",
                "expires_in": 14400,
                "refresh_expires_in": 86400,
            },
        )
    )

    new_token = await service.handle_unauthorized()

    assert new_token == "rotated.jwt"
    # The access-token cache must have been invalidated. handle_unauthorized
    # also frees the refresh lock at the end, so check `any_await` rather
    # than the last call.
    mock_redis.delete.assert_any_await(TokenService.REDIS_KEY_ACCESS_TOKEN)


# ---------------------------------------------------------------------------
# validate_on_startup
# ---------------------------------------------------------------------------
async def test_validate_on_startup_with_valid_token_caches_and_returns(
    service: TokenService,
    mock_redis: AsyncMock,
    fake_repo: MagicMock,
    fresh_token: OAuthToken,
) -> None:
    fake_repo.get_current_token = AsyncMock(return_value=fresh_token)

    await service.validate_on_startup()

    mock_redis.set.assert_awaited()


async def test_validate_on_startup_empty_db_with_no_env_token_raises(
    session_factory: MagicMock,
    repository_factory: MagicMock,
    fake_repo: MagicMock,
    mock_redis: AsyncMock,
    mock_http_client: AsyncMock,
) -> None:
    fake_repo.get_current_token = AsyncMock(return_value=None)
    svc = TokenService(
        session_factory=session_factory,
        redis_client=mock_redis,
        http_client=mock_http_client,
        tiny_client_id="cid",
        tiny_client_secret="csec",
        tiny_initial_refresh_token="",  # no bootstrap token
        repository_factory=repository_factory,
    )

    with pytest.raises(ConfigurationException):
        await svc.validate_on_startup()


async def test_validate_on_startup_empty_db_bootstraps_from_env(
    service: TokenService,
    fake_repo: MagicMock,
    mock_redis: AsyncMock,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    fake_repo.get_current_token = AsyncMock(return_value=None)
    mock_http_client.post = AsyncMock(
        return_value=make_response(
            200,
            json_body={
                "access_token": "bootstrap.access",
                "refresh_token": "bootstrap.refresh.new",
                "expires_in": 14400,
                "refresh_expires_in": 86400,
            },
        )
    )

    await service.validate_on_startup()

    # The env's bootstrap-refresh token must have been used in the form body.
    body = mock_http_client.post.await_args.kwargs["data"]
    assert body["refresh_token"] == "bootstrap-refresh"
    fake_repo.save_token.assert_awaited()


async def test_validate_on_startup_fully_expired_raises_configuration(
    service: TokenService,
    fake_repo: MagicMock,
    fully_expired_token: OAuthToken,
) -> None:
    fake_repo.get_current_token = AsyncMock(return_value=fully_expired_token)

    with pytest.raises(ConfigurationException):
        await service.validate_on_startup()


# ---------------------------------------------------------------------------
# is_token_expiring_soon
# ---------------------------------------------------------------------------
def test_is_token_expiring_soon_true_for_soon_expiring(
    service: TokenService, expiring_soon_token: OAuthToken
) -> None:
    assert service.is_token_expiring_soon(expiring_soon_token) is True


def test_is_token_expiring_soon_false_for_fresh_token(
    service: TokenService, fresh_token: OAuthToken
) -> None:
    assert service.is_token_expiring_soon(fresh_token) is False
