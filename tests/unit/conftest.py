"""Shared fixtures for the unit-test suite.

Unit tests are fast (< 1ms each), fully mocked, and never touch live
infrastructure (no Postgres / Redis / RabbitMQ / Tiny API). Whenever a
class under test takes an external collaborator, we hand it a
``unittest.mock.AsyncMock`` from a fixture below.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from tiny_mirror.domain.models import OAuthToken


@pytest.fixture
def fresh_token() -> OAuthToken:
    """Token that's well within validity (4h to expiry)."""
    now = datetime.now(UTC)
    return OAuthToken(
        access_token="fresh.access.token",
        refresh_token="fresh.refresh.token",
        expires_at=now + timedelta(hours=4),
        refresh_expires_at=now + timedelta(days=1),
    )


@pytest.fixture
def expiring_soon_token() -> OAuthToken:
    """Token with 10 minutes until expiry — under the 30-min threshold."""
    now = datetime.now(UTC)
    return OAuthToken(
        access_token="soon.access.token",
        refresh_token="soon.refresh.token",
        expires_at=now + timedelta(minutes=10),
        refresh_expires_at=now + timedelta(days=1),
    )


@pytest.fixture
def expired_token() -> OAuthToken:
    """Token whose access_token has already expired but refresh is still valid."""
    now = datetime.now(UTC)
    return OAuthToken(
        access_token="expired.access.token",
        refresh_token="still.refresh.token",
        expires_at=now - timedelta(minutes=5),
        refresh_expires_at=now + timedelta(hours=12),
    )


@pytest.fixture
def fully_expired_token() -> OAuthToken:
    """Both access and refresh have expired."""
    now = datetime.now(UTC)
    return OAuthToken(
        access_token="dead.access.token",
        refresh_token="dead.refresh.token",
        expires_at=now - timedelta(hours=2),
        refresh_expires_at=now - timedelta(minutes=30),
    )


@pytest.fixture
def mock_redis() -> AsyncMock:
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock(return_value=True)
    redis.delete = AsyncMock(return_value=1)
    redis.ttl = AsyncMock(return_value=0)
    return redis


@pytest.fixture
def mock_http_response() -> MagicMock:
    """Build a MagicMock that quacks like httpx.Response. Override
    ``status_code``, ``headers`` and ``json()`` per test.
    """
    response = MagicMock()
    response.status_code = 200
    response.headers = {}
    response.text = ""
    response.json = MagicMock(return_value={})
    return response


@pytest.fixture
def mock_http_client(mock_http_response: MagicMock) -> AsyncMock:
    client = AsyncMock()
    client.request = AsyncMock(return_value=mock_http_response)
    client.post = AsyncMock(return_value=mock_http_response)
    return client


@pytest.fixture
def make_response():
    """Factory that returns a configured response mock."""

    def _make(
        status_code: int = 200,
        json_body: Any | None = None,
        text: str = "",
        headers: dict[str, str] | None = None,
    ) -> MagicMock:
        resp = MagicMock()
        resp.status_code = status_code
        resp.headers = headers or {}
        resp.text = text
        resp.json = MagicMock(return_value=json_body if json_body is not None else {})
        return resp

    return _make
