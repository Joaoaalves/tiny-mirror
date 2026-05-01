"""Shared fixtures and gate for the end-to-end test suite.

The whole suite is skipped unless ``E2E_TINY_ACCESS_TOKEN`` is set in the
environment. The variable is treated as a pure on/off flag — its value is
not used as a token (the live :class:`TokenService` rotates a real one
through the database). This keeps CI safe (no credentials → silent skip)
while letting the operator opt in locally with a single export.

Per-test data is pinned via env vars:
    E2E_TINY_TEST_PRODUCT_ID  — known active product (any tipo)
    E2E_TINY_TEST_KIT_ID      — known tipo=K product (kit), optional
    E2E_TINY_TEST_ORDER_ID    — known order id, optional

Tests that need a missing pin are skipped individually rather than
failing.

**Event-loop note:** Postgres / Redis / RabbitMQ clients are module-level
singletons that bind to whatever asyncio loop creates the first
connection. pytest-asyncio gives each test its own loop, so every
fixture below disposes / closes the global before re-initializing — that
forces fresh connections on the current test's loop and avoids the
infamous ``got Future ... attached to a different loop`` error.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from tiny_mirror.config import settings
from tiny_mirror.database import (
    AsyncSessionLocal,
    engine,
    initialize_database,
)
from tiny_mirror.infrastructure.external.rate_limiter import RateLimiter
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.queue.topology import setup_topology
from tiny_mirror.rabbitmq import (
    close_rabbitmq,
    get_channel,
    initialize_rabbitmq,
)
from tiny_mirror.redis_client import close_redis, get_redis, initialize_redis
from tiny_mirror.services.token_service import TokenService


E2E_ENABLED = bool(os.environ.get("E2E_TINY_ACCESS_TOKEN"))


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Skip every collected test under tests/e2e/ when the gate is off.

    Pytest invokes this hook session-wide, not per directory, so we have
    to filter for our subpath ourselves — otherwise a sibling suite
    (tests/unit) would be skipped just because we are.
    """
    if E2E_ENABLED:
        return
    skip_marker = pytest.mark.skip(reason="E2E_TINY_ACCESS_TOKEN not set")
    for item in items:
        if "tests/e2e/" in str(item.fspath) or item.get_closest_marker("e2e"):
            item.add_marker(skip_marker)


# ---------------------------------------------------------------------------
# Per-test pinned data
# ---------------------------------------------------------------------------
def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"{name} not set")
    return value


@pytest.fixture
def e2e_product_id() -> int:
    return int(_require_env("E2E_TINY_TEST_PRODUCT_ID"))


@pytest.fixture
def e2e_kit_id() -> int:
    return int(_require_env("E2E_TINY_TEST_KIT_ID"))


@pytest.fixture
def e2e_order_id() -> int:
    return int(_require_env("E2E_TINY_TEST_ORDER_ID"))


# ---------------------------------------------------------------------------
# Live infrastructure fixtures
# ---------------------------------------------------------------------------
@pytest_asyncio.fixture
async def live_http_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        yield client


@pytest_asyncio.fixture
async def live_db() -> AsyncIterator[None]:
    # Drop any pooled connections held over from a previous test's loop.
    await engine.dispose()
    await initialize_database()
    yield


@pytest_asyncio.fixture
async def live_redis() -> AsyncIterator[None]:
    # Re-initialize on the current loop. close_redis is a no-op when the
    # global is already None.
    await close_redis()
    await initialize_redis()
    yield
    await close_redis()


@pytest_asyncio.fixture
async def live_rabbitmq() -> AsyncIterator[QueuePublisher]:
    await close_rabbitmq()
    await initialize_rabbitmq()
    channel = get_channel()
    await setup_topology(channel)
    yield QueuePublisher(channel)
    await close_rabbitmq()


@pytest_asyncio.fixture
async def live_token_service(
    live_db: None,
    live_redis: None,
    live_http_client: httpx.AsyncClient,
) -> TokenService:
    """Live TokenService bootstrapped from .env, rotating a real token.

    On the very first run against an empty oauth_tokens table, this
    triggers the env-based bootstrap. On subsequent runs the row is
    already there and we just rotate if needed.
    """
    service = TokenService(
        session_factory=AsyncSessionLocal,
        redis_client=get_redis(),
        http_client=live_http_client,
        tiny_client_id=settings.tiny_client_id,
        tiny_client_secret=settings.tiny_client_secret,
        tiny_initial_refresh_token=settings.tiny_refresh_token,
    )
    await service.validate_on_startup()
    return service


@pytest_asyncio.fixture
async def live_tiny_client(
    live_token_service: TokenService,
    live_http_client: httpx.AsyncClient,
) -> TinyAPIClient:
    return TinyAPIClient(
        token_service=live_token_service,
        rate_limiter=RateLimiter(get_redis()),
        http_client=live_http_client,
    )
