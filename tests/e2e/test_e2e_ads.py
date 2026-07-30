"""End-to-end coverage for the /ads routes (Mercado Ads read bridge).

Boots the real app over ASGITransport like test_e2e_api. The Mercado
Ads upstream is NOT called here — what this suite pins down live is the
route wiring itself: API-key protection on every /ads route, the 503
fail-closed answer when the ML overlay is not configured, and parameter
validation happening before any upstream call.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from tiny_mirror.config import settings
from tiny_mirror.main import create_app
from tiny_mirror.queue.publisher import QueuePublisher

pytestmark = pytest.mark.e2e


@pytest_asyncio.fixture
async def http_client(
    live_db: None,
    live_redis: None,
    live_rabbitmq: QueuePublisher,
    live_http_client: httpx.AsyncClient,
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app()
    app.state.http_client = live_http_client
    app.state.queue_publisher = live_rabbitmq
    # ML overlay deliberately absent — the 503 path is the contract here.
    app.state.ml_ads_service = None

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        yield client


def _auth() -> dict[str, str]:
    return {"X-API-Key": settings.api_key}


@pytest.mark.asyncio
async def test_ads_routes_require_api_key(http_client: httpx.AsyncClient) -> None:
    for path in (
        "/ads/campaigns",
        "/ads/campaigns/1/metrics",
        "/ads/items/metrics",
        "/ads/summary/daily",
    ):
        resp = await http_client.get(path)
        assert resp.status_code == 401, path


@pytest.mark.asyncio
async def test_ads_unconfigured_returns_503(http_client: httpx.AsyncClient) -> None:
    resp = await http_client.get("/ads/summary/daily", headers=_auth())
    assert resp.status_code == 503
    assert "not configured" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_ads_window_validation_precedes_upstream(
    http_client: httpx.AsyncClient,
) -> None:
    # inverted window → 422 straight from the route, before the service
    # gate (and therefore before any upstream call could happen)
    resp = await http_client.get(
        "/ads/summary/daily",
        params={"from": "2026-07-28", "to": "2026-07-01"},
        headers=_auth(),
    )
    assert resp.status_code == 422
    assert ">= 'from'" in resp.json()["detail"]

    resp = await http_client.get(
        "/ads/campaigns/1/metrics",
        params={"granularity": "hour"},
        headers=_auth(),
    )
    assert resp.status_code == 422
    assert "granularity" in resp.json()["detail"]
