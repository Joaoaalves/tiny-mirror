"""End-to-end coverage for ml_item_health.

Live ML reads for a few real listings (multiget, performance, purchase
experience with its 302 redirect) upserted into live Postgres; skipped when
ML_CLIENT_ID is not configured. Upserts are idempotent.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select

from tiny_mirror.config import settings
from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import MLItemHealthORM, MLListingORM
from tiny_mirror.redis_client import get_redis
from tiny_mirror.services.mercadolivre_token_service import MercadoLivreTokenService
from tiny_mirror.services.ml_item_health_service import MLItemHealthService

pytestmark = pytest.mark.e2e


@pytest_asyncio.fixture
async def live_health_service(
    live_db: None, live_redis: None
) -> AsyncIterator[MLItemHealthService]:
    if not settings.ml_client_id:
        pytest.skip("ML_CLIENT_ID not configured")
    async with httpx.AsyncClient(timeout=30.0) as http:
        tokens = MercadoLivreTokenService(
            session_factory=AsyncSessionLocal,
            redis_client=get_redis(),
            http_client=http,
            ml_client_id=settings.ml_client_id,
            ml_client_secret=settings.ml_client_secret,
            ml_initial_refresh_token=settings.ml_refresh_token,
        )
        await tokens.validate_on_startup()
        yield MLItemHealthService(token_service=tokens, http_client=http)


async def test_live_health_for_three_active_listings(
    live_health_service: MLItemHealthService,
) -> None:
    async with AsyncSessionLocal() as session:
        mlbs = [
            m
            for (m,) in await session.execute(
                select(MLListingORM.mlb_id).where(MLListingORM.status == "active").limit(3)
            )
        ]
    if not mlbs:
        pytest.skip("no active listing mirrored")

    async def only_these() -> dict[str, str | None]:
        return dict.fromkeys(mlbs, "active")

    live_health_service._listings = only_these  # type: ignore[method-assign]

    stats = await live_health_service.sync()

    assert stats["items"] == len(mlbs)
    assert stats["quality"] >= 1
    async with AsyncSessionLocal() as session:
        rows = (
            (await session.execute(select(MLItemHealthORM).where(MLItemHealthORM.mlb_id.in_(mlbs))))
            .scalars()
            .all()
        )
    assert {r.mlb_id for r in rows} == set(mlbs)
    assert all(r.item_fetched_at is not None for r in rows)
