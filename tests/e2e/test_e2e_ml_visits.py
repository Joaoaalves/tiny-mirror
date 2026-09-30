"""End-to-end coverage for ml_item_visits_daily.

Live ML read (GET /items/{id}/visits/time_window) + upsert into live
Postgres for one real listing; skipped when ML_CLIENT_ID is not configured.
The upsert is idempotent, so re-running only refreshes the same rows.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import func, select

from tiny_mirror.config import settings
from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import MLItemVisitsDailyORM, MLListingORM
from tiny_mirror.redis_client import get_redis
from tiny_mirror.services.mercadolivre_token_service import MercadoLivreTokenService
from tiny_mirror.services.ml_visits_sync_service import MLVisitsSyncService

pytestmark = pytest.mark.e2e


@pytest_asyncio.fixture
async def live_visits_service(
    live_db: None, live_redis: None
) -> AsyncIterator[MLVisitsSyncService]:
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
        yield MLVisitsSyncService(token_service=tokens, http_client=http)


async def test_sync_one_live_listing(live_visits_service: MLVisitsSyncService) -> None:
    async with AsyncSessionLocal() as session:
        mlb = (
            await session.execute(
                select(MLListingORM.mlb_id).where(MLListingORM.status == "active").limit(1)
            )
        ).scalar_one_or_none()
    if mlb is None:
        pytest.skip("no active listing mirrored")
    live_visits_service._listing_ids = lambda: _one(mlb)  # type: ignore[method-assign]

    stats = await live_visits_service.sync(days=7)

    assert stats["listings_failed"] == 0
    assert stats["rows"] >= 7
    async with AsyncSessionLocal() as session:
        stored = (
            await session.execute(
                select(func.count())
                .select_from(MLItemVisitsDailyORM)
                .where(MLItemVisitsDailyORM.mlb_id == mlb)
            )
        ).scalar_one()
    assert stored >= 7


async def _one(mlb: str) -> list[str]:
    return [mlb]
