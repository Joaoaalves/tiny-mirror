"""End-to-end coverage for ml_seller_reputation_daily + ml_infractions.

Live ML reads (GET /users/{id}, GET /moderations/infractions/{id}) upserted
into live Postgres; skipped when ML_CLIENT_ID is not configured. Both writes
are idempotent (snapshot per day, infractions keyed by id).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import func, select

from tiny_mirror.config import settings
from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import MLInfractionORM, MLSellerReputationDailyORM
from tiny_mirror.redis_client import get_redis
from tiny_mirror.services.mercadolivre_token_service import MercadoLivreTokenService
from tiny_mirror.services.ml_account_health_service import MLAccountHealthService

pytestmark = pytest.mark.e2e


@pytest_asyncio.fixture
async def live_account_service(
    live_db: None, live_redis: None
) -> AsyncIterator[MLAccountHealthService]:
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
        yield MLAccountHealthService(
            token_service=tokens, http_client=http, ml_user_id=settings.ml_user_id
        )


async def test_live_reputation_snapshot(live_account_service: MLAccountHealthService) -> None:
    assert await live_account_service.sync_reputation() is True

    today = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    async with AsyncSessionLocal() as session:
        row = await session.get(MLSellerReputationDailyORM, today)
    assert row is not None
    assert row.level_id
    assert (row.transactions_total or 0) > 0


async def test_live_infractions_are_mirrored_idempotently(
    live_account_service: MLAccountHealthService,
) -> None:
    first = await live_account_service.sync_infractions()
    second = await live_account_service.sync_infractions()

    assert second["infractions_new"] == 0
    async with AsyncSessionLocal() as session:
        stored = (
            await session.execute(select(func.count()).select_from(MLInfractionORM))
        ).scalar_one()
    assert stored >= first["infractions_new"]
    assert stored <= first["infractions_total_ml"]
