"""End-to-end coverage for ml_claims / ml_claim_reasons / v_ml_claims.

Live ML read (GET claims search + reasons) upserted into live Postgres;
skipped when ML_CLIENT_ID is not configured. The sync is incremental and
idempotent: a second run right after the first finds nothing new.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text

from tiny_mirror.config import settings
from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.redis_client import get_redis
from tiny_mirror.services.mercadolivre_token_service import MercadoLivreTokenService
from tiny_mirror.services.ml_claims_sync_service import MLClaimsSyncService

pytestmark = pytest.mark.e2e


@pytest_asyncio.fixture
async def live_claims_service(
    live_db: None, live_redis: None
) -> AsyncIterator[MLClaimsSyncService]:
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
        yield MLClaimsSyncService(token_service=tokens, http_client=http)


async def test_live_claims_sync_is_incremental(live_claims_service: MLClaimsSyncService) -> None:
    await live_claims_service.sync()
    second = await live_claims_service.sync()

    assert second["reasons_new"] == 0
    async with AsyncSessionLocal() as session:
        missing_reason = (
            await session.execute(
                text(
                    "SELECT count(*) FROM v_ml_claims "
                    "WHERE reason_id IS NOT NULL AND reason_name IS NULL"
                )
            )
        ).scalar_one()
    assert missing_reason == 0
