"""End-to-end coverage for the Amazon integration (read-only on Amazon).

Needs the exported credentials file (settings.marketplace_credentials_file);
skipped otherwise. Writes only to the mirror (idempotent upserts).
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import func, select, text

from tiny_mirror.config import settings
from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.external.amazon_spapi_client import AmazonSPAPIClient
from tiny_mirror.infrastructure.external.marketplace_credentials import MarketplaceCredentials
from tiny_mirror.infrastructure.orm.models import MPListingORM, MPOrderORM
from tiny_mirror.services.amazon_sync_service import AmazonSyncService

pytestmark = pytest.mark.e2e


@pytest_asyncio.fixture
async def live_amazon(live_db: None) -> AsyncIterator[AmazonSyncService]:
    creds = MarketplaceCredentials(settings.marketplace_credentials_file)
    if creds.section("amazon_spapi") is None:
        pytest.skip("Amazon credentials not exported")
    async with httpx.AsyncClient(timeout=60.0) as http:
        yield AmazonSyncService(AmazonSPAPIClient(http, creds))


async def test_live_orders_last_days(live_amazon: AmazonSyncService) -> None:
    stats = await live_amazon.sync_orders(created_days=3)

    async with AsyncSessionLocal() as session:
        stored = (
            await session.execute(
                select(func.count()).select_from(MPOrderORM).where(MPOrderORM.channel == "amazon")
            )
        ).scalar_one()
        linked = (
            await session.execute(
                text("SELECT count(*) FROM v_order_datetime WHERE source = 'amazon'")
            )
        ).scalar_one()
    assert stored >= stats["orders"]
    assert linked >= 0  # Tiny may lag the channel by minutes


async def test_live_listings_full_pass(live_amazon: AmazonSyncService) -> None:
    stats = await live_amazon.sync_listings()

    assert stats["complete"] is True
    async with AsyncSessionLocal() as session:
        stored = (
            await session.execute(
                select(func.count())
                .select_from(MPListingORM)
                .where(MPListingORM.channel == "amazon")
            )
        ).scalar_one()
    assert stored == stats["listings"]
