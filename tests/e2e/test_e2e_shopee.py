"""End-to-end coverage for the Shopee integration (read-only on Shopee).

Needs the exported credentials file with a live shop token (OpenClaw keeps it
fresh); skipped otherwise. Writes only to the mirror (idempotent upserts).
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import func, select

from tiny_mirror.config import settings
from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.external.marketplace_credentials import MarketplaceCredentials
from tiny_mirror.infrastructure.external.shopee_client import ShopeeClient
from tiny_mirror.infrastructure.orm.models import MPListingORM, MPOrderORM
from tiny_mirror.services.shopee_sync_service import ShopeeSyncService

pytestmark = pytest.mark.e2e


@pytest_asyncio.fixture
async def live_shopee(live_db: None) -> AsyncIterator[ShopeeSyncService]:
    creds = MarketplaceCredentials(settings.marketplace_credentials_file)
    section = creds.section("shopee_seller")
    if section is None or int(section.get("expires_at") or 0) <= time.time() + 120:
        pytest.skip("Shopee credentials not exported or token about to expire")
    async with httpx.AsyncClient(timeout=60.0) as http:
        yield ShopeeSyncService(ShopeeClient(http, creds))


async def test_live_orders_last_days(live_shopee: ShopeeSyncService) -> None:
    stats = await live_shopee.sync_orders(created_days=3)

    async with AsyncSessionLocal() as session:
        stored = (
            await session.execute(
                select(func.count()).select_from(MPOrderORM).where(MPOrderORM.channel == "shopee")
            )
        ).scalar_one()
    assert stored >= stats["orders"]


async def test_live_listings_full_pass(live_shopee: ShopeeSyncService) -> None:
    stats = await live_shopee.sync_listings()

    async with AsyncSessionLocal() as session:
        stored = (
            await session.execute(
                select(func.count())
                .select_from(MPListingORM)
                .where(MPListingORM.channel == "shopee")
            )
        ).scalar_one()
    assert stored == stats["listings"] >= stats["items"]
