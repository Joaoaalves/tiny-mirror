"""End-to-end coverage for the TikTok Shop integration (read-only on TikTok).

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
from tiny_mirror.infrastructure.external.tiktok_shop_client import TikTokShopClient
from tiny_mirror.infrastructure.orm.models import MPListingORM, MPOrderORM
from tiny_mirror.services.tiktok_sync_service import TikTokSyncService

pytestmark = pytest.mark.e2e


@pytest_asyncio.fixture
async def live_tiktok(live_db: None) -> AsyncIterator[TikTokSyncService]:
    creds = MarketplaceCredentials(settings.marketplace_credentials_file)
    section = creds.section("tiktok_shop")
    if section is None or int(section.get("expires_at") or 0) <= time.time() + 120:
        pytest.skip("TikTok credentials not exported or token about to expire")
    async with httpx.AsyncClient(timeout=60.0) as http:
        yield TikTokSyncService(TikTokShopClient(http, creds))


async def test_live_orders_last_days(live_tiktok: TikTokSyncService) -> None:
    stats = await live_tiktok.sync_orders(created_days=3)

    async with AsyncSessionLocal() as session:
        stored = (
            await session.execute(
                select(func.count()).select_from(MPOrderORM).where(MPOrderORM.channel == "tiktok")
            )
        ).scalar_one()
    assert stored >= stats["orders"]


async def test_live_listings_full_pass(live_tiktok: TikTokSyncService) -> None:
    stats = await live_tiktok.sync_listings()

    async with AsyncSessionLocal() as session:
        stored = (
            await session.execute(
                select(func.count())
                .select_from(MPListingORM)
                .where(MPListingORM.channel == "tiktok")
            )
        ).scalar_one()
    assert stored == stats["listings"] >= stats["products"]

    async with AsyncSessionLocal() as session:
        with_detail = (
            await session.execute(
                select(func.count())
                .select_from(MPListingORM)
                .where(
                    MPListingORM.channel == "tiktok",
                    MPListingORM.raw["detail"].isnot(None),
                    MPListingORM.raw["product"]["main_images"].isnot(None),
                )
            )
        ).scalar_one()
    assert with_detail > 0
    if stats["details_failed"] == 0:
        assert with_detail == stored

    # second pass: nothing changed, so every detail is reused (no detail calls)
    again = await live_tiktok.sync_listings()
    assert again["details_fetched"] + again["details_failed"] <= 5


async def test_live_product_detail_has_images(live_tiktok: TikTokSyncService) -> None:
    page = await live_tiktok._client.search("/product/202309/products/search", {"page_size": 1}, {})
    product_id = str(page["products"][0]["id"])

    detail = await live_tiktok._client.get_product(product_id)

    assert str(detail["id"]) == product_id
    assert detail["main_images"][0]["thumb_urls"][0].startswith("https://")
    assert all("id" in sku for sku in detail["skus"])
