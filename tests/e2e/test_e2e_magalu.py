"""End-to-end coverage for the Magalu integration (read-only on Magalu).

Needs the exported credentials file with a live store token (OpenClaw keeps it
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
from tiny_mirror.infrastructure.external.magalu_client import ORDERS, SKUS, MagaluClient
from tiny_mirror.infrastructure.external.marketplace_credentials import MarketplaceCredentials
from tiny_mirror.infrastructure.orm.models import MPListingORM, MPOrderORM, OrderORM
from tiny_mirror.services.magalu_sync_service import MagaluSyncService

pytestmark = pytest.mark.e2e


@pytest_asyncio.fixture
async def live_magalu(live_db: None) -> AsyncIterator[MagaluSyncService]:
    creds = MarketplaceCredentials(settings.marketplace_credentials_file)
    section = creds.section("magalu")
    if section is None or int(section.get("expires_at") or 0) <= time.time() + 300:
        pytest.skip("Magalu credentials not exported or token about to expire")
    async with httpx.AsyncClient(timeout=60.0) as http:
        yield MagaluSyncService(MagaluClient(http, creds))


async def test_live_orders_last_days_join_tiny(live_magalu: MagaluSyncService) -> None:
    stats = await live_magalu.sync_orders(created_days=7)

    async with AsyncSessionLocal() as session:
        stored = (
            await session.execute(
                select(func.count()).select_from(MPOrderORM).where(MPOrderORM.channel == "magalu")
            )
        ).scalar_one()
        joined = (
            await session.execute(
                select(func.count())
                .select_from(MPOrderORM)
                .join(OrderORM, OrderORM.ecommerce_order_number == MPOrderORM.order_id)
                .where(MPOrderORM.channel == "magalu")
            )
        ).scalar_one()
        pii = (
            await session.execute(
                select(func.count())
                .select_from(MPOrderORM)
                .where(MPOrderORM.channel == "magalu", MPOrderORM.raw["customer"].isnot(None))
            )
        ).scalar_one()
    assert stored >= stats["orders"]
    assert pii == 0
    if stats["orders"]:
        assert joined > 0  # "LU-" + code matches the Tiny order number


async def test_live_listings_full_pass(live_magalu: MagaluSyncService) -> None:
    stats = await live_magalu.sync_listings()

    async with AsyncSessionLocal() as session:
        stored = (
            await session.execute(
                select(func.count())
                .select_from(MPListingORM)
                .where(MPListingORM.channel == "magalu")
            )
        ).scalar_one()
        priced = (
            await session.execute(
                select(func.count())
                .select_from(MPListingORM)
                .where(MPListingORM.channel == "magalu", MPListingORM.price.isnot(None))
            )
        ).scalar_one()
    assert stored == stats["listings"] == stats["skus"]
    assert priced >= stats["listings"] - stats["offers_failed"]


async def test_live_read_endpoints_answer(live_magalu: MagaluSyncService) -> None:
    client = live_magalu._client
    orders = await client.get(ORDERS, {"_limit": 1})
    skus = await client.get(SKUS, {"_limit": 1})

    assert isinstance(orders.get("results"), list)
    assert isinstance(skus.get("results"), list)
    if skus["results"]:
        sku = str(skus["results"][0]["sku"])
        assert isinstance((await client.get_price(sku)).get("results"), list)
        assert isinstance((await client.get_stock(sku)).get("results"), list)
