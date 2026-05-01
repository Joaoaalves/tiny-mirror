"""End-to-end coverage for stage 04 (stock endpoint) and stage 08
(stock sync into Postgres + deposit-level upsert).
"""

from __future__ import annotations

import pytest
from sqlalchemy import func, select

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.infrastructure.orm.models import StockDepositORM, StockORM
from tiny_mirror.infrastructure.repositories.product_repository import (
    PostgreSQLProductRepository,
)
from tiny_mirror.infrastructure.repositories.stock_repository import (
    PostgreSQLStockRepository,
)
from tiny_mirror.infrastructure.repositories.sync_log_repository import (
    SyncLogRepository,
)
from tiny_mirror.mappers.stock_mapper import StockMapper
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.rabbitmq import get_channel
from tiny_mirror.services.product_sync_service import ProductSyncService
from tiny_mirror.services.stock_sync_service import StockSyncService

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# Stage 04 — TinyAPIClient stock endpoint
# ---------------------------------------------------------------------------
async def test_get_stock_returns_deposits_and_balance(
    live_tiny_client: TinyAPIClient,
    e2e_product_id: int,
) -> None:
    stock = await live_tiny_client.get_stock(e2e_product_id)

    assert "depositos" in stock and isinstance(stock["depositos"], list)
    assert "saldo" in stock
    assert "disponivel" in stock


# ---------------------------------------------------------------------------
# Stage 08 — StockMapper
# ---------------------------------------------------------------------------
async def test_stock_mapper_translates_real_payload(
    live_tiny_client: TinyAPIClient,
    e2e_product_id: int,
) -> None:
    raw = await live_tiny_client.get_stock(e2e_product_id)

    mapped = StockMapper.from_tiny_api(raw)
    deposits = StockMapper.extract_deposits(raw)

    assert mapped["product_tiny_id"] == e2e_product_id
    assert isinstance(mapped["balance"], float)
    assert isinstance(mapped["reserved"], float)
    assert isinstance(mapped["available"], float)
    # Tiny calls the SKU `codigo` on this endpoint.
    assert isinstance(mapped["sku"], str)

    assert isinstance(deposits, list)
    if deposits:
        first = deposits[0]
        for required in ("deposit_tiny_id", "deposit_name", "ignore", "balance"):
            assert required in first
        assert isinstance(first["ignore"], bool)


def test_stock_mapper_handles_none_numeric_fields() -> None:
    raw = {
        "id": 42,
        "nome": "Stub Product",
        "codigo": "SKU-42",
        "saldo": None,
        "reservado": None,
        "disponivel": None,
        "depositos": [],
    }
    mapped = StockMapper.from_tiny_api(raw)
    assert mapped["balance"] == 0.0
    assert mapped["reserved"] == 0.0
    assert mapped["available"] == 0.0


def test_stock_mapper_extract_deposits_returns_empty_when_no_deposits() -> None:
    raw = {"id": 42, "depositos": []}
    assert StockMapper.extract_deposits(raw) == []
    raw_no_field = {"id": 42}
    assert StockMapper.extract_deposits(raw_no_field) == []


# ---------------------------------------------------------------------------
# Stage 08 — StockSyncService persistence end-to-end
# ---------------------------------------------------------------------------
async def test_process_stock_item_persists_stock_and_deposits(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
    e2e_product_id: int,
) -> None:
    # Stock has a FK to products; ensure the product exists first.
    product_svc = ProductSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)
    stock_svc = StockSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)
    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("stock")
    await product_svc.process_product_item(e2e_product_id, sync_log_id)

    await stock_svc.process_stock_item(e2e_product_id, sync_log_id)

    async with AsyncSessionLocal() as session:
        row = await PostgreSQLStockRepository(session).get_by_product_tiny_id(e2e_product_id)
    assert row is not None
    assert int(row["product_tiny_id"]) == e2e_product_id
    assert isinstance(row["deposits"], list)
    # The Tiny test account has at least one deposit per product.
    assert len(row["deposits"]) >= 1
    for d in row["deposits"]:
        assert isinstance(d["deposit_name"], str)
        assert isinstance(d["ignore"], bool)


async def test_process_stock_item_is_idempotent(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
    e2e_product_id: int,
) -> None:
    product_svc = ProductSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)
    stock_svc = StockSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)
    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("stock")
    await product_svc.process_product_item(e2e_product_id, sync_log_id)

    await stock_svc.process_stock_item(e2e_product_id, sync_log_id)
    async with AsyncSessionLocal() as session:
        first_deposit_count = (
            await session.execute(
                select(func.count(StockDepositORM.id)).where(
                    StockDepositORM.product_tiny_id == e2e_product_id
                )
            )
        ).scalar_one()

    await stock_svc.process_stock_item(e2e_product_id, sync_log_id)

    async with AsyncSessionLocal() as session:
        stock_count = (
            await session.execute(
                select(func.count(StockORM.product_tiny_id)).where(
                    StockORM.product_tiny_id == e2e_product_id
                )
            )
        ).scalar_one()
        second_deposit_count = (
            await session.execute(
                select(func.count(StockDepositORM.id)).where(
                    StockDepositORM.product_tiny_id == e2e_product_id
                )
            )
        ).scalar_one()

    assert int(stock_count) == 1, "second sync must not duplicate the stock row"
    assert int(first_deposit_count) == int(
        second_deposit_count
    ), "deposit count must be stable across re-syncs (atomic replace, not append)"


async def test_process_stock_item_skips_when_product_not_synced(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
) -> None:
    """If the product isn't in our products table, the FK on stock would
    raise. Service must degrade to a warning + skip instead.
    """
    stock_svc = StockSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)
    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("stock")

    # Pick an id that we haven't synced (use a placeholder that almost
    # certainly isn't in our DB but exists in Tiny). We use a known
    # product id and verify it is NOT in our DB before the call.
    candidate = 999999999  # very unlikely to be in the live store

    async with AsyncSessionLocal() as session:
        product_row = await PostgreSQLProductRepository(session).get_by_tiny_id(candidate)
    assert product_row is None, "test prerequisite: product should not be in DB"

    # Must not raise.
    await stock_svc.process_stock_item(candidate, sync_log_id)


async def test_run_full_sync_publishes_one_message_per_active_product(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
    e2e_product_id: int,
) -> None:
    """run_full_sync must publish exactly one stock.item per row in
    products.situation='A'. Seed at least one such row first.
    """
    # Seed: ensure at least one active product row.
    product_svc = ProductSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)
    async with AsyncSessionLocal() as session:
        seed_log = await SyncLogRepository(session).create_sync_log("products")
    await product_svc.process_product_item(e2e_product_id, seed_log)

    channel = get_channel()
    queue = await channel.get_queue("tiny.sync.stock.item")
    while True:
        leftover = await queue.get(no_ack=True, fail=False)
        if leftover is None:
            break

    stock_svc = StockSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)
    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("stock")
        active_count = len(await PostgreSQLProductRepository(session).list_active())

    await stock_svc.run_full_sync(sync_log_id)

    drained = 0
    while True:
        msg = await queue.get(no_ack=True, fail=False)
        if msg is None:
            break
        drained += 1
    assert drained == active_count


async def test_run_incremental_sync_for_products_empty_list_is_noop(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
) -> None:
    channel = get_channel()
    queue = await channel.get_queue("tiny.sync.stock.item")
    while True:
        leftover = await queue.get(no_ack=True, fail=False)
        if leftover is None:
            break

    stock_svc = StockSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)
    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("stock")

    # Empty list — must not raise and must not publish anything.
    await stock_svc.run_incremental_sync_for_products([], sync_log_id)

    msg = await queue.get(no_ack=True, fail=False)
    assert msg is None, "expected no messages for an empty product list"


async def test_run_incremental_sync_for_products_publishes_one_per_id(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
    e2e_product_id: int,
) -> None:
    channel = get_channel()
    queue = await channel.get_queue("tiny.sync.stock.item")
    while True:
        leftover = await queue.get(no_ack=True, fail=False)
        if leftover is None:
            break

    stock_svc = StockSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)
    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("stock")

    await stock_svc.run_incremental_sync_for_products(
        [e2e_product_id, e2e_product_id + 1, e2e_product_id + 2], sync_log_id
    )

    drained = 0
    while True:
        msg = await queue.get(no_ack=True, fail=False)
        if msg is None:
            break
        drained += 1
    assert drained == 3
