"""End-to-end coverage for stage 04 (orders endpoints) and stage 07
(order sync into Postgres + fan-out to stock / sale-buckets).

Tests assume:
- live Postgres / Redis / RabbitMQ via docker-compose
- a working refresh token in .env (TokenService bootstraps automatically)
- ``E2E_TINY_TEST_ORDER_ID`` set to a known order id (skipped otherwise)
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from sqlalchemy import delete, func, select

from tiny_mirror.config import settings
from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.infrastructure.orm.models import OrderItemORM, OrderORM, SyncLogORM
from tiny_mirror.infrastructure.repositories.order_repository import (
    PostgreSQLOrderRepository,
)
from tiny_mirror.infrastructure.repositories.sync_log_repository import (
    SyncLogRepository,
)
from tiny_mirror.mappers.order_mapper import OrderMapper
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.rabbitmq import get_channel
from tiny_mirror.redis_client import get_redis
from tiny_mirror.services.order_sync_service import (
    PENDING_KEY_PREFIX,
    OrderSyncService,
    _created_before,
)

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# Stage 04 — TinyAPIClient orders endpoints (read-only client surface)
# ---------------------------------------------------------------------------
async def test_list_orders_returns_valid_structure(
    live_tiny_client: TinyAPIClient,
) -> None:
    response = await live_tiny_client.list_orders(limit=1)

    assert "itens" in response and isinstance(response["itens"], list)
    assert "paginacao" in response
    assert (
        response["paginacao"].get("total", 0) >= 1
    ), "expected at least one order in the live Tiny account"

    item = response["itens"][0]
    for required in ("id", "numeroPedido", "situacao"):
        assert required in item, f"missing field {required!r} in list item"


async def test_get_order_returns_items_array(
    live_tiny_client: TinyAPIClient,
    e2e_order_id: int,
) -> None:
    detail = await live_tiny_client.get_order(e2e_order_id)

    assert int(detail["id"]) == e2e_order_id
    assert "itens" in detail and isinstance(detail["itens"], list)


# ---------------------------------------------------------------------------
# Stage 07 — OrderMapper
# ---------------------------------------------------------------------------
async def test_order_mapper_translates_real_order_payload(
    live_tiny_client: TinyAPIClient,
    e2e_order_id: int,
) -> None:
    raw = await live_tiny_client.get_order(e2e_order_id)

    mapped = OrderMapper.from_tiny_api(raw)
    items = OrderMapper.extract_items(raw)

    assert mapped["tiny_id"] == int(raw["id"])
    assert mapped["order_number"] == int(raw["numeroPedido"])
    assert isinstance(mapped["situation"], int)
    # JSONB fields preserved as dict / None
    assert isinstance(mapped["customer"], dict)
    # 'itens' must NEVER leak into the order dict — it goes in extract_items
    assert "itens" not in mapped
    # Per-item fields normalized
    assert isinstance(items, list)
    if items:
        first = items[0]
        for required in (
            "product_sku",
            "quantity",
            "unit_value",
            "product_description",
        ):
            assert required in first


def test_situation_name_to_code_covers_every_tiny_status() -> None:
    expected = {
        "Aberta": 0,
        "Faturada": 1,
        "Cancelada": 2,
        "Aprovada": 3,
        "Preparando Envio": 4,
        "Enviada": 5,
        "Entregue": 6,
        "Pronto Envio": 7,
        "Dados Incompletos": 8,
        "Nao Entregue": 9,
    }
    assert OrderMapper.SITUATION_NAME_TO_CODE == expected


# ---------------------------------------------------------------------------
# Stage 07 — OrderSyncService persistence end-to-end
# ---------------------------------------------------------------------------
async def test_process_order_item_persists_order_and_items(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
    e2e_order_id: int,
) -> None:
    service = OrderSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)
    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("orders")

    await service.process_order_item(e2e_order_id, sync_log_id)

    async with AsyncSessionLocal() as session:
        row = await PostgreSQLOrderRepository(session).get_by_tiny_id(e2e_order_id)
    assert row is not None
    assert int(row["tiny_id"]) == e2e_order_id
    assert isinstance(row["situation"], int)
    assert isinstance(row["items"], list)
    assert len(row["items"]) >= 1, "expected at least one line item"

    first_item = row["items"][0]
    assert first_item["product_sku"], "product_sku must be persisted"
    # product_tiny_id is allowed to be None when the product hasn't been
    # synced yet (stage 06 anticipates this).


async def test_process_order_item_is_idempotent(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
    e2e_order_id: int,
) -> None:
    service = OrderSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)
    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("orders")

    await service.process_order_item(e2e_order_id, sync_log_id)
    # Capture the item count after first run.
    async with AsyncSessionLocal() as session:
        first_items = (
            await session.execute(
                select(func.count(OrderItemORM.id)).where(
                    OrderItemORM.order_tiny_id == e2e_order_id
                )
            )
        ).scalar_one()

    await service.process_order_item(e2e_order_id, sync_log_id)
    async with AsyncSessionLocal() as session:
        order_count = (
            await session.execute(
                select(func.count(OrderORM.tiny_id)).where(OrderORM.tiny_id == e2e_order_id)
            )
        ).scalar_one()
        second_items = (
            await session.execute(
                select(func.count(OrderItemORM.id)).where(
                    OrderItemORM.order_tiny_id == e2e_order_id
                )
            )
        ).scalar_one()

    assert int(order_count) == 1, "second sync must not duplicate the order row"
    assert int(first_items) == int(
        second_items
    ), "second sync must replace items, not duplicate them"


async def test_run_incremental_sync_publishes_orders_and_buckets(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
    e2e_order_id: int,
) -> None:
    """Sanity-check the incremental fan-out: orders.item per recent order
    that we have not synced yet, and exactly one buckets.refresh. Stock
    is intentionally NOT fanned out — the daily stock cron is the single
    owner of stock freshness.
    """
    # Seed: persist the pinned order so the incremental run has something
    # to skip (filter_new_order_ids must filter it out).
    seeder = OrderSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)
    async with AsyncSessionLocal() as session:
        seed_log_id = await SyncLogRepository(session).create_sync_log("orders")
    await seeder.process_order_item(e2e_order_id, seed_log_id)

    channel = get_channel()
    orders_q = await channel.get_queue("tiny.sync.orders.item")
    buckets_q = await channel.get_queue("tiny.sync.buckets.refresh")
    for q in (orders_q, buckets_q):
        while True:
            leftover = await q.get(no_ack=True, fail=False)
            if leftover is None:
                break

    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("orders")

    service = OrderSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)
    await service.run_incremental_sync(sync_log_id)

    # orders.item count must equal the listing for the same window after
    # the service's own filters: rows created before the age cutoff are
    # skipped, and so are ids already mirrored. Any drained id must be one
    # of those. The live account may genuinely have zero recent orders.
    expected_ids = await _recent_unmirrored_ids(live_tiny_client, service)
    expected_orders = len(expected_ids)

    drained_orders = 0
    while True:
        msg = await orders_q.get(no_ack=True, fail=False)
        if msg is None:
            break
        drained_orders += 1
        assert json.loads(msg.body)["order_tiny_id"] in expected_ids

    assert drained_orders == expected_orders

    # buckets.refresh: exactly one message
    buckets_msg = await buckets_q.get(no_ack=True, fail=False)
    assert buckets_msg is not None, "expected exactly one buckets.refresh message"
    extra = await buckets_q.get(no_ack=True, fail=False)
    assert extra is None, "expected exactly one buckets.refresh message"


async def test_run_historical_sync_emits_one_window_message_per_7_days(
    live_rabbitmq: QueuePublisher,
    live_tiny_client: TinyAPIClient,
) -> None:
    """run_historical_sync(days=N) must publish exactly ceil(N/7) messages
    to orders.full, each with is_historical=True and a 7-day window.
    """
    import json
    import math

    channel = get_channel()
    full_q = await channel.get_queue("tiny.sync.orders.full")
    while True:
        leftover = await full_q.get(no_ack=True, fail=False)
        if leftover is None:
            break

    service = OrderSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)
    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("orders")

    days = 21
    await service.run_historical_sync(days=days, sync_log_id=sync_log_id)

    expected_windows = math.ceil(days / 7)

    drained = 0
    saw_historical = False
    while True:
        msg = await full_q.get(no_ack=True, fail=False)
        if msg is None:
            break
        body = json.loads(msg.body.decode("utf-8"))
        assert body.get("is_historical") is True
        assert body.get("date_from") and body.get("date_to")
        saw_historical = True
        drained += 1

    assert drained == expected_windows
    assert saw_historical


async def test_run_incremental_sync_records_total_enqueued(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
) -> None:
    service = OrderSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)
    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("orders")

    await service.run_incremental_sync(sync_log_id)

    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(select(SyncLogORM).where(SyncLogORM.id == sync_log_id))
        ).scalar_one()
    metadata = row.sync_metadata or {}
    assert "total_enqueued" in metadata
    assert isinstance(metadata["total_enqueued"], int)


async def _recent_unmirrored_ids(tiny_client: TinyAPIClient, service: OrderSyncService) -> set[int]:
    """Ids the incremental run should publish: listed as updated in the
    2-hour window, created on/after the age cutoff, not yet mirrored."""
    cutoff = OrderSyncService._cutoff_date()
    recent: list[int] = []
    offset = 0
    while True:
        page = await tiny_client.list_orders(
            updated_after=datetime.now(UTC) - timedelta(hours=2),
            limit=100,
            offset=offset,
        )
        items = page.get("itens", []) or []
        recent += [int(i["id"]) for i in items if not _created_before(i, cutoff)]
        offset += 100
        if len(items) < 100:
            break
    return set(await service._filter_new_order_ids(recent))


# ---------------------------------------------------------------------------
# 2026-09-30 — NF deletion storm guards
# ---------------------------------------------------------------------------
_OLD_ORDER_ID = 9_990_000_001
_RECENT_ORDER_ID = 9_990_000_002


def _synthetic_order(tiny_id: int, order_date: date, invoice_id: int | None) -> dict[str, object]:
    return {
        "tiny_id": tiny_id,
        "order_number": tiny_id - 9_000_000_000,
        "invoice_id": invoice_id,
        "invoice_date": date(2025, 12, 12) if invoice_id else None,
        "customer": {},
        "situation": 6,
        "order_date": order_date,
        "synced_at": datetime.now(UTC),
    }


async def _delete_synthetic_orders() -> None:
    async with AsyncSessionLocal() as session:
        await session.execute(
            delete(OrderORM).where(OrderORM.tiny_id.in_([_OLD_ORDER_ID, _RECENT_ORDER_ID]))
        )
        await session.commit()


async def test_upsert_keeps_nf_link_of_old_order_when_tiny_zeroes_it(live_db: None) -> None:
    """Deleting an old NF in Tiny zeroes the order's idNotaFiscal; the
    mirror must keep the last known link for orders before the cutoff,
    while recent orders keep following Tiny."""
    cutoff = OrderSyncService._cutoff_date()
    recent_date = datetime.now(UTC).date()
    await _delete_synthetic_orders()
    try:
        async with AsyncSessionLocal() as session:
            repo = PostgreSQLOrderRepository(session)
            await repo.upsert(_synthetic_order(_OLD_ORDER_ID, date(2025, 11, 24), 123456))
            await repo.upsert(_synthetic_order(_RECENT_ORDER_ID, recent_date, 777))

            # Tiny now returns idNotaFiscal=0 and no invoice date for both.
            await repo.upsert(
                _synthetic_order(_OLD_ORDER_ID, date(2025, 11, 24), 0),
                invoice_link_frozen_before=cutoff,
            )
            await repo.upsert(
                _synthetic_order(_RECENT_ORDER_ID, recent_date, 0),
                invoice_link_frozen_before=cutoff,
            )

        async with AsyncSessionLocal() as session:
            rows = {
                r.tiny_id: r
                for r in (
                    await session.execute(
                        select(OrderORM).where(
                            OrderORM.tiny_id.in_([_OLD_ORDER_ID, _RECENT_ORDER_ID])
                        )
                    )
                ).scalars()
            }
        assert rows[_OLD_ORDER_ID].invoice_id == 123456
        assert rows[_OLD_ORDER_ID].invoice_date == date(2025, 12, 12)
        assert rows[_RECENT_ORDER_ID].invoice_id == 0
        assert rows[_RECENT_ORDER_ID].invoice_date is None

        # A genuinely new NF on an old order still replaces the link.
        async with AsyncSessionLocal() as session:
            await PostgreSQLOrderRepository(session).upsert(
                _synthetic_order(_OLD_ORDER_ID, date(2025, 11, 24), 999999),
                invoice_link_frozen_before=cutoff,
            )
            row = await PostgreSQLOrderRepository(session).get_by_tiny_id(_OLD_ORDER_ID)
        assert row is not None and int(row["invoice_id"]) == 999999
    finally:
        await _delete_synthetic_orders()


async def test_pending_claim_dedupes_against_live_redis(
    live_redis: None, live_rabbitmq: QueuePublisher
) -> None:
    redis = get_redis()
    service = OrderSyncService(
        tiny_client=MagicMock(), queue_publisher=live_rabbitmq, redis_client=redis
    )
    key = f"{PENDING_KEY_PREFIX}{_OLD_ORDER_ID}"
    await redis.delete(key)
    try:
        assert await service._claim_pending(_OLD_ORDER_ID) is True
        assert await service._claim_pending(_OLD_ORDER_ID) is False
        ttl = await redis.ttl(key)
        assert 0 < ttl <= settings.orders_item_pending_ttl_seconds

        await service._release_pending(_OLD_ORDER_ID)
        assert await redis.exists(key) == 0
        assert await service._claim_pending(_OLD_ORDER_ID) is True
    finally:
        await redis.delete(key)
