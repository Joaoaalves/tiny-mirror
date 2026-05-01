"""End-to-end coverage for stage 12 — APScheduler jobs and initial-sync.

Each test builds a fresh FastAPI app with the live infrastructure
fixtures populated on app.state, calls ``setup_scheduler``, inspects
the configured jobs / behavior, and shuts the scheduler down.

The individual job functions are exercised directly (they only emit
queue messages and DB rows), so we don't have to wait for cron timers.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import pytest_asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from sqlalchemy import delete, select

from tiny_mirror.config import settings
from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.exceptions import QueueException
from tiny_mirror.infrastructure.orm.models import OrderORM, ProductORM, SyncLogORM
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.rabbitmq import get_channel
from tiny_mirror.scheduler import jobs as jobs_module
from tiny_mirror.scheduler.jobs import (
    check_and_trigger_initial_sync,
    orders_sync_job,
    products_sync_job,
    sale_buckets_refresh_job,
    setup_scheduler,
    shutdown_scheduler,
    stock_full_sync_job,
    token_rotation_job,
)
from tiny_mirror.services.token_service import TokenService

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest_asyncio.fixture
async def scheduler_app(
    live_db: None,
    live_redis: None,
    live_rabbitmq: QueuePublisher,
    live_token_service: TokenService,
) -> AsyncIterator[FastAPI]:
    """Build a minimal FastAPI app with app.state populated as the
    lifespan would. The lifespan itself is bypassed."""
    app = FastAPI()
    app.state.queue_publisher = live_rabbitmq
    app.state.token_service = live_token_service
    yield app


async def _drain(queue_name: str) -> list[dict[str, Any]]:
    channel = get_channel()
    queue = await channel.get_queue(queue_name)
    out: list[dict[str, Any]] = []
    while True:
        msg = await queue.get(no_ack=True, fail=False)
        if msg is None:
            break
        out.append(json.loads(msg.body.decode("utf-8")))
    return out


# ---------------------------------------------------------------------------
# setup_scheduler
# ---------------------------------------------------------------------------
async def test_setup_scheduler_registers_five_jobs_and_runs(
    scheduler_app: FastAPI,
) -> None:
    sched: AsyncIOScheduler | None = None
    try:
        sched = setup_scheduler(scheduler_app)
        assert sched.running is True
        ids = sorted(job.id for job in sched.get_jobs())
        assert ids == [
            "orders_sync",
            "products_sync",
            "sale_buckets_refresh",
            "stock_full_sync",
            "token_rotation",
        ]
    finally:
        shutdown_scheduler(sched)


async def test_setup_scheduler_is_idempotent_via_replace_existing(
    scheduler_app: FastAPI,
) -> None:
    sched_a = setup_scheduler(scheduler_app)
    sched_b = setup_scheduler(scheduler_app)
    try:
        assert len(sched_a.get_jobs()) == 5
        assert len(sched_b.get_jobs()) == 5
    finally:
        shutdown_scheduler(sched_a)
        shutdown_scheduler(sched_b)


async def test_invalid_cron_string_raises_in_setup(
    scheduler_app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "sync_products_cron", "not a cron expression")
    with pytest.raises(ValueError):
        setup_scheduler(scheduler_app)


# ---------------------------------------------------------------------------
# Individual jobs (exercise the function bodies, no cron timing involved)
# ---------------------------------------------------------------------------
async def test_products_sync_job_creates_log_and_publishes(
    live_rabbitmq: QueuePublisher, live_db: None
) -> None:
    leftover = await _drain("tiny.sync.products.full")
    assert leftover == []  # ensure clean

    await products_sync_job(live_rabbitmq)

    msgs = await _drain("tiny.sync.products.full")
    assert len(msgs) == 1
    body = msgs[0]
    assert body["triggered_by"] == "scheduler"
    assert isinstance(body["sync_log_id"], int)


async def test_orders_sync_job_creates_log_and_publishes(
    live_rabbitmq: QueuePublisher, live_db: None
) -> None:
    await _drain("tiny.sync.orders.full")

    await orders_sync_job(live_rabbitmq)

    msgs = await _drain("tiny.sync.orders.full")
    assert len(msgs) == 1
    body = msgs[0]
    assert body["is_historical"] is False
    assert body["lookback_hours"] == 2
    assert body["date_from"] is None
    assert body["date_to"] is None
    assert isinstance(body["sync_log_id"], int)


async def test_stock_full_sync_job_creates_log_and_publishes(
    live_rabbitmq: QueuePublisher, live_db: None
) -> None:
    await _drain("tiny.sync.stock.full")

    await stock_full_sync_job(live_rabbitmq)

    msgs = await _drain("tiny.sync.stock.full")
    assert len(msgs) == 1
    assert isinstance(msgs[0]["sync_log_id"], int)


async def test_sale_buckets_refresh_job_emits_one_message(
    live_rabbitmq: QueuePublisher, live_db: None
) -> None:
    await _drain("tiny.sync.buckets.refresh")

    await sale_buckets_refresh_job(live_rabbitmq)

    msgs = await _drain("tiny.sync.buckets.refresh")
    assert len(msgs) == 1
    body = msgs[0]
    assert body["triggered_by"] == "scheduler"
    assert "date_from" in body and "date_to" in body


async def test_failing_job_marks_sync_log_failed(
    live_rabbitmq: QueuePublisher,
    live_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When publish_sync_message raises, the job catches the exception
    AND updates the sync_log row to status=failed with a useful error
    message — so the trigger failure stays observable in /sync/logs."""
    async def _broken(*args, **kwargs):
        raise QueueException("simulated failure")

    monkeypatch.setattr(QueuePublisher, "publish_sync_message", _broken)

    await products_sync_job(live_rabbitmq)

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(SyncLogORM)
            .where(SyncLogORM.sync_type == "products")
            .where(SyncLogORM.status == "failed")
            .order_by(SyncLogORM.id.desc())
            .limit(1)
        )
        row = result.scalar_one_or_none()
    assert row is not None
    assert row.error_message is not None
    assert "simulated failure" in row.error_message


# ---------------------------------------------------------------------------
# token_rotation_job
# ---------------------------------------------------------------------------
async def test_token_rotation_job_calls_refresh(
    live_token_service: TokenService,
) -> None:
    """A successful rotation logs success and returns without error."""
    await token_rotation_job(
        live_token_service,
        max_retries=1,
        retry_delay_seconds=0,
    )


async def test_token_rotation_job_retries_on_failure_then_gives_up(
    live_token_service: TokenService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When refresh keeps failing, the job retries up to max_retries and
    then logs CRITICAL without raising — APScheduler keeps scheduling."""
    call_count = 0

    async def _broken_refresh():
        nonlocal call_count
        call_count += 1
        raise RuntimeError("simulated")

    monkeypatch.setattr(live_token_service, "refresh_tokens", _broken_refresh)

    await token_rotation_job(
        live_token_service,
        max_retries=3,
        retry_delay_seconds=0,
    )
    assert call_count == 3


async def test_token_rotation_job_succeeds_after_retry(
    live_token_service: TokenService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Job stops retrying once refresh_tokens succeeds."""
    call_count = 0
    real_refresh = live_token_service.refresh_tokens

    async def _maybe_succeed():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise RuntimeError("transient")
        return await real_refresh()

    monkeypatch.setattr(live_token_service, "refresh_tokens", _maybe_succeed)

    await token_rotation_job(
        live_token_service,
        max_retries=5,
        retry_delay_seconds=0,
    )
    assert call_count == 2


# ---------------------------------------------------------------------------
# check_and_trigger_initial_sync
# ---------------------------------------------------------------------------
@pytest_asyncio.fixture
async def empty_products_table(live_db: None) -> AsyncIterator[None]:
    """Clear the products table for the duration of the test so the
    initial-sync logic kicks in. Restores nothing — the user can resync
    via POST /sync/products if they need the data back.
    """
    async with AsyncSessionLocal() as session:
        await session.execute(delete(ProductORM))
        await session.commit()
    yield


async def test_initial_sync_triggers_when_products_table_is_empty(
    scheduler_app: FastAPI,
    live_rabbitmq: QueuePublisher,
    empty_products_table: None,
) -> None:
    # Drain the three target queues first.
    for queue_name in (
        "tiny.sync.products.full",
        "tiny.sync.orders.full",
        "tiny.sync.stock.full",
    ):
        await _drain(queue_name)

    await check_and_trigger_initial_sync(scheduler_app)

    products_msgs = await _drain("tiny.sync.products.full")
    orders_msgs = await _drain("tiny.sync.orders.full")
    stock_msgs = await _drain("tiny.sync.stock.full")

    assert len(products_msgs) == 1
    assert len(orders_msgs) == 1
    assert len(stock_msgs) == 1

    assert products_msgs[0]["triggered_by"] == "initial_sync"

    orders_body = orders_msgs[0]
    assert orders_body["is_historical"] is True
    assert orders_body["date_from"] and orders_body["date_to"]

    # And every triggered message references a sync_log row created with
    # metadata.triggered_by == 'initial_sync'.
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(SyncLogORM).where(
                SyncLogORM.id.in_(
                    [
                        products_msgs[0]["sync_log_id"],
                        orders_msgs[0]["sync_log_id"],
                        stock_msgs[0]["sync_log_id"],
                    ]
                )
            )
        )
        rows = list(result.scalars().all())
    assert len(rows) == 3
    for row in rows:
        assert (row.sync_metadata or {}).get("triggered_by") == "initial_sync"


@pytest_asyncio.fixture
async def seeded_one_product(live_db: None) -> AsyncIterator[None]:
    pid = 99500001
    async with AsyncSessionLocal() as session:
        # Drop any previous sentinel.
        await session.execute(delete(ProductORM).where(ProductORM.tiny_id == pid))
        session.add(
            ProductORM(
                tiny_id=pid,
                sku="STAGE12-NONEMPTY",
                description="seeded for stage12 initial-sync test",
                type="P",
                situation="A",
                synced_at=datetime.now(UTC),
                prices={},
            )
        )
        await session.commit()
    yield
    async with AsyncSessionLocal() as session:
        await session.execute(delete(ProductORM).where(ProductORM.tiny_id == pid))
        await session.commit()


async def test_initial_sync_does_not_trigger_when_products_exist(
    scheduler_app: FastAPI,
    live_rabbitmq: QueuePublisher,
    seeded_one_product: None,
) -> None:
    for queue_name in (
        "tiny.sync.products.full",
        "tiny.sync.orders.full",
        "tiny.sync.stock.full",
    ):
        await _drain(queue_name)

    await check_and_trigger_initial_sync(scheduler_app)

    assert await _drain("tiny.sync.products.full") == []
    assert await _drain("tiny.sync.orders.full") == []
    assert await _drain("tiny.sync.stock.full") == []


# Touch jobs_module so the import is exercised (otherwise unused above).
_ = jobs_module
