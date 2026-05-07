"""End-to-end coverage for invoice sync (NF synchronization).

Tests assume:
- live Postgres / Redis / RabbitMQ via docker-compose
- a working refresh token in .env (TokenService bootstraps automatically)
"""

from __future__ import annotations

import json
import math
from datetime import UTC, date, datetime

import pytest
from sqlalchemy import select

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.infrastructure.orm.models import SyncLogORM
from tiny_mirror.infrastructure.repositories.invoice_repository import (
    PostgreSQLInvoiceRepository,
)
from tiny_mirror.infrastructure.repositories.sync_log_repository import SyncLogRepository
from tiny_mirror.mappers.invoice_mapper import InvoiceMapper
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.rabbitmq import get_channel
from tiny_mirror.services.invoice_sync_service import (
    COLD_START_FROM,
    COLD_START_WINDOW_DAYS,
    InvoiceSyncService,
)

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# TinyAPIClient — /notas endpoint
# ---------------------------------------------------------------------------
async def test_list_invoices_returns_valid_structure(
    live_tiny_client: TinyAPIClient,
) -> None:
    response = await live_tiny_client.list_invoices(limit=1)

    assert "itens" in response and isinstance(response["itens"], list)
    assert "paginacao" in response
    assert (
        response["paginacao"].get("total", 0) >= 1
    ), "expected at least one invoice in the live Tiny account"

    item = response["itens"][0]
    for required in ("id", "numero", "situacao", "dataEmissao"):
        assert required in item, f"missing field {required!r} in /notas list item"


async def test_list_invoices_date_filter_returns_subset(
    live_tiny_client: TinyAPIClient,
) -> None:
    today = datetime.now(UTC).date()
    date_from = date(today.year, today.month, 1)

    response = await live_tiny_client.list_invoices(
        date_initial=date_from,
        date_final=today,
        limit=5,
    )

    assert "itens" in response
    assert "paginacao" in response


# ---------------------------------------------------------------------------
# InvoiceMapper — real API payload
# ---------------------------------------------------------------------------
async def test_invoice_mapper_translates_real_payload(
    live_tiny_client: TinyAPIClient,
) -> None:
    response = await live_tiny_client.list_invoices(limit=1)
    items = response.get("itens") or []
    if not items:
        pytest.skip("no invoices in Tiny account")

    raw = items[0]
    mapped = InvoiceMapper.from_tiny_api(raw)

    assert mapped["tiny_id"] == int(raw["id"])
    assert mapped["number"] == str(raw.get("numero") or "")
    assert isinstance(mapped["issue_date"], date)
    assert isinstance(mapped["synced_at"], datetime)
    assert mapped["synced_at"].tzinfo is not None
    # No raw Portuguese keys should survive mapping.
    for pt_key in ("situacao", "dataEmissao", "chaveAcesso", "valorFrete"):
        assert pt_key not in mapped, f"Portuguese key {pt_key!r} must not appear in mapped dict"


# ---------------------------------------------------------------------------
# PostgreSQLInvoiceRepository — persistence + upsert idempotency
# ---------------------------------------------------------------------------
async def test_upsert_batch_persists_to_db(
    live_tiny_client: TinyAPIClient,
    live_db: None,
) -> None:
    response = await live_tiny_client.list_invoices(limit=2)
    items = response.get("itens") or []
    if not items:
        pytest.skip("no invoices in Tiny account")

    invoices = [InvoiceMapper.from_tiny_api(item) for item in items]

    async with AsyncSessionLocal() as session:
        repo = PostgreSQLInvoiceRepository(session)
        count = await repo.upsert_batch(invoices)

    assert count == len(invoices)

    async with AsyncSessionLocal() as session:
        repo = PostgreSQLInvoiceRepository(session)
        row = await repo.get_by_tiny_id(invoices[0]["tiny_id"])

    assert row is not None
    assert row["tiny_id"] == invoices[0]["tiny_id"]
    assert row["number"] == invoices[0]["number"]


async def test_upsert_batch_is_idempotent(
    live_tiny_client: TinyAPIClient,
    live_db: None,
) -> None:
    response = await live_tiny_client.list_invoices(limit=1)
    items = response.get("itens") or []
    if not items:
        pytest.skip("no invoices in Tiny account")

    invoices = [InvoiceMapper.from_tiny_api(items[0])]
    tiny_id = invoices[0]["tiny_id"]

    async with AsyncSessionLocal() as session:
        await PostgreSQLInvoiceRepository(session).upsert_batch(invoices)

    async with AsyncSessionLocal() as session:
        repo = PostgreSQLInvoiceRepository(session)
        count_before = await repo.count()
        await repo.upsert_batch(invoices)
        count_after = await repo.count()

    assert count_after == count_before, "second upsert must not duplicate rows"

    async with AsyncSessionLocal() as session:
        row = await PostgreSQLInvoiceRepository(session).get_by_tiny_id(tiny_id)
    assert row is not None


# ---------------------------------------------------------------------------
# InvoiceSyncService — incremental path
# ---------------------------------------------------------------------------
async def test_run_incremental_sync_upserts_recent_invoices(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
    live_db: None,
) -> None:
    service = InvoiceSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)

    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("invoices")

    await service.run_incremental_sync(sync_log_id)

    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(select(SyncLogORM).where(SyncLogORM.id == sync_log_id))
        ).scalar_one()

    assert row.status == "completed"
    assert row.items_processed >= 0


async def test_run_incremental_sync_closes_sync_log(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
    live_db: None,
) -> None:
    service = InvoiceSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)

    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("invoices")

    await service.run_incremental_sync(sync_log_id)

    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(select(SyncLogORM).where(SyncLogORM.id == sync_log_id))
        ).scalar_one()

    assert row.status == "completed", "incremental sync must close the sync_log"
    assert row.completed_at is not None


# ---------------------------------------------------------------------------
# InvoiceSyncService — date range worker
# ---------------------------------------------------------------------------
async def test_run_date_range_sync_returns_upsert_count(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
    live_db: None,
) -> None:
    today = datetime.now(UTC).date()
    date_from = date(today.year, today.month, 1)

    service = InvoiceSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)
    total = await service.run_date_range_sync(date_from, today, sync_log_id=None)

    assert isinstance(total, int)
    assert total >= 0

    # Must match what list_invoices reports for the same window.
    head = await live_tiny_client.list_invoices(
        date_initial=date_from,
        date_final=today,
        limit=1,
    )
    api_total = int(head.get("paginacao", {}).get("total", 0))
    assert total == api_total


# ---------------------------------------------------------------------------
# InvoiceSyncService — cold start fan-out
# ---------------------------------------------------------------------------
async def test_run_cold_start_publishes_correct_window_count(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
    live_db: None,
) -> None:
    """run_cold_start must publish exactly ceil((today - COLD_START_FROM) / 30) messages."""
    channel = get_channel()
    invoices_q = await channel.get_queue("tiny.sync.invoices.full")
    while True:
        leftover = await invoices_q.get(no_ack=True, fail=False)
        if leftover is None:
            break

    service = InvoiceSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)

    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("invoices")

    await service.run_cold_start(sync_log_id)

    today = datetime.now(UTC).date()
    total_days = (today - COLD_START_FROM).days
    expected_windows = math.ceil(total_days / COLD_START_WINDOW_DAYS)

    drained = 0
    while True:
        msg = await invoices_q.get(no_ack=True, fail=False)
        if msg is None:
            break
        body = json.loads(msg.body.decode("utf-8"))
        assert body.get("is_cold_start_window") is True
        assert body.get("date_from") and body.get("date_to")
        assert body.get("sync_log_id") == sync_log_id
        drained += 1

    assert drained == expected_windows


async def test_run_cold_start_sets_total_enqueued_to_window_count(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
    live_db: None,
) -> None:
    """total_enqueued must equal the number of windows, not the number of NFs.

    This is the race-free design: each window increments by 1 on completion,
    so try_finalize closes the log once all windows are done regardless of
    new NFs created during the ~15-minute cold start.
    """
    service = InvoiceSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)

    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("invoices")

    await service.run_cold_start(sync_log_id)

    today = datetime.now(UTC).date()
    total_days = (today - COLD_START_FROM).days
    expected_windows = math.ceil(total_days / COLD_START_WINDOW_DAYS)

    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(select(SyncLogORM).where(SyncLogORM.id == sync_log_id))
        ).scalar_one()

    metadata = row.sync_metadata or {}
    assert metadata.get("total_enqueued") == expected_windows


# ---------------------------------------------------------------------------
# InvoiceSyncService — finalize_cold_start_window
# ---------------------------------------------------------------------------
async def test_finalize_cold_start_window_increments_processed(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
    live_db: None,
) -> None:
    service = InvoiceSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)

    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log(
            "invoices", metadata={"total_enqueued": 3}
        )

    await service.finalize_cold_start_window(sync_log_id)

    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(select(SyncLogORM).where(SyncLogORM.id == sync_log_id))
        ).scalar_one()

    assert row.items_processed == 1
    assert row.status == "running", "log must stay running until all 3 windows complete"


async def test_finalize_cold_start_window_closes_log_on_last_window(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
    live_db: None,
) -> None:
    service = InvoiceSyncService(tiny_client=live_tiny_client, queue_publisher=live_rabbitmq)

    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log(
            "invoices", metadata={"total_enqueued": 1}
        )

    await service.finalize_cold_start_window(sync_log_id)

    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(select(SyncLogORM).where(SyncLogORM.id == sync_log_id))
        ).scalar_one()

    assert row.status == "completed", "log must close when the only window finishes"
    assert row.completed_at is not None
