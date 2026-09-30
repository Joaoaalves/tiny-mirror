"""Unit tests for :class:`tiny_mirror.services.order_sync_service.OrderSyncService`.

Focus: the guards against bulk ``dataAtualizacao`` bumps (2026-09-30 — the
deletion of old NFs in Tiny re-listed ~20k 2025 orders as "updated today"
and flooded ``tiny.sync.orders.item`` with 44k messages):

- listing rows created before the cutoff are skipped (incremental + reconcile)
- an id already pending on the item queue is not published again
- processing an order releases its pending mark and freezes old NF links
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.dialects import postgresql

from tiny_mirror.config import settings
from tiny_mirror.infrastructure.repositories.order_repository import (
    PostgreSQLOrderRepository,
)
from tiny_mirror.services import order_sync_service as mod
from tiny_mirror.services.order_sync_service import (
    PENDING_KEY_PREFIX,
    OrderSyncService,
    _created_before,
)

pytestmark = pytest.mark.unit

TODAY = datetime.now(UTC).date()
RECENT = (TODAY - timedelta(days=1)).isoformat()
OLD = (TODAY - timedelta(days=settings.orders_sync_max_age_days + 30)).isoformat()


class FakeRedis:
    """Minimal async Redis double: SET NX EX + DELETE."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.set_calls: list[dict[str, Any]] = []

    async def set(self, key: str, value: str, *, nx: bool, ex: int) -> bool | None:
        self.set_calls.append({"key": key, "nx": nx, "ex": ex})
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    async def delete(self, key: str) -> int:
        return 1 if self.store.pop(key, None) is not None else 0


class BrokenRedis:
    async def set(self, *args: Any, **kwargs: Any) -> bool:
        raise ConnectionError("redis down")

    async def delete(self, *args: Any, **kwargs: Any) -> int:
        raise ConnectionError("redis down")


def _listing(*rows: tuple[int, str | None]) -> dict[str, Any]:
    itens = []
    for order_id, created in rows:
        row: dict[str, Any] = {"id": order_id, "situacao": 6}
        if created is not None:
            row["dataCriacao"] = created
        itens.append(row)
    return {"itens": itens, "paginacao": {"total": len(itens)}}


def _service(listing: dict[str, Any], redis_client: Any = None) -> OrderSyncService:
    tiny = MagicMock()
    tiny.list_orders = AsyncMock(return_value=listing)
    publisher = MagicMock()
    publisher.publish_sync_message = AsyncMock()
    service = OrderSyncService(
        tiny_client=tiny, queue_publisher=publisher, redis_client=redis_client
    )
    service._filter_new_order_ids = AsyncMock(side_effect=lambda ids: ids)  # type: ignore[method-assign]
    service._record_total_enqueued = AsyncMock()  # type: ignore[method-assign]
    return service


def _published_order_ids(service: OrderSyncService) -> list[int]:
    publish = service._publisher.publish_sync_message
    return [
        c.args[1]["order_tiny_id"] for c in publish.await_args_list if c.args[0] == "orders.item"
    ]


# ---------------------------------------------------------------------------
# _created_before (pure)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("row", "cutoff", "expected"),
    [
        ({"dataCriacao": "2025-11-24"}, date(2026, 7, 1), True),
        ({"dataCriacao": "2026-07-01"}, date(2026, 7, 1), False),
        ({"dataCriacao": "2026-09-30 10:12:00"}, date(2026, 7, 1), False),
        ({"dataCriacao": "2025-11-24"}, None, False),
        ({}, date(2026, 7, 1), False),
        ({"dataCriacao": None}, date(2026, 7, 1), False),
        ({"dataCriacao": "24/11/2025"}, date(2026, 7, 1), False),
        ({"dataCriacao": "0000-00-00"}, date(2026, 7, 1), False),
    ],
)
def test_created_before(row: dict[str, Any], cutoff: date | None, expected: bool) -> None:
    assert _created_before(row, cutoff) is expected


def test_cutoff_date_follows_setting() -> None:
    assert OrderSyncService._cutoff_date() == TODAY - timedelta(
        days=settings.orders_sync_max_age_days
    )


# ---------------------------------------------------------------------------
# Fan-out: age cutoff
# ---------------------------------------------------------------------------
async def test_incremental_skips_orders_created_before_cutoff() -> None:
    service = _service(_listing((1, OLD), (2, RECENT), (3, OLD), (4, None)))

    await service.run_incremental_sync(sync_log_id=7)

    assert _published_order_ids(service) == [2, 4]
    service._filter_new_order_ids.assert_awaited_once_with([2, 4])
    service._record_total_enqueued.assert_awaited_once_with(7, 2)


async def test_reconciliation_skips_old_orders_but_refetches_known_recent_ones() -> None:
    service = _service(_listing((1, OLD), (2, RECENT)))

    await service.run_reconciliation_sync(TODAY - timedelta(days=1), sync_log_id=9)

    # skip_existing=False: the DB filter is bypassed, only the age cut applies.
    service._filter_new_order_ids.assert_not_awaited()
    assert _published_order_ids(service) == [2]
    service._record_total_enqueued.assert_awaited_once_with(9, 1)


async def test_date_range_backfill_is_not_age_filtered() -> None:
    service = _service(_listing((1, OLD), (2, RECENT)))

    await service.run_date_range_sync(date(2025, 11, 1), date(2025, 11, 8), sync_log_id=3)

    assert _published_order_ids(service) == [1, 2]


async def test_page_of_only_old_orders_still_paginates_to_the_end() -> None:
    full_old_page = {
        "itens": [{"id": i, "dataCriacao": OLD} for i in range(100)],
        "paginacao": {"total": 101},
    }
    last_page = _listing((500, RECENT))
    service = _service(full_old_page)
    service._tiny.list_orders = AsyncMock(side_effect=[full_old_page, last_page])

    await service.run_incremental_sync(sync_log_id=1)

    assert service._tiny.list_orders.await_count == 2
    assert _published_order_ids(service) == [500]


# ---------------------------------------------------------------------------
# Fan-out: pending-id dedupe
# ---------------------------------------------------------------------------
async def test_pending_order_is_not_published_twice_across_runs() -> None:
    redis = FakeRedis()
    service = _service(_listing((10, RECENT), (11, RECENT)), redis_client=redis)

    await service.run_incremental_sync(sync_log_id=1)
    await service.run_incremental_sync(sync_log_id=2)

    assert _published_order_ids(service) == [10, 11]
    assert set(redis.store) == {f"{PENDING_KEY_PREFIX}10", f"{PENDING_KEY_PREFIX}11"}
    assert all(
        c["nx"] and c["ex"] == settings.orders_item_pending_ttl_seconds for c in redis.set_calls
    )


async def test_pending_claim_fails_open_when_redis_is_down() -> None:
    service = _service(_listing((10, RECENT)), redis_client=BrokenRedis())

    await service.run_incremental_sync(sync_log_id=1)

    assert _published_order_ids(service) == [10]


async def test_publish_failure_releases_the_pending_claim() -> None:
    redis = FakeRedis()
    service = _service(_listing((10, RECENT)), redis_client=redis)
    service._publisher.publish_sync_message = AsyncMock(side_effect=RuntimeError("amqp down"))

    with pytest.raises(RuntimeError):
        await service.run_incremental_sync(sync_log_id=1)

    assert redis.store == {}


async def test_no_redis_client_disables_dedupe() -> None:
    service = _service(_listing((10, RECENT)))

    await service.run_incremental_sync(sync_log_id=1)
    await service.run_incremental_sync(sync_log_id=2)

    assert _published_order_ids(service) == [10, 10]


# ---------------------------------------------------------------------------
# process_order_item
# ---------------------------------------------------------------------------
@asynccontextmanager
async def _fake_session() -> Any:
    yield MagicMock()


async def test_process_order_item_releases_pending_and_freezes_old_nf_links() -> None:
    redis = FakeRedis()
    redis.store[f"{PENDING_KEY_PREFIX}42"] = "1"
    tiny = MagicMock()
    tiny.get_order = AsyncMock(
        return_value={
            "id": 42,
            "numeroPedido": 1001,
            "situacao": 6,
            "data": "2025-11-24",
            "idNotaFiscal": 0,
            "itens": [],
        }
    )
    service = OrderSyncService(tiny_client=tiny, queue_publisher=MagicMock(), redis_client=redis)
    orders_repo = MagicMock()
    orders_repo.upsert = AsyncMock(return_value="updated")
    orders_repo.upsert_items = AsyncMock()
    sync_logs = MagicMock()
    sync_logs.increment_processed = AsyncMock()
    sync_logs.try_finalize = AsyncMock()

    with (
        patch.object(mod, "AsyncSessionLocal", _fake_session),
        patch.object(mod, "PostgreSQLOrderRepository", return_value=orders_repo),
        patch.object(mod, "SyncLogRepository", return_value=sync_logs),
    ):
        await service.process_order_item(42, sync_log_id=5)

    assert redis.store == {}
    orders_repo.upsert.assert_awaited_once()
    assert orders_repo.upsert.await_args.kwargs == {
        "invoice_link_frozen_before": OrderSyncService._cutoff_date()
    }
    assert orders_repo.upsert.await_args.args[0]["invoice_id"] == 0


# ---------------------------------------------------------------------------
# Repository: frozen NF link SQL
# ---------------------------------------------------------------------------
def _order_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "tiny_id": 42,
        "order_number": 1001,
        "invoice_id": 0,
        "invoice_date": None,
        "customer": {},
        "situation": 6,
        "order_date": date(2025, 11, 24),
        "synced_at": datetime.now(UTC),
    }
    row.update(overrides)
    return row


async def _compiled_upsert(**kwargs: Any) -> str:
    session = MagicMock()
    result = MagicMock()
    result.scalar_one.return_value = False
    session.execute = AsyncMock(return_value=result)
    session.commit = AsyncMock()

    action = await PostgreSQLOrderRepository(session).upsert(_order_row(), **kwargs)

    assert action == "updated"
    stmt = session.execute.await_args.args[0]
    return str(stmt.compile(dialect=postgresql.dialect()))


async def test_upsert_freezes_invoice_link_for_old_orders() -> None:
    sql = await _compiled_upsert(invoice_link_frozen_before=date(2026, 7, 2))
    update_clause = sql.split("DO UPDATE SET", 1)[1]

    assert "invoice_id = CASE WHEN" in update_clause
    assert "orders.order_date <" in update_clause
    assert "orders.invoice_id > " in update_clause
    assert "excluded.invoice_id IS NULL OR excluded.invoice_id = " in update_clause
    assert "ELSE excluded.invoice_id END" in update_clause
    assert "invoice_date = CASE WHEN" in update_clause
    assert "ELSE excluded.invoice_date END" in update_clause


async def test_upsert_without_cutoff_overwrites_invoice_link() -> None:
    sql = await _compiled_upsert()
    update_clause = sql.split("DO UPDATE SET", 1)[1]

    assert "CASE" not in update_clause
    assert "invoice_id = excluded.invoice_id" in update_clause
