"""Unit tests for the supplier-entry pass of the invoice sync.

Tiny filters ``/notas`` by issue date, and a supplier NF is registered only
when the goods arrive, so the 2-day incremental window missed most of them.
Tiny and DB are mocked.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.services import invoice_sync_service as mod
from tiny_mirror.services.invoice_sync_service import InvoiceSyncService

pytestmark = pytest.mark.unit


def _service(tiny: Any | None = None) -> InvoiceSyncService:
    return InvoiceSyncService(tiny_client=tiny or MagicMock(), queue_publisher=MagicMock())


@asynccontextmanager
async def _dummy_session() -> Any:
    yield MagicMock()


async def test_incremental_also_rereads_supplier_entries_of_last_90_days() -> None:
    service = _service()
    calls: list[tuple[date, date, dict[str, Any]]] = []

    async def fake_range(date_from: date, date_to: date, _log: Any, **kwargs: Any) -> int:
        calls.append((date_from, date_to, kwargs))
        return 3 if kwargs else 10

    service.run_date_range_sync = fake_range  # type: ignore[method-assign]
    sync_logs = MagicMock()
    sync_logs.update_sync_log_complete = AsyncMock()

    with (
        patch.object(mod, "AsyncSessionLocal", _dummy_session),
        patch.object(mod, "SyncLogRepository", return_value=sync_logs),
    ):
        await service.run_incremental_sync(sync_log_id=7)

    recent, supplier = calls
    assert recent[1] - recent[0] == timedelta(days=mod.INCREMENTAL_LOOKBACK_DAYS)
    assert recent[2] == {}
    assert supplier[1] - supplier[0] == timedelta(days=mod.SUPPLIER_ENTRY_LOOKBACK_DAYS)
    assert supplier[2] == {
        "invoice_type": "E",
        "status": "8",
        "items_only_when_missing": True,
    }
    sync_logs.update_sync_log_complete.assert_awaited_once_with(
        7, items_processed=13, items_failed=0
    )


async def test_list_invoices_sends_type_and_status_filters() -> None:
    client = TinyAPIClient.__new__(TinyAPIClient)
    client._request = AsyncMock(return_value={"itens": []})  # type: ignore[method-assign]

    await client.list_invoices(
        date_initial=date(2026, 8, 1),
        date_final=date(2026, 11, 18),
        limit=100,
        offset=200,
        invoice_type="E",
        status="8",
    )

    client._request.assert_awaited_once_with(
        "GET",
        "/notas",
        params={
            "limit": 100,
            "offset": 200,
            "dataInicial": "2026-08-01",
            "dataFinal": "2026-11-18",
            "tipo": "E",
            "situacao": "8",
        },
    )


async def test_list_invoices_omits_filters_when_not_given() -> None:
    client = TinyAPIClient.__new__(TinyAPIClient)
    client._request = AsyncMock(return_value={"itens": []})  # type: ignore[method-assign]

    await client.list_invoices(limit=1)

    client._request.assert_awaited_once_with("GET", "/notas", params={"limit": 1, "offset": 0})


async def test_supplier_pass_fetches_items_only_for_nfs_without_lines() -> None:
    tiny = MagicMock()
    tiny.list_invoices = AsyncMock(
        return_value={"itens": [{"id": 1}, {"id": 2}], "paginacao": {"total": 2}}
    )
    service = _service(tiny)
    fetched: list[int] = []

    async def fake_items(tiny_id: int) -> int:
        fetched.append(tiny_id)
        return 1

    service.sync_items_for_invoice = fake_items  # type: ignore[method-assign]
    repo = MagicMock()
    repo.upsert_batch = AsyncMock(return_value=2)

    async def without_items(invoices: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [inv for inv in invoices if inv["tiny_id"] == 2]

    with (
        patch.object(mod, "AsyncSessionLocal", _dummy_session),
        patch.object(mod, "PostgreSQLInvoiceRepository", return_value=repo),
        patch.object(mod, "SyncLogRepository", return_value=MagicMock()),
        patch.object(mod.InvoiceMapper, "from_tiny_api", lambda item: {"tiny_id": item["id"]}),
        patch.object(mod, "_without_items", without_items),
    ):
        total = await service.sync_supplier_entries(
            date(2026, 8, 20), date(2026, 11, 18), sync_log_id=None
        )

    assert total == 2
    assert fetched == [2]
    kwargs = tiny.list_invoices.await_args.kwargs
    assert kwargs["invoice_type"] == "E"
    assert kwargs["status"] == "8"
    assert kwargs["date_initial"] == date(2026, 8, 20)


async def test_regular_window_still_fetches_items_for_every_nf() -> None:
    tiny = MagicMock()
    tiny.list_invoices = AsyncMock(
        return_value={"itens": [{"id": 1}, {"id": 2}], "paginacao": {"total": 2}}
    )
    service = _service(tiny)
    fetched: list[int] = []

    async def fake_items(tiny_id: int) -> int:
        fetched.append(tiny_id)
        return 1

    service.sync_items_for_invoice = fake_items  # type: ignore[method-assign]
    repo = MagicMock()
    repo.upsert_batch = AsyncMock(return_value=2)
    filter_spy = AsyncMock()

    with (
        patch.object(mod, "AsyncSessionLocal", _dummy_session),
        patch.object(mod, "PostgreSQLInvoiceRepository", return_value=repo),
        patch.object(mod, "SyncLogRepository", return_value=MagicMock()),
        patch.object(mod.InvoiceMapper, "from_tiny_api", lambda item: {"tiny_id": item["id"]}),
        patch.object(mod, "_without_items", filter_spy),
    ):
        await service.run_date_range_sync(date(2026, 11, 16), date(2026, 11, 18), None)

    assert fetched == [1, 2]
    filter_spy.assert_not_awaited()
    assert tiny.list_invoices.await_args.kwargs["invoice_type"] is None


async def test_without_items_drops_nfs_that_already_have_lines() -> None:
    @asynccontextmanager
    async def session_factory() -> Any:
        session = MagicMock()
        result = MagicMock()
        result.all.return_value = [(1,)]
        session.execute = AsyncMock(return_value=result)
        yield session

    with patch.object(mod, "AsyncSessionLocal", session_factory):
        kept = await mod._without_items([{"tiny_id": 1}, {"tiny_id": 2}])

    assert kept == [{"tiny_id": 2}]


async def test_without_items_skips_query_for_empty_page() -> None:
    with patch.object(mod, "AsyncSessionLocal", MagicMock(side_effect=AssertionError)):
        assert await mod._without_items([]) == []
