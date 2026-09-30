"""Unit tests for InvoiceSyncService.backfill_missing_items.

The 2026-05-07 cold start stored NF headers only; this fills the lines of
the NFs that have none (the DASH needs return NFs per SKU). Tiny and DB are
mocked.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.dialects import postgresql

from tiny_mirror.exceptions import TinyNotFoundException
from tiny_mirror.services import invoice_sync_service as mod
from tiny_mirror.services.invoice_sync_service import InvoiceSyncService

pytestmark = pytest.mark.unit


def _session_returning(ids: list[int], captured: list[Any]) -> Any:
    @asynccontextmanager
    async def factory() -> Any:
        session = MagicMock()
        result = MagicMock()
        result.all.return_value = [(i,) for i in ids]

        async def execute(stmt: Any) -> Any:
            captured.append(stmt)
            return result

        session.execute = execute
        yield session

    return factory


def _service() -> InvoiceSyncService:
    return InvoiceSyncService(tiny_client=MagicMock(), queue_publisher=MagicMock())


async def test_fills_lines_and_counts_gone_and_failed() -> None:
    service = _service()
    outcomes: dict[int, Any] = {
        3: 2,
        2: TinyNotFoundException("Nota fiscal não encontrada", "invoice", 2),
        1: RuntimeError("timeout"),
    }

    async def fake_sync(tiny_id: int) -> int:
        outcome = outcomes[tiny_id]
        if isinstance(outcome, Exception):
            raise outcome
        return int(outcome)

    service.sync_items_for_invoice = fake_sync  # type: ignore[method-assign]

    with patch.object(mod, "AsyncSessionLocal", _session_returning([3, 2, 1], [])):
        stats = await service.backfill_missing_items(origin_type="devolucao")

    assert stats == {"candidates": 3, "filled": 1, "lines": 2, "gone_in_tiny": 1, "failed": 1}


async def test_query_selects_only_nfs_without_lines_newest_first() -> None:
    captured: list[Any] = []
    service = _service()
    service.sync_items_for_invoice = AsyncMock(return_value=1)  # type: ignore[method-assign]

    with patch.object(mod, "AsyncSessionLocal", _session_returning([], captured)):
        await service.backfill_missing_items(
            origin_type="devolucao", date_from=date(2026, 1, 1), date_to=date(2026, 4, 30), limit=50
        )

    sql = str(
        captured[0].compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    )
    assert (
        "NOT (EXISTS (SELECT" in sql and "invoice_items.invoice_tiny_id = invoices.tiny_id" in sql
    )
    assert "invoices.origin_type = 'devolucao'" in sql
    assert "invoices.issue_date >= '2026-01-01'" in sql
    assert "invoices.issue_date <= '2026-04-30'" in sql
    assert "ORDER BY invoices.issue_date DESC" in sql
    assert "LIMIT 50" in sql


async def test_no_filters_means_every_type() -> None:
    captured: list[Any] = []
    service = _service()

    with patch.object(mod, "AsyncSessionLocal", _session_returning([], captured)):
        stats = await service.backfill_missing_items()

    sql = str(captured[0].compile(dialect=postgresql.dialect()))
    assert "origin_type" not in sql.split("WHERE", 1)[1]
    assert stats["candidates"] == 0
