"""Unit tests for ManualStatusSyncService.

The status source is now injected (``SheetsManualStatusFetcher`` in prod) —
these tests use a fake source and assert the service's validation / DB-update
behaviour. Parsing of the spreadsheet itself is covered by
``test_sheets_manual_status.py``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from tiny_mirror.services.manual_status_sync_service import (
    ManualStatusSyncError,
    ManualStatusSyncService,
)
from tiny_mirror.services.sheets_manual_status import SheetsManualStatusError

pytestmark = pytest.mark.unit


def _fake_source(payload: Any = None, error: str | None = None) -> Any:
    """Source stand-in whose ``fetch_statuses()`` returns ``payload`` or raises."""
    src = MagicMock()
    if error is not None:
        src.fetch_statuses = AsyncMock(side_effect=SheetsManualStatusError(error))
    else:
        src.fetch_statuses = AsyncMock(return_value=payload)
    return src


@pytest.mark.asyncio
async def test_fetch_filters_invalid_statuses_and_blank_skus() -> None:
    svc = ManualStatusSyncService(
        source=_fake_source(
            {
                "SKU-A": "queima",
                "SKU-B": "analise",
                "SKU-C": "normal",
                "SKU-D": "bogus",  # status inválido
                "   ": "queima",  # sku vazio
                " SKU-E ": "queima",  # espaços são aparados
            }
        )
    )
    assert await svc.fetch() == {
        "SKU-A": "queima",
        "SKU-B": "analise",
        "SKU-C": "normal",
        "SKU-E": "queima",
    }


@pytest.mark.asyncio
async def test_fetch_propagates_source_error() -> None:
    svc = ManualStatusSyncService(source=_fake_source(error="sheets HTTP 403"))
    with pytest.raises(ManualStatusSyncError, match="403"):
        await svc.fetch()


@pytest.mark.asyncio
async def test_fetch_refuses_empty_payload() -> None:
    """Guarda-chuva: um payload vazio faria o apply() resetar TUDO pra 'normal'."""
    svc = ManualStatusSyncService(source=_fake_source({}))
    with pytest.raises(ManualStatusSyncError, match="no valid SKUs"):
        await svc.fetch()


@pytest.mark.asyncio
async def test_fetch_refuses_payload_with_only_invalid_statuses() -> None:
    svc = ManualStatusSyncService(source=_fake_source({"SKU-A": "bogus"}))
    with pytest.raises(ManualStatusSyncError, match="no valid SKUs"):
        await svc.fetch()


@pytest.mark.asyncio
async def test_apply_buckets_updates_per_status_and_clears_others() -> None:
    statuses = {
        "SKU-A": "queima",
        "SKU-B": "queima",
        "SKU-C": "analise",
        "SKU-D": "normal",
    }
    matched_per_call = [
        [("SKU-A",), ("SKU-B",)],
        [("SKU-C",)],
        [("SKU-D",)],
        [],
    ]
    session = MagicMock()
    session.commit = AsyncMock()
    captured: list[tuple[str, dict[str, Any]]] = []
    call_index = {"i": 0}

    async def fake_execute(stmt: Any, params: dict[str, Any]) -> Any:
        captured.append((str(stmt), dict(params)))
        rows = matched_per_call[call_index["i"]]
        call_index["i"] += 1
        result = MagicMock()
        result.fetchall = MagicMock(return_value=rows)
        return result

    session.execute = AsyncMock(side_effect=fake_execute)

    svc = ManualStatusSyncService(source=_fake_source({}))
    stats = await svc.apply(session, statuses)
    assert stats == {
        "queima": 2,
        "analise": 1,
        "normal": 1,
        "cleared": 0,
        "unmatched_in_db": 0,
    }
    by_status = {p["status"]: p["skus"] for _, p in captured if "status" in p}
    assert sorted(by_status["queima"]) == ["SKU-A", "SKU-B"]
    assert by_status["analise"] == ["SKU-C"]
    assert by_status["normal"] == ["SKU-D"]
    clear_call = captured[-1][1]
    assert sorted(clear_call["matched"]) == ["SKU-A", "SKU-B", "SKU-C", "SKU-D"]
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_apply_counts_unmatched_skus() -> None:
    statuses = {"SKU-NEW": "queima", "SKU-EXISTING": "analise"}
    matched_per_call = [[], [("SKU-EXISTING",)], []]
    session = MagicMock()
    session.commit = AsyncMock()
    call_index = {"i": 0}

    async def fake_execute(stmt: Any, params: dict[str, Any]) -> Any:
        rows = matched_per_call[call_index["i"]]
        call_index["i"] += 1
        result = MagicMock()
        result.fetchall = MagicMock(return_value=rows)
        return result

    session.execute = AsyncMock(side_effect=fake_execute)

    svc = ManualStatusSyncService(source=_fake_source({}))
    stats = await svc.apply(session, statuses)
    assert stats["unmatched_in_db"] == 1
    assert stats["queima"] == 0
    assert stats["analise"] == 1


@pytest.mark.asyncio
async def test_apply_empty_statuses_is_noop() -> None:
    session = MagicMock()
    session.commit = AsyncMock()
    session.execute = AsyncMock()
    svc = ManualStatusSyncService(source=_fake_source({}))
    stats = await svc.apply(session, {})
    assert stats == {
        "queima": 0,
        "analise": 0,
        "normal": 0,
        "cleared": 0,
        "unmatched_in_db": 0,
    }
    session.execute.assert_not_called()
    session.commit.assert_not_called()
