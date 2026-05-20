"""Unit tests for ManualStatusSyncService (post GASClient refactor).

The GAS HTTP plumbing is now isolated in GASClient — these tests inject
a fake GASClient and assert the service's parsing / DB-update behaviour.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from tiny_mirror.services.gas_client import GASClientError
from tiny_mirror.services.manual_status_sync_service import (
    ManualStatusSyncError,
    ManualStatusSyncService,
)

pytestmark = pytest.mark.unit


def _fake_gas(payload: Any = None, error: str | None = None) -> Any:
    """Build a GASClient stand-in whose ``manual_status()`` returns
    ``payload`` or raises ``GASClientError(error)``.
    """
    gas = MagicMock()
    if error is not None:
        gas.manual_status = AsyncMock(side_effect=GASClientError(error))
    else:
        gas.manual_status = AsyncMock(return_value=payload)
    return gas


@pytest.mark.asyncio
async def test_fetch_parses_payload_and_filters_invalid_statuses() -> None:
    payload = {
        "generatedAt": "2026-05-19T20:30:00Z",
        "sheet": "GERAL",
        "counts": {"queima": 2, "analise": 1, "normal": 1},
        "skus": {
            "SKU-A": {"status": "queima", "row": 3},
            "SKU-B": {"status": "analise", "row": 4},
            "SKU-C": {"status": "normal", "row": 5},
            "SKU-D": {"status": "queima", "row": 6},
            "SKU-E": {"status": "bogus", "row": 7},
            "SKU-F": "not-a-dict",
        },
    }
    svc = ManualStatusSyncService(gas=_fake_gas(payload))
    result = await svc.fetch()
    assert result == {
        "SKU-A": "queima",
        "SKU-B": "analise",
        "SKU-C": "normal",
        "SKU-D": "queima",
    }


@pytest.mark.asyncio
async def test_fetch_propagates_gas_client_error() -> None:
    svc = ManualStatusSyncService(gas=_fake_gas(error="unauthorized"))
    with pytest.raises(ManualStatusSyncError, match="unauthorized"):
        await svc.fetch()


@pytest.mark.asyncio
async def test_fetch_raises_on_missing_skus_field() -> None:
    svc = ManualStatusSyncService(gas=_fake_gas({"counts": {}}))
    with pytest.raises(ManualStatusSyncError, match="missing 'skus'"):
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

    svc = ManualStatusSyncService(gas=_fake_gas({}))
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

    svc = ManualStatusSyncService(gas=_fake_gas({}))
    stats = await svc.apply(session, statuses)
    assert stats["unmatched_in_db"] == 1
    assert stats["queima"] == 0
    assert stats["analise"] == 1


@pytest.mark.asyncio
async def test_apply_empty_statuses_is_noop() -> None:
    session = MagicMock()
    session.commit = AsyncMock()
    session.execute = AsyncMock()
    svc = ManualStatusSyncService(gas=_fake_gas({}))
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
