"""Unit tests for ManualStatusSyncService.

Covers the GAS-side payload parsing and the DB-side bulk-update flow.
The DB is mocked at the AsyncSession layer; we assert on the executed
SQL parameters rather than spinning up Postgres.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from tiny_mirror.services.manual_status_sync_service import (
    ManualStatusSyncError,
    ManualStatusSyncService,
)

pytestmark = pytest.mark.unit


def _mock_http(response_json: Any | None = None, status: int = 200) -> httpx.AsyncClient:
    mock = AsyncMock(spec=httpx.AsyncClient)
    resp = MagicMock()
    resp.status_code = status
    resp.text = json.dumps(response_json) if response_json is not None else ""
    resp.json = MagicMock(return_value=response_json)
    mock.get = AsyncMock(return_value=resp)
    return mock


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
            "SKU-E": {"status": "bogus", "row": 7},  # filtered out
            "SKU-F": "not-a-dict",  # filtered out
        },
    }
    http = _mock_http(payload)
    svc = ManualStatusSyncService(http, "https://script/exec", "tok")

    result = await svc.fetch()

    assert result == {
        "SKU-A": "queima",
        "SKU-B": "analise",
        "SKU-C": "normal",
        "SKU-D": "queima",
    }


@pytest.mark.asyncio
async def test_fetch_rejects_empty_config() -> None:
    svc = ManualStatusSyncService(_mock_http({}), "", "")
    with pytest.raises(ManualStatusSyncError, match="not configured"):
        await svc.fetch()


@pytest.mark.asyncio
async def test_fetch_raises_on_http_error() -> None:
    http = _mock_http({"oops": True}, status=500)
    svc = ManualStatusSyncService(http, "https://script/exec", "tok")
    with pytest.raises(ManualStatusSyncError, match="HTTP 500"):
        await svc.fetch()


@pytest.mark.asyncio
async def test_fetch_raises_on_payload_error_field() -> None:
    http = _mock_http({"error": "unauthorized"})
    svc = ManualStatusSyncService(http, "https://script/exec", "tok")
    with pytest.raises(ManualStatusSyncError, match="unauthorized"):
        await svc.fetch()


@pytest.mark.asyncio
async def test_fetch_raises_on_missing_skus_field() -> None:
    http = _mock_http({"counts": {}})
    svc = ManualStatusSyncService(http, "https://script/exec", "tok")
    with pytest.raises(ManualStatusSyncError, match="missing 'skus'"):
        await svc.fetch()


@pytest.mark.asyncio
async def test_fetch_wraps_request_errors() -> None:
    http = AsyncMock(spec=httpx.AsyncClient)
    http.get = AsyncMock(side_effect=httpx.ConnectError("down"))
    svc = ManualStatusSyncService(http, "https://script/exec", "tok")
    with pytest.raises(ManualStatusSyncError, match="request failed"):
        await svc.fetch()


@pytest.mark.asyncio
async def test_apply_buckets_updates_per_status_and_clears_others() -> None:
    """Apply must issue one UPDATE per status bucket (queima, analise, normal)
    plus one clearing UPDATE for SKUs no longer marked. Verify each bucket
    receives the SKU list it owns.
    """
    statuses = {
        "SKU-A": "queima",
        "SKU-B": "queima",
        "SKU-C": "analise",
        "SKU-D": "normal",
    }
    # Each UPDATE returns the matched SKU list as 1-tuples to mimic
    # `RETURNING sku`. Final clearing returns an empty set.
    matched_per_call = [
        [("SKU-A",), ("SKU-B",)],  # queima
        [("SKU-C",)],  # analise
        [("SKU-D",)],  # normal
        [],  # cleared
    ]

    session = MagicMock()
    session.commit = AsyncMock()

    captured: list[tuple[str, dict[str, Any]]] = []
    call_index = {"i": 0}

    async def fake_execute(stmt: Any, params: dict[str, Any]) -> Any:
        # Record the SQL text and params for assertions.
        captured.append((str(stmt), dict(params)))
        rows = matched_per_call[call_index["i"]]
        call_index["i"] += 1
        result = MagicMock()
        result.fetchall = MagicMock(return_value=rows)
        return result

    session.execute = AsyncMock(side_effect=fake_execute)

    svc = ManualStatusSyncService(_mock_http({}), "https://script/exec", "tok")
    stats = await svc.apply(session, statuses)

    assert stats == {
        "queima": 2,
        "analise": 1,
        "normal": 1,
        "cleared": 0,
        "unmatched_in_db": 0,
    }
    # Verify each per-status UPDATE got the right SKU list.
    by_status = {p["status"]: p["skus"] for sql, p in captured if "status" in p}
    assert sorted(by_status["queima"]) == ["SKU-A", "SKU-B"]
    assert by_status["analise"] == ["SKU-C"]
    assert by_status["normal"] == ["SKU-D"]
    # The final clearing UPDATE excludes all matched SKUs.
    clear_call = captured[-1][1]
    assert sorted(clear_call["matched"]) == ["SKU-A", "SKU-B", "SKU-C", "SKU-D"]
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_apply_counts_unmatched_skus() -> None:
    """SKUs present in the GAS payload but missing from the products table
    should not raise; they must show up in the unmatched_in_db stat.
    """
    statuses = {"SKU-NEW": "queima", "SKU-EXISTING": "analise"}

    matched_per_call = [
        [],  # queima — SKU-NEW not in DB
        [("SKU-EXISTING",)],  # analise
        [],  # cleared
    ]
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

    svc = ManualStatusSyncService(_mock_http({}), "https://script/exec", "tok")
    stats = await svc.apply(session, statuses)

    assert stats["unmatched_in_db"] == 1
    assert stats["queima"] == 0
    assert stats["analise"] == 1


@pytest.mark.asyncio
async def test_apply_empty_statuses_is_noop() -> None:
    session = MagicMock()
    session.commit = AsyncMock()
    session.execute = AsyncMock()
    svc = ManualStatusSyncService(_mock_http({}), "https://script/exec", "tok")
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
