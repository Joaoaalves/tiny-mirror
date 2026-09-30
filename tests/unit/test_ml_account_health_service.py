"""Unit tests for :mod:`tiny_mirror.services.ml_account_health_service`.

ML HTTP mocked (httpx.MockTransport), DB patched. Payload shapes mirror live
/users/{id} and /moderations/infractions/{id} responses read on 2026-09-30.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from tiny_mirror.services import ml_account_health_service as mod
from tiny_mirror.services.ml_account_health_service import (
    MLAccountHealthService,
    _ts,
    parse_infraction,
    parse_reputation,
)

pytestmark = pytest.mark.unit

USER = {
    "id": 227584372,
    "seller_reputation": {
        "level_id": "5_green",
        "power_seller_status": "platinum",
        "transactions": {
            "canceled": 3133,
            "completed": 59642,
            "period": "historic",
            "ratings": {"negative": 0.14, "neutral": 0.04, "positive": 0.82},
            "total": 62775,
        },
        "metrics": {
            "sales": {"period": "60 days", "completed": 9000},
            "claims": {"period": "60 days", "rate": 0.0098, "value": 90},
            "delayed_handling_time": {"period": "60 days", "rate": 0.001, "value": 9},
            "cancellations": {"period": "60 days", "rate": 0.002, "value": 18},
        },
    },
}


def _infraction(i: int) -> dict[str, Any]:
    return {
        "id": str(6709476328 - i),
        "date_created": "2026-09-29T14:02:34.453-0400",
        "element_id": f"MLB-ESP-{i}",
        "element_type": "ESP",
        "filter_subgroup": "PQT",
        "reason": "",
        "remedy": "",
        "related_item_id": "MLB3351786643" if i % 2 else None,
        "site_id": "MLB",
        "user_id": "227584372",
    }


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def test_parse_reputation_extracts_typed_columns() -> None:
    row = parse_reputation(USER, date(2026, 9, 30))

    assert row is not None
    assert row["snapshot_date"] == date(2026, 9, 30)
    assert row["level_id"] == "5_green"
    assert row["power_seller_status"] == "platinum"
    assert (
        row["transactions_total"],
        row["transactions_completed"],
        row["transactions_canceled"],
    ) == (
        62775,
        59642,
        3133,
    )
    assert row["rating_positive"] == Decimal("0.82")
    assert row["metrics"]["claims"]["value"] == 90
    assert row["raw"] is USER["seller_reputation"]


def test_parse_reputation_without_block_returns_none() -> None:
    assert parse_reputation({"id": 1}, date(2026, 9, 30)) is None


def test_parse_infraction_blanks_become_null() -> None:
    row = parse_infraction(_infraction(0))

    assert row is not None
    assert row["infraction_id"] == "6709476328"
    assert row["reason"] is None and row["remedy"] is None
    assert row["related_item_id"] is None
    assert row["date_created"] == datetime(2026, 9, 29, 18, 2, 34, 453000, tzinfo=UTC)


def test_parse_infraction_without_id_is_skipped() -> None:
    assert parse_infraction({"id": ""}) is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            "2026-09-29T14:02:34.453-0400",
            datetime(2026, 9, 29, 14, 2, 34, 453000, tzinfo=timezone(timedelta(hours=-4))),
        ),
        (
            "2026-09-29T14:02:34.000-04:00",
            datetime(2026, 9, 29, 14, 2, 34, tzinfo=timezone(timedelta(hours=-4))),
        ),
        ("2026-09-29T18:02:34Z", datetime(2026, 9, 29, 18, 2, 34, tzinfo=UTC)),
        ("ontem", None),
        (None, None),
    ],
)
def test_ts_accepts_both_ml_offset_formats(raw: Any, expected: datetime | None) -> None:
    assert _ts(raw) == expected


# ---------------------------------------------------------------------------
# Sync (HTTP mocked, DB patched)
# ---------------------------------------------------------------------------
def _service(handler: Any) -> MLAccountHealthService:
    tok = MagicMock()
    tok.get_valid_access_token = AsyncMock(return_value="tok")
    tok.handle_unauthorized = AsyncMock(return_value="tok2")
    return MLAccountHealthService(
        token_service=tok,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        ml_user_id="227584372",
    )


def _infractions_handler(all_rows: list[dict[str, Any]], calls: list[int]) -> Any:
    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["offset"])
        limit = int(request.url.params["limit"])
        assert limit <= 20
        calls.append(offset)
        page = all_rows[offset : offset + limit]
        return httpx.Response(
            200,
            json={
                "infractions": page,
                "paging": {"offset": offset, "limit": limit, "total": len(all_rows)},
            },
        )

    return handler


def _session_with(known_ids: list[str], captured: list[Any]) -> Any:
    @asynccontextmanager
    async def factory() -> Any:
        session = MagicMock()

        async def execute(stmt: Any) -> Any:
            captured.append(stmt)
            return [(i,) for i in known_ids]

        session.execute = execute
        session.commit = AsyncMock()
        yield session

    return factory


async def test_first_run_loads_the_whole_history() -> None:
    rows = [_infraction(i) for i in range(45)]
    calls: list[int] = []
    captured: list[Any] = []
    service = _service(_infractions_handler(rows, calls))

    with patch.object(mod, "AsyncSessionLocal", _session_with([], captured)):
        stats = await service.sync_infractions()

    assert calls == [0, 20, 40]
    assert stats == {"infractions_new": 45, "infractions_total_ml": 45}


async def test_later_runs_stop_at_the_first_fully_known_page() -> None:
    rows = [_infraction(i) for i in range(60)]
    known = [str(6709476328 - i) for i in range(3, 60)]  # 3 newest are new
    calls: list[int] = []
    service = _service(_infractions_handler(rows, calls))

    with patch.object(mod, "AsyncSessionLocal", _session_with(known, [])):
        stats = await service.sync_infractions()

    assert calls == [0, 20]  # page 2 is all known -> stop
    assert stats["infractions_new"] == 3


async def test_reputation_snapshot_is_upserted_once_per_day() -> None:
    captured: list[Any] = []
    service = _service(lambda r: httpx.Response(200, json=USER))

    with patch.object(mod, "AsyncSessionLocal", _session_with([], captured)):
        assert await service.sync_reputation() is True

    sql = str(captured[0].compile())
    assert "ON CONFLICT (snapshot_date) DO UPDATE" in sql


async def test_reputation_without_block_is_not_stored() -> None:
    captured: list[Any] = []
    service = _service(lambda r: httpx.Response(200, json={"id": 1}))

    with patch.object(mod, "AsyncSessionLocal", _session_with([], captured)):
        assert await service.sync_reputation() is False

    assert captured == []
