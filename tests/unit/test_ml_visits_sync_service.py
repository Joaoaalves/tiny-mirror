"""Unit tests for :mod:`tiny_mirror.services.ml_visits_sync_service`.

ML HTTP mocked with httpx.MockTransport; payloads mirror a live
/items/{id}/visits/time_window response read on 2026-09-30.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from tiny_mirror.services import ml_visits_sync_service as mod
from tiny_mirror.services.ml_visits_sync_service import (
    MAX_DAYS,
    MLVisitsSyncService,
    parse_time_window,
)

pytestmark = pytest.mark.unit

WINDOW = {
    "item_id": "MLB3351786643",
    "total_visits": 21,
    "last": 3,
    "unit": "day",
    "results": [
        {"date": "2026-09-27T00:00:00Z", "total": 13, "visits_detail": []},
        {"date": "2026-09-28T00:00:00Z", "total": 2, "visits_detail": []},
        {"date": "2026-09-29T00:00:00Z", "total": 6, "visits_detail": []},
    ],
}


def test_parse_time_window_one_row_per_day() -> None:
    rows = parse_time_window("MLB3351786643", WINDOW)

    assert [(r["visit_date"], r["visits"]) for r in rows] == [
        (date(2026, 9, 27), 13),
        (date(2026, 9, 28), 2),
        (date(2026, 9, 29), 6),
    ]
    assert {r["mlb_id"] for r in rows} == {"MLB3351786643"}


def test_parse_time_window_skips_malformed_buckets() -> None:
    payload = {
        "results": [
            {"date": None, "total": 1},
            {"date": "ontem", "total": 1},
            {"date": "2026-09-29T00:00:00Z", "total": "x"},
            {"date": "2026-09-30T00:00:00Z"},
        ]
    }

    rows = parse_time_window("MLB1", payload)

    assert [(r["visit_date"], r["visits"]) for r in rows] == [(date(2026, 9, 30), 0)]


def _service(handler: Any) -> MLVisitsSyncService:
    tok = MagicMock()
    tok.get_valid_access_token = AsyncMock(return_value="tok")
    tok.handle_unauthorized = AsyncMock(return_value="tok2")
    return MLVisitsSyncService(
        token_service=tok, http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
    )


@asynccontextmanager
async def _capturing_session(captured: list[Any]) -> Any:
    session = MagicMock()

    async def execute(stmt: Any) -> None:
        captured.append(stmt)

    session.execute = execute
    session.commit = AsyncMock()
    yield session


async def test_sync_fetches_every_listing_and_counts_failures() -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        mlb = request.url.path.split("/")[2]
        seen.append((mlb, request.url.params["last"]))
        if mlb == "MLB2":
            return httpx.Response(404)
        return httpx.Response(200, json=WINDOW)

    service = _service(handler)
    service._listing_ids = AsyncMock(return_value=["MLB1", "MLB2", "MLB3"])  # type: ignore[method-assign]
    captured: list[Any] = []

    with patch.object(mod, "AsyncSessionLocal", lambda: _capturing_session(captured)):
        stats = await service.sync(days=3)

    assert sorted(seen) == [("MLB1", "3"), ("MLB2", "3"), ("MLB3", "3")]
    assert stats == {
        "days": 3,
        "listings": 3,
        "listings_failed": 1,
        "rows": 6,
        "visits": 42,
    }
    assert len(captured) == 1  # one upsert chunk


@pytest.mark.parametrize(("asked", "sent"), [(500, MAX_DAYS), (0, 1), (30, 30)])
async def test_window_is_clamped_to_ml_limits(asked: int, sent: int) -> None:
    lasts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        lasts.append(request.url.params["last"])
        return httpx.Response(200, json={"results": []})

    service = _service(handler)
    service._listing_ids = AsyncMock(return_value=["MLB1"])  # type: ignore[method-assign]

    stats = await service.sync(days=asked)

    assert lasts == [str(sent)] and stats["days"] == sent


async def test_expired_token_is_refreshed() -> None:
    auth: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        auth.append(request.headers["Authorization"])
        return httpx.Response(401 if len(auth) == 1 else 200, json=WINDOW)

    service = _service(handler)

    rows = await service._fetch("MLB1", 3)

    assert rows is not None and len(rows) == 3
    assert auth == ["Bearer tok", "Bearer tok2"]


# ---------------------------------------------------------------------------
# 429 handling (2026-09-30: 236/623 listings got 429 on the first backfill)
# ---------------------------------------------------------------------------
async def test_rate_limited_listing_is_retried_with_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(mod.asyncio, "sleep", fake_sleep)
    answers = iter([429, 429, 200])

    def handler(request: httpx.Request) -> httpx.Response:
        status = next(answers)
        headers = {"Retry-After": "3"} if len(sleeps) == 0 and status == 429 else {}
        return httpx.Response(status, json=WINDOW if status == 200 else {}, headers=headers)

    rows = await _service(handler)._fetch("MLB1", 3)

    assert rows is not None and len(rows) == 3
    assert sleeps[0] == 3.0  # Retry-After honoured
    assert 2.0 <= sleeps[1] <= 2.5  # then exponential with jitter


async def test_rate_limit_gives_up_after_max_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(mod.asyncio, "sleep", fake_sleep)

    rows = await _service(lambda r: httpx.Response(429))._fetch("MLB1", 3)

    assert rows is None
    assert len(sleeps) == mod._MAX_ATTEMPTS - 1
    assert all(s <= mod._MAX_BACKOFF_SECONDS for s in sleeps)


async def test_other_errors_are_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []

    async def fake_sleep(seconds: float) -> None:
        raise AssertionError("must not sleep")

    monkeypatch.setattr(mod.asyncio, "sleep", fake_sleep)

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(404)

    assert await _service(handler)._fetch("MLB1", 3) is None
    assert len(calls) == 1
