"""Unit tests for :mod:`tiny_mirror.services.ml_claims_sync_service`.

ML HTTP mocked (httpx.MockTransport), DB patched. Payload shapes mirror live
/post-purchase/v1/claims responses read on 2026-09-30.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from tiny_mirror.services import ml_claims_sync_service as mod
from tiny_mirror.services.ml_claims_sync_service import (
    MLClaimsSyncService,
    parse_claim,
    parse_reason,
)

pytestmark = pytest.mark.unit


def _claim(
    claim_id: int, status: str = "closed", updated: str = "2026-09-30T12:40:30.000-04:00"
) -> dict[str, Any]:
    return {
        "id": claim_id,
        "type": "mediations",
        "stage": "dispute",
        "status": status,
        "reason_id": "PDD9952",
        "resource": "order",
        "resource_id": 2000018568836576,
        "parent_id": None,
        "fulfilled": True,
        "quantity_type": "total",
        "date_created": "2026-09-22T18:39:34.000-04:00",
        "last_updated": updated,
        "resolution": {
            "reason": "item_returned",
            "closed_by": "mediator",
            "benefited": ["complainant"],
        },
        "players": [{"role": "complainant", "type": "buyer"}],
    }


def test_parse_claim_links_to_ml_order_and_keeps_resolution() -> None:
    row = parse_claim(_claim(5581688206))

    assert row is not None
    assert row["resource"] == "order" and row["resource_id"] == 2000018568836576
    assert row["reason_id"] == "PDD9952"
    assert row["resolution"]["reason"] == "item_returned"
    assert row["last_updated"] == datetime(2026, 9, 30, 16, 40, 30, tzinfo=UTC)


def test_parse_claim_without_id_is_skipped() -> None:
    assert parse_claim({"id": None}) is None


def test_parse_reason() -> None:
    row = parse_reason(
        "PDD9952",
        {
            "id": "PDD9952",
            "name": "missing_accessories",
            "detail": "Chegou bem",
            "flow": "post_purchase_delivered",
        },
    )

    assert (row["name"], row["detail"], row["flow"]) == (
        "missing_accessories",
        "Chegou bem",
        "post_purchase_delivered",
    )


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------
def _session(known: list[tuple[int, datetime]], reasons: list[str], captured: list[Any]) -> Any:
    @asynccontextmanager
    async def factory() -> Any:
        session = MagicMock()
        calls = {"n": 0}

        async def execute(stmt: Any) -> Any:
            calls["n"] += 1
            captured.append(stmt)
            result = MagicMock()
            if calls["n"] == 1:
                result.all.return_value = known
            else:
                result.__iter__ = lambda self: iter([(r,) for r in reasons])
            return result

        session.execute = execute
        session.commit = AsyncMock()
        yield session

    return factory


def _service(by_status: dict[str, list[dict[str, Any]]], calls: list[str]) -> MLClaimsSyncService:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/search"):
            status = request.url.params["status"]
            offset = int(request.url.params["offset"])
            calls.append(f"{status}@{offset}")
            rows = by_status.get(status, [])
            return httpx.Response(
                200,
                json={
                    "data": rows[offset : offset + 100],
                    "paging": {"total": len(rows), "offset": offset, "limit": 100},
                },
            )
        if "/reasons/" in path:
            calls.append(path.rsplit("/", 1)[1])
            return httpx.Response(
                200, json={"name": "missing_accessories", "detail": "x", "flow": "f"}
            )
        return httpx.Response(404)

    tok = MagicMock()
    tok.get_valid_access_token = AsyncMock(return_value="tok")
    tok.handle_unauthorized = AsyncMock(return_value="tok2")
    return MLClaimsSyncService(
        token_service=tok, http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
    )


async def test_first_run_loads_every_page_of_both_statuses_and_resolves_reasons() -> None:
    calls: list[str] = []
    service = _service(
        {"opened": [_claim(1, "opened")], "closed": [_claim(i) for i in range(100, 350)]}, calls
    )

    with patch.object(mod, "AsyncSessionLocal", _session([], [], [])):
        stats = await service.sync()

    assert calls == ["opened@0", "closed@0", "closed@100", "closed@200", "PDD9952"]
    assert stats == {"claims_upserted": 251, "opened": 1, "reasons_new": 1}


async def test_later_run_stops_at_first_unchanged_page_and_skips_known_reasons() -> None:
    calls: list[str] = []
    same = "2026-09-30T12:40:30.000-04:00"
    known_ts = datetime(2026, 9, 30, 16, 40, 30, tzinfo=UTC)
    closed = [_claim(i, updated=same) for i in range(100, 350)]
    closed[0] = _claim(100, updated="2026-09-30T13:00:00.000-04:00")  # changed since last run
    known = [(i, known_ts) for i in range(100, 350)] + [(1, known_ts)]
    service = _service({"opened": [_claim(1, "opened", updated=same)], "closed": closed}, calls)

    with patch.object(mod, "AsyncSessionLocal", _session(known, ["PDD9952"], [])):
        stats = await service.sync()

    assert calls == ["opened@0", "closed@0", "closed@100"]
    assert stats == {"claims_upserted": 1, "opened": 0, "reasons_new": 0}
