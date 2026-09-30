"""Unit tests for :mod:`tiny_mirror.services.ml_item_health_service`.

ML HTTP mocked (httpx.MockTransport), DB patched. Payload shapes mirror live
responses read on 2026-09-30 (multiget, /item/{id}/performance,
purchase_experience after its 302 redirect).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from tiny_mirror.services import ml_item_health_service as mod
from tiny_mirror.services.ml_item_health_service import (
    MLItemHealthService,
    parse_experience,
    parse_multiget_entry,
    parse_performance,
)

pytestmark = pytest.mark.unit

PERFORMANCE = {
    "entity_type": "USER_PRODUCT",
    "score": 91,
    "level": "good",
    "level_wording": "Profissional",
    "calculated_at": "2026-09-30T13:10:46.979Z",
    "buckets": [
        {
            "key": "USER_PRODUCT",
            "title": "Dados do produto",
            "variables": [
                {
                    "key": "UP_PICTURES",
                    "title": "Melhore as fotos",
                    "score": 100,
                    "status": "COMPLETED",
                },
            ],
        },
        {
            "key": "MLB1",
            "title": "Condições de venda",
            "variables": [
                {
                    "key": "UP_FREE_SHIPPING",
                    "title": "Ofereça frete grátis",
                    "score": 0,
                    "status": "PENDING",
                },
                {"key": "UP_PROMOTIONS", "title": "Participe", "score": 100, "status": "COMPLETED"},
            ],
        },
    ],
}

EXPERIENCE = {
    "reputation": {"color": "green", "text": "Boa", "value": 100},
    "status": {"id": "active"},
    "title": {"text": "Experiência de compra"},
}


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def test_parse_multiget_entry_keeps_moderation_state() -> None:
    row = parse_multiget_entry(
        {
            "code": 200,
            "body": {
                "id": "MLB5459079850",
                "status": "under_review",
                "sub_status": ["forbidden"],
                "tags": ["immediate_payment"],
            },
        }
    )

    assert row is not None
    assert row["item_status"] == "under_review"
    assert row["item_sub_status"] == ["forbidden"]
    assert row["item_tags"] == ["immediate_payment"]


def test_parse_multiget_entry_skips_errors() -> None:
    assert parse_multiget_entry({"code": 404, "body": {"id": "MLB1"}}) is None


def test_parse_performance_lists_only_pending_variables() -> None:
    row = parse_performance("MLB1", PERFORMANCE)

    assert row is not None
    assert row["quality_score"] == Decimal("91.00")
    assert row["quality_level"] == "good"
    assert row["quality_pending"] == [
        {
            "bucket": "Condições de venda",
            "key": "UP_FREE_SHIPPING",
            "title": "Ofereça frete grátis",
            "score": 0,
        }
    ]
    assert row["quality_calculated_at"] is not None


def test_parse_performance_without_score_is_skipped() -> None:
    assert parse_performance("MLB1", {"message": "not found"}) is None


def test_parse_experience() -> None:
    row = parse_experience("MLB1", EXPERIENCE)

    assert row is not None
    assert (row["experience_color"], row["experience_text"], row["experience_value"]) == (
        "green",
        "Boa",
        Decimal("100.00"),
    )
    assert row["experience_status"] == "active"


def test_parse_experience_without_reputation_is_skipped() -> None:
    assert parse_experience("MLB1", {"status": {"id": "unknown"}}) is None


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------
def _service(handler: Any) -> MLItemHealthService:
    tok = MagicMock()
    tok.get_valid_access_token = AsyncMock(return_value="tok")
    tok.handle_unauthorized = AsyncMock(return_value="tok2")
    return MLItemHealthService(
        token_service=tok, http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
    )


def _capture(captured: list[Any]) -> Any:
    @asynccontextmanager
    async def factory() -> Any:
        session = MagicMock()

        async def execute(stmt: Any) -> None:
            captured.append(stmt)

        session.execute = execute
        session.commit = AsyncMock()
        yield session

    return factory


def _handler(calls: list[str]) -> Any:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        calls.append(path)
        if path == "/items":
            ids = request.url.params["ids"].split(",")
            status = {"MLB1": "active", "MLB2": "paused", "MLB3": "under_review"}
            return httpx.Response(
                200,
                json=[
                    {
                        "code": 200,
                        "body": {"id": i, "status": status[i], "sub_status": [], "tags": []},
                    }
                    for i in ids
                ],
            )
        if path.endswith("/performance"):
            return httpx.Response(200, json=PERFORMANCE)
        if path == "/reputation/items/MLB3/purchase_experience/integrators":
            return httpx.Response(403, json={"blocked_by": "PolicyAgent"})
        if path.startswith("/reputation/items/"):
            up = path.split("/")[3].replace("MLB", "MLBU")
            return httpx.Response(
                302,
                headers={
                    "Location": f"https://api.mercadolibre.com/reputation/user_products/{up}/purchase_experience/integrators"
                },
            )
        if path.startswith("/reputation/user_products/"):
            return httpx.Response(200, json=EXPERIENCE)
        return httpx.Response(404)

    return handler


async def test_sync_reads_detail_only_for_active_or_under_review() -> None:
    calls: list[str] = []
    captured: list[Any] = []
    service = _service(_handler(calls))
    service._listings = AsyncMock(  # type: ignore[method-assign]
        return_value={"MLB1": "active", "MLB2": "paused", "MLB3": "active"}
    )

    with patch.object(mod, "AsyncSessionLocal", _capture(captured)):
        stats = await service.sync()

    assert stats == {
        "listings": 3,
        "items": 3,
        "detail_listings": 2,  # MLB2 is paused
        "quality": 2,
        "experience": 1,  # MLB3 answered 403 -> previous value kept
        "under_review": 1,
    }
    assert "/item/MLB2/performance" not in calls
    assert len(captured) == 3  # one upsert per part


async def test_failed_part_does_not_touch_other_columns() -> None:
    captured: list[Any] = []
    service = _service(_handler([]))
    service._listings = AsyncMock(return_value={"MLB3": "active"})  # type: ignore[method-assign]

    with patch.object(mod, "AsyncSessionLocal", _capture(captured)):
        await service.sync()

    updates = [str(stmt.compile()).split("DO UPDATE SET", 1)[1] for stmt in captured]
    assert all("experience_" not in u for u in updates)  # 403 -> no experience write
    assert any("quality_score" in u for u in updates)


async def test_multiget_batches_twenty_ids_per_call() -> None:
    seen: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/items":
            ids = request.url.params["ids"].split(",")
            seen.append(len(ids))
            return httpx.Response(200, json=[])
        return httpx.Response(404)

    service = _service(handler)
    service._listings = AsyncMock(  # type: ignore[method-assign]
        return_value={f"MLB{i}": "paused" for i in range(45)}
    )

    with patch.object(mod, "AsyncSessionLocal", _capture([])):
        await service.sync()

    assert sorted(seen) == [5, 20, 20]
