"""Unit tests for cost_refresh_service.refresh_all_from_bulk."""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from tiny_mirror.services.cost_refresh_service import (
    CostRefreshError,
    refresh_all_from_bulk,
)
from tiny_mirror.services.gas_client import GASClientError

pytestmark = pytest.mark.unit


def _fake_gas(payload: Any = None, error: str | None = None) -> Any:
    gas = MagicMock()
    if error is not None:
        gas.costs_all = AsyncMock(side_effect=GASClientError(error))
    else:
        gas.costs_all = AsyncMock(return_value=payload)
    return gas


@pytest.mark.asyncio
async def test_refresh_upserts_every_item_and_returns_stats(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    payload = {
        "generatedAt": "2026-05-19T20:30:00Z",
        "difalPct": 0.115,
        "count": 2,
        "items": {
            "MLB1": {
                "sku": "A",
                "active": True,
                "baseCost": 10,
                "commissionPct": 11.5,
                "commissionLabel": "Classico 11,5%",
                "listPrice": 50,
                "promoPrice": 35,
                "discountPct": 30,
                "currentMarginPct": 12,
                "currentMarginValue": 4.2,
                "freightBands": [{"min": 0, "max": 18.99, "cost": 5.65}],
            },
            "MLB2": {
                "sku": "B",
                "active": False,
                "baseCost": None,
                "commissionPct": 16.5,
                "commissionLabel": "Premium 16,5%",
                "listPrice": 60,
                "promoPrice": 42,
                "discountPct": 30,
                "currentMarginPct": 5,
                "currentMarginValue": 2.1,
                "freightBands": [],
            },
        },
    }

    upsert_calls: list[dict[str, Any]] = []

    class FakeRepo:
        def __init__(self, _session: Any) -> None:
            pass

        async def upsert(self, mlb_id: str, **kwargs: Any) -> None:
            upsert_calls.append({"mlb_id": mlb_id, **kwargs})

    monkeypatch.setattr(
        "tiny_mirror.services.cost_refresh_service.MLCostsSnapshotRepository",
        FakeRepo,
    )

    session = MagicMock()
    session.commit = AsyncMock()
    stats = await refresh_all_from_bulk(session, _fake_gas(payload))
    assert stats == {"received": 2, "upserted": 2, "skipped_no_data": 0}
    assert {c["mlb_id"] for c in upsert_calls} == {"MLB1", "MLB2"}
    # check Decimal conversion happened
    mlb1 = next(c for c in upsert_calls if c["mlb_id"] == "MLB1")
    assert mlb1["base_cost"] == Decimal("10")
    assert mlb1["commission_pct"] == Decimal("11.5")
    assert mlb1["sku"] == "A"
    # MLB2 had baseCost=None; should be persisted as None
    mlb2 = next(c for c in upsert_calls if c["mlb_id"] == "MLB2")
    assert mlb2["base_cost"] is None
    session.commit.assert_awaited()


@pytest.mark.asyncio
async def test_refresh_raises_when_gas_returns_error() -> None:
    session = MagicMock()
    session.commit = AsyncMock()
    with pytest.raises(CostRefreshError, match="unauthorized"):
        await refresh_all_from_bulk(session, _fake_gas(error="unauthorized"))


@pytest.mark.asyncio
async def test_refresh_raises_when_no_items() -> None:
    session = MagicMock()
    session.commit = AsyncMock()
    with pytest.raises(CostRefreshError, match="0 items"):
        await refresh_all_from_bulk(
            session,
            _fake_gas({"generatedAt": "x", "difalPct": 0.115, "items": {}}),
        )
