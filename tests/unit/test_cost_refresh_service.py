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
            "MLB3884049149": {
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
            "MLB3644929145": {
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
    session.execute = AsyncMock(return_value=MagicMock(all=MagicMock(return_value=[])))
    stats = await refresh_all_from_bulk(session, _fake_gas(payload))
    assert stats == {
        "received": 2,
        "upserted": 2,
        "sku_fallback_upserts": 0,
        "skipped_no_data": 0,
        "skipped_invalid_id": 0,
    }
    assert {c["mlb_id"] for c in upsert_calls} == {"MLB3884049149", "MLB3644929145"}
    mlb1 = next(c for c in upsert_calls if c["mlb_id"] == "MLB3884049149")
    assert mlb1["base_cost"] == Decimal("10")
    assert mlb1["commission_pct"] == Decimal("11.5")
    assert mlb1["sku"] == "A"
    mlb2 = next(c for c in upsert_calls if c["mlb_id"] == "MLB3644929145")
    assert mlb2["base_cost"] is None
    session.commit.assert_awaited()


@pytest.mark.asyncio
async def test_refresh_skips_malformed_mlb_ids(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Spreadsheet sometimes has cells like 'MLB123 / 456' that must not
    reach the DB (mlb_id is varchar(20))."""
    payload = {
        "items": {
            "MLB3884049149": {"sku": "OK", "active": True, "baseCost": 10},
            "MLB4078501557 / 4078501557": {"sku": "BAD", "active": True, "baseCost": 5},
            "not-an-mlb": {"sku": "WORSE", "active": True, "baseCost": 1},
        }
    }
    calls: list[str] = []

    class FakeRepo:
        def __init__(self, _s: Any) -> None:
            pass

        async def upsert(self, mlb_id: str, **_: Any) -> None:
            calls.append(mlb_id)

    monkeypatch.setattr(
        "tiny_mirror.services.cost_refresh_service.MLCostsSnapshotRepository",
        FakeRepo,
    )
    session = MagicMock()
    session.commit = AsyncMock()
    session.execute = AsyncMock(return_value=MagicMock(all=MagicMock(return_value=[])))
    stats = await refresh_all_from_bulk(session, _fake_gas(payload))
    assert calls == ["MLB3884049149"]
    assert stats["skipped_invalid_id"] == 2
    assert stats["upserted"] == 1


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


@pytest.mark.asyncio
async def test_refresh_sku_fallback_for_active_listing_missing_mlb(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Anúncio ATIVO cujo MLB não está na aba "Mercado Livre" herda a linha do
    MESMO SKU (coluna F). A linha própria, quando existe, vence; sem match por
    SKU não inventa nada."""
    payload = {
        "items": {
            "MLB1111111111": {
                "sku": "PROD-A",
                "active": True,
                "baseCost": 10,
                "commissionPct": 11.5,
                "listPrice": 50,
            },
            # linha inativa do MESMO SKU — a ativa acima deve ser a preferida
            "MLB2222222222": {"sku": "PROD-A", "active": False, "baseCost": None},
        }
    }
    upserts: list[dict[str, Any]] = []

    class FakeRepo:
        def __init__(self, _s: Any) -> None:
            pass

        async def upsert(self, mlb_id: str, **kwargs: Any) -> None:
            upserts.append({"mlb_id": mlb_id, **kwargs})

    monkeypatch.setattr(
        "tiny_mirror.services.cost_refresh_service.MLCostsSnapshotRepository",
        FakeRepo,
    )
    session = MagicMock()
    session.commit = AsyncMock()
    # ml_listings ativos: MLB999… (mesmo SKU, sem linha própria) herda;
    # MLB1111… tem linha própria (não duplica); MLB888… SKU sem match (nada).
    listings = [
        ("MLB9999999999", "PROD-A"),
        ("MLB1111111111", "PROD-A"),
        ("MLB8888888888", "SEM-MATCH"),
    ]
    session.execute = AsyncMock(return_value=MagicMock(all=MagicMock(return_value=listings)))

    stats = await refresh_all_from_bulk(session, _fake_gas(payload))
    assert stats["sku_fallback_upserts"] == 1
    fb = [u for u in upserts if u["mlb_id"] == "MLB9999999999"]
    assert len(fb) == 1
    # herdou a linha ATIVA (base_cost 10), não a inativa sem custo
    assert fb[0]["base_cost"] == Decimal("10")
    assert fb[0]["sku"] == "PROD-A"
    # a linha própria não foi upsertada de novo pelo fallback
    assert sum(1 for u in upserts if u["mlb_id"] == "MLB1111111111") == 1
