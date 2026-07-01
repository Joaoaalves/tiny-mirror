"""Service-orchestration tests for Estoque Full (DB-free via a fake repo).

Covers the parts _not_ exercised by the pure-logic tests: Novos filtering
(excludes tracked/dismissed/non-qualifying), snapshot capture on track, and
the dismiss/restore lifecycle. The SQL itself is out of scope here (needs a
live DB — covered by e2e when infra is available)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from tiny_mirror.services.ml_fl_tracking_service import MLFlTrackingService

pytestmark = pytest.mark.unit


def _metric(
    mlb: str, *, sku: str, stock_full: int, sold_30d: int, rev_90d: float
) -> dict[str, Any]:
    return {
        "mlb_id": mlb,
        "sku": sku,
        "title": None,
        "permalink": None,
        "stock_full": stock_full,
        "stock_galpao": 0,
        "status_base": "slow",
        "sold_30d": sold_30d,
        "rev_90d": rev_90d,
        "product_created_at": datetime(2020, 1, 1, tzinfo=UTC),
        "promo_original": 100,
        "promo_price": 80,
        "promo_type": "DEAL",
        "promo_seller_pct": 20,
        "promo_meli_pct": 0,
    }


class FakeRepo:
    def __init__(self, metrics: list[dict[str, Any]]) -> None:
        self._metrics = metrics
        self.tracked: set[str] = set()
        self.dismissals: list[Any] = []
        self.created: list[Any] = []
        self.events: list[Any] = []
        self._next_id = 1

    async def fetch_metrics(self) -> list[dict[str, Any]]:
        return self._metrics

    async def active_tracking_mlbs(self) -> set[str]:
        return set(self.tracked)

    async def list_dismissals(self) -> list[Any]:
        return self.dismissals

    async def get_active_by_mlb(self, mlb_id: str) -> Any:
        return None

    async def create_tracking(self, **values: Any) -> Any:
        row = SimpleNamespace(
            id=self._next_id,
            moved_at=datetime.now(UTC),
            finalized_at=None,
            finalized_by=None,
            final_stock_full=None,
            final_daily_rate_30d=None,
            final_promo_pct=None,
            final_snapshot=None,
            result_summary=None,
            **values,
        )
        self._next_id += 1
        self.created.append(row)
        self.tracked.add(values["mlb_id"])
        return row

    async def add_event(self, tracking_id: int, **kw: Any) -> Any:
        ev = SimpleNamespace(id=len(self.events) + 1, tracking_id=tracking_id, **kw)
        self.events.append(ev)
        return ev

    async def delete_dismissal(self, mlb_id: str) -> bool:
        before = len(self.dismissals)
        self.dismissals = [d for d in self.dismissals if d.mlb_id != mlb_id]
        return len(self.dismissals) != before

    async def upsert_dismissal(
        self, mlb_id: str, *, kind: str, ignore_days: int, now: datetime, **kw: Any
    ) -> Any:
        until = now + timedelta(days=ignore_days) if kind == "ignore" else None
        d = SimpleNamespace(
            mlb_id=mlb_id,
            kind=kind,
            until=until,
            created_by=kw.get("created_by"),
            created_at=now,
            sku=kw.get("sku"),
        )
        self.dismissals = [x for x in self.dismissals if x.mlb_id != mlb_id] + [d]
        return d


def _svc(repo: FakeRepo) -> MLFlTrackingService:
    svc = MLFlTrackingService(AsyncMock(), ignore_days=7)
    svc._repo = repo  # type: ignore[assignment]
    svc._session = AsyncMock()
    return svc


async def test_list_novos_filters_tracked_dismissed_and_low_coverage() -> None:
    metrics = [
        _metric("MLB-OVER", sku="S1", stock_full=100, sold_30d=30, rev_90d=500),  # cov ~100d → in
        _metric("MLB-LOW", sku="S2", stock_full=10, sold_30d=30, rev_90d=400),  # cov 10d → out
        _metric("MLB-TRACKED", sku="S3", stock_full=200, sold_30d=1, rev_90d=300),  # tracked → out
        _metric("MLB-REMOVED", sku="S4", stock_full=200, sold_30d=1, rev_90d=200),  # removed → out
        _metric("MLB-NOSALE", sku="S5", stock_full=5, sold_30d=0, rev_90d=0),  # cov ∞ → in
    ]
    repo = FakeRepo(metrics)
    repo.tracked.add("MLB-TRACKED")
    repo.dismissals.append(
        SimpleNamespace(
            mlb_id="MLB-REMOVED",
            kind="remove",
            until=None,
            created_by=None,
            created_at=datetime.now(UTC),
            sku="S4",
        )
    )

    rows = await _svc(repo).list_novos()
    ids = {r["mlb_id"] for r in rows}
    assert ids == {"MLB-OVER", "MLB-NOSALE"}


async def test_expired_ignore_reappears_in_novos() -> None:
    metrics = [_metric("MLB-A", sku="S1", stock_full=100, sold_30d=1, rev_90d=100)]
    repo = FakeRepo(metrics)
    # ignore that already expired → should NOT be filtered out
    repo.dismissals.append(
        SimpleNamespace(
            mlb_id="MLB-A",
            kind="ignore",
            until=datetime.now(UTC) - timedelta(days=1),
            created_by=None,
            created_at=datetime.now(UTC),
            sku="S1",
        )
    )
    rows = await _svc(repo).list_novos()
    assert {r["mlb_id"] for r in rows} == {"MLB-A"}


async def test_track_captures_initial_snapshot() -> None:
    metrics = [_metric("MLB-A", sku="S1", stock_full=60, sold_30d=30, rev_90d=100)]
    repo = FakeRepo(metrics)
    out = await _svc(repo).track("MLB-A", moved_by="joao@x.com")

    assert out["mlb_id"] == "MLB-A"
    created = repo.created[0]
    assert created.initial_stock_full == 60
    assert float(created.initial_daily_rate_30d) == 1.0  # 30/30
    assert float(created.initial_promo_pct) == 20.0  # (100-80)/100
    assert created.initial_snapshot["surplus"] == 30  # 60 - 30
    # a status_change event was recorded
    assert any(e.event_type == "status_change" for e in repo.events)


async def test_track_unknown_mlb_raises() -> None:
    repo = FakeRepo([])
    with pytest.raises(ValueError):
        await _svc(repo).track("MLB-GHOST", moved_by=None)


async def test_dismiss_ignore_then_restore() -> None:
    metrics = [_metric("MLB-A", sku="S1", stock_full=100, sold_30d=1, rev_90d=100)]
    repo = FakeRepo(metrics)
    svc = _svc(repo)

    res = await svc.dismiss("MLB-A", kind="ignore", created_by="joao@x.com")
    assert res["kind"] == "ignore" and res["until"] is not None
    # now filtered out of novos
    assert await svc.list_novos() == []

    ok = await svc.restore("MLB-A")
    assert ok is True
    assert {r["mlb_id"] for r in await svc.list_novos()} == {"MLB-A"}
