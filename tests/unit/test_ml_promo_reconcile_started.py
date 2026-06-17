"""Unit tests for MLPromotionService.reconcile_started_promos.

This is the cap-independent sweep that records every active listing's STARTED
promos in the mirror — closing the blind spot where ``generate_pending_decisions``
only walks SKUs that have a cap, so cap-less / SKU-less listings with an active
campaign (e.g. SELLER_CAMPAIGN) wrongly showed as "sem promoção".

The DB plumbing is faked; the test pins the behaviour that matters: only
``started`` promos are upserted, the live started set is passed to the expirer
(so finished campaigns drop), and a cap is never required.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, ClassVar

import pytest

import tiny_mirror.services.ml_promotion_service as svc_mod
from tiny_mirror.services.ml_promotion_service import MLPromotionService

pytestmark = pytest.mark.unit


class _FakeResult:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def all(self) -> list[Any]:
        return self._rows

    def one_or_none(self) -> Any:
        return self._rows[0] if self._rows else None


class _Row:
    def __init__(self, mlb_id: str, sku: str | None) -> None:
        self.mlb_id = mlb_id
        self.sku = sku


class _FakeSession:
    def __init__(self, rows: list[_Row]) -> None:
        self._rows = rows
        self.committed = False

    async def execute(self, *_a: Any, **_k: Any) -> _FakeResult:
        return _FakeResult(self._rows)

    async def commit(self) -> None:
        self.committed = True


class _FakeSnapRepo:
    def __init__(self, *_a: Any, **_k: Any) -> None:
        pass

    async def get(self, _mlb: str) -> Any:
        return None  # force fallback to original_price from the promo body


class _FakeDecisionsRepo:
    """Records upsert/expire calls so the test can assert on them."""

    upserts: ClassVar[list[dict[str, Any]]] = []
    expires: ClassVar[list[dict[str, Any]]] = []

    def __init__(self, *_a: Any, **_k: Any) -> None:
        pass

    async def upsert_started(self, **kwargs: Any) -> None:
        _FakeDecisionsRepo.upserts.append(kwargs)

    async def expire_disappeared_started(
        self, *, mlb_id: str, seen_promo_keys: set[str], reason: str = "campaign_ended"
    ) -> int:
        _FakeDecisionsRepo.expires.append({"mlb_id": mlb_id, "seen": set(seen_promo_keys)})
        return 0


@pytest.fixture(autouse=True)
def _patch_repos(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeDecisionsRepo.upserts = []
    _FakeDecisionsRepo.expires = []
    monkeypatch.setattr(svc_mod, "MLCostsSnapshotRepository", _FakeSnapRepo)
    monkeypatch.setattr(
        "tiny_mirror.infrastructure.repositories.ml_promo_repository.MLPromoDecisionRepository",
        _FakeDecisionsRepo,
    )


def _make_service(promos_by_mlb: dict[str, list[dict[str, Any]]]) -> MLPromotionService:
    s = MLPromotionService(token_service=object(), http_client=object())

    async def _fake_fetch(mlb_id: str) -> list[dict[str, Any]]:
        return promos_by_mlb.get(mlb_id, [])

    s.fetch_eligible_promos = _fake_fetch  # type: ignore[assignment]
    return s


@pytest.mark.asyncio
async def test_records_only_started_and_ignores_others() -> None:
    promos = {
        "MLB1": [
            {
                "id": "CAMP-9",
                "type": "SELLER_CAMPAIGN",
                "status": "started",
                "price": 80.0,
                "original_price": 100.0,
                "name": "Junho",
            },
            {"id": "CAND-1", "type": "DEAL", "status": "candidate", "price": 70.0},
            {"type": "PRICE_DISCOUNT", "status": "pending", "price": 90.0},
        ],
    }
    svc = _make_service(promos)
    session = _FakeSession([_Row("MLB1", "SKU-A")])

    stats = await svc.reconcile_started_promos(session)  # type: ignore[arg-type]

    assert stats["mlbs_scanned"] == 1
    assert stats["started_upserted"] == 1
    assert session.committed is True
    # Only the started SELLER_CAMPAIGN was written.
    assert len(_FakeDecisionsRepo.upserts) == 1
    up = _FakeDecisionsRepo.upserts[0]
    assert up["mlb_id"] == "MLB1"
    assert up["promo_type"] == "SELLER_CAMPAIGN"
    assert up["promo_key"] == "CAMP-9"
    assert up["target_price"] == Decimal("80.0")
    assert up["list_price"] == Decimal("100.0")  # fallen back to original_price
    # Expirer got the live started set so finished campaigns drop.
    assert _FakeDecisionsRepo.expires == [{"mlb_id": "MLB1", "seen": {"CAMP-9"}}]


@pytest.mark.asyncio
async def test_capless_and_skuless_listing_still_recorded() -> None:
    # No cap involved anywhere here, and the listing has no SKU mapped — this is
    # exactly the blind spot. It must still be recorded (sku falls back to mlb_id).
    promos = {
        "MLB7": [
            {"id": "CAMP-X", "type": "SELLER_CAMPAIGN", "status": "started", "price": 49.9},
        ],
    }
    svc = _make_service(promos)
    session = _FakeSession([_Row("MLB7", None)])

    stats = await svc.reconcile_started_promos(session)  # type: ignore[arg-type]

    assert stats["started_upserted"] == 1
    up = _FakeDecisionsRepo.upserts[0]
    assert up["sku"] == "MLB7"  # mlb_id fallback when SKU is missing


@pytest.mark.asyncio
async def test_no_started_promo_expires_everything_for_that_mlb() -> None:
    promos = {"MLB2": [{"id": "D1", "type": "DEAL", "status": "candidate", "price": 10.0}]}
    svc = _make_service(promos)
    session = _FakeSession([_Row("MLB2", "SKU-B")])

    stats = await svc.reconcile_started_promos(session)  # type: ignore[arg-type]

    assert stats["started_upserted"] == 0
    # Empty seen set → expirer wipes all stale started rows for the MLB.
    assert _FakeDecisionsRepo.expires == [{"mlb_id": "MLB2", "seen": set()}]


@pytest.mark.asyncio
async def test_only_mlb_filters_to_single_listing() -> None:
    promos = {"MLB5": [{"id": "C5", "type": "SELLER_CAMPAIGN", "status": "started", "price": 30.0}]}
    svc = _make_service(promos)
    session = _FakeSession([_Row("MLB5", "SKU-E")])

    stats = await svc.reconcile_started_promos(session, only_mlb="MLB5")  # type: ignore[arg-type]

    assert stats["mlbs_scanned"] == 1
    assert stats["started_upserted"] == 1
