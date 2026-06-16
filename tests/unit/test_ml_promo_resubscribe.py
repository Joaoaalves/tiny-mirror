"""Unit tests for the re-subscribe queue (`ml_resubscribe_service`).

Raising a promo price = exit + re-enroll, but the ML lags re-suggesting the
offer. These tests pin the poller's decision logic on a single due job: wait
while the offer is absent, re-enroll the moment it reappears as a candidate,
short-circuit when it's already active, and give up (alert) on deadline /
exhausted attempts.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from tiny_mirror.services.ml_resubscribe_service import (
    ResubscribeService,
    find_offer_by_status,
)

pytestmark = pytest.mark.unit


# --- find_offer_by_status (pure) -------------------------------------------
def test_find_offer_matches_type_and_status_case_insensitive() -> None:
    promos = [
        {"type": "deal", "status": "Candidate", "id": "P1"},
        {"type": "SELLER_CAMPAIGN", "status": "started", "id": "P2"},
    ]
    assert find_offer_by_status(promos, "DEAL", status="candidate")["id"] == "P1"
    assert find_offer_by_status(promos, "seller_campaign", status="STARTED")["id"] == "P2"


def test_find_offer_prefers_exact_id_but_falls_back_to_same_type() -> None:
    promos = [
        {"type": "DEAL", "status": "candidate", "id": "OLD"},
        {"type": "DEAL", "status": "candidate", "id": "NEW"},
    ]
    # Exact id wins.
    assert find_offer_by_status(promos, "DEAL", status="candidate", promo_id="NEW")["id"] == "NEW"
    # Unknown id → first same-type candidate (ML may re-issue under a new id).
    assert find_offer_by_status(promos, "DEAL", status="candidate", promo_id="GONE")["id"] == "OLD"


def test_find_offer_returns_none_when_absent() -> None:
    promos = [{"type": "SMART", "status": "started", "id": "X"}]
    assert find_offer_by_status(promos, "DEAL", status="candidate") is None


# --- _process_one (decision logic) -----------------------------------------
class _FakeRepo:
    """Records terminal/bump calls; mimics bump_attempt's max-attempts flip."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def mark_done(self, job: Any) -> None:
        job.status = "done"
        self.calls.append("done")

    async def mark_failed(self, job: Any, *, error: str, status_code: int | None = None) -> None:
        job.status = "failed"
        job.last_error = error
        self.calls.append("failed")

    async def cancel(self, job: Any) -> None:  # pragma: no cover - unused here
        job.status = "cancelled"

    async def bump_attempt(
        self,
        job: Any,
        *,
        next_attempt_at: datetime,
        error: str | None = None,
        status_code: int | None = None,
    ) -> None:
        job.attempts += 1
        job.next_attempt_at = next_attempt_at
        job.last_error = error
        if job.attempts >= job.max_attempts:
            job.status = "failed"
        self.calls.append("bump")


def _job(**over: Any) -> SimpleNamespace:
    base: dict[str, Any] = {
        "id": 1,
        "mlb_id": "MLB1",
        "sku": "SKU1",
        "promo_type": "SELLER_CAMPAIGN",
        "promo_id": "P-1",
        "target_price": Decimal("49.90"),
        "status": "pending",
        "attempts": 0,
        "max_attempts": 10,
        "deadline": datetime.now(UTC) + timedelta(hours=1),
        "op_id": "op123",
        "decided_by": "op@x.com",
        "last_error": None,
        "last_status_code": None,
    }
    base.update(over)
    return SimpleNamespace(**base)


def _fake_session() -> Any:
    s = MagicMock()
    s.add = MagicMock()
    s.flush = AsyncMock()
    s.commit = AsyncMock()
    return s


def _svc(promos: list[dict[str, Any]], *, enter: dict[str, Any] | None = None) -> Any:
    svc = MagicMock()
    svc.fetch_eligible_promos = AsyncMock(return_value=promos)
    svc.modify_promotion = AsyncMock(return_value=enter or {"status_code": 200, "response": {}})
    svc.create_price_discount = AsyncMock(
        return_value=enter or {"status_code": 200, "response": {}}
    )
    return svc


@pytest.mark.asyncio
async def test_deadline_passed_fails_and_alerts() -> None:
    svc = _svc([])
    service = ResubscribeService(promotion_service=svc)
    repo = _FakeRepo()
    job = _job(deadline=datetime.now(UTC) - timedelta(minutes=1))
    out = await service._process_one(_fake_session(), repo, job, datetime.now(UTC))
    assert out == "deadline"
    assert "failed" in repo.calls
    svc.fetch_eligible_promos.assert_not_called()  # short-circuits before hitting ML


@pytest.mark.asyncio
async def test_already_started_marks_done_without_write() -> None:
    svc = _svc([{"type": "SELLER_CAMPAIGN", "status": "started", "id": "P-1"}])
    service = ResubscribeService(promotion_service=svc)
    repo = _FakeRepo()
    job = _job()
    out = await service._process_one(_fake_session(), repo, job, datetime.now(UTC))
    assert out == "already_active"
    assert repo.calls == ["done"]
    svc.modify_promotion.assert_not_called()


@pytest.mark.asyncio
async def test_candidate_absent_waits() -> None:
    svc = _svc([{"type": "SMART", "status": "started", "id": "Z"}])
    service = ResubscribeService(promotion_service=svc)
    repo = _FakeRepo()
    job = _job()
    out = await service._process_one(_fake_session(), repo, job, datetime.now(UTC))
    assert out == "waiting"
    assert repo.calls == ["bump"]
    svc.modify_promotion.assert_not_called()


@pytest.mark.asyncio
async def test_candidate_back_reenrolls_at_target_price() -> None:
    svc = _svc([{"type": "SELLER_CAMPAIGN", "status": "candidate", "id": "P-NEW"}])
    service = ResubscribeService(promotion_service=svc)
    repo = _FakeRepo()
    job = _job()
    out = await service._process_one(_fake_session(), repo, job, datetime.now(UTC))
    assert out == "resubscribed"
    assert repo.calls == ["done"]
    _, kwargs = svc.modify_promotion.call_args
    # Re-enrolls using the candidate's CURRENT id + the queued target price.
    assert kwargs["promotion_id"] == "P-NEW"
    assert kwargs["deal_price"] == 49.90
    assert kwargs["promotion_type"] == "SELLER_CAMPAIGN"


@pytest.mark.asyncio
async def test_candidate_back_but_reenter_fails_bumps_attempt() -> None:
    svc = _svc(
        [{"type": "SELLER_CAMPAIGN", "status": "candidate", "id": "P-1"}],
        enter={"status_code": 400, "response": {"message": "deal_price too high"}},
    )
    service = ResubscribeService(promotion_service=svc)
    repo = _FakeRepo()
    job = _job()
    out = await service._process_one(_fake_session(), repo, job, datetime.now(UTC))
    assert out == "waiting"
    assert repo.calls == ["bump"]


@pytest.mark.asyncio
async def test_exhausted_attempts_flips_to_failed() -> None:
    svc = _svc([])  # offer absent → bump
    service = ResubscribeService(promotion_service=svc)
    repo = _FakeRepo()
    job = _job(attempts=9, max_attempts=10)  # next bump hits the cap
    out = await service._process_one(_fake_session(), repo, job, datetime.now(UTC))
    assert out == "failed"
    assert "bump" in repo.calls
