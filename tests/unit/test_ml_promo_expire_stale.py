"""Unit tests for MLPromotionService._stale_reason.

The orchestration loop in ``expire_stale_decisions`` is integration —
it walks the pending queue and writes back. The interesting logic
lives in the pure ``_stale_reason`` classifier, so that's where the
tests live. Each test pins one rule (list_price / cap / floor / age)
plus a negative case, and the priority test makes sure we record the
*first* failing reason when multiple apply (so the recorded reason
matches what the operator sees when they generate fresh decisions).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from tiny_mirror.services.ml_promotion_service import MLPromotionService

pytestmark = pytest.mark.unit

NOW = datetime(2026, 5, 29, 12, 0, tzinfo=UTC)


# Tiny duck-typed stand-ins for the ORM rows. The classifier reads only
# a handful of attributes; using @dataclass keeps the tests readable
# without dragging in the ORM (no DB needed).
@dataclass
class FakeDecision:
    list_price: Decimal | None
    cap_pct: Decimal | None
    floor_price: Decimal | None
    created_at: datetime


@dataclass
class FakeSnap:
    list_price: Decimal | None


@dataclass
class FakeCap:
    max_seller_share_pct: Decimal
    margin_floor_price: Decimal | None


# The defaults used by every test that isn't explicitly probing one
# threshold. Match the Settings defaults.
DEFAULT_THRESH: dict[str, Any] = {
    "price_drift_pct": 5.0,
    "cap_drift_pct": 2.0,
    "floor_drift_pct": 5.0,
    "age_days": 14,
}


def _row(**overrides: Any) -> FakeDecision:
    base: dict[str, Any] = {
        "list_price": Decimal("100.00"),
        "cap_pct": Decimal("30.00"),
        "floor_price": Decimal("60.00"),
        # 1 day old — comfortably under stale_age.
        "created_at": NOW - timedelta(days=1),
    }
    base.update(overrides)
    return FakeDecision(**base)


def test_no_drift_returns_none() -> None:
    reason = MLPromotionService._stale_reason(
        _row(),
        snap=FakeSnap(list_price=Decimal("100.00")),
        cap=FakeCap(
            max_seller_share_pct=Decimal("30.00"),
            margin_floor_price=Decimal("60.00"),
        ),
        now=NOW,
        **DEFAULT_THRESH,
    )
    assert reason is None


def test_list_price_drift_above_threshold() -> None:
    # 100 → 106 = +6%, just over the 5% default.
    reason = MLPromotionService._stale_reason(
        _row(),
        snap=FakeSnap(list_price=Decimal("106.00")),
        cap=FakeCap(
            max_seller_share_pct=Decimal("30.00"),
            margin_floor_price=Decimal("60.00"),
        ),
        now=NOW,
        **DEFAULT_THRESH,
    )
    assert reason == "list_price_drift"


def test_list_price_drift_at_threshold_is_not_expired() -> None:
    # Exactly 5% — must NOT trip; we use strict `>` so the boundary is
    # safe (otherwise tiny FX rounding would expire everything).
    reason = MLPromotionService._stale_reason(
        _row(),
        snap=FakeSnap(list_price=Decimal("105.00")),
        cap=FakeCap(
            max_seller_share_pct=Decimal("30.00"),
            margin_floor_price=Decimal("60.00"),
        ),
        now=NOW,
        **DEFAULT_THRESH,
    )
    assert reason is None


def test_missing_snapshot_is_not_drift() -> None:
    # No current snapshot → no signal, not stale. Same shape as a row
    # whose MLB is brand-new and hasn't been re-snapshotted yet.
    reason = MLPromotionService._stale_reason(
        _row(),
        snap=None,
        cap=FakeCap(
            max_seller_share_pct=Decimal("30.00"),
            margin_floor_price=Decimal("60.00"),
        ),
        now=NOW,
        **DEFAULT_THRESH,
    )
    assert reason is None


def test_cap_change_above_threshold_pp() -> None:
    # 30pp → 33pp = 3 percentage points, over the 2pp default.
    reason = MLPromotionService._stale_reason(
        _row(),
        snap=FakeSnap(list_price=Decimal("100.00")),
        cap=FakeCap(
            max_seller_share_pct=Decimal("33.00"),
            margin_floor_price=Decimal("60.00"),
        ),
        now=NOW,
        **DEFAULT_THRESH,
    )
    assert reason == "cap_changed"


def test_floor_drift_above_threshold() -> None:
    # 60 → 64 = +6.67%, over the 5% default.
    reason = MLPromotionService._stale_reason(
        _row(),
        snap=FakeSnap(list_price=Decimal("100.00")),
        cap=FakeCap(
            max_seller_share_pct=Decimal("30.00"),
            margin_floor_price=Decimal("64.00"),
        ),
        now=NOW,
        **DEFAULT_THRESH,
    )
    assert reason == "floor_changed"


def test_stale_age_when_old_enough() -> None:
    reason = MLPromotionService._stale_reason(
        _row(created_at=NOW - timedelta(days=15)),
        snap=FakeSnap(list_price=Decimal("100.00")),
        cap=FakeCap(
            max_seller_share_pct=Decimal("30.00"),
            margin_floor_price=Decimal("60.00"),
        ),
        now=NOW,
        **DEFAULT_THRESH,
    )
    assert reason == "stale_age"


def test_priority_list_price_beats_cap_when_both_drift() -> None:
    # Both list_price AND cap moved; the priority order must surface
    # list_price_drift (more economically meaningful — the price itself
    # is what the operator is approving).
    reason = MLPromotionService._stale_reason(
        _row(),
        snap=FakeSnap(list_price=Decimal("110.00")),  # +10%
        cap=FakeCap(
            max_seller_share_pct=Decimal("35.00"),  # +5pp
            margin_floor_price=Decimal("60.00"),
        ),
        now=NOW,
        **DEFAULT_THRESH,
    )
    assert reason == "list_price_drift"


def test_age_only_triggers_when_no_drift() -> None:
    # Old AND drifted → drift wins (drift is the better signal that
    # the row is wrong, not just stale).
    reason = MLPromotionService._stale_reason(
        _row(created_at=NOW - timedelta(days=30)),
        snap=FakeSnap(list_price=Decimal("120.00")),
        cap=FakeCap(
            max_seller_share_pct=Decimal("30.00"),
            margin_floor_price=Decimal("60.00"),
        ),
        now=NOW,
        **DEFAULT_THRESH,
    )
    assert reason == "list_price_drift"


def test_zero_list_price_does_not_divide_by_zero() -> None:
    # Defensive — bad data must not crash the cron.
    reason = MLPromotionService._stale_reason(
        _row(list_price=Decimal("0")),
        snap=FakeSnap(list_price=Decimal("100.00")),
        cap=FakeCap(
            max_seller_share_pct=Decimal("30.00"),
            margin_floor_price=Decimal("60.00"),
        ),
        now=NOW,
        **DEFAULT_THRESH,
    )
    # Skips price/floor drift, no cap change, not old → None.
    assert reason is None
