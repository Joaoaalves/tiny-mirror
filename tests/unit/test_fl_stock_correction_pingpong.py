"""Unit tests for the ping-pong guard in FLStockCorrectionService.

The 2026-06-01 audit on `fl_stock_corrections_log` (30-day window) found
that ~22% of applied corrections were the second leg of a ping-pong —
ML's Inventory API lagging behind a Tiny NF, so we applied a +N
correction, then a matching -N within 12-24h, leaving Tiny temporarily
wrong both times. The fix: refuse to apply a correction whose sign
opposes the previous correction for the same SKU within the last 48h,
when the sum is within ±1 of zero.

Test data pinned to real cases from the audit so the heuristic stays
calibrated to observed reality.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from tiny_mirror.services.fl_stock_correction_service import _is_ping_pong

pytestmark = pytest.mark.unit


def _prev(delta: int, hours_ago: float = 12.0) -> dict:
    return {
        "delta": delta,
        "created_at": datetime.now(UTC) - timedelta(hours=hours_ago),
    }


def test_no_recent_correction_means_apply() -> None:
    # No history → apply normally (False = not a ping-pong).
    assert _is_ping_pong(delta=2, recent_correction=None) is False


def test_zero_delta_is_never_ping_pong() -> None:
    # A correction of 0 would never happen (it'd return "aligned"),
    # but defensive: don't classify a zero either side as ping-pong.
    assert _is_ping_pong(delta=0, recent_correction=_prev(-2)) is False
    assert _is_ping_pong(delta=2, recent_correction=_prev(0)) is False


def test_del_port_can_fm_case_blocked() -> None:
    # The motivating case: prev=-1 (Tiny was high, subtracted), now
    # +2 (Tiny low, would add). Sum = +1, within tolerance → SKIP.
    assert _is_ping_pong(delta=2, recent_correction=_prev(-1)) is True


def test_bub_mosq_carr_perfect_cancel() -> None:
    # Audit case: +1 then -1 = sum 0 → SKIP.
    assert _is_ping_pong(delta=-1, recent_correction=_prev(1)) is True


def test_rta_gav6_p_large_magnitude_cancel() -> None:
    # Audit case: -9 then +9 = sum 0. Magnitude shouldn't matter; the
    # cancel pattern is the signal.
    assert _is_ping_pong(delta=9, recent_correction=_prev(-9)) is True


def test_pre_sbt_morchampignon_cancel() -> None:
    # Audit case: -3 then +3.
    assert _is_ping_pong(delta=3, recent_correction=_prev(-3)) is True


def test_same_direction_is_not_ping_pong() -> None:
    # Two successive corrections in the SAME direction = real drift
    # (e.g., more sales arrived; ML is catching up over multiple runs).
    # Must apply normally.
    assert _is_ping_pong(delta=2, recent_correction=_prev(3)) is False
    assert _is_ping_pong(delta=-1, recent_correction=_prev(-4)) is False


def test_partial_offset_outside_tolerance_applies() -> None:
    # prev=-5, now +8: sum = +3, beyond the ±1 tolerance — means real
    # drift accumulated beyond the lag, apply the correction.
    assert _is_ping_pong(delta=8, recent_correction=_prev(-5)) is False


def test_partial_offset_within_tolerance_blocks() -> None:
    # prev=-5, now +4: sum = -1, at the tolerance boundary — still a
    # ping-pong. The 1-unit gap is small enough to attribute to a
    # single sale that landed between the pair.
    assert _is_ping_pong(delta=4, recent_correction=_prev(-5)) is True
    # And the symmetric case.
    assert _is_ping_pong(delta=-4, recent_correction=_prev(5)) is True


def test_age_filter_is_caller_responsibility() -> None:
    # The 48h window is enforced by _recent_correction's SQL — the
    # pure helper trusts the caller to only pass rows within window.
    # This test documents that: an "old" row is still classified as
    # ping-pong here, because if it reached us it WAS recent.
    assert _is_ping_pong(delta=2, recent_correction=_prev(-1, hours_ago=47.0)) is True
