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

from tiny_mirror.services.fl_stock_correction_service import (
    COOLDOWN_HOURS,
    COOLDOWN_MAX_MAGNITUDE,
    _is_in_cooldown,
    _is_ping_pong,
)

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


# ─── _is_in_cooldown ──────────────────────────────────────────────────
#
# Cooldown protects high-velocity SKUs from oscillation: when we just
# corrected a SKU and a SMALL drift is detected again shortly after,
# the new drift is almost certainly ML's Inventory still catching up
# with Tiny's NF — not fresh real drift. Skip. Real drift that
# accumulates beyond the magnitude threshold passes through.


def test_cooldown_no_recent_correction_applies() -> None:
    assert _is_in_cooldown(delta=2, recent_correction=None) is False


def test_cooldown_small_delta_inside_window_blocks() -> None:
    # DEL-VIS-ETIQ-BRNC 27/05 cluster: prev=-102, current=-1, 0.8h
    # later. |delta|=1 ≤ 5 → block.
    assert _is_in_cooldown(delta=-1, recent_correction=_prev(-102, hours_ago=0.8)) is True
    # And the next two in the cluster.
    assert _is_in_cooldown(delta=-3, recent_correction=_prev(-1, hours_ago=1.0)) is True
    assert _is_in_cooldown(delta=-1, recent_correction=_prev(-3, hours_ago=1.0)) is True


def test_cooldown_large_delta_inside_window_applies() -> None:
    # The legitimate -102 drift would have had no prev correction; but
    # even if it had one in window, magnitude > 5 must pass through.
    # This is the principle that lets us catch real drift on high-vel
    # SKUs (DEL-VIS-ETIQ-BRNC -102, +79).
    assert _is_in_cooldown(delta=-102, recent_correction=_prev(-1, hours_ago=2.0)) is False
    assert _is_in_cooldown(delta=79, recent_correction=_prev(1, hours_ago=2.0)) is False


def test_cooldown_window_boundary() -> None:
    # 12h is the default window. Just inside = blocked, just outside =
    # applied. Magnitude small in both.
    assert _is_in_cooldown(delta=1, recent_correction=_prev(-1, hours_ago=11.9)) is True
    assert _is_in_cooldown(delta=1, recent_correction=_prev(-1, hours_ago=12.5)) is False


def test_cooldown_magnitude_boundary() -> None:
    # |delta| == max_magnitude (5) is "small". |delta| > 5 passes.
    assert _is_in_cooldown(delta=5, recent_correction=_prev(-1, hours_ago=2.0)) is True
    assert _is_in_cooldown(delta=-5, recent_correction=_prev(1, hours_ago=2.0)) is True
    assert _is_in_cooldown(delta=6, recent_correction=_prev(-1, hours_ago=2.0)) is False


def test_cooldown_constants_are_audit_calibrated() -> None:
    # Calibrated against the 2026-06-01 audit. If these defaults
    # change, the audit assumptions in the docstring should be
    # revisited.
    assert COOLDOWN_HOURS == 12.0
    assert COOLDOWN_MAX_MAGNITUDE == 5


def test_cooldown_independent_of_ping_pong_direction() -> None:
    # Cooldown doesn't care about sign — same-direction repeats are
    # also lag-suspicious (the -1, -3, -1 cluster all went the same
    # way). Both directions inside window get blocked when small.
    assert _is_in_cooldown(delta=2, recent_correction=_prev(3, hours_ago=2.0)) is True
    assert _is_in_cooldown(delta=-2, recent_correction=_prev(-3, hours_ago=2.0)) is True
