"""Rules for flagging suspicious Galpão balance changes (galpao_anomalies)."""

from __future__ import annotations

import pytest

from tiny_mirror.services.stock_sync_service import classify_galpao_anomaly

pytestmark = pytest.mark.unit


def test_small_sale_like_drop_is_not_flagged() -> None:
    assert classify_galpao_anomaly(30, 28, is_kit=False) is None
    assert classify_galpao_anomaly(5, 4, is_kit=False) is None


def test_no_change_is_not_flagged() -> None:
    assert classify_galpao_anomaly(10, 10, is_kit=False) is None


def test_big_drop_is_flagged() -> None:
    reason = classify_galpao_anomaly(644, 173, is_kit=False)
    assert reason is not None
    assert "queda -471" in reason
    assert "-73%" in reason


def test_zeroing_from_meaningful_stock_is_flagged() -> None:
    reason = classify_galpao_anomaly(6, 0, is_kit=False)
    assert reason == "zerou"


def test_zeroing_from_tiny_stock_is_not_flagged() -> None:
    # 2 -> 0 is a plausible sale, not an anomaly
    assert classify_galpao_anomaly(2, 0, is_kit=False) is None


def test_half_drop_on_large_stock_is_flagged() -> None:
    reason = classify_galpao_anomaly(40, 20, is_kit=False)
    assert reason is not None
    assert "-50%" in reason


def test_unexplained_jump_is_flagged() -> None:
    reason = classify_galpao_anomaly(0, 21, is_kit=False)
    assert reason == "salto +21"


def test_small_jump_is_not_flagged() -> None:
    assert classify_galpao_anomaly(10, 14, is_kit=False) is None


def test_kit_is_never_flagged() -> None:
    # kit galpão stock is derived from components — echoes, not anomalies
    assert classify_galpao_anomaly(644, 0, is_kit=True) is None
    assert classify_galpao_anomaly(0, 500, is_kit=True) is None
