"""Unit tests for the Estoque Full pure logic (ABC curve, status, promo %,
coverage, Novos qualification). DB-free — exercises the module functions."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from tiny_mirror.services.ml_fl_tracking_service import (
    _build_row,
    _qualifies_novos,
    classify_status,
    compute_abc,
    coverage_days,
    promo_pct,
)

pytestmark = pytest.mark.unit

_TODAY = datetime(2026, 7, 1, tzinfo=UTC)


def test_compute_abc_pareto_80_15_5() -> None:
    # Revenues: 800, 150, 40, 10 → total 1000.
    metrics = [
        {"mlb_id": "A1", "rev_90d": 800},  # cum 80% → A
        {"mlb_id": "B1", "rev_90d": 150},  # cum 95% → B
        {"mlb_id": "C1", "rev_90d": 40},  # cum 99% → C
        {"mlb_id": "C2", "rev_90d": 10},  # cum 100% → C
    ]
    abc = compute_abc(metrics)
    assert abc == {"A1": "A", "B1": "B", "C1": "C", "C2": "C"}


def test_compute_abc_zero_revenue_is_c() -> None:
    metrics = [
        {"mlb_id": "A1", "rev_90d": 100},
        {"mlb_id": "Z1", "rev_90d": 0},
    ]
    abc = compute_abc(metrics)
    assert abc["A1"] == "A"
    assert abc["Z1"] == "C"


def test_compute_abc_empty_universe_all_c() -> None:
    metrics = [{"mlb_id": "X", "rev_90d": 0}, {"mlb_id": "Y", "rev_90d": 0}]
    assert compute_abc(metrics) == {"X": "C", "Y": "C"}


def test_classify_status_new_product_wins() -> None:
    # 10 days old → "Novo" overrides even a zombie status_base.
    assert classify_status("zombie", age_days=10) == "Novo"


def test_classify_status_maps_coverage_signals() -> None:
    assert classify_status("zombie", age_days=200) == "Zumbi"
    assert classify_status("discontinue", age_days=None) == "Descontinuado"
    assert classify_status("slow", age_days=None) == "Lento"
    assert classify_status(None, age_days=None) == "Monitorar"


def test_promo_pct() -> None:
    assert promo_pct(100, 85) == 15.0
    assert promo_pct(100, 100) is None  # no discount
    assert promo_pct(None, 50) is None
    assert promo_pct(0, 0) is None


def test_coverage_days_infinite_when_no_sales() -> None:
    assert coverage_days(50, 0) is None
    assert coverage_days(60, 30) == 60.0  # rate 1/d → 60 days


def test_qualifies_novos() -> None:
    row_over = {"stock_full": 100, "coverage_days": 45.0}
    row_infinite = {"stock_full": 20, "coverage_days": None}
    row_under = {"stock_full": 100, "coverage_days": 12.0}
    row_no_stock = {"stock_full": 0, "coverage_days": None}
    assert _qualifies_novos(row_over) is True
    assert _qualifies_novos(row_infinite) is True
    assert _qualifies_novos(row_under) is False
    assert _qualifies_novos(row_no_stock) is False


def test_build_row_surplus_and_promo() -> None:
    metric = {
        "mlb_id": "MLB1",
        "sku": "SKU-1",
        "title": "t",
        "permalink": "u",
        "stock_full": 31,
        "stock_galpao": 5,
        "status_base": "slow",
        "sold_30d": 30,
        "rev_90d": 500,
        "product_created_at": _TODAY - timedelta(days=200),
        "promo_original": 100,
        "promo_price": 85,
        "promo_type": "DEAL",
        "promo_seller_pct": 15,
        "promo_meli_pct": 0,
    }
    row = _build_row(metric, "A", _TODAY)
    assert row["surplus"] == 1  # 31 stock - 30 sold
    assert row["daily_rate_30d"] == 1.0
    assert row["coverage_days"] == 31.0
    assert row["promo_pct"] == 15.0
    assert row["curve"] == "A"
    assert row["status"] == "Lento"


def test_build_row_new_product_status() -> None:
    metric = {
        "mlb_id": "MLB2",
        "sku": "SKU-2",
        "stock_full": 10,
        "stock_galpao": 0,
        "status_base": "zombie",
        "sold_30d": 0,
        "rev_90d": 0,
        "product_created_at": _TODAY - timedelta(days=5),
        "promo_original": None,
        "promo_price": None,
    }
    row = _build_row(metric, "C", _TODAY)
    assert row["status"] == "Novo"
    assert row["coverage_days"] is None  # no sales
    assert row["promo_pct"] is None
