"""Unit tests for cap_recompute_service.

Builds fake MLCostsSnapshotORM-shaped objects so the calculator can be
exercised without spinning up Postgres. All expected numbers come from
the same Controle 4.0 snapshots used in test_pricing_service.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest

from tiny_mirror.services.cap_recompute_service import (
    DEFAULT_CAP_PCT,
    MIN_MARGIN_PCT,
    CapCalculation,
    _consolidate_sku,
    _pick_best_started_promo,
    calc_cap_for_snapshot,
    calc_cap_from_active_promo,
)

pytestmark = pytest.mark.unit


@dataclass
class FakeSnap:
    """Quack-types as MLCostsSnapshotORM for the read-only fields we touch."""

    mlb_id: str
    sku: str
    base_cost: Decimal | None
    commission_pct: Decimal | None
    list_price: Decimal | None
    freight_bands: list[dict[str, Any]] | None
    fetch_error: str | None = None
    sheet_discount_pct: Decimal | None = None
    sheet_promo_price: Decimal | None = None


BUB_PATIN_BANDS = [
    {"min": 0, "max": 18.99, "cost": 5.65},
    {"min": 19, "max": 48.99, "cost": 6.55},
    {"min": 49, "max": 78.99, "cost": 7.75},
    {"min": 79, "max": 99.99, "cost": 12.35},
    {"min": 100, "max": 119.99, "cost": 14.35},
    {"min": 120, "max": 149.99, "cost": 16.45},
    {"min": 150, "max": 199.99, "cost": 18.45},
    {"min": 200, "max": None, "cost": 20.95},
]

SLF_KITBAN_BANDS = [
    {"min": 0, "max": 18.99, "cost": 6.05},
    {"min": 19, "max": 48.99, "cost": 6.75},
    {"min": 49, "max": 78.99, "cost": 7.95},
    {"min": 79, "max": 99.99, "cost": 13.85},
    {"min": 100, "max": 119.99, "cost": 16.15},
    {"min": 120, "max": 149.99, "cost": 18.45},
    {"min": 150, "max": 199.99, "cost": 20.75},
    {"min": 200, "max": None, "cost": 23.65},
]


# ---------------------------------------------------------------------------
# calc_cap_from_active_promo — baseline is the live STARTED promo
# ---------------------------------------------------------------------------
def test_active_promo_anchors_cap_to_seller_pct() -> None:
    """If the operator runs a 25% PRICE_DISCOUNT today, the cap MUST be 25%
    (or higher), never lower — otherwise the live promo would violate the
    cap, defeating the whole 'today is the baseline' policy."""
    snap = FakeSnap(
        mlb_id="MLB-X",
        sku="X-SKU",
        base_cost=Decimal("16.89"),
        commission_pct=Decimal("11.5"),
        list_price=Decimal("57.00"),
        freight_bands=BUB_PATIN_BANDS,
        sheet_discount_pct=Decimal("30"),
    )
    active = {
        "type": "PRICE_DISCOUNT",
        "status": "started",
        "original_price": 57.0,
        "price": 42.75,  # 25% off
        "meli_percentage": 0,
    }
    calc = calc_cap_from_active_promo(snap, active)  # type: ignore[arg-type]
    assert not calc.skipped
    assert calc.cap_pct == Decimal("25.00")
    assert calc.floor_price == Decimal("42.75")
    assert calc.source == "active_promo"
    assert "PRICE_DISCOUNT" in calc.reason


def test_active_promo_subtracts_meli_share_from_cap() -> None:
    """Cap is on SELLER share — ML's banca contribution doesn't count
    against it. 30% total with 10% meli => seller cap = 20%."""
    snap = FakeSnap(
        mlb_id="MLB-X",
        sku="X-SKU",
        base_cost=Decimal("16.89"),
        commission_pct=Decimal("11.5"),
        list_price=Decimal("100.00"),
        freight_bands=BUB_PATIN_BANDS,
    )
    active = {
        "type": "DEAL",
        "status": "started",
        "original_price": 100.0,
        "price": 70.0,
        "meli_percentage": 10,  # 10% banca
    }
    calc = calc_cap_from_active_promo(snap, active)  # type: ignore[arg-type]
    assert calc.cap_pct == Decimal("20.00")
    assert calc.floor_price == Decimal("70.00")


def test_active_promo_floor_eq_promo_price_means_no_alert() -> None:
    """The whole point: floor = promo price → engine's
    `price < floor` check is False on today's state."""
    snap = FakeSnap(
        mlb_id="MLB-X",
        sku="X-SKU",
        base_cost=Decimal("40"),
        commission_pct=Decimal("16.5"),
        list_price=Decimal("55"),  # tight margin
        freight_bands=SLF_KITBAN_BANDS,
        sheet_discount_pct=Decimal("30"),
    )
    active = {
        "type": "PRICE_DISCOUNT",
        "status": "started",
        "original_price": 55.0,
        "price": 49.5,  # 10% off — would alert under old margin-10% rule
        "meli_percentage": 0,
    }
    calc = calc_cap_from_active_promo(snap, active)  # type: ignore[arg-type]
    # Accept reality: cap = 10, floor = 49.5, even if margin is tight.
    assert calc.cap_pct == Decimal("10.00")
    assert calc.floor_price == Decimal("49.50")
    # `49.5 < 49.5` is False → no floor violation. Mission accomplished.


# ---------------------------------------------------------------------------
# calc_cap_for_snapshot — fallback when no STARTED promo is live
# ---------------------------------------------------------------------------
def test_fallback_sheet_30_caps_at_30() -> None:
    """Fallback path: no live promo, fat margin, cap = sheet = 30%."""
    snap = FakeSnap(
        mlb_id="MLB3884049149",
        sku="BUB-PATIN-BANH-COLOR",
        base_cost=Decimal("16.89"),
        commission_pct=Decimal("11.5"),
        list_price=Decimal("57.00"),
        freight_bands=BUB_PATIN_BANDS,
        sheet_discount_pct=Decimal("30"),
    )
    calc = calc_cap_for_snapshot(snap)  # type: ignore[arg-type]
    assert not calc.skipped
    assert calc.cap_pct == DEFAULT_CAP_PCT
    assert calc.source == "fallback"
    assert calc.margin_pct_at_floor is not None
    assert calc.margin_pct_at_floor >= Decimal("10")
    assert "sem promo ativa" in calc.reason


def test_fallback_no_sheet_uses_default_30() -> None:
    snap = FakeSnap(
        mlb_id="MLB1",
        sku="X",
        base_cost=Decimal("16.89"),
        commission_pct=Decimal("11.5"),
        list_price=Decimal("57.00"),
        freight_bands=BUB_PATIN_BANDS,
        sheet_discount_pct=None,
    )
    calc = calc_cap_for_snapshot(snap)  # type: ignore[arg-type]
    assert not calc.skipped
    assert calc.cap_pct == DEFAULT_CAP_PCT


def test_fallback_aggressive_sheet_clipped_by_margin() -> None:
    """30% sheet but margin only allows ~17%: clip to 17%."""
    snap = FakeSnap(
        mlb_id="MLB-FAKE-KITBAN",
        sku="SLF-KITBAN-2PC-PR",
        base_cost=Decimal("25.72"),
        commission_pct=Decimal("16.5"),
        list_price=Decimal("65.57"),
        freight_bands=SLF_KITBAN_BANDS,
        sheet_discount_pct=Decimal("30"),
    )
    calc = calc_cap_for_snapshot(snap)  # type: ignore[arg-type]
    assert not calc.skipped
    assert Decimal("15") < calc.cap_pct < Decimal("20")
    assert calc.cap_pct < DEFAULT_CAP_PCT
    assert calc.margin_pct_at_floor is not None
    assert calc.margin_pct_at_floor >= Decimal("10")


def test_fallback_zero_cap_when_floor_unreachable() -> None:
    snap = FakeSnap(
        mlb_id="MLB-TIGHT",
        sku="TIGHT-SKU",
        base_cost=Decimal("40"),
        commission_pct=Decimal("16.5"),
        list_price=Decimal("55"),
        freight_bands=SLF_KITBAN_BANDS,
        sheet_discount_pct=Decimal("30"),
    )
    calc = calc_cap_for_snapshot(snap)  # type: ignore[arg-type]
    assert not calc.skipped
    assert calc.cap_pct == Decimal(0)
    assert "inatingivel" in calc.reason


def test_calc_uses_policy_constants() -> None:
    assert MIN_MARGIN_PCT == Decimal("10")
    assert DEFAULT_CAP_PCT == Decimal("30")


def test_fallback_skips_when_freight_bands_missing() -> None:
    snap = FakeSnap(
        mlb_id="MLB-X",
        sku="X-SKU",
        base_cost=Decimal("10"),
        commission_pct=Decimal("11.5"),
        list_price=Decimal("50"),
        freight_bands=None,
    )
    calc = calc_cap_for_snapshot(snap)  # type: ignore[arg-type]
    assert calc.skipped
    assert "missing" in calc.reason


def test_fallback_skips_when_fetch_error_set() -> None:
    snap = FakeSnap(
        mlb_id="MLB-X",
        sku="X-SKU",
        base_cost=Decimal("10"),
        commission_pct=Decimal("11.5"),
        list_price=Decimal("50"),
        freight_bands=BUB_PATIN_BANDS,
        fetch_error="HTTP 500",
    )
    calc = calc_cap_for_snapshot(snap)  # type: ignore[arg-type]
    assert calc.skipped
    assert "fetch_error" in calc.reason


# ---------------------------------------------------------------------------
# _pick_best_started_promo — pick the most aggressive STARTED promo
# ---------------------------------------------------------------------------
def test_pick_best_started_returns_largest_seller_pct() -> None:
    promos = [
        {
            "status": "candidate",
            "type": "DEAL",
            "original_price": 100,
            "price": 50,
            "meli_percentage": 0,
        },
        {
            "status": "started",
            "type": "PRICE_DISCOUNT",
            "original_price": 100,
            "price": 80,
            "meli_percentage": 0,
        },
        {
            "status": "started",
            "type": "DEAL",
            "original_price": 100,
            "price": 60,
            "meli_percentage": 10,
        },
    ]
    best = _pick_best_started_promo(promos)
    assert best is not None
    # Total -40% with -10% meli = -30% seller (DEAL)
    # vs total -20% with 0% meli = -20% seller (PRICE_DISCOUNT)
    # DEAL wins (more aggressive seller share).
    assert best["type"] == "DEAL"


def test_pick_best_started_returns_none_when_only_candidates() -> None:
    promos = [
        {
            "status": "candidate",
            "type": "DEAL",
            "original_price": 100,
            "price": 50,
            "meli_percentage": 0,
        },
    ]
    assert _pick_best_started_promo(promos) is None


def test_pick_best_started_ignores_promos_with_zero_price() -> None:
    promos = [
        {
            "status": "started",
            "type": "DEAL",
            "original_price": 0,
            "price": 50,
            "meli_percentage": 0,
        },
        {
            "status": "started",
            "type": "PRICE_DISCOUNT",
            "original_price": 100,
            "price": 80,
            "meli_percentage": 0,
        },
    ]
    best = _pick_best_started_promo(promos)
    assert best is not None
    assert best["type"] == "PRICE_DISCOUNT"


# ---------------------------------------------------------------------------
# _consolidate_sku — per-SKU consolidation must never alert on live promos
# ---------------------------------------------------------------------------
def test_consolidate_picks_max_cap_so_no_active_promo_violates() -> None:
    """If two MLBs of a SKU run different active discounts, the SKU cap
    must equal the LARGER of the two, otherwise the deeper promo would
    look like a cap violation."""
    weak = CapCalculation(
        mlb_id="MLB-weak",
        sku="S",
        cap_pct=Decimal("15"),
        floor_price=Decimal("85"),
        margin_pct_at_floor=Decimal("12"),
        list_price=Decimal("100"),
        reason="from active 15%",
        skipped=False,
        source="active_promo",
    )
    deep = CapCalculation(
        mlb_id="MLB-deep",
        sku="S",
        cap_pct=Decimal("40"),
        floor_price=Decimal("60"),
        margin_pct_at_floor=Decimal("8"),
        list_price=Decimal("100"),
        reason="from active 40%",
        skipped=False,
        source="active_promo",
    )
    picked = _consolidate_sku([weak, deep])
    assert picked.cap_pct == Decimal("40")
    assert picked.floor_price == Decimal("60")  # min of the two floors
    assert picked.source == "active_promo"


def test_consolidate_floor_is_min_across_mlbs() -> None:
    a = CapCalculation(
        mlb_id="MLB-a",
        sku="S",
        cap_pct=Decimal("30"),
        floor_price=Decimal("70"),
        margin_pct_at_floor=Decimal("12"),
        list_price=Decimal("100"),
        reason="a",
        skipped=False,
        source="active_promo",
    )
    b = CapCalculation(
        mlb_id="MLB-b",
        sku="S",
        cap_pct=Decimal("20"),
        floor_price=Decimal("50"),
        margin_pct_at_floor=Decimal("8"),
        list_price=Decimal("60"),
        reason="b",
        skipped=False,
        source="active_promo",
    )
    picked = _consolidate_sku([a, b])
    assert picked.cap_pct == Decimal("30")
    assert picked.floor_price == Decimal("50")


def test_consolidate_returns_skipped_when_all_skipped() -> None:
    skipped = CapCalculation(
        mlb_id="X",
        sku="S",
        cap_pct=Decimal(0),
        floor_price=None,
        margin_pct_at_floor=None,
        list_price=None,
        reason="missing",
        skipped=True,
    )
    picked = _consolidate_sku([skipped])
    assert picked.mlb_id == "X"
    assert picked.skipped
