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
    ABSOLUTE_MAX_CAP_PCT,
    TARGET_MARGIN_PCT,
    CapCalculation,
    _conservative_pick,
    calc_cap_for_snapshot,
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


def test_bub_patin_caps_at_30_pct() -> None:
    """BUB-PATIN has fat margin: at floor for 10% margin the implied
    discount is way above 30%, so the cap clips to the commercial ceiling."""
    snap = FakeSnap(
        mlb_id="MLB3884049149",
        sku="BUB-PATIN-BANH-COLOR",
        base_cost=Decimal("16.89"),
        commission_pct=Decimal("11.5"),
        list_price=Decimal("57.00"),
        freight_bands=BUB_PATIN_BANDS,
    )
    calc = calc_cap_for_snapshot(snap)  # type: ignore[arg-type]
    assert not calc.skipped
    assert calc.cap_pct == ABSOLUTE_MAX_CAP_PCT
    assert "clipado em 30%" in calc.reason
    # And the floor (10% margin price) should be well below the 30% cap price (R$ 39.90).
    assert calc.floor_price is not None
    assert calc.floor_price < Decimal("39.90")


def test_slf_kitban_tight_margin_yields_intermediate_cap() -> None:
    """SLF-KITBAN-2PC-PR at sheet_promo_price 45.90 had only 1.26% margin
    on the sheet — the sheet was discounting too aggressively. At list
    price (65.57) the margin is ~20%, so the cap can be wider than 0 but
    must be tighter than the global 30% ceiling. Expected cap ~17%.
    """
    snap = FakeSnap(
        mlb_id="MLB-FAKE-KITBAN",
        sku="SLF-KITBAN-2PC-PR",
        base_cost=Decimal("25.72"),
        commission_pct=Decimal("16.5"),
        list_price=Decimal("65.57"),
        freight_bands=SLF_KITBAN_BANDS,
    )
    calc = calc_cap_for_snapshot(snap)  # type: ignore[arg-type]
    assert not calc.skipped
    assert Decimal("15") < calc.cap_pct < Decimal("20")
    assert calc.cap_pct < ABSOLUTE_MAX_CAP_PCT
    assert calc.margin_pct_at_floor is not None
    assert calc.margin_pct_at_floor >= Decimal("10")


def test_high_commission_sku_with_low_list_price_yields_zero_cap() -> None:
    """A SKU whose full list price already yields less than 10% margin
    must get cap=0 (cannot promote without burning money)."""
    snap = FakeSnap(
        mlb_id="MLB-TIGHT",
        sku="TIGHT-SKU",
        base_cost=Decimal("40"),
        commission_pct=Decimal("16.5"),
        list_price=Decimal("55"),  # 55 - 40 - 9.07 - 6.33 - 7.95 ≈ -8 → negative margin
        freight_bands=SLF_KITBAN_BANDS,
    )
    calc = calc_cap_for_snapshot(snap)  # type: ignore[arg-type]
    assert not calc.skipped
    assert calc.cap_pct == Decimal(0)
    assert "inatingivel" in calc.reason


def test_calc_uses_target_margin_pct_module_constant() -> None:
    """Sanity: the policy constants are visible to anyone re-tuning them."""
    assert TARGET_MARGIN_PCT == Decimal("10")
    assert ABSOLUTE_MAX_CAP_PCT == Decimal("30")


def test_calc_skips_when_freight_bands_missing() -> None:
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


def test_calc_skips_when_fetch_error_set() -> None:
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


def test_conservative_pick_prefers_lowest_cap_across_mlbs() -> None:
    """When a SKU has multiple variations with different costs, the cap
    must protect the most expensive (lowest-margin) MLB."""
    cheap = CapCalculation(
        mlb_id="MLB-cheap",
        sku="S",
        cap_pct=Decimal("30"),
        floor_price=Decimal("20"),
        margin_pct_at_floor=Decimal("10"),
        list_price=Decimal("100"),
        reason="cheap variant",
        skipped=False,
    )
    expensive = CapCalculation(
        mlb_id="MLB-expensive",
        sku="S",
        cap_pct=Decimal("12"),
        floor_price=Decimal("88"),
        margin_pct_at_floor=Decimal("10"),
        list_price=Decimal("100"),
        reason="expensive variant",
        skipped=False,
    )
    picked = _conservative_pick([cheap, expensive])
    assert picked.mlb_id == "MLB-expensive"  # the tighter cap wins


def test_conservative_pick_falls_back_to_skipped_only_if_no_other_option() -> None:
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
    picked = _conservative_pick([skipped])
    assert picked.mlb_id == "X"
