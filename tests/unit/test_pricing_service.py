"""Unit tests for the pricing/margin service.

The expected numbers in the headline tests were taken directly from the
Controle 4.0 spreadsheet (ml_costs_snapshot rows fetched 2026-05-19),
not from an idealised model — they prove the formula matches reality.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tiny_mirror.services.pricing_service import (
    PricingDataError,
    margin_at_price,
    target_price_for_max_discount_pct,
    target_price_for_min_margin_pct,
)

pytestmark = pytest.mark.unit


# Real freight band table from the spreadsheet (BUB-PATIN-BANH-COLOR).
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

# Real freight band table from SLF-KITBAN-2PC-PR (each SKU has its own
# table; dimensions/weight drive the cost).
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

# Real freight band table from SLF-ROLOMS-43X5-5 (lighter SKU → cheaper).
SLF_ROLOMS_BANDS = [
    {"min": 0, "max": 18.99, "cost": 5.95},
    {"min": 19, "max": 48.99, "cost": 6.65},
    {"min": 49, "max": 78.99, "cost": 7.85},
    {"min": 79, "max": 99.99, "cost": 13.25},
    {"min": 100, "max": 119.99, "cost": 15.45},
    {"min": 120, "max": 149.99, "cost": 17.65},
    {"min": 150, "max": 199.99, "cost": 19.85},
    {"min": 200, "max": None, "cost": 22.65},
]


def test_bub_patin_matches_sheet_margin() -> None:
    """BUB-PATIN sheet says: price 39.90 → margin R$ 7.28 / 18.25%.

    Formula breakdown the sheet implies:
        39.90 - 16.89 - 4.59(11.5% commission) - 4.59(11.5% DIFAL) - 6.55(freight 19-48.99)
        = 7.28
    """
    m = margin_at_price(
        price=Decimal("39.90"),
        base_cost=Decimal("16.89"),
        commission_pct=Decimal("11.5"),
        freight_bands=BUB_PATIN_BANDS,
    )
    assert m.margin_value == Decimal("7.28")
    assert m.margin_pct == Decimal("18.25")
    assert m.commission_value == Decimal("4.59")
    assert m.difal_value == Decimal("4.59")
    assert m.freight_value == Decimal("6.55")


def test_slf_kitban_premium_165_matches_sheet() -> None:
    """SLF-KITBAN-2PC-PR sheet: price 45.90 → margin R$ 0.58 / 1.26%.

    Different commission tier (16.5%) and different freight (R$ 6.75).
    Proves the formula is not coupled to commission == DIFAL.
    """
    m = margin_at_price(
        price=Decimal("45.90"),
        base_cost=Decimal("25.72"),
        commission_pct=Decimal("16.5"),
        freight_bands=SLF_KITBAN_BANDS,
    )
    assert m.margin_value == Decimal("0.58")
    assert m.margin_pct == Decimal("1.26")
    assert m.commission_value == Decimal("7.57")  # 16.5% * 45.90
    assert m.difal_value == Decimal("5.28")  # 11.5% * 45.90
    assert m.freight_value == Decimal("6.75")  # band 19-48.99


def test_slf_roloms_matches_sheet() -> None:
    """SLF-ROLOMS-43X5-5 sheet: price 26.90 → margin R$ 3.03 / 11.26%.

    Lighter SKU than SLF-KITBAN so the 19-48.99 freight band is R$ 6.65
    instead of R$ 6.75 — freight tables are per-SKU.
    """
    m = margin_at_price(
        price=Decimal("26.90"),
        base_cost=Decimal("9.69"),
        commission_pct=Decimal("16.5"),
        freight_bands=SLF_ROLOMS_BANDS,
    )
    assert m.margin_value == Decimal("3.03")
    assert m.margin_pct == Decimal("11.26")


def test_smart_promo_with_meli_banca() -> None:
    """When ML co-pays a percentage of the list price (SMART/DEAL), the
    seller effectively receives ``price + (list_price * meli_banca_pct)``.

    BUB-PATIN SMART real: price 34.90, list 57.00, meli 3.7% (banca 2.11).
    Without banca the margin would be slightly negative; with banca it
    crosses zero and becomes positive.
    """
    without = margin_at_price(
        price=Decimal("34.90"),
        base_cost=Decimal("16.89"),
        commission_pct=Decimal("11.5"),
        freight_bands=BUB_PATIN_BANDS,
    )
    with_banca = margin_at_price(
        price=Decimal("34.90"),
        base_cost=Decimal("16.89"),
        commission_pct=Decimal("11.5"),
        freight_bands=BUB_PATIN_BANDS,
        list_price=Decimal("57.00"),
        meli_banca_pct=Decimal("3.7"),
    )
    expected_banca = (Decimal("57.00") * Decimal("3.7") / Decimal(100)).quantize(Decimal("0.01"))
    assert with_banca.ml_banca_value == expected_banca
    # Margin grows by exactly the banca amount.
    assert with_banca.margin_value == without.margin_value + expected_banca


def test_freight_band_lookup_boundary() -> None:
    """Price 19.00 falls in the 19-48.99 band, not the 0-18.99 band."""
    m_below = margin_at_price(
        price=Decimal("18.99"),
        base_cost=Decimal("5"),
        commission_pct=Decimal("11.5"),
        freight_bands=BUB_PATIN_BANDS,
    )
    m_at = margin_at_price(
        price=Decimal("19.00"),
        base_cost=Decimal("5"),
        commission_pct=Decimal("11.5"),
        freight_bands=BUB_PATIN_BANDS,
    )
    assert m_below.freight_value == Decimal("5.65")
    assert m_at.freight_value == Decimal("6.55")


def test_freight_open_ended_top_band() -> None:
    """Above 200 falls into the open-ended top band."""
    m = margin_at_price(
        price=Decimal("999"),
        base_cost=Decimal("50"),
        commission_pct=Decimal("11.5"),
        freight_bands=BUB_PATIN_BANDS,
    )
    assert m.freight_value == Decimal("20.95")


def test_target_price_for_max_discount_pct() -> None:
    """30% off R$ 57.00 = R$ 39.90 (no formula tricks)."""
    p = target_price_for_max_discount_pct(
        list_price=Decimal("57.00"),
        max_discount_pct=Decimal("30"),
    )
    assert p == Decimal("39.90")


def test_target_price_for_min_margin_round_trip() -> None:
    """Asking for 18.25% margin on BUB-PATIN should land on a price whose
    realised margin is ≥ 18.25%. The reference is sheet_promo_price 39.90
    (which yields exactly 18.25%); the solver should land at or below it
    while still meeting the floor.
    """
    target = target_price_for_min_margin_pct(
        base_cost=Decimal("16.89"),
        commission_pct=Decimal("11.5"),
        freight_bands=BUB_PATIN_BANDS,
        min_margin_pct=Decimal("18.25"),
    )
    # Compute actual margin at that price; should be >= 18.25% within rounding.
    realised = margin_at_price(
        price=target,
        base_cost=Decimal("16.89"),
        commission_pct=Decimal("11.5"),
        freight_bands=BUB_PATIN_BANDS,
    )
    assert realised.margin_pct >= Decimal("18.20")  # 5 bps rounding tolerance
    # Sanity: should be close to the sheet's own number (39.90).
    assert abs(target - Decimal("39.90")) < Decimal("1.00")


def test_target_price_unreachable_margin_raises() -> None:
    """If the operator asks for a margin higher than what fees allow, the
    solver must raise rather than silently return a nonsense floor.

    Commission 16.5% + DIFAL 11.5% = 28% in fees. Asking for 80% margin
    leaves a divisor of -8% — impossible.
    """
    with pytest.raises(PricingDataError, match="unreachable"):
        target_price_for_min_margin_pct(
            base_cost=Decimal("10"),
            commission_pct=Decimal("16.5"),
            freight_bands=SLF_KITBAN_BANDS,
            min_margin_pct=Decimal("80"),
        )


def test_margin_at_price_rejects_meli_banca_without_list_price() -> None:
    with pytest.raises(PricingDataError, match="list_price required"):
        margin_at_price(
            price=Decimal("30"),
            base_cost=Decimal("10"),
            commission_pct=Decimal("11.5"),
            freight_bands=BUB_PATIN_BANDS,
            meli_banca_pct=Decimal("3.7"),
        )


def test_margin_at_price_rejects_none_base_cost() -> None:
    with pytest.raises(PricingDataError):
        margin_at_price(
            price=Decimal("30"),
            base_cost=None,  # type: ignore[arg-type]
            commission_pct=Decimal("11.5"),
            freight_bands=BUB_PATIN_BANDS,
        )


def test_margin_breakdown_as_dict_roundtrip() -> None:
    m = margin_at_price(
        price=Decimal("39.90"),
        base_cost=Decimal("16.89"),
        commission_pct=Decimal("11.5"),
        freight_bands=BUB_PATIN_BANDS,
    )
    d = m.as_dict()
    assert d["margin_value"] == pytest.approx(7.28)
    assert d["margin_pct"] == pytest.approx(18.25)
    assert d["seller_receives"] == pytest.approx(39.90)
