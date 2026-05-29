"""Unit tests for bulk-act helpers used by POST /decisions/bulk-act.

The endpoint orchestrates a SQL SELECT + UPDATE; the interesting
business logic is in two pure helpers:

- ``_row_delta_pct`` — (target - list) / list * 100, with None / zero
  guards. This is what the dry-run preview surfaces as ``avg_delta_pct``
  so the operator sees how steep the slice they're about to ignore is.
- ``_row_passes_delta_range`` — the range gate. ``None``/``None`` is
  the wildcard; a row whose Δ% can't be computed and the operator
  asked for a slice is rejected (not in the slice).

These cover the cases that drove the design: SELLER_COUPON_CAMPAIGN
rows tend to have target_price=None (cupom is a % off applied at
checkout, not a fixed price), so a ``max_delta_pct=-5`` slice should
exclude them rather than silently flip them.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pytest

from tiny_mirror.services.ml_promotion_service import MLPromotionService

pytestmark = pytest.mark.unit


@dataclass
class FakeRow:
    target_price: Decimal | None
    list_price: Decimal | None


def test_row_delta_pct_typical_discount() -> None:
    # 100 → 85 = -15% (the usual case).
    d = MLPromotionService._row_delta_pct(
        FakeRow(target_price=Decimal("85.00"), list_price=Decimal("100.00"))
    )
    assert d is not None
    assert d == pytest.approx(-15.0)


def test_row_delta_pct_missing_target_returns_none() -> None:
    # SELLER_COUPON_CAMPAIGN rows often have no target_price — the
    # discount is a % at checkout, not a fixed price.
    d = MLPromotionService._row_delta_pct(FakeRow(target_price=None, list_price=Decimal("100.00")))
    assert d is None


def test_row_delta_pct_zero_list_returns_none_not_zero_div() -> None:
    d = MLPromotionService._row_delta_pct(
        FakeRow(target_price=Decimal("50.00"), list_price=Decimal("0"))
    )
    assert d is None


def test_row_passes_delta_range_no_bounds_is_wildcard() -> None:
    # Both bounds None → every row passes, even those with no Δ%.
    assert (
        MLPromotionService._row_passes_delta_range(
            FakeRow(target_price=None, list_price=None), None, None
        )
        is True
    )


def test_row_passes_delta_range_inside_window() -> None:
    # -15% with window [-20, -10] → in.
    row = FakeRow(target_price=Decimal("85.00"), list_price=Decimal("100.00"))
    assert MLPromotionService._row_passes_delta_range(row, -20.0, -10.0) is True


def test_row_passes_delta_range_outside_window() -> None:
    # -5% with window [-20, -10] → out (drop too shallow for the slice).
    row = FakeRow(target_price=Decimal("95.00"), list_price=Decimal("100.00"))
    assert MLPromotionService._row_passes_delta_range(row, -20.0, -10.0) is False


def test_row_passes_delta_range_max_only_for_steep_discounts() -> None:
    # max_delta_pct=-15 → only price drops of 15%+ pass (target <= 85).
    deep = FakeRow(target_price=Decimal("80.00"), list_price=Decimal("100.00"))  # -20%
    shallow = FakeRow(target_price=Decimal("90.00"), list_price=Decimal("100.00"))  # -10%
    assert MLPromotionService._row_passes_delta_range(deep, None, -15.0) is True
    assert MLPromotionService._row_passes_delta_range(shallow, None, -15.0) is False


def test_row_passes_delta_range_unknown_delta_rejected_when_range_set() -> None:
    # Operator asked for a Δ% slice; a row with no computable Δ% isn't
    # in any slice — reject it rather than silently flipping its status.
    coupon = FakeRow(target_price=None, list_price=Decimal("100.00"))
    assert MLPromotionService._row_passes_delta_range(coupon, None, -15.0) is False
