"""Coverage for score_candidate_promo's 3 fixes + new structure fields.

Bug 1 — case 2 (min_discounted_price) no longer denies when min < floor
  as long as the computed target (= max(min, floor, target_at_cap)) is safe.

Bug 2 — case 2 prefers suggested_discounted_price when it lies inside the
  admissible band [lower, upper].

Bug 3 — case 2 respects max_discounted_price: when [lower, upper] is empty
  the promo is denied with reason `interval_empty: ...`.

Opportunity — every entry carries `structure_type`, `is_fixed_price`,
  `exposure_boost` (1.3 for DOD/LIGHTNING/DEAL).
"""

from __future__ import annotations

import pytest

from tiny_mirror.services.ml_promotion_service import score_candidate_promo

pytestmark = pytest.mark.unit


def test_min_below_floor_still_accepts_when_target_safe():
    """STOLF-style: min=35 below floor=42, but max=50 → target=42 is safe."""
    out = score_candidate_promo(
        {
            "type": "DEAL",
            "min_discounted_price": 35.0,
            "max_discounted_price": 50.0,
            "meli_percentage": 0,
        },
        cap_seller_pct=20.0,
        margin_floor_price=42.0,
        list_price=50.0,
    )
    assert out is not None
    assert out["accepted"] is True
    assert out["denied_reason"] is None
    assert out["target_price"] == 42.0
    assert out["structure_type"] == "INTERVAL"
    assert out["is_fixed_price"] is False


def test_suggested_within_band_is_preferred():
    out = score_candidate_promo(
        {
            "type": "DOD",
            "min_discounted_price": 35.0,
            "max_discounted_price": 50.0,
            "suggested_discounted_price": 45.0,
            "meli_percentage": 5,
        },
        cap_seller_pct=20.0,
        margin_floor_price=40.0,
        list_price=50.0,
    )
    assert out["accepted"] is True
    assert out["target_price"] == 45.0
    assert out["constraint"] == "suggested_within_interval"
    assert out["exposure_boost"] == 1.3
    assert "ML 5.0%" in out["reason"]
    assert "Sugerido ML" in out["reason"]


def test_suggested_outside_band_falls_back_to_lower():
    """suggested=30 below lower=35 → must use lower bound, not suggested."""
    out = score_candidate_promo(
        {
            "type": "PRICE_DISCOUNT",
            "min_discounted_price": 35.0,
            "max_discounted_price": 50.0,
            "suggested_discounted_price": 30.0,
            "meli_percentage": 0,
        },
        cap_seller_pct=30.0,
        margin_floor_price=20.0,
        list_price=50.0,
    )
    assert out["accepted"] is True
    assert out["target_price"] == 35.0
    assert out["constraint"] == "min_discounted_price"


def test_interval_empty_when_floor_above_max():
    out = score_candidate_promo(
        {
            "type": "LIGHTNING",
            "min_discounted_price": 30.0,
            "max_discounted_price": 38.0,
            "meli_percentage": 0,
        },
        cap_seller_pct=20.0,
        margin_floor_price=42.0,
        list_price=50.0,
    )
    assert out["accepted"] is False
    # English machine code in denied_reason, Portuguese in user-facing reason.
    assert "interval_empty" in out["denied_reason"]
    assert "piso R$ 42.00" in out["denied_reason"]
    assert "Piso R$ 42.00" in out["reason"]
    assert "máx ML" in out["reason"]


def test_smart_marks_fixed_price():
    out = score_candidate_promo(
        {
            "type": "SMART",
            "price": 45.0,
            "seller_percentage": 10.0,
            "original_price": 50.0,
            "meli_percentage": 0,
        },
        cap_seller_pct=20.0,
        margin_floor_price=40.0,
        list_price=50.0,
    )
    assert out["is_fixed_price"] is True
    assert out["structure_type"] == "FIXED_PRICE"
    assert out["exposure_boost"] == 1.0


def test_seller_coupon_is_fixed_pct():
    out = score_candidate_promo(
        {"type": "SELLER_COUPON_CAMPAIGN", "fixed_percentage": 10.0, "meli_percentage": 0},
        cap_seller_pct=20.0,
        margin_floor_price=40.0,
        list_price=50.0,
    )
    assert out["structure_type"] == "FIXED_PCT"
    assert out["is_fixed_price"] is True


def test_copay_breakdown_in_reason_when_meli_pct():
    out = score_candidate_promo(
        {
            "type": "PRICE_DISCOUNT",
            "min_discounted_price": 30.0,
            "max_discounted_price": 45.0,
            "meli_percentage": 5,
        },
        cap_seller_pct=15.0,
        margin_floor_price=35.0,
        list_price=50.0,
    )
    # PT format: "...· você 15.0% + ML 5.0%"
    assert "você 15.0%" in out["reason"]
    assert "ML 5.0%" in out["reason"]


def test_copay_omitted_when_meli_pct_zero():
    out = score_candidate_promo(
        {"type": "DEAL", "min_discounted_price": 35.0, "max_discounted_price": 50.0},
        cap_seller_pct=20.0,
        margin_floor_price=20.0,
        list_price=50.0,
    )
    assert "ML " not in out["reason"]
    assert "você" not in out["reason"]


def test_cap_exceeded_still_denies_fixed_pct():
    """Bug-1 fix must not weaken existing cap_exceeded denial path."""
    out = score_candidate_promo(
        {"type": "SELLER_COUPON_CAMPAIGN", "fixed_percentage": 50.0, "meli_percentage": 0},
        cap_seller_pct=20.0,
        margin_floor_price=10.0,
        list_price=50.0,
    )
    assert out["accepted"] is False
    assert "cap_exceeded" in out["denied_reason"]
    assert "Cap excedido" in out["reason"]


def test_reason_uses_portuguese_for_accepted_interval():
    """Smoke that the user-facing reason is in PT (no English keywords)."""
    out = score_candidate_promo(
        {
            "type": "DEAL",
            "min_discounted_price": 35.0,
            "max_discounted_price": 50.0,
            "meli_percentage": 0,
        },
        cap_seller_pct=20.0,
        margin_floor_price=42.0,
        list_price=50.0,
    )
    # English tokens must not appear in the user-facing reason.
    assert "lower" not in out["reason"]
    assert "suggested" not in out["reason"]
    assert "min " not in out["reason"]
    assert "floor" not in out["reason"].lower() or "piso" in out["reason"].lower()
