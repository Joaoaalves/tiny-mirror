"""Unit tests for the AS-IS promotion mirror mapping.

The DB upsert/delete loop is integration (needs Postgres); the interesting,
fragile bit is ``promo_to_row`` — it maps the raw ML promo dict to a row exactly
as ML reports it (no cap/floor/decision logic). These tests pin that mapping,
including the promo_key rule and the conditional co-participation case that the
old decision-coupled path got wrong (recording a fabricated price).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tiny_mirror.services.promotion_mirror_service import promo_to_row

pytestmark = pytest.mark.unit


def test_maps_campaign_with_id_as_is() -> None:
    p = {
        "id": "C-MLB4282370",
        "type": "SELLER_CAMPAIGN",
        "sub_type": "FLEXIBLE_PERCENTAGE",
        "status": "started",
        "price": 78.9,
        "original_price": 107.0,
        "name": "Junho da OFFSHOP",
        "start_date": "2026-06-01T03:00:00Z",
        "finish_date": "2026-07-01T02:59:59Z",
    }
    row = promo_to_row("MLB1", "SKU-A", p)
    assert row["promo_key"] == "C-MLB4282370"
    assert row["promotion_id"] == "C-MLB4282370"
    assert row["promotion_type"] == "SELLER_CAMPAIGN"
    assert row["sub_type"] == "FLEXIBLE_PERCENTAGE"
    assert row["status"] == "started"
    assert row["price"] == Decimal("78.9")
    assert row["original_price"] == Decimal("107.0")
    assert row["start_date"] is not None and row["finish_date"] is not None
    assert row["raw"] is p  # raw stored verbatim


def test_price_discount_without_id_keys_on_type() -> None:
    # Seller PRICE_DISCOUNT has no ML id → promo_key falls back to the type.
    row = promo_to_row(
        "MLB2", "SKU-B", {"type": "PRICE_DISCOUNT", "status": "started", "price": 9.9}
    )
    assert row["promotion_id"] is None
    assert row["promo_key"] == "PRICE_DISCOUNT"


def test_conditional_coparticipation_stored_as_is() -> None:
    # SMART/PRICE_MATCHING: store ML's price/min/max/seller% verbatim — NO cap or
    # floor applied, NO fabricated target. The UI decides how to display the
    # conditional nature; the mirror is pure fact.
    p = {
        "id": "P-MLB17131030",
        "type": "PRICE_MATCHING",
        "status": "started",
        "price": 54.55,
        "original_price": 81.28,
        "min_discounted_price": 12.0,
        "max_discounted_price": 60.0,
        "suggested_discounted_price": 50.0,
        "seller_percentage": 29.6,
        "meli_percentage": 3.3,
        "ref_id": "CANDIDATE-xyz",
    }
    row = promo_to_row("MLB3", None, p)
    assert row["promotion_type"] == "PRICE_MATCHING"
    assert row["price"] == Decimal("54.55")
    assert row["min_price"] == Decimal("12.0")
    assert row["max_price"] == Decimal("60.0")
    assert row["suggested_price"] == Decimal("50.0")
    assert row["seller_percentage"] == Decimal("29.6")
    assert row["meli_percentage"] == Decimal("3.3")
    assert row["offer_id"] == "CANDIDATE-xyz"  # ref_id fallback
    assert row["sku"] is None


def test_candidate_status_preserved() -> None:
    row = promo_to_row(
        "MLB4", "SKU-D", {"id": "P-1", "type": "DEAL", "status": "candidate", "price": 0}
    )
    assert row["status"] == "candidate"
