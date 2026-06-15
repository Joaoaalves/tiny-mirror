"""Unit tests for MLPromotionService._build_apply_body.

The dispatcher is what defines which (promo_type, decision_kind) the
executor knows how to enroll. Anything that returns None here will be
recorded as ml_apply_status='skipped' at runtime — explicit so the
operator sees in the UI that we did NOT touch ML for that row.

Each test pins one known shape we send to ML. When the live endpoint
rejects one of these as the wrong shape, fix the dispatcher and update
the test — the test IS the contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pytest

from tiny_mirror.services.ml_promotion_service import MLPromotionService

pytestmark = pytest.mark.unit


@dataclass
class FakeRow:
    id: int = 1
    mlb_id: str = "MLB123"
    promo_type: str = "DEAL"
    decision_kind: str = "would_activate"
    promo_id: str | None = "PROMO_XYZ"
    target_price: Decimal | None = Decimal("78.90")
    target_total_pct: Decimal | None = Decimal("30.0")
    stock_chosen: int | None = None


def test_deal_would_activate_uses_deal_price() -> None:
    body = MLPromotionService._build_apply_body(
        FakeRow(promo_type="DEAL", decision_kind="would_activate")
    )
    assert body == {
        "promotion_id": "PROMO_XYZ",
        "promotion_type": "DEAL",
        "deal_price": 78.9,
    }


def test_lightning_would_activate_same_shape_as_deal() -> None:
    body = MLPromotionService._build_apply_body(
        FakeRow(promo_type="LIGHTNING", decision_kind="would_activate")
    )
    assert body is not None
    assert body["promotion_type"] == "LIGHTNING"
    assert body["deal_price"] == 78.9


def test_lightning_includes_stock_when_chosen() -> None:
    # Doc ML "Specify items for a lightning deal": o body leva `stock` (qtd).
    body = MLPromotionService._build_apply_body(
        FakeRow(promo_type="LIGHTNING", decision_kind="would_activate", stock_chosen=12)
    )
    assert body is not None
    assert body["stock"] == 12


def test_dod_does_not_send_stock() -> None:
    # Doc ML "Daily Deal": o POST é {deal_price, promotion_type}; `stock` ali é
    # informativo (estoque mínimo), NÃO um campo do request — mandar pode dar 400.
    body = MLPromotionService._build_apply_body(
        FakeRow(promo_type="DOD", decision_kind="would_activate", stock_chosen=12)
    )
    assert body is not None
    assert "stock" not in body


def test_seller_coupon_uses_discount_percentage() -> None:
    body = MLPromotionService._build_apply_body(
        FakeRow(promo_type="SELLER_COUPON_CAMPAIGN", decision_kind="would_activate"),
    )
    assert body == {
        "promotion_id": "PROMO_XYZ",
        "promotion_type": "SELLER_COUPON_CAMPAIGN",
        "discount_percentage": 30.0,
    }


def test_seller_coupon_requires_target_total_pct() -> None:
    body = MLPromotionService._build_apply_body(
        FakeRow(
            promo_type="SELLER_COUPON_CAMPAIGN",
            decision_kind="would_activate",
            target_total_pct=None,
        )
    )
    # Without a % the executor must skip — sending an empty body would
    # surprise ML.
    assert body is None


def test_price_discount_create_has_no_promotion_id() -> None:
    body = MLPromotionService._build_apply_body(
        FakeRow(
            promo_type="PRICE_DISCOUNT",
            decision_kind="create_price_discount",
            promo_id=None,
        )
    )
    assert body == {
        "promotion_type": "PRICE_DISCOUNT",
        "deal_price": 78.9,
    }
    # Critical: a CREATE must not carry a promotion_id (ML treats that
    # as 'enrol in this existing campaign' which is the opposite).
    assert body is not None and "promotion_id" not in body


def test_price_discount_would_activate_carries_promotion_id() -> None:
    body = MLPromotionService._build_apply_body(
        FakeRow(promo_type="PRICE_DISCOUNT", decision_kind="would_activate")
    )
    assert body == {
        "promotion_id": "PROMO_XYZ",
        "promotion_type": "PRICE_DISCOUNT",
        "deal_price": 78.9,
    }


def test_smart_is_skipped() -> None:
    # SMART is ML-managed; sending a POST would be wrong. Skip is the
    # correct behaviour, not failure.
    body = MLPromotionService._build_apply_body(
        FakeRow(promo_type="SMART", decision_kind="would_activate")
    )
    assert body is None


def test_unknown_kind_is_skipped() -> None:
    # A row whose decision_kind we don't have a shape for — better to
    # skip than to invent one.
    body = MLPromotionService._build_apply_body(
        FakeRow(promo_type="DEAL", decision_kind="something_new")
    )
    assert body is None


def test_missing_promo_id_for_existing_campaign_is_skipped() -> None:
    # would_activate REQUIRES a promo_id (it's the campaign we're
    # enrolling in). A row with None there is malformed — skip.
    body = MLPromotionService._build_apply_body(
        FakeRow(promo_type="DEAL", decision_kind="would_activate", promo_id=None)
    )
    assert body is None
