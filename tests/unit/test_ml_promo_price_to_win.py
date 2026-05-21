"""Tests for the price_to_win-aware policy in decide_for_item.

Invariant tested at the bottom: the CAP is inviolable. No combination of
price_to_win signal can make the engine recommend a price below the
operator's cap floor (= max of margin_floor_price, list_price * (1-cap/100)).
"""

from __future__ import annotations

from typing import Any

import pytest

from tiny_mirror.services.ml_promotion_service import decide_for_item

pytestmark = pytest.mark.unit


def _costs(list_price: float, promo_price: float | None = None) -> dict[str, Any]:
    return {"listPrice": list_price, "promoPrice": promo_price, "freightBands": []}


def test_winning_with_maximum_share_returns_keep_winning_when_opted_in() -> None:
    """Policy (2026-05-21): keep_winning is opt-in per SKU via skip_when_winning.

    When the flag is on AND we already win with maximum share, the engine
    leaves the item alone. Default (flag off) keeps pushing discounts so
    the engine maximises promo coverage regardless of catalog state.
    """
    dec = decide_for_item(
        promos=[],
        costs=_costs(57.0, 39.90),
        cap_seller_pct=30,
        margin_floor_price=39.90,
        price_to_win_info={
            "current_price": 34.90,
            "price_to_win": 34.90,
            "status": "winning",
            "visit_share": "maximum",
        },
        skip_when_winning=True,
    )
    assert dec.action == "keep_winning"
    assert dec.catalog_status == "winning"
    assert dec.visit_share == "maximum"
    assert dec.price_to_win == 34.90


def test_winning_with_maximum_share_default_keeps_pushing() -> None:
    """Without the opt-in flag, the engine still pushes promos even when winning."""
    dec = decide_for_item(
        promos=[],
        costs=_costs(57.0, 39.90),
        cap_seller_pct=30,
        margin_floor_price=39.90,
        price_to_win_info={
            "current_price": 34.90,
            "price_to_win": 34.90,
            "status": "winning",
            "visit_share": "maximum",
        },
    )
    assert dec.action != "keep_winning"
    assert dec.catalog_status == "winning"
    assert dec.visit_share == "maximum"


def test_winning_but_low_share_runs_normal_engine() -> None:
    """When share drops below maximum, the cap-only engine runs normally."""
    dec = decide_for_item(
        promos=[],
        costs=_costs(57.0, 39.90),
        cap_seller_pct=30,
        margin_floor_price=39.90,
        price_to_win_info={
            "current_price": 34.90,
            "price_to_win": 34.90,
            "status": "winning",
            "visit_share": "medium",
        },
    )
    assert dec.action != "keep_winning"
    # context is still annotated for the report
    assert dec.catalog_status == "winning"
    assert dec.visit_share == "medium"


def test_losing_with_price_to_win_above_floor_caps_at_cap() -> None:
    """Policy: when losing and price_to_win >= floor, the engine still
    respects the cap. If cap can't reach price_to_win, still_losing flag is set.
    """
    dec = decide_for_item(
        promos=[],
        costs=_costs(100.0, 90.0),  # list=100, sheet floor=90
        cap_seller_pct=5,  # cap allows only -5% → R$ 95.00
        margin_floor_price=90.0,
        price_to_win_info={
            "current_price": 100.0,
            "price_to_win": 92.0,  # competitor at 92 (above our floor of 90)
            "status": "losing",
            "visit_share": "low",
        },
    )
    # Cap forces target >= 95 (max of floor=90 and 100*(1-5%)=95). Still losing vs 92.
    assert dec.action == "create_price_discount"
    assert dec.target_price is not None
    assert dec.target_price >= 95.0
    assert dec.still_losing is True
    assert dec.catalog_status == "losing"


def test_losing_with_price_to_win_below_floor_stays_at_floor_and_flags() -> None:
    """Policy: when losing and price_to_win < floor, engine caps at floor
    (cap is inviolable) and flags still_losing=True.
    """
    dec = decide_for_item(
        promos=[],
        costs=_costs(100.0, 90.0),
        cap_seller_pct=30,
        margin_floor_price=90.0,  # absolute floor
        price_to_win_info={
            "current_price": 100.0,
            "price_to_win": 70.0,  # competitor BELOW our floor
            "status": "losing",
            "visit_share": "low",
        },
    )
    # Cap allows down to 70, but margin_floor is 90 → target = 90.
    assert dec.action == "create_price_discount"
    assert dec.target_price == 90.0
    assert dec.still_losing is True


def test_no_price_to_win_info_keeps_legacy_behavior() -> None:
    """price_to_win_info=None must not change any existing behavior."""
    dec = decide_for_item(
        promos=[],
        costs=_costs(57.0, 39.90),
        cap_seller_pct=30,
        margin_floor_price=39.90,
        price_to_win_info=None,
    )
    assert dec.action in ("create_price_discount", "skip")
    assert dec.catalog_status is None
    assert dec.price_to_win is None
    assert dec.still_losing is False


# -----------------------------------------------------------------------------
# Invariant: cap is inviolable. Try every combination of price_to_win values
# and the engine MUST never recommend a price below max(floor, list*(1-cap%)).
# -----------------------------------------------------------------------------
@pytest.mark.parametrize("ptw", [10.0, 50.0, 60.0, 70.0, 90.0, 95.0, 100.0, 200.0])
@pytest.mark.parametrize("status", ["winning", "losing"])
@pytest.mark.parametrize("share", ["maximum", "medium", "low"])
def test_cap_is_inviolable(ptw: float, status: str, share: str) -> None:
    """No price_to_win value (high or absurdly low) can break the cap floor.

    Setup: list=100, cap=20% (=R$80 by cap), margin_floor=85 (BRL).
    Effective minimum target = max(80, 85) = 85. The engine MUST NOT
    return target_price below 85, regardless of price_to_win input.
    """
    list_price = 100.0
    cap_pct = 20.0
    margin_floor = 85.0
    cap_min_by_pct = list_price * (1 - cap_pct / 100)
    minimum_allowed = max(margin_floor, cap_min_by_pct)

    dec = decide_for_item(
        promos=[],
        costs=_costs(list_price, margin_floor),
        cap_seller_pct=cap_pct,
        margin_floor_price=margin_floor,
        price_to_win_info={
            "current_price": list_price,
            "price_to_win": ptw,
            "status": status,
            "visit_share": share,
        },
    )

    if dec.target_price is not None:
        assert dec.target_price >= minimum_allowed - 0.01, (
            f"CAP VIOLATED: target={dec.target_price} < min_allowed={minimum_allowed} "
            f"(ptw={ptw}, status={status}, share={share}, action={dec.action})"
        )
