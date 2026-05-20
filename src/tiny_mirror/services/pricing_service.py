"""Real-time profitability / margin math for ML listings.

The formula was reverse-engineered from the Controle 4.0 spreadsheet
(sheet "MERCADO LIVRE"), validated against multiple SKUs across
different commission tiers (Classico 11.5%, Premium 16.5%):

    margin_R$ = price
              - base_cost
              - (price * commission_pct)           # ML commission
              - (price * DIFAL_PCT)                # interstate tax (DIFAL)
              - freight_band(price)                # ML-charged seller freight
              + (list_price * meli_banca_pct)      # ML co-pay on SMART/DEAL

`DIFAL_PCT` is a sheet-wide constant (currently 11.5%). Override via the
``MARGIN_DIFAL_PCT`` env var if the operator changes it.

This module is intentionally pure and stateless — it operates on an
``MLCostsSnapshotORM`` row + a price. Side effects (DB writes, alerts)
live in callers.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from tiny_mirror.config import settings


@dataclass(frozen=True)
class MarginBreakdown:
    """Per-component view of how a price decomposes into margin.

    All values in BRL Decimal except percentages (also Decimal but as %).
    """

    price: Decimal
    base_cost: Decimal
    commission_value: Decimal
    commission_pct: Decimal
    difal_value: Decimal
    difal_pct: Decimal
    freight_value: Decimal
    ml_banca_value: Decimal  # extra received from ML (SMART/DEAL co-pay)
    margin_value: Decimal
    margin_pct: Decimal  # margin_value / price * 100
    seller_receives: Decimal  # price + ml_banca_value (gross before deductions)

    def as_dict(self) -> dict[str, Any]:
        return {
            "price": float(self.price),
            "base_cost": float(self.base_cost),
            "commission_value": float(self.commission_value),
            "commission_pct": float(self.commission_pct),
            "difal_value": float(self.difal_value),
            "difal_pct": float(self.difal_pct),
            "freight_value": float(self.freight_value),
            "ml_banca_value": float(self.ml_banca_value),
            "margin_value": float(self.margin_value),
            "margin_pct": float(self.margin_pct),
            "seller_receives": float(self.seller_receives),
        }


class PricingDataError(ValueError):
    """Raised when the snapshot is missing required fields."""


def _difal_pct() -> Decimal:
    """Currently 11.5% — sheet-wide constant for the seller's tax regime."""
    return Decimal(str(getattr(settings, "margin_difal_pct", 0.115)))


def _freight_for(price: Decimal, freight_bands: list[dict[str, Any]] | None) -> Decimal:
    """Lookup the ML-charged seller freight for the band containing ``price``.

    Bands are inclusive on both ends; ``max=None`` means open-ended.
    Returns 0 if no band matches (defensive; the sheet always has a 200+ band).
    """
    if not freight_bands:
        return Decimal("0")
    p = float(price)
    for band in freight_bands:
        lo = band.get("min")
        hi = band.get("max")
        if lo is None:
            continue
        if p < float(lo):
            continue
        if hi is not None and p > float(hi):
            continue
        return Decimal(str(band.get("cost", 0)))
    return Decimal("0")


def _dec(v: Any) -> Decimal:
    if v is None:
        raise PricingDataError("required field is None")
    return v if isinstance(v, Decimal) else Decimal(str(v))


def margin_at_price(
    *,
    price: Decimal | float | int,
    base_cost: Decimal | float | int,
    commission_pct: Decimal | float | int,
    freight_bands: list[dict[str, Any]] | None,
    list_price: Decimal | float | int | None = None,
    meli_banca_pct: Decimal | float | int = 0,
) -> MarginBreakdown:
    """Compute margin breakdown at a given selling price.

    Arguments:
        price: the price the customer pays (BRL).
        base_cost: product cost (BRL).
        commission_pct: ML commission tier for this SKU, in **percent**
            (e.g. 11.5 not 0.115). Matches the snapshot's commission_pct.
        freight_bands: list of ``{min, max, cost}`` dicts from the snapshot.
        list_price: original (non-promo) price. Required only when
            ``meli_banca_pct > 0`` (SMART/DEAL co-pay is on the list price).
        meli_banca_pct: ML co-pay percentage for the promo (also in
            **percent**, e.g. 3.7). Zero for PRICE_DISCOUNT.

    Returns a ``MarginBreakdown`` with all components and the final margin
    in both BRL and % of price.
    """
    p = _dec(price)
    cost = _dec(base_cost)
    comm_pct = _dec(commission_pct)
    difal_pct = _difal_pct() * Decimal(100)  # express as % to match commission_pct unit

    if meli_banca_pct and list_price is None:
        raise PricingDataError("list_price required when meli_banca_pct > 0")

    comm_val = (p * comm_pct / Decimal(100)).quantize(Decimal("0.01"))
    difal_val = (p * difal_pct / Decimal(100)).quantize(Decimal("0.01"))
    freight_val = _freight_for(p, freight_bands).quantize(Decimal("0.01"))

    banca_val = Decimal("0")
    if meli_banca_pct and list_price is not None:
        banca_val = (_dec(list_price) * _dec(meli_banca_pct) / Decimal(100)).quantize(
            Decimal("0.01")
        )

    margin = (p + banca_val - cost - comm_val - difal_val - freight_val).quantize(Decimal("0.01"))
    margin_pct = (
        (margin / p * Decimal(100)).quantize(Decimal("0.01")) if p != Decimal(0) else Decimal("0")
    )

    return MarginBreakdown(
        price=p,
        base_cost=cost,
        commission_value=comm_val,
        commission_pct=comm_pct,
        difal_value=difal_val,
        difal_pct=difal_pct,
        freight_value=freight_val,
        ml_banca_value=banca_val,
        margin_value=margin,
        margin_pct=margin_pct,
        seller_receives=(p + banca_val).quantize(Decimal("0.01")),
    )


def target_price_for_max_discount_pct(
    *,
    list_price: Decimal | float | int,
    max_discount_pct: Decimal | float | int,
) -> Decimal:
    """The price that results from applying ``max_discount_pct`` to ``list_price``.

    Example: list_price=57, max_discount_pct=30 → 39.90.
    This is what the operator-supplied "% máxima de desconto" cap becomes
    in BRL. The caller is responsible for checking that the resulting
    margin (via ``margin_at_price``) is acceptable.
    """
    lp = _dec(list_price)
    pct = _dec(max_discount_pct)
    return (lp * (Decimal(100) - pct) / Decimal(100)).quantize(Decimal("0.01"))


def target_price_for_min_margin_pct(
    *,
    base_cost: Decimal | float | int,
    commission_pct: Decimal | float | int,
    freight_bands: list[dict[str, Any]] | None,
    min_margin_pct: Decimal | float | int,
    list_price: Decimal | float | int | None = None,
    meli_banca_pct: Decimal | float | int = 0,
) -> Decimal:
    """Solve for the lowest price that still yields ``min_margin_pct`` margin.

    Closed-form when freight is constant in a given band; the freight
    discontinuities mean we evaluate band-by-band and pick the highest
    valid floor across the band breakpoints below the unconstrained
    minimum.

    Formula (per band, freight = F constant within band):
        margin_pct/100 * P = P + banca - cost - P*(c+d)/100 - F
        P * [1 - margin_pct/100 - (c+d)/100] = cost + F - banca
        P = (cost + F - banca) / [1 - margin_pct/100 - (c+d)/100]

    If the divisor is <= 0 (margin_pct too high relative to fees), the
    target is unreachable — we return Decimal('NaN-like') signalled by
    raising. Caller should fall back to list_price or a sensible cap.
    """
    cost = _dec(base_cost)
    comm_pct = _dec(commission_pct)
    margin_pct = _dec(min_margin_pct)
    difal_pct = _difal_pct() * Decimal(100)

    banca = Decimal("0")
    if meli_banca_pct and list_price is not None:
        banca = _dec(list_price) * _dec(meli_banca_pct) / Decimal(100)

    fee_pct = comm_pct + difal_pct  # both expressed as %
    divisor = Decimal(100) - margin_pct - fee_pct  # all in % units, sum back to 100
    if divisor <= 0:
        raise PricingDataError(
            f"margin {margin_pct}% unreachable: fees ({fee_pct}%) leave no headroom"
        )

    bands = freight_bands or [{"min": 0, "max": None, "cost": 0}]
    # Try the unconstrained candidate per band; valid if it falls inside the band.
    candidates: list[Decimal] = []
    for band in bands:
        freight = Decimal(str(band.get("cost", 0)))
        # numerator in same units (BRL); divisor is in %, so multiply by 100
        p_target = ((cost + freight - banca) * Decimal(100) / divisor).quantize(Decimal("0.01"))
        lo = Decimal(str(band.get("min", 0)))
        hi_raw = band.get("max")
        hi = Decimal(str(hi_raw)) if hi_raw is not None else None
        if p_target < lo:
            # margin achievable at a price below this band — band's lower bound is the floor
            candidates.append(lo)
        elif hi is not None and p_target > hi:
            # margin not achievable in this band; skip
            continue
        else:
            candidates.append(p_target)
    if not candidates:
        raise PricingDataError("no freight band yields the target margin")
    # The lowest price that satisfies the margin is the floor we want.
    return min(candidates)
