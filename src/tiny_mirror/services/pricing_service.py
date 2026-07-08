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


def _commission_pct_for(
    price: Decimal,
    commission_bands: list[dict[str, Any]] | None,
    fallback_pct: Decimal,
) -> Decimal:
    """Nominal ML commission % for the band containing ``price``.

    ML's per-listing sale_fee is ``percentage_fee(price) x price`` (fixed_fee is
    0 for the categories we sell — verified against Mercado Turbo, 278/290 exact).
    The percentage is NOT constant: many categories drop it a few points in a
    mid-price band (e.g. gold_pro 17% → 14% between ~R$150-500 → 17%), so a single
    scalar can't reproduce it. ``commission_bands`` is a list of
    ``{min, max, pct}`` (inclusive ends, ``max=None`` open-ended) probed from
    ``/sites/MLB/listing_prices`` per (category, listing_type). Falls back to the
    scalar snapshot % when no band matches or no schedule is present.
    """
    if not commission_bands:
        return fallback_pct
    p = float(price)
    for band in commission_bands:
        lo = band.get("min")
        hi = band.get("max")
        if lo is None:
            continue
        if p < float(lo):
            continue
        if hi is not None and p > float(hi):
            continue
        pct = band.get("pct")
        return Decimal(str(pct)) if pct is not None else fallback_pct
    return fallback_pct


def _dec(v: Any) -> Decimal:
    if v is None:
        raise PricingDataError("required field is None")
    return v if isinstance(v, Decimal) else Decimal(str(v))


def apply_flex_calibration(
    logistic_type: str | None,
    commission_pct: Any,
    freight_bands: list[dict[str, Any]] | None,
    calib: Any,
) -> tuple[Any, list[dict[str, Any]] | None, list[dict[str, Any]] | None]:
    """Return effective ``(commission_pct, freight_bands, commission_bands)``.

    For **Flex** (non-fulfillment) listings that have a per-MLB calibration row
    (``ml_flex_fee_calibration``), freight becomes a synthetic 2-band table split
    at R$79 (the free-shipping cliff: ML covers ~100% under R$79 and ~10% above)
    and commission becomes the **nominal ML fee schedule** (``commission_bands``,
    price-banded, from ``/sites/MLB/listing_prices`` — this is what Mercado Turbo
    charges). Fulfillment listings, an unknown ``logistic_type``, or a missing/
    empty calibration return the snapshot values UNCHANGED with no commission
    bands — fulfillment fees are already correct and must never be overridden.
    This is the single source of truth for the override; both the caps enrichment
    and the floor recompute use it.
    """
    if logistic_type is None or logistic_type == "fulfillment" or calib is None:
        return commission_pct, freight_bands, None
    # Flex with a calibration row: ALWAYS replace the (wrong) generic freight
    # bands with the calibrated 2-band Flex table — fallback rows for listings
    # without sales still carry the global Flex mean, so they don't silently
    # revert to the fulfillment-style bands.
    #
    # Commission: prefer the nominal price-banded schedule (matches Mercado Turbo
    # exactly); fall back to the real historical median, then the snapshot %.
    comm_bands = getattr(calib, "commission_bands", None) or None
    real_comm = getattr(calib, "real_comm_pct", None)
    eff_comm = real_comm if real_comm is not None else commission_pct
    # ``payback`` (ML freight subsidy) rides along on each band for the UI; the
    # margin math only reads ``cost`` so the extra key is harmless.
    bands: list[dict[str, Any]] = [
        {
            "min": 0,
            "max": 78.99,
            "cost": float(getattr(calib, "freight_per_unit_lt79", 0) or 0),
            "payback": float(getattr(calib, "payback_per_unit_lt79", 0) or 0),
        },
        {
            "min": 79,
            "max": None,
            "cost": float(getattr(calib, "freight_per_unit_ge79", 0) or 0),
            "payback": float(getattr(calib, "payback_per_unit_ge79", 0) or 0),
        },
    ]
    return eff_comm, bands, comm_bands


def margin_at_price(
    *,
    price: Decimal | float | int,
    base_cost: Decimal | float | int,
    commission_pct: Decimal | float | int,
    freight_bands: list[dict[str, Any]] | None,
    list_price: Decimal | float | int | None = None,
    meli_banca_pct: Decimal | float | int = 0,
    commission_bands: list[dict[str, Any]] | None = None,
) -> MarginBreakdown:
    """Compute margin breakdown at a given selling price.

    Arguments:
        price: the price the customer pays (BRL).
        base_cost: product cost (BRL).
        commission_pct: ML commission tier for this SKU, in **percent**
            (e.g. 11.5 not 0.115). Matches the snapshot's commission_pct.
            Used as the fallback when ``commission_bands`` is absent or has no
            band for ``price``.
        freight_bands: list of ``{min, max, cost}`` dicts from the snapshot.
        list_price: original (non-promo) price. Required only when
            ``meli_banca_pct > 0`` (SMART/DEAL co-pay is on the list price).
        meli_banca_pct: ML co-pay percentage for the promo (also in
            **percent**, e.g. 3.7). Zero for PRICE_DISCOUNT.
        commission_bands: optional price-banded nominal ML fee schedule
            (``{min, max, pct}``). When present, the commission % is looked up
            by band — reproduces Mercado Turbo's per-price tarifa exactly.

    Returns a ``MarginBreakdown`` with all components and the final margin
    in both BRL and % of price.
    """
    p = _dec(price)
    cost = _dec(base_cost)
    comm_pct = _commission_pct_for(p, commission_bands, _dec(commission_pct))
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


def _segment_breakpoints(
    freight_bands: list[dict[str, Any]],
    commission_bands: list[dict[str, Any]] | None,
) -> list[tuple[Decimal, Decimal | None]]:
    """Union the freight and commission band edges into disjoint ``(lo, hi)``
    segments over which BOTH freight and commission % are constant. Needed by
    the floor solver: with a price-banded commission the closed-form is only
    valid inside a segment where every fee is flat."""
    edges: set[Decimal] = {Decimal("0")}
    for src in (freight_bands, commission_bands or []):
        for b in src:
            lo = b.get("min")
            hi = b.get("max")
            if lo is not None:
                edges.add(Decimal(str(lo)))
            if hi is not None:
                # segment boundary sits just above the inclusive band max
                edges.add(Decimal(str(hi)))
    ordered = sorted(edges)
    segments: list[tuple[Decimal, Decimal | None]] = []
    for i, lo in enumerate(ordered):
        hi = ordered[i + 1] if i + 1 < len(ordered) else None
        segments.append((lo, hi))
    return segments


def target_price_for_min_margin_pct(
    *,
    base_cost: Decimal | float | int,
    commission_pct: Decimal | float | int,
    freight_bands: list[dict[str, Any]] | None,
    min_margin_pct: Decimal | float | int,
    list_price: Decimal | float | int | None = None,
    meli_banca_pct: Decimal | float | int = 0,
    commission_bands: list[dict[str, Any]] | None = None,
) -> Decimal:
    """Solve for the lowest price that still yields ``min_margin_pct`` margin.

    Closed-form when freight AND commission are constant in a segment; the
    discontinuities (freight bands + the nominal commission schedule) mean we
    evaluate segment-by-segment and pick the lowest valid floor.

    Formula (per segment, freight = F, commission = c, both constant):
        margin_pct/100 * P = P + banca - cost - P*(c+d)/100 - F
        P * [1 - margin_pct/100 - (c+d)/100] = cost + F - banca
        P = (cost + F - banca) / [1 - margin_pct/100 - (c+d)/100]

    If every segment's divisor is <= 0 (margin_pct too high relative to fees),
    the target is unreachable and we raise. Caller should fall back to
    list_price or a sensible cap.
    """
    cost = _dec(base_cost)
    fallback_comm = _dec(commission_pct)
    margin_pct = _dec(min_margin_pct)
    difal_pct = _difal_pct() * Decimal(100)

    banca = Decimal("0")
    if meli_banca_pct and list_price is not None:
        banca = _dec(list_price) * _dec(meli_banca_pct) / Decimal(100)

    bands = freight_bands or [{"min": 0, "max": None, "cost": 0}]
    segments = _segment_breakpoints(bands, commission_bands)
    # Evaluate the unconstrained candidate per segment; valid if it falls inside.
    candidates: list[Decimal] = []
    any_headroom = False
    for lo, hi in segments:
        # sample the fees at the segment's interior (its lower edge is inside it)
        freight = _freight_for(lo, bands)
        comm = _commission_pct_for(lo, commission_bands, fallback_comm)
        divisor = Decimal(100) - margin_pct - (comm + difal_pct)
        if divisor <= 0:
            continue  # this segment can't yield the margin; try the next
        any_headroom = True
        # numerator in same units (BRL); divisor is in %, so multiply by 100
        p_target = ((cost + freight - banca) * Decimal(100) / divisor).quantize(Decimal("0.01"))
        if p_target < lo:
            # margin achievable below this segment — its lower bound is the floor
            candidates.append(lo)
        elif hi is not None and p_target > hi:
            # margin not achievable in this segment; skip
            continue
        else:
            candidates.append(p_target)
    if not any_headroom:
        raise PricingDataError(
            f"margin {margin_pct}% unreachable: fees leave no headroom in any band"
        )
    if not candidates:
        raise PricingDataError("no freight band yields the target margin")
    # The lowest price that satisfies the margin is the floor we want.
    return min(candidates)
