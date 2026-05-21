"""Recompute ``ml_promo_caps`` using the operator's CURRENT active promos
as the baseline (2026-05-21 policy iteration).

Per-MLB strategy
----------------
For each MLB we fetch its ML promos and look for STARTED entries:

- **With at least one STARTED promo** the cap is anchored to the *most
  aggressive* STARTED seller share — i.e. the largest ``seller_pct`` of any
  promo that is live right now. The floor is set to that promo's price.
  By construction, this promo (and every less-aggressive sibling promo)
  cannot trigger ``floor_violated`` later: the operator already accepted
  that price as their reality, so it is the truth, not an alert.

- **Without any STARTED promo** we fall back to the legacy logic the
  operator manages in the Drive sheet: ``base_cap = sheet_discount_pct``
  (defaulting to ``DEFAULT_CAP_PCT``), then clipped by ``MIN_MARGIN_PCT``.
  The expectation is that those MLBs *should* have a promo and the
  fallback gives them a sensible permission window with the 10% margin
  protection intact.

Storage
-------
``ml_promo_caps`` is keyed by ``mlb_id`` (re-keyed 2026-05-21). Each
MLB has its own cap row — no SKU-level consolidation is needed because
the engine evaluates per-MLB and the operator edits per anúncio.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.orm.models import MLCostsSnapshotORM
from tiny_mirror.infrastructure.repositories.ml_promo_repository import (
    MLCostsSnapshotRepository,
    MLPromoCapRepository,
)
from tiny_mirror.services.ml_promotion_service import MLPromotionService
from tiny_mirror.services.pricing_service import (
    PricingDataError,
    margin_at_price,
    target_price_for_min_margin_pct,
)

logger = structlog.get_logger(__name__)

# Policy constants (clarified 2026-05-21):
#
# - MIN_MARGIN_PCT is the inviolable floor *only when there is no active
#   STARTED promo*. When the operator already runs a promo below 10%
#   margin, that promo IS the cap baseline and we trust the operator.
# - DEFAULT_CAP_PCT applies to fallback only (no sheet value, no active
#   promo). The operator's Drive sheet usually has 30% per row.
MIN_MARGIN_PCT: Decimal = Decimal("10")
DEFAULT_CAP_PCT: Decimal = Decimal("30")


@dataclass(frozen=True)
class CapCalculation:
    """Per-MLB cap calculation result.

    ``cap_pct`` is the % to write into ``max_seller_share_pct``.
    ``floor_price`` is the BRL value that goes into ``margin_floor_price``.
    ``reason`` is a short human-readable note (stored on the cap row).
    ``source`` is either ``"active_promo"`` (cap derived from a STARTED
    promo) or ``"fallback"`` (sheet/default + 10% margin clip).
    """

    mlb_id: str
    sku: str
    cap_pct: Decimal
    floor_price: Decimal | None
    margin_pct_at_floor: Decimal | None
    list_price: Decimal | None
    reason: str
    skipped: bool
    source: str = "fallback"


def _baseline_from_active_promos(
    promos: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Synthesise a per-MLB baseline from every STARTED promo.

    The cap is on **seller** share and the floor is on **price**, and those
    two constraints can be set by *different* STARTED promos when ML's
    ``meli_percentage`` (banca) varies across them. Compute each side
    independently so neither constraint is violated:

      - ``max_seller_pct`` — the cap (max across STARTED).
      - ``min_price`` — the floor in BRL (min across STARTED).

    The engine's ``floor_violated`` check picks ``best_started`` by total%
    (which corresponds to the lowest price on a given MLB). Setting floor
    to that exact price makes ``price < floor`` strictly false on today's
    state regardless of how many STARTED promos co-exist.

    Returns ``None`` when no usable STARTED promo exists.
    """
    parsed: list[dict[str, Any]] = []
    for p in promos:
        if (p.get("status") or "").lower() != "started":
            continue
        original = p.get("original_price") or 0
        price = p.get("price") or 0
        if not original or not price or original <= 0:
            continue
        orig_dec = Decimal(str(original))
        price_dec = Decimal(str(price))
        total_pct = (orig_dec - price_dec) / orig_dec * Decimal(100)
        meli_pct = Decimal(str(p.get("meli_percentage") or 0))
        seller_pct = total_pct - meli_pct
        parsed.append(
            {
                "promo": p,
                "price": price_dec,
                "original": orig_dec,
                "seller_pct": seller_pct,
                "total_pct": total_pct,
            }
        )
    if not parsed:
        return None

    max_seller = max(parsed, key=lambda x: x["seller_pct"])
    min_price = min(parsed, key=lambda x: x["price"])
    return {
        "max_seller_pct": max_seller["seller_pct"],
        "min_price": min_price["price"],
        "original_price": max_seller["original"],
        "type_of_max_seller": max_seller["promo"].get("type"),
        "type_of_min_price": min_price["promo"].get("type"),
        "n_started": len(parsed),
    }


def calc_cap_from_active_promo(
    snap: MLCostsSnapshotORM,
    baseline: dict[str, Any],
) -> CapCalculation:
    """Anchor cap + floor to live STARTED promos on this MLB.

    ``baseline`` is the dict returned by ``_baseline_from_active_promos``.
    By using ``min_price`` as the floor (independent of which promo sets the
    cap), the engine's check ``best_started["_price"] < floor_price`` is
    strictly false for every STARTED promo on this MLB.
    """
    original = baseline["original_price"]
    floor_price = baseline["min_price"].quantize(Decimal("0.01"))
    seller_pct = baseline["max_seller_pct"].quantize(Decimal("0.01"))
    if seller_pct < Decimal(0):
        seller_pct = Decimal(0)
    cap_type = baseline["type_of_max_seller"]
    floor_type = baseline["type_of_min_price"]

    margin_at_floor: Decimal | None = None
    if snap.base_cost is not None and snap.commission_pct is not None and snap.freight_bands:
        try:
            realised = margin_at_price(
                price=floor_price,
                base_cost=snap.base_cost,
                commission_pct=snap.commission_pct,
                freight_bands=snap.freight_bands,
            )
            margin_at_floor = realised.margin_pct
        except PricingDataError:
            margin_at_floor = None

    margin_note = f" margem {margin_at_floor}%" if margin_at_floor is not None else ""
    if cap_type == floor_type:
        reason = (
            f"baseline da promo ativa {cap_type} -{seller_pct}% seller "
            f"(piso R$ {floor_price}{margin_note})"
        )
    else:
        reason = (
            f"baseline composto: cap {seller_pct}% (de {cap_type}) "
            f"+ piso R$ {floor_price} (de {floor_type}){margin_note}"
        )

    return CapCalculation(
        mlb_id=snap.mlb_id,
        sku=snap.sku,
        cap_pct=seller_pct,
        floor_price=floor_price,
        margin_pct_at_floor=margin_at_floor,
        list_price=original,
        reason=reason,
        skipped=False,
        source="active_promo",
    )


def calc_cap_for_snapshot(snap: MLCostsSnapshotORM) -> CapCalculation:
    """Fallback derivation for MLBs that have NO active STARTED promo.

    Same legacy logic the operator was already comfortable with:
      1. base cap = ``snap.sheet_discount_pct`` or ``DEFAULT_CAP_PCT``;
      2. compute the cap that still keeps ``MIN_MARGIN_PCT`` margin;
      3. final cap = ``min(base, margin-protected)``;
      4. when the SKU can't even reach the margin floor at list price,
         cap = 0 (no promo allowed).
    """
    if snap.fetch_error:
        return CapCalculation(
            mlb_id=snap.mlb_id,
            sku=snap.sku,
            cap_pct=Decimal(0),
            floor_price=None,
            margin_pct_at_floor=None,
            list_price=None,
            reason=f"snapshot fetch_error: {snap.fetch_error[:80]}",
            skipped=True,
        )
    if (
        snap.base_cost is None
        or snap.commission_pct is None
        or snap.list_price is None
        or snap.list_price <= Decimal(0)
        or not snap.freight_bands
    ):
        return CapCalculation(
            mlb_id=snap.mlb_id,
            sku=snap.sku,
            cap_pct=Decimal(0),
            floor_price=None,
            margin_pct_at_floor=None,
            list_price=snap.list_price,
            reason="snapshot missing base_cost/commission/list_price/freight_bands",
            skipped=True,
        )

    list_price = snap.list_price
    base_cap: Decimal = (
        snap.sheet_discount_pct if snap.sheet_discount_pct is not None else DEFAULT_CAP_PCT
    )

    full_price_margin = margin_at_price(
        price=list_price,
        base_cost=snap.base_cost,
        commission_pct=snap.commission_pct,
        freight_bands=snap.freight_bands,
    )
    if full_price_margin.margin_pct < MIN_MARGIN_PCT:
        return CapCalculation(
            mlb_id=snap.mlb_id,
            sku=snap.sku,
            cap_pct=Decimal(0),
            floor_price=list_price,
            margin_pct_at_floor=full_price_margin.margin_pct,
            list_price=list_price,
            reason=(
                f"margem {MIN_MARGIN_PCT}% inatingivel: a list_price "
                f"R$ {list_price} so rende {full_price_margin.margin_pct}% margem"
            ),
            skipped=False,
        )

    try:
        floor_price = target_price_for_min_margin_pct(
            base_cost=snap.base_cost,
            commission_pct=snap.commission_pct,
            freight_bands=snap.freight_bands,
            min_margin_pct=MIN_MARGIN_PCT,
        )
    except PricingDataError as exc:
        return CapCalculation(
            mlb_id=snap.mlb_id,
            sku=snap.sku,
            cap_pct=Decimal(0),
            floor_price=None,
            margin_pct_at_floor=None,
            list_price=list_price,
            reason=f"solver: {exc}",
            skipped=True,
        )

    cap_by_margin = ((list_price - floor_price) / list_price * Decimal(100)).quantize(
        Decimal("0.01")
    )
    if cap_by_margin < Decimal(0):
        cap_by_margin = Decimal(0)

    cap = min(base_cap, cap_by_margin).quantize(Decimal("0.01"))
    target_price_at_cap = (list_price * (Decimal(100) - cap) / Decimal(100)).quantize(
        Decimal("0.01")
    )
    realised = margin_at_price(
        price=target_price_at_cap,
        base_cost=snap.base_cost,
        commission_pct=snap.commission_pct,
        freight_bands=snap.freight_bands,
    )

    if cap < base_cap:
        reason = (
            f"sem promo ativa; planilha pede {base_cap}% mas o piso de "
            f"{MIN_MARGIN_PCT}% margem so permite {cap}%"
        )
    else:
        reason = (
            f"sem promo ativa; cap={cap}% (= planilha), piso R$ {target_price_at_cap} "
            f"-> margem {realised.margin_pct}% no piso"
        )

    return CapCalculation(
        mlb_id=snap.mlb_id,
        sku=snap.sku,
        cap_pct=cap,
        floor_price=target_price_at_cap,
        margin_pct_at_floor=realised.margin_pct,
        list_price=list_price,
        reason=reason,
        skipped=False,
        source="fallback",
    )


async def _fetch_active_baseline_for_mlb(
    service: MLPromotionService, mlb_id: str
) -> dict[str, Any] | None:
    """Wrap ML promo fetch + STARTED-baseline synthesis in one call.

    Returns ``None`` on network failure or when no STARTED promo exists.
    Errors are swallowed (logged at debug) so a single flaky MLB doesn't
    abort the whole recompute.
    """
    try:
        promos = await service.fetch_eligible_promos(mlb_id)
    except Exception as exc:  # pragma: no cover — network noise
        logger.debug("recompute_promo_fetch_failed", mlb_id=mlb_id, error=str(exc))
        return None
    return _baseline_from_active_promos(promos)


async def recompute_all_caps(
    session: AsyncSession,
    *,
    service: MLPromotionService | None = None,
    actor: str | None = None,
) -> dict[str, Any]:
    """Recompute caps for every SKU that has at least one cost snapshot.

    When ``service`` is provided, each MLB's live promos are fetched and
    any STARTED promo becomes the cap baseline (no alert by construction).
    When ``service`` is None, only the fallback path runs (legacy
    sheet + 10% margin behaviour).
    """
    snap_repo = MLCostsSnapshotRepository(session)
    cap_repo = MLPromoCapRepository(session)

    from sqlalchemy import select

    result = await session.execute(select(MLCostsSnapshotORM))
    snapshots = list(result.scalars().all())
    assert snap_repo is not None

    by_sku: dict[str, list[MLCostsSnapshotORM]] = {}
    for snap in snapshots:
        if not snap.sku:
            continue
        by_sku.setdefault(snap.sku, []).append(snap)

    actor = actor or f"auto-cap-recompute-{datetime.now(UTC).date().isoformat()}"
    stats = {
        "snapshots_read": len(snapshots),
        "mlbs_processed": 0,
        "mlbs_skipped": 0,
        "mlbs_from_active_promo": 0,
        "mlbs_fallback_sheet": 0,
        "mlbs_zero_cap": 0,
        "mlbs_fetched_promos": 0,
    }
    examples: list[dict[str, Any]] = []

    # Per-MLB upsert — no SKU-level consolidation anymore. Each anúncio gets
    # its own cap, derived from its own active STARTED promos (or fallback).
    for sku, snaps in sorted(by_sku.items()):
        for snap in snaps:
            baseline: dict[str, Any] | None = None
            if service is not None and not snap.fetch_error:
                baseline = await _fetch_active_baseline_for_mlb(service, snap.mlb_id)
                stats["mlbs_fetched_promos"] += 1
            calc = (
                calc_cap_from_active_promo(snap, baseline)
                if baseline is not None
                else calc_cap_for_snapshot(snap)
            )

            if calc.skipped:
                stats["mlbs_skipped"] += 1
                continue
            stats["mlbs_processed"] += 1
            if calc.source == "active_promo":
                stats["mlbs_from_active_promo"] += 1
            else:
                stats["mlbs_fallback_sheet"] += 1
            if calc.cap_pct == Decimal(0):
                stats["mlbs_zero_cap"] += 1

            await cap_repo.upsert(
                snap.mlb_id,
                sku=sku,
                max_seller_share_pct=calc.cap_pct,
                margin_floor_price=calc.floor_price,
                notes=calc.reason[:500],
                updated_by=actor,
            )
            if len(examples) < 10:
                examples.append(
                    {
                        "sku": sku,
                        "mlb_id": snap.mlb_id,
                        "cap_pct": float(calc.cap_pct),
                        "floor_price": float(calc.floor_price)
                        if calc.floor_price is not None
                        else None,
                        "source": calc.source,
                        "reason": calc.reason,
                    }
                )

    await session.commit()
    logger.info("cap_recompute_done", **stats)
    return {**stats, "examples": examples}
