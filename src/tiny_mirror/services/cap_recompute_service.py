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

Per-SKU consolidation
---------------------
``ml_promo_caps`` is keyed by SKU. Across the SKU's MLBs we take
``max(cap_pct)`` (so no live promo is ever above the cap) and
``min(floor_price)`` (so no live promo is ever below the floor).
SKUs where every MLB was skipped stay skipped.
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


def _pick_best_started_promo(promos: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Return the STARTED promo with the largest seller share.

    "seller share" = ``original_price - price`` minus the ML share
    (``meli_percentage``). That matches how ``cap_seller_pct`` is
    interpreted elsewhere — the cap is on the seller's contribution,
    not the total visible discount.
    """
    best: dict[str, Any] | None = None
    best_seller_pct = Decimal("-1")
    for p in promos:
        if (p.get("status") or "").lower() != "started":
            continue
        original = p.get("original_price") or 0
        price = p.get("price") or 0
        if not original or not price or original <= 0:
            continue
        total_pct = (
            (Decimal(str(original)) - Decimal(str(price))) / Decimal(str(original)) * Decimal(100)
        )
        meli_pct = Decimal(str(p.get("meli_percentage") or 0))
        seller_pct = total_pct - meli_pct
        if seller_pct > best_seller_pct:
            best_seller_pct = seller_pct
            best = p
    return best


def calc_cap_from_active_promo(
    snap: MLCostsSnapshotORM,
    active_promo: dict[str, Any],
) -> CapCalculation:
    """Anchor cap + floor to a live STARTED promo.

    Invariant: the engine evaluates ``floor_violated`` as
    ``best_started["_price"] < floor_price``. By setting
    ``floor_price`` to exactly that promo's price, every less-aggressive
    sibling promo and the promo itself satisfy ``price >= floor``,
    so no alert fires on today's state.
    """
    original = Decimal(str(active_promo.get("original_price") or 0))
    price = Decimal(str(active_promo.get("price") or 0))
    meli_pct = Decimal(str(active_promo.get("meli_percentage") or 0))
    promo_type = str(active_promo.get("type") or "?")

    total_pct = ((original - price) / original * Decimal(100)).quantize(Decimal("0.01"))
    seller_pct = (total_pct - meli_pct).quantize(Decimal("0.01"))
    if seller_pct < Decimal(0):
        seller_pct = Decimal(0)
    floor_price = price.quantize(Decimal("0.01"))

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
    reason = (
        f"baseline da promo ativa {promo_type} -{seller_pct}% seller "
        f"(piso R$ {floor_price}{margin_note})"
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


def _consolidate_sku(rows: list[CapCalculation]) -> CapCalculation:
    """Consolidate per-MLB calcs into a single SKU row.

    Rules:
      - drop ``skipped`` rows; if everything is skipped, return the first
        skipped row as-is so the caller can record it;
      - cap_pct = ``max`` across MLBs (so no live promo is above cap);
      - floor_price = ``min`` of the non-None floors (so no live promo
        is below the floor).
    """
    non_skipped = [r for r in rows if not r.skipped]
    if not non_skipped:
        return rows[0]

    chosen_cap = max(non_skipped, key=lambda r: r.cap_pct)
    floors = [r.floor_price for r in non_skipped if r.floor_price is not None]
    floor_price = min(floors) if floors else None

    # Prefer the reason from the row that has the LOWEST floor (most
    # aggressive baseline), because that one is the binding constraint.
    floor_row = chosen_cap
    if floors:
        floor_row = min(
            (r for r in non_skipped if r.floor_price is not None),
            key=lambda r: r.floor_price,  # type: ignore[arg-type,return-value]
        )

    source = "active_promo" if any(r.source == "active_promo" for r in non_skipped) else "fallback"
    if len(non_skipped) > 1:
        reason = f"{chosen_cap.reason} | floor de {floor_row.mlb_id}: R$ {floor_price}"
    else:
        reason = chosen_cap.reason

    return CapCalculation(
        mlb_id=chosen_cap.mlb_id,
        sku=chosen_cap.sku,
        cap_pct=chosen_cap.cap_pct,
        floor_price=floor_price,
        margin_pct_at_floor=chosen_cap.margin_pct_at_floor,
        list_price=chosen_cap.list_price,
        reason=reason,
        skipped=False,
        source=source,
    )


# Back-compat alias for older imports/tests that referenced the conservative
# pick. The new implementation is a MAX-cap consolidation (no alerts on live
# promos), so the legacy "_conservative_pick" name is misleading; we keep
# the symbol pointing at _consolidate_sku for callers who still import it.
_conservative_pick = _consolidate_sku


async def _fetch_active_promo_for_mlb(
    service: MLPromotionService, mlb_id: str
) -> dict[str, Any] | None:
    """Wrap ML promo fetch + best-STARTED selection in one call.

    Returns None on network failure / no STARTED promo. Errors are
    swallowed (logged at debug) so a single flaky MLB doesn't abort
    the whole recompute.
    """
    try:
        promos = await service.fetch_eligible_promos(mlb_id)
    except Exception as exc:  # pragma: no cover — network noise
        logger.debug("recompute_promo_fetch_failed", mlb_id=mlb_id, error=str(exc))
        return None
    return _pick_best_started_promo(promos)


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
        "skus_processed": 0,
        "skus_skipped": 0,
        "skus_from_active_promo": 0,
        "skus_fallback_sheet": 0,
        "skus_zero_cap": 0,
        "mlbs_fetched_promos": 0,
        "mlbs_with_active_promo": 0,
    }
    examples: list[dict[str, Any]] = []

    for sku, snaps in sorted(by_sku.items()):
        per_mlb_calcs: list[CapCalculation] = []
        for snap in snaps:
            active_promo: dict[str, Any] | None = None
            if service is not None and not snap.fetch_error:
                active_promo = await _fetch_active_promo_for_mlb(service, snap.mlb_id)
                stats["mlbs_fetched_promos"] += 1
                if active_promo is not None:
                    stats["mlbs_with_active_promo"] += 1
            if active_promo is not None:
                per_mlb_calcs.append(calc_cap_from_active_promo(snap, active_promo))
            else:
                per_mlb_calcs.append(calc_cap_for_snapshot(snap))

        chosen = _consolidate_sku(per_mlb_calcs)
        if chosen.skipped:
            stats["skus_skipped"] += 1
            continue
        stats["skus_processed"] += 1
        if chosen.source == "active_promo":
            stats["skus_from_active_promo"] += 1
        else:
            stats["skus_fallback_sheet"] += 1
        if chosen.cap_pct == Decimal(0):
            stats["skus_zero_cap"] += 1

        await cap_repo.upsert(
            sku=sku,
            max_seller_share_pct=chosen.cap_pct,
            margin_floor_price=chosen.floor_price,
            notes=chosen.reason[:500],
            updated_by=actor,
        )
        if len(examples) < 10:
            examples.append(
                {
                    "sku": sku,
                    "cap_pct": float(chosen.cap_pct),
                    "floor_price": float(chosen.floor_price)
                    if chosen.floor_price is not None
                    else None,
                    "source": chosen.source,
                    "reason": chosen.reason,
                }
            )

    await session.commit()
    logger.info("cap_recompute_done", **stats)
    return {**stats, "examples": examples}
