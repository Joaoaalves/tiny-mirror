"""Recompute ``ml_promo_caps`` from cost snapshots.

For each MLB with a fresh cost snapshot, derive:
- the lowest selling price that still yields ``TARGET_MARGIN_PCT`` margin,
- the discount % vs. ``list_price`` that produces it,
- the resulting cap = ``min(discount_at_floor_pct, ABSOLUTE_MAX_CAP_PCT)``.

When a SKU has multiple MLBs (variations / catalogue listings) we pick the
**most conservative** cap so no MLB ever ends up below the margin target.

Skips SKUs without a usable snapshot (missing cost / commission / list_price
/ freight bands) and writes a ``notes`` explaining the outcome.
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
from tiny_mirror.services.pricing_service import (
    PricingDataError,
    margin_at_price,
    target_price_for_min_margin_pct,
)

logger = structlog.get_logger(__name__)

# Operator-stated defaults; both could be lifted into config later, but
# keeping them here keeps the policy obvious to anyone reading the code.
TARGET_MARGIN_PCT: Decimal = Decimal("10")
ABSOLUTE_MAX_CAP_PCT: Decimal = Decimal("30")


@dataclass(frozen=True)
class CapCalculation:
    """Per-MLB cap calculation result.

    ``cap_pct`` is the % to write into ``max_seller_share_pct``.
    ``floor_price`` is the BRL value that goes into ``margin_floor_price``.
    ``reason`` is a short human-readable note (stored on the cap row).
    """

    mlb_id: str
    sku: str
    cap_pct: Decimal  # always in [0, ABSOLUTE_MAX_CAP_PCT]
    floor_price: Decimal | None
    margin_pct_at_floor: Decimal | None
    list_price: Decimal | None
    reason: str
    skipped: bool


def calc_cap_for_snapshot(snap: MLCostsSnapshotORM) -> CapCalculation:
    """Pure calculation, no DB. Returns a CapCalculation describing what to
    write (or why the snapshot was skipped)."""
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

    # Margin at full list price (no discount) — sanity check.
    full_price_margin = margin_at_price(
        price=list_price,
        base_cost=snap.base_cost,
        commission_pct=snap.commission_pct,
        freight_bands=snap.freight_bands,
    )
    if full_price_margin.margin_pct < TARGET_MARGIN_PCT:
        # Can't even hit 10% at list price — no promo allowed.
        return CapCalculation(
            mlb_id=snap.mlb_id,
            sku=snap.sku,
            cap_pct=Decimal(0),
            floor_price=list_price,
            margin_pct_at_floor=full_price_margin.margin_pct,
            list_price=list_price,
            reason=(
                f"margem {TARGET_MARGIN_PCT}% inatingivel: a list_price "
                f"R$ {list_price} so rende {full_price_margin.margin_pct}% margem"
            ),
            skipped=False,
        )

    try:
        floor_price = target_price_for_min_margin_pct(
            base_cost=snap.base_cost,
            commission_pct=snap.commission_pct,
            freight_bands=snap.freight_bands,
            min_margin_pct=TARGET_MARGIN_PCT,
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

    # Floor above list price means 0 discount allowed.
    if floor_price >= list_price:
        return CapCalculation(
            mlb_id=snap.mlb_id,
            sku=snap.sku,
            cap_pct=Decimal(0),
            floor_price=list_price,
            margin_pct_at_floor=full_price_margin.margin_pct,
            list_price=list_price,
            reason=(
                f"piso R$ {floor_price.quantize(Decimal('0.01'))} "
                f">= list R$ {list_price.quantize(Decimal('0.01'))}; sem espaco pra promo"
            ),
            skipped=False,
        )

    realised = margin_at_price(
        price=floor_price,
        base_cost=snap.base_cost,
        commission_pct=snap.commission_pct,
        freight_bands=snap.freight_bands,
    )
    discount_at_floor = ((list_price - floor_price) / list_price * Decimal(100)).quantize(
        Decimal("0.01")
    )
    cap = min(discount_at_floor, ABSOLUTE_MAX_CAP_PCT).quantize(Decimal("0.01"))
    if cap < Decimal(0):
        cap = Decimal(0)

    if cap == ABSOLUTE_MAX_CAP_PCT and discount_at_floor > ABSOLUTE_MAX_CAP_PCT:
        reason = (
            f"clipado em {ABSOLUTE_MAX_CAP_PCT}% (margem 10% permitiria "
            f"{discount_at_floor}%, mas teto comercial fixo)"
        )
    else:
        reason = (
            f"cap={cap}%, piso R$ {floor_price.quantize(Decimal('0.01'))} "
            f"-> margem {realised.margin_pct}% no piso"
        )

    return CapCalculation(
        mlb_id=snap.mlb_id,
        sku=snap.sku,
        cap_pct=cap,
        floor_price=floor_price,
        margin_pct_at_floor=realised.margin_pct,
        list_price=list_price,
        reason=reason,
        skipped=False,
    )


def _conservative_pick(rows: list[CapCalculation]) -> CapCalculation:
    """For a SKU with multiple MLBs, pick the row whose ``cap_pct`` is the
    LOWEST — the floor that protects all variations."""
    non_skipped = [r for r in rows if not r.skipped]
    pool = non_skipped or rows
    return min(pool, key=lambda r: (r.cap_pct, -(r.floor_price or Decimal(0))))


async def recompute_all_caps(
    session: AsyncSession,
    *,
    actor: str | None = None,
) -> dict[str, Any]:
    """Recompute caps for every SKU that has at least one cost snapshot.

    Strategy: read all snapshots → group by SKU → compute per-MLB → pick
    the conservative per SKU → upsert into ``ml_promo_caps``.

    Returns a stats dict suitable for logging / API response.
    """
    snap_repo = MLCostsSnapshotRepository(session)
    cap_repo = MLPromoCapRepository(session)

    # Pull every snapshot in one shot — N is small (≈400).
    from sqlalchemy import select

    result = await session.execute(select(MLCostsSnapshotORM))
    snapshots = list(result.scalars().all())
    assert snap_repo is not None  # repo not used beyond holding the session

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
        "skus_capped_at_30": 0,
        "skus_below_30": 0,
        "skus_zero_cap": 0,
    }
    examples: list[dict[str, Any]] = []

    for sku, snaps in sorted(by_sku.items()):
        calcs = [calc_cap_for_snapshot(s) for s in snaps]
        chosen = _conservative_pick(calcs)
        if chosen.skipped:
            stats["skus_skipped"] += 1
            continue
        stats["skus_processed"] += 1
        if chosen.cap_pct == ABSOLUTE_MAX_CAP_PCT:
            stats["skus_capped_at_30"] += 1
        elif chosen.cap_pct == Decimal(0):
            stats["skus_zero_cap"] += 1
        else:
            stats["skus_below_30"] += 1

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
                    "reason": chosen.reason,
                }
            )

    await session.commit()
    logger.info("cap_recompute_done", **stats)
    return {**stats, "examples": examples}
