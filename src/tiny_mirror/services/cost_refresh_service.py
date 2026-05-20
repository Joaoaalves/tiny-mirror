"""Bulk refresh of ``ml_costs_snapshot`` from the GAS bulk endpoint.

Replaces the legacy per-MLB loop (~6s * N) with a single HTTP call to
``?action=costs_all`` (~15-30s for any N). The payload also carries
``difalPct`` so the pricing layer can pick up tax-regime changes without
a code/env edit.

Sister to ``ml_promotion_service.refresh_costs_for_mlb`` which stays
around for ad-hoc one-MLB refreshes.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.repositories.ml_promo_repository import (
    MLCostsSnapshotRepository,
)
from tiny_mirror.services.gas_client import GASClient, GASClientError

logger = structlog.get_logger(__name__)


class CostRefreshError(Exception):
    """Raised when the GAS bulk endpoint cannot be reached or returns no items."""


def _decimal(v: Any) -> Decimal | None:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:  # pragma: no cover — defensive
        return None


async def refresh_all_from_bulk(
    session: AsyncSession,
    gas: GASClient,
) -> dict[str, int]:
    """Pull the full cost dump from GAS and upsert every row.

    Returns counts for telemetry. Commits inside batches of 50 so the
    transaction does not balloon while ~400 upserts run.
    """
    try:
        payload = await gas.costs_all()
    except GASClientError as exc:
        raise CostRefreshError(str(exc)) from exc

    items: dict[str, Any] = payload.get("items") or {}
    if not items:
        raise CostRefreshError("GAS costs_all returned 0 items")

    snap_repo = MLCostsSnapshotRepository(session)
    ok = 0
    skipped_no_data = 0
    batch_size = 50

    for i, (mlb_id, row) in enumerate(items.items()):
        if not isinstance(row, dict):
            skipped_no_data += 1
            continue
        await snap_repo.upsert(
            mlb_id=mlb_id,
            sku=row.get("sku") or "",
            active_on_sheet=bool(row.get("active")),
            base_cost=_decimal(row.get("baseCost")),
            commission_pct=_decimal(row.get("commissionPct")),
            commission_label=row.get("commissionLabel"),
            list_price=_decimal(row.get("listPrice")),
            sheet_promo_price=_decimal(row.get("promoPrice")),
            sheet_discount_pct=_decimal(row.get("discountPct")),
            sheet_margin_pct=_decimal(row.get("currentMarginPct")),
            sheet_margin_value=_decimal(row.get("currentMarginValue")),
            freight_bands=row.get("freightBands"),
            fetch_error=None,
        )
        ok += 1
        if (i + 1) % batch_size == 0:
            await session.commit()

    await session.commit()

    stats = {
        "received": len(items),
        "upserted": ok,
        "skipped_no_data": skipped_no_data,
    }
    logger.info(
        "cost_refresh_bulk_ok",
        difal_pct=payload.get("difalPct"),
        generated_at=payload.get("generatedAt"),
        **stats,
    )
    return stats
