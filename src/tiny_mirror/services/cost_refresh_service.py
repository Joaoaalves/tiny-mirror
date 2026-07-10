"""Bulk refresh of ``ml_costs_snapshot`` from the GAS bulk endpoint.

Replaces the legacy per-MLB loop (~6s * N) with a single HTTP call to
``?action=costs_all`` (~15-30s for any N). The payload also carries
``difalPct`` so the pricing layer can pick up tax-regime changes without
a code/env edit.

Double lookup: the "Mercado Livre" sheet tab is indexed by MLB, but some
active listings are missing from it (duplicated/recreated ads). Those
inherit the row of the SAME SKU (sheet column F) — cost belongs to the
product, not the ad. A listing's own row always wins when present.

Sister to ``ml_promotion_service.refresh_costs_for_mlb`` which stays
around for ad-hoc one-MLB refreshes.
"""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.repositories.ml_promo_repository import (
    MLCostsSnapshotRepository,
)
from tiny_mirror.services.gas_client import GASClient, GASClientError

logger = structlog.get_logger(__name__)

# Real ML listing ids look like "MLB" + 10-13 digits. The spreadsheet
# occasionally has malformed cells ("MLB123 / 456", "MLB-pending", etc.)
# that must not reach the DB — products.mlb_id is varchar(20).
_VALID_MLB_RE = re.compile(r"^MLB\d{6,16}$")


class CostRefreshError(Exception):
    """Raised when the GAS bulk endpoint cannot be reached or returns no items."""


def _decimal(v: Any) -> Decimal | None:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:  # pragma: no cover — defensive
        return None


def _row_kwargs(sku: str, row: dict[str, Any]) -> dict[str, Any]:
    """Campos do upsert a partir de uma linha da aba "Mercado Livre"."""
    return {
        "sku": sku,
        "active_on_sheet": bool(row.get("active")),
        "base_cost": _decimal(row.get("baseCost")),
        "commission_pct": _decimal(row.get("commissionPct")),
        "commission_label": row.get("commissionLabel"),
        "list_price": _decimal(row.get("listPrice")),
        "sheet_promo_price": _decimal(row.get("promoPrice")),
        "sheet_discount_pct": _decimal(row.get("discountPct")),
        "sheet_margin_pct": _decimal(row.get("currentMarginPct")),
        "sheet_margin_value": _decimal(row.get("currentMarginValue")),
        "freight_bands": row.get("freightBands"),
        "fetch_error": None,
    }


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
    skipped_invalid_id = 0
    batch_size = 50

    upserted_ids: set[str] = set()
    for i, (mlb_id, row) in enumerate(items.items()):
        if not isinstance(row, dict):
            skipped_no_data += 1
            continue
        if not _VALID_MLB_RE.match(mlb_id):
            skipped_invalid_id += 1
            logger.warning("cost_refresh_invalid_mlb_id", mlb_id=mlb_id)
            continue
        await snap_repo.upsert(mlb_id=mlb_id, **_row_kwargs(row.get("sku") or "", row))
        upserted_ids.add(mlb_id)
        ok += 1
        if (i + 1) % batch_size == 0:
            await session.commit()

    await session.commit()

    # --- Verificação dupla por SKU (coluna F da aba "Mercado Livre") ---------
    # A aba é indexada por MLB, mas nem todo anúncio está lá (duplicados/
    # recriados). Anúncio ATIVO cujo MLB não veio no dump herda a linha do
    # MESMO SKU — custo é do PRODUTO, não do anúncio. A linha própria, quando
    # existir no dump, sempre vence (foi upsertada acima; aqui só entram os
    # MLBs ausentes).
    sku_rows: dict[str, dict[str, Any]] = {}
    for mlb_id, row in items.items():
        if not isinstance(row, dict) or not _VALID_MLB_RE.match(mlb_id):
            continue
        sku = str(row.get("sku") or "").strip()
        if not sku:
            continue
        cur = sku_rows.get(sku)
        # preferimos a linha ATIVA e com custo preenchido
        rank = (bool(row.get("active")), row.get("baseCost") is not None)
        if cur is None or rank > (bool(cur.get("active")), cur.get("baseCost") is not None):
            sku_rows[sku] = row

    listings = (
        await session.execute(
            text("SELECT mlb_id, sku FROM ml_listings WHERE status = 'active' AND sku IS NOT NULL")
        )
    ).all()
    sku_fallback = 0
    for mlb_id, listing_sku in listings:
        if mlb_id in upserted_ids:
            continue
        row = sku_rows.get(str(listing_sku).strip())
        if row is None:
            continue
        await snap_repo.upsert(mlb_id=mlb_id, **_row_kwargs(str(listing_sku).strip(), row))
        sku_fallback += 1
        logger.info("cost_refresh_sku_fallback", mlb_id=mlb_id, sku=listing_sku)
    await session.commit()

    stats = {
        "received": len(items),
        "upserted": ok,
        "sku_fallback_upserts": sku_fallback,
        "skipped_no_data": skipped_no_data,
        "skipped_invalid_id": skipped_invalid_id,
    }
    logger.info(
        "cost_refresh_bulk_ok",
        difal_pct=payload.get("difalPct"),
        generated_at=payload.get("generatedAt"),
        **stats,
    )
    return stats
