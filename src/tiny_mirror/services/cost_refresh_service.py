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

# ── Fail-loud guards ─────────────────────────────────────────────────────────
# O payload vem de um Apps Script que lê a aba "Mercado Livre" por POSIÇÃO de
# coluna. Se alguém inserir/mover uma coluna, ele continua devolvendo um JSON
# estruturalmente válido — só que com os valores errados. Foi exatamente assim
# que o manual_status passou a devolver COD. FAB como se fosse SKU (2026-07-08)
# e ninguém foi avisado.
#
# `ml_costs_snapshot` alimenta caps e margens de promoções: gravar lixo aqui é
# pior que não gravar nada. Então, antes de qualquer upsert, exigimos que a
# FORMA do payload bata. Se não bater, abortamos alto (CostRefreshError) em vez
# de degradar em silêncio.
#
# Tetos calibrados sobre o payload real de 2026-07-10 (515 itens):
#   mlb_id inválido 0.4% · sku vazio 0% · baseCost ausente 0% · bands ausente 0%
# Uma coluna deslocada leva qualquer um desses para perto de 100%.
_MAX_INVALID_MLB_RATIO = 0.05
_MAX_MISSING_SKU_RATIO = 0.05
_MAX_MISSING_COST_RATIO = 0.10
_MAX_MISSING_BANDS_RATIO = 0.10

# Proporção precisa de amostra: com 2 itens, uma célula malformada já dá 50%.
# Um layout deslocado sempre vem com a aba inteira (~500 linhas), então só
# aplicamos os tetos a partir daqui. Abaixo disso, o payload é anômalo por
# outro motivo (config/parcial) e o skip por linha já protege o banco.
_MIN_SAMPLE_FOR_RATIOS = 20


class CostRefreshError(Exception):
    """Raised when the GAS bulk endpoint cannot be reached or returns no items."""


def _assert_payload_sane(items: dict[str, Any]) -> dict[str, float]:
    """Aborta se a forma do payload indicar que a planilha mudou de layout.

    Retorna as proporções observadas (telemetria). Levanta ``CostRefreshError``
    assim que qualquer teto for estourado — antes de gravar qualquer linha.
    """
    total = len(items)
    if not total:
        raise CostRefreshError("GAS costs_all returned 0 items")

    invalid_mlb = [k for k in items if not _VALID_MLB_RE.match(k)]
    rows = [(k, v) for k, v in items.items() if isinstance(v, dict)]
    missing_sku = [k for k, v in rows if not str(v.get("sku") or "").strip()]
    missing_cost = [k for k, v in rows if not v.get("baseCost")]
    missing_bands = [k for k, v in rows if not v.get("freightBands")]

    ratios = {
        "invalid_mlb": len(invalid_mlb) / total,
        "missing_sku": len(missing_sku) / total,
        "missing_cost": len(missing_cost) / total,
        "missing_bands": len(missing_bands) / total,
    }
    if total < _MIN_SAMPLE_FOR_RATIOS:
        logger.warning(
            "cost_refresh_small_payload_ratios_not_enforced",
            received=total,
            min_sample=_MIN_SAMPLE_FOR_RATIOS,
            **{f"ratio_{k}": round(v, 4) for k, v in ratios.items()},
        )
        return ratios

    checks = (
        ("invalid_mlb", _MAX_INVALID_MLB_RATIO, invalid_mlb, "chave não parece um MLB"),
        ("missing_sku", _MAX_MISSING_SKU_RATIO, missing_sku, "sku vazio"),
        ("missing_cost", _MAX_MISSING_COST_RATIO, missing_cost, "baseCost ausente/zero"),
        ("missing_bands", _MAX_MISSING_BANDS_RATIO, missing_bands, "freightBands ausente"),
    )
    for name, ceiling, offenders, what in checks:
        if ratios[name] > ceiling:
            raise CostRefreshError(
                f"payload da planilha parece ter mudado de layout: {what} em "
                f"{len(offenders)}/{total} itens ({ratios[name]:.1%} > teto {ceiling:.0%}). "
                f"Exemplos: {offenders[:3]}. Verifique a aba 'Mercado Livre' antes de "
                f"deixar isto gravar em ml_costs_snapshot."
            )
    return ratios


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
    # Falha ALTO se a forma do payload indicar layout mudado — antes de gravar.
    ratios = _assert_payload_sane(items)

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
        **{f"ratio_{k}": round(v, 4) for k, v in ratios.items()},
    )
    return stats
