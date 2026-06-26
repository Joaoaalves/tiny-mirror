"""REST endpoints for the ML promotion automation feature.

Surfaces 4 groups:
- caps       — user-set caps per SKU (GET/PUT, bulk upsert)
- costs      — read snapshot + force refresh from GAS
- eligible   — pass-through to ML API (live)
- evaluate   — run decision algorithm (always dry_run in this phase)
- log        — audit history of actions
- alerts     — anomalies inbox (list + acknowledge)
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Annotated, Any, Literal

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.api.dependencies import db_session
from tiny_mirror.config import settings
from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.repositories.ml_listing_repository import (
    MLListingRepository,
)
from tiny_mirror.infrastructure.repositories.ml_promo_repository import (
    MLCostsSnapshotRepository,
    MLPromoActionRepository,
    MLPromoAlertRepository,
    MLPromoCapRepository,
    MLPromoDecisionRepository,
    MLPromoResubscribeRepository,
)
from tiny_mirror.redis_client import get_redis
from tiny_mirror.services import feature_flags
from tiny_mirror.services.ml_promotion_service import (
    CO_PARTICIPATION_TYPES,
    EDITABLE_INPLACE_TYPES,
    PRICE_EDITABLE_TYPES,
    MLPromotionService,
    _parse_iso_dt,
    _to_dec,
)
from tiny_mirror.services.promo_learning_service import load_samples, recommend
from tiny_mirror.services.promotion_mirror_service import PromotionMirrorService

logger = structlog.get_logger(__name__)

# Contextvars an operation may bind. Cleared after every request (the _op_ctx
# teardown below) so promo-operation fields never leak into unrelated requests.
_OP_CTX_KEYS = (
    "promo_op",
    "op_id",
    "decision_id",
    "mlb_id",
    "sku",
    "promo_type",
    "promo_id",
    "actor",
    "apply_mode",
)


async def _op_ctx() -> AsyncIterator[None]:
    """Router-wide dependency: stamp a fresh ``op_id`` on every request and
    guarantee the per-operation log context is torn down afterwards.

    Combined with ``_bind_op`` (called inside the mutation endpoints), this
    makes every log line of one operation — including the ML request/response
    lines emitted deep in the service — share the same ``op_id`` + domain
    fields, so a failed promotion write can be reconstructed end-to-end in Seq
    by filtering on a single id.
    """
    structlog.contextvars.bind_contextvars(op_id=uuid.uuid4().hex[:12])
    try:
        yield
    finally:
        structlog.contextvars.unbind_contextvars(*_OP_CTX_KEYS)


def _bind_op(op: str, **fields: Any) -> None:
    """Bind the operation name + domain context onto structlog contextvars so
    every subsequent log in this request carries them. ``None`` values are
    dropped to keep the log object clean."""
    structlog.contextvars.bind_contextvars(
        promo_op=op, **{k: v for k, v in fields.items() if v is not None}
    )


def _done(result: str, *, level: str = "info", **fields: Any) -> None:
    """Emit the terminal ``promo_op.done`` log for the current operation,
    carrying the bound op context + the per-call outcome fields."""
    clean = {k: v for k, v in fields.items() if v is not None}
    if level == "warning":
        logger.warning("promo_op.done", result=result, **clean)
    else:
        logger.info("promo_op.done", result=result, **clean)


def _fnum(v: Any) -> float | None:
    """Coerce Decimal/str/None → float|None for log fields."""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


router = APIRouter(dependencies=[Depends(_op_ctx)])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
class CapIn(BaseModel):
    """Cap bulk-upsert input — one entry per anúncio (per MLB).

    `sku` is required so newly-introduced rows know their grouping key;
    `mlb_id` is the canonical identity. Both must be present.
    """

    model_config = ConfigDict(extra="forbid")
    mlb_id: str
    sku: str
    max_seller_share_pct: Decimal = Field(..., gt=0, le=100)
    margin_floor_price: Decimal | None = Field(default=None, ge=0)
    auto_apply: bool | None = None
    freight_band_opt: bool | None = None
    skip_when_winning: bool | None = None
    excluded_promo_types: list[str] | None = None
    notes: str | None = None


class CapsBulkIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    items: list[CapIn]
    updated_by: str | None = None


class CapOut(BaseModel):
    """Per-MLB cap row enriched with the matching cost snapshot + catalog
    status. The frontend groups these by SKU client-side."""

    model_config = ConfigDict(from_attributes=True)
    mlb_id: str
    sku: str
    max_seller_share_pct: Decimal
    margin_floor_price: Decimal | None
    auto_apply: bool
    has_active_promo: bool = False
    active_promo_price: Decimal | None = None
    freight_band_opt: bool
    skip_when_winning: bool
    excluded_promo_types: list[str]
    notes: str | None
    updated_by: str | None
    updated_at: datetime
    # Joined from ml_listings (the MLB's listing row) — type + status + title.
    logistic_type: str | None = None
    listing_status: str | None = None
    available_quantity: int | None = None
    listing_title: str | None = None
    listing_thumbnail: str | None = None
    permalink: str | None = None
    # Estoque por depósito (vindo de stock_deposits via SKU). O front escolhe
    # qual exibir pelo logistic_type: FLEX → galpão (warehouse_available);
    # FULFILLMENT → Full (full_available) + a caminho (full_in_transit).
    warehouse_available: int | None = None
    full_available: int | None = None
    full_in_transit: int | None = None
    # Full listing price on ML (item.price). The displayed "preço cheio" —
    # product price comes from ML, not the planilha.
    ml_list_price: Decimal | None = None
    # MLB ids linked via ML item_relations (catalog↔traditional). Non-empty
    # ⇒ a promo on one applies to both; act only on the catalog side.
    linked_mlb_ids: list[str] = Field(default_factory=list)
    # Joined from ml_costs_snapshot — the pricing inputs the dashboard
    # uses to recompute margin live while editing the cap.
    base_cost: Decimal | None = None
    commission_pct: Decimal | None = None
    difal_pct: Decimal | None = None
    list_price: Decimal | None = None
    sheet_promo_price: Decimal | None = None
    freight_bands: Any | None = None
    margin_at_floor_value: Decimal | None = None
    margin_at_floor_pct: Decimal | None = None
    # Joined from ml_catalog_status (price_to_win / buy-box).
    catalog_listing: bool | None = None
    catalog_status: str | None = None
    visit_share: str | None = None
    current_price: Decimal | None = None
    price_to_win: Decimal | None = None
    winner_price: Decimal | None = None
    competitors_sharing_first_place: int | None = None
    # True when the underlying MLB is still active in ml_listings. False
    # means the cap row is orphan and the UI should hide it by default.
    has_active_listing: bool | None = None


class CostSnapshotOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    mlb_id: str
    sku: str
    active_on_sheet: bool
    base_cost: Decimal | None
    commission_pct: Decimal | None
    commission_label: str | None
    list_price: Decimal | None
    sheet_promo_price: Decimal | None
    sheet_discount_pct: Decimal | None
    sheet_margin_pct: Decimal | None
    sheet_margin_value: Decimal | None
    freight_bands: Any | None
    fetch_error: str | None
    fetched_at: datetime
    # DIFAL (sheet-wide constant). Sent on every snapshot so the frontend
    # can mirror the margin formula locally without a separate config call.
    difal_pct: Decimal | None = None


class ActionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    sku: str
    mlb_id: str
    action: str
    promo_type: str | None
    promo_id: str | None
    price_before: Decimal | None
    price_after: Decimal | None
    total_pct: Decimal | None
    seller_pct: Decimal | None
    meli_pct: Decimal | None
    reason: str | None
    ml_response: Any | None
    dry_run: bool
    at: datetime
    decided_by: str | None = None
    context: Any | None = None


class AlertOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    sku: str
    mlb_id: str
    kind: str
    message: str
    data: Any | None
    acknowledged: bool
    acknowledged_by: str | None
    acknowledged_at: datetime | None
    at: datetime


class EvaluateOut(BaseModel):
    sku: str
    mlb_id: str
    action: str
    reason: str
    dry_run: bool
    floor_price: float | None
    current_price: float | None
    current_total_pct: float | None
    current_promo_type: str | None
    target_price: float | None
    target_total_pct: float | None
    target_seller_pct: float | None
    target_promo_type: str | None
    target_promo_name: str | None
    floor_violated: bool
    freight_opt_net_gain: float | None


# ---------------------------------------------------------------------------
# Decisions (approval queue)
# ---------------------------------------------------------------------------
class DecisionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    mlb_id: str
    sku: str
    promo_key: str
    promo_id: str | None
    promo_type: str
    promo_name: str | None
    decision_kind: str
    target_price: Decimal | None
    target_total_pct: Decimal | None
    target_seller_pct: Decimal | None
    meli_percentage: Decimal | None
    constraint_used: str | None
    list_price: Decimal | None
    cap_pct: Decimal | None
    floor_price: Decimal | None
    min_price: Decimal | None = None
    max_price: Decimal | None = None
    stock_min: int | None = None
    stock_max: int | None = None
    stock_chosen: int | None = None
    reason: str
    status: str
    promo_start_date: datetime | None = None
    promo_finish_date: datetime | None = None
    created_at: datetime
    decided_at: datetime | None
    decided_by: str | None
    notes: str | None
    expired_at: datetime | None = None
    expired_reason: str | None = None
    ml_apply_status: str | None = None
    ml_apply_status_code: int | None = None
    ml_apply_response: str | None = None
    ml_applied_at: datetime | None = None
    # Co-participação CONDICIONAL (SMART/PRICE_MATCHING/MARKETPLACE_CAMPAIGN): o ML
    # define o preço dinamicamente; o ``target_price`` é só o piso, NÃO o preço
    # ativo. O front mostra "preço dinâmico" em vez de fingir um desconto fixo.
    is_dynamic: bool = False


class DecisionDecideIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    decided_by: str | None = None
    notes: str | None = None
    # Override do alvo digitado pelo operador antes de aprovar. Quando
    # presente, o servidor re-valida contra floor_price/cap_pct + meli
    # da própria linha; rejeita 422 se violar. target_total_pct e
    # target_seller_pct são recalculados a partir do novo target_price
    # + list_price + meli_percentage.
    target_price: Decimal | None = None
    # Unidades a reservar na oferta (LIGHTNING/DOD) — escolhida pelo operador
    # dentro de [stock_min, stock_max]. Gravada em stock_chosen e enviada no POST.
    units: int | None = None


class BulkDecideIn(BaseModel):
    """Bulk-act on every pending decision matching a filter.

    Designed for queue cleanup at scale — e.g. ignoring the 1k+ rows
    SELLER_COUPON_CAMPAIGN tends to flood with — without the operator
    having to checkbox every row. The endpoint is dry-run by default
    so the UI can call it twice: first to preview the count and the
    breakdown, then again with ``dry_run=False`` only after the
    operator confirms.

    Approve is intentionally NOT permitted here. Per-row approve has
    extra validation (target_price override + floor/cap re-check); the
    bulk lane never carries those overrides, so we restrict it to the
    DB-only paths (ignore / reject) that need no validation. A future
    bulk-approve would need to stream individual approves through the
    per-row endpoint to preserve that validation.
    """

    model_config = ConfigDict(extra="forbid")
    action: Literal["ignore", "reject"]
    # Filter knobs. None = match everything.
    promo_types: list[str] | None = None
    # Δ% relative to list_price: (target - list) / list * 100. Negative
    # = price drop (the usual case). Defining the range as min/max
    # gives the operator e.g. "ignore everything with discount >15%"
    # via max_delta_pct=-15.
    min_delta_pct: float | None = None
    max_delta_pct: float | None = None
    skus: list[str] | None = None
    dry_run: bool = True
    decided_by: str | None = None
    notes: str | None = None


# ---------------------------------------------------------------------------
# Dependency: service (resolved per-request from app.state)
# ---------------------------------------------------------------------------
def _service_dep(request: Request) -> MLPromotionService:
    ml_token_service = getattr(request.app.state, "ml_token_service", None)
    http_client = request.app.state.http_client
    if ml_token_service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="ML token service not configured (ML_CLIENT_ID missing)",
        )
    return MLPromotionService(token_service=ml_token_service, http_client=http_client)


# ---------------------------------------------------------------------------
# Context snapshot helper (used by approve/reject/ignore/reprice/direct-create)
# ---------------------------------------------------------------------------
async def _build_decision_context(
    session: AsyncSession,
    mlb_id: str,
    sku: str,
    *,
    price_before: float | None = None,
    price_after: float | None = None,
    list_price: float | None = None,
    floor_price: float | None = None,
    promo_type: str | None = None,
    source: str | None = None,
) -> dict[str, Any]:
    """Snapshot RICO do contexto no momento da ação — alimenta o dataset de
    automação (contexto → decisão).

    Captura tudo que pesa numa decisão de promoção pra depois aprender a política:
    buy-box (catálogo/winner/visit_share), margem + custo, desconto, ESTOQUE +
    cobertura, velocidade de vendas (7/30/90d + momentum), se JÁ havia promoção
    ativa, logística e status do anúncio. Todos os campos são best-effort: uma
    query que falha vira None e NUNCA bloqueia a ação principal.
    """
    from tiny_mirror.infrastructure.orm.models import (
        MLCatalogStatusORM,
        MLCostsSnapshotORM,
        MLListingORM,
        MLPromoCapORM,
    )

    ctx: dict[str, Any] = {
        "mlb_id": mlb_id,
        "sku": sku,
        "promo_type": promo_type,
        "source": source,  # 'single' | 'bulk' | etc. — como a ação foi disparada
        # preço
        "price_before": price_before,
        "price_after": price_after,
        "list_price": list_price,
        "floor_price": floor_price,
        "discount_pct": None,
        # margem / custo
        "margin_pct": None,
        "margin_value": None,
        "base_cost": None,
        "commission_pct": None,
        "cap_pct": None,
        # catálogo / buy-box
        "catalog_listing": None,
        "catalog_status": None,
        "current_price": None,
        "price_to_win": None,
        "winner_price": None,
        "visit_share": None,
        "competitors_sharing_first_place": None,
        # demanda
        "momentum": None,
        "sales_7d": None,
        "sales_30d": None,
        "sales_90d": None,
        "avg_daily_sales": None,
        # estoque
        "stock_available": None,
        "coverage_days": None,
        # anúncio
        "logistic_type": None,
        "listing_status": None,
        "has_active_promo": None,
    }

    try:
        # Catálogo / buy-box
        cat = await session.get(MLCatalogStatusORM, mlb_id)
        if cat:
            ctx["catalog_listing"] = cat.catalog_listing
            ctx["catalog_status"] = cat.status
            ctx["current_price"] = float(cat.current_price) if cat.current_price else None
            ctx["price_to_win"] = float(cat.price_to_win) if cat.price_to_win else None
            ctx["winner_price"] = float(cat.winner_price) if cat.winner_price else None
            ctx["visit_share"] = cat.visit_share
            ctx["competitors_sharing_first_place"] = cat.competitors_sharing_first_place

        # Anúncio: estoque + logística + status
        listing = await session.get(MLListingORM, mlb_id)
        if listing:
            ctx["stock_available"] = listing.available_quantity
            ctx["logistic_type"] = listing.logistic_type
            ctx["listing_status"] = listing.status

        # Teto de share do vendedor (cap)
        cap = await session.get(MLPromoCapORM, mlb_id)
        if cap and cap.max_seller_share_pct is not None:
            ctx["cap_pct"] = float(cap.max_seller_share_pct)

        # Demanda: momentum (mv_coverage) + janelas de vendas (ml_sales_daily)
        mom_row = await session.execute(
            text("SELECT momentum_15v30 FROM mv_coverage WHERE sku = :sku LIMIT 1"),
            {"sku": sku},
        )
        mom = mom_row.scalar_one_or_none()
        if mom is not None:
            ctx["momentum"] = float(mom)

        sales = (
            await session.execute(
                text(
                    "SELECT "
                    "COALESCE(SUM(qty) FILTER (WHERE sale_date >= CURRENT_DATE - 7), 0) AS d7, "
                    "COALESCE(SUM(qty) FILTER (WHERE sale_date >= CURRENT_DATE - 30), 0) AS d30, "
                    "COALESCE(SUM(qty) FILTER (WHERE sale_date >= CURRENT_DATE - 90), 0) AS d90 "
                    "FROM ml_sales_daily WHERE mlb_id = :mlb"
                ),
                {"mlb": mlb_id},
            )
        ).one_or_none()
        if sales is not None:
            ctx["sales_7d"] = int(sales.d7 or 0)
            ctx["sales_30d"] = int(sales.d30 or 0)
            ctx["sales_90d"] = int(sales.d90 or 0)
            avg = (sales.d90 or 0) / 90.0
            ctx["avg_daily_sales"] = round(avg, 3)
            if avg > 0 and ctx["stock_available"] is not None:
                ctx["coverage_days"] = round(float(ctx["stock_available"]) / avg, 1)

        # Já tinha promoção ativa? Fonte = espelho AS-IS ml_promotions (started),
        # não mais o motor de decisão.
        active = await session.execute(
            text(
                "SELECT COUNT(*) FROM ml_promotions " "WHERE mlb_id = :mlb AND status = 'started'"
            ),
            {"mlb": mlb_id},
        )
        ctx["has_active_promo"] = (active.scalar_one_or_none() or 0) > 0

        # Margem + desconto a partir do custo
        cost = await session.get(MLCostsSnapshotORM, mlb_id)
        ref_price = price_after or price_before or ctx["current_price"]
        if cost and cost.base_cost is not None:
            ctx["base_cost"] = float(cost.base_cost)
        if cost and cost.commission_pct is not None:
            ctx["commission_pct"] = float(cost.commission_pct)
        if cost and ref_price and cost.base_cost and cost.commission_pct:
            p = float(ref_price)
            commission = p * float(cost.commission_pct) / 100
            freight = 0.0
            if cost.freight_bands:
                for band in cost.freight_bands:
                    b_min = float(band.get("min", 0))
                    b_max = band.get("max")
                    if p >= b_min and (b_max is None or p <= float(b_max)):
                        freight = float(band.get("cost", 0))
                        break
            net = p - float(cost.base_cost) - commission - freight
            ctx["margin_value"] = round(net, 2)
            ctx["margin_pct"] = round((net / p) * 100, 2) if p else None

        # Desconto % vs preço de tabela
        lp = list_price or (float(cost.list_price) if cost and cost.list_price else None)
        if lp and ref_price:
            ctx["discount_pct"] = round(((lp - float(ref_price)) / lp) * 100, 2)
            if not ctx["list_price"]:
                ctx["list_price"] = lp

    except Exception:
        # Never let context capture crash the main operation. CRÍTICO: uma query
        # best-effort que falha aqui ABORTA a transação no Postgres; sem o rollback,
        # a sessão fica poluída e a PRÓXIMA operação do caller (ex.: gravar o log da
        # ação) quebra com "current transaction is aborted" → 500, mesmo quando a
        # escrita no ML JÁ deu certo. O rollback devolve a sessão utilizável.
        try:
            await session.rollback()
        except Exception:
            pass

    return ctx


# ---------------------------------------------------------------------------
# CAPS
# ---------------------------------------------------------------------------
async def _effective_fees(session: AsyncSession, mlb_id: str, snap: Any) -> tuple[Any, Any]:
    """Return ``(commission_pct, freight_bands)`` for margin math, applying the
    per-MLB **Flex** fee calibration when one exists.

    Fulfillment listings (and any MLB with unknown logistic_type or no
    calibration row) return the snapshot values UNCHANGED — fulfillment fees are
    already correct and must never be overridden. For Flex listings with a
    calibration row, commission_pct becomes the real effective rate and
    freight_bands becomes a synthetic 2-band table split at R$79 (the
    free-shipping cliff), so both margin engines and the frontend pick up the
    real seller freight without any further changes.
    """
    from sqlalchemy import select

    from tiny_mirror.infrastructure.orm.models import (
        MLFlexFeeCalibrationORM,
        MLListingORM,
    )
    from tiny_mirror.services.pricing_service import apply_flex_calibration

    base_comm = snap.commission_pct if snap is not None else None
    base_bands = snap.freight_bands if snap is not None else None

    logistic_type = (
        await session.execute(
            select(MLListingORM.logistic_type).where(MLListingORM.mlb_id == mlb_id)
        )
    ).scalar_one_or_none()
    if logistic_type is None or logistic_type == "fulfillment":
        return base_comm, base_bands

    calib = await session.get(MLFlexFeeCalibrationORM, mlb_id)
    return apply_flex_calibration(logistic_type, base_comm, base_bands, calib)


_FULL_DEPOSIT_NAME = "Full Mercado Livre"


async def _stock_by_skus(session: AsyncSession, skus: list[str]) -> dict[str, dict[str, int]]:
    """Estoque por SKU a partir de ``stock_deposits`` (juntado por ``stock.sku``).
    Retorna, por SKU:
      - ``full_in_transfer``: in_transfer do depósito Full (transferência interna
        do ML; entra no estoque EFETIVO junto com o available do anúncio).
      - ``pending_full``: ``pending_full_qty`` = unidades enviadas galpão→Full que
        o ML ainda não confirmou (= "enviados/a caminho", MESMA fonte da reposição).
      - ``warehouse_available``: galpão (depósitos não-Full, não-ignorados) p/ FLEX.
    Uma query só."""
    skus = list({s for s in skus if s})
    if not skus:
        return {}
    rows = (
        (
            await session.execute(
                text(
                    "SELECT s.sku, "
                    "COALESCE(SUM(sd.in_transfer) FILTER (WHERE sd.deposit_name = :full), 0) "
                    "  AS full_in_transfer, "
                    "COALESCE((SELECT SUM(ft.quantity - ft.quantity_received) "
                    "  FROM fulfillment_transfers ft "
                    "  WHERE ft.product_sku = s.sku AND ft.status = 'pending'), 0) "
                    "  AS pending_full, "
                    "COALESCE(SUM(sd.available) FILTER "
                    "  (WHERE sd.deposit_name <> :full AND NOT sd.ignore), 0) "
                    "  AS warehouse_available "
                    "FROM stock s JOIN stock_deposits sd "
                    "  ON sd.product_tiny_id = s.product_tiny_id "
                    "WHERE s.sku = ANY(:skus) GROUP BY s.sku"
                ),
                {"skus": skus, "full": _FULL_DEPOSIT_NAME},
            )
        )
        .mappings()
        .all()
    )
    return {
        r["sku"]: {
            "full_in_transfer": int(r["full_in_transfer"]),
            "pending_full": int(r["pending_full"]),
            "warehouse_available": int(r["warehouse_available"]),
        }
        for r in rows
    }


async def _enrich_cap(
    session: AsyncSession,
    cap: Any,
) -> CapOut:
    """Attach the matching cost snapshot, catalog status, and listing row
    to a per-MLB cap. Each enrichment lookup is keyed by ``cap.mlb_id``.
    """
    from sqlalchemy import select

    from tiny_mirror.config import settings
    from tiny_mirror.infrastructure.orm.models import MLCatalogStatusORM, MLListingORM
    from tiny_mirror.services.pricing_service import PricingDataError, margin_at_price

    snap_repo = MLCostsSnapshotRepository(session)
    snap = await snap_repo.get(cap.mlb_id)

    out = CapOut.model_validate(cap)
    out.difal_pct = Decimal(str(settings.margin_difal_pct))

    if snap is not None:
        out.list_price = snap.list_price
        out.base_cost = snap.base_cost
        # Flex listings: override the (wrong) spreadsheet commission + generic
        # freight bands with the per-MLB calibration. Fulfillment is unchanged.
        eff_commission_pct, eff_freight_bands = await _effective_fees(session, cap.mlb_id, snap)
        out.commission_pct = eff_commission_pct
        out.freight_bands = eff_freight_bands
        out.sheet_promo_price = snap.sheet_promo_price

        floor_price = cap.margin_floor_price or snap.sheet_promo_price
        if (
            floor_price is not None
            and snap.base_cost is not None
            and eff_commission_pct is not None
            and eff_freight_bands
        ):
            try:
                breakdown = margin_at_price(
                    price=floor_price,
                    base_cost=snap.base_cost,
                    commission_pct=eff_commission_pct,
                    freight_bands=eff_freight_bands,
                )
                out.margin_at_floor_value = breakdown.margin_value
                out.margin_at_floor_pct = breakdown.margin_pct
            except PricingDataError:
                pass

    listing = (
        await session.execute(select(MLListingORM).where(MLListingORM.mlb_id == cap.mlb_id))
    ).scalar_one_or_none()
    if listing is not None:
        out.logistic_type = listing.logistic_type
        out.listing_status = listing.status
        out.available_quantity = listing.available_quantity
        out.listing_title = listing.title
        out.listing_thumbnail = listing.thumbnail
        out.permalink = listing.permalink
        out.ml_list_price = listing.price
        out.linked_mlb_ids = list(listing.linked_mlb_ids or [])
        out.has_active_listing = listing.status == "active"
    else:
        out.has_active_listing = False

    st = (await _stock_by_skus(session, [cap.sku])).get(cap.sku)
    in_transfer = st["full_in_transfer"] if st is not None else 0
    if st is not None:
        out.warehouse_available = st["warehouse_available"]  # galpão p/ FLEX
        out.full_in_transit = st["pending_full"]  # enviados galpão→Full (= reposição)
    # FULL → estoque EFETIVO = available do anúncio (por MLB, real no ML) + o que o
    # ML está transferindo internamente (in_transfer, vira vendável em horas). Bate
    # com o stock_full_ml da reposição. A cobertura no front soma os 'enviados'
    # (full_in_transit) por cima.
    out.full_available = (out.available_quantity or 0) + in_transfer

    cat = (
        await session.execute(
            select(MLCatalogStatusORM).where(MLCatalogStatusORM.mlb_id == cap.mlb_id)
        )
    ).scalar_one_or_none()
    if cat is not None:
        out.catalog_listing = bool(cat.catalog_listing)
        out.catalog_status = cat.status
        out.visit_share = cat.visit_share
        out.current_price = cat.current_price
        out.price_to_win = cat.price_to_win
        out.winner_price = cat.winner_price
        out.competitors_sharing_first_place = cat.competitors_sharing_first_place

    return out


@router.get("/caps", response_model=list[CapOut])
async def list_caps(
    only_auto: Annotated[bool | None, Query(description="filter by auto_apply")] = None,
    include_orphans: Annotated[
        bool,
        Query(
            description=(
                "Include caps whose MLB is not active in ml_listings. "
                "False (default) hides them — they cannot be acted on."
            ),
        ),
    ] = False,
    limit: int = Query(default=200, ge=1, le=2000),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(db_session),
) -> list[CapOut]:
    repo = MLPromoCapRepository(session)
    rows, _ = await repo.list_all(only_auto=only_auto, limit=limit, offset=offset)
    enriched = [await _enrich_cap(session, r) for r in rows]
    if not include_orphans:
        enriched = [c for c in enriched if c.has_active_listing]
    return enriched


@router.get("/caps/by-sku/{sku}", response_model=list[CapOut])
async def list_caps_by_sku(
    sku: str,
    session: AsyncSession = Depends(db_session),
) -> list[CapOut]:
    """All per-MLB caps for a SKU. Used by the drawer + kit grouping flows."""
    repo = MLPromoCapRepository(session)
    rows = await repo.get_by_sku(sku)
    return [await _enrich_cap(session, r) for r in rows]


@router.get("/caps/{mlb_id}", response_model=CapOut)
async def get_cap(
    mlb_id: str,
    session: AsyncSession = Depends(db_session),
) -> CapOut:
    repo = MLPromoCapRepository(session)
    row = await repo.get(mlb_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"no cap for mlb_id={mlb_id}")
    return await _enrich_cap(session, row)


@router.put("/caps", response_model=list[CapOut])
async def bulk_upsert_caps(
    body: CapsBulkIn,
    session: AsyncSession = Depends(db_session),
) -> list[CapOut]:
    repo = MLPromoCapRepository(session)
    out: list[CapOut] = []
    for item in body.items:
        row = await repo.upsert(
            item.mlb_id,
            sku=item.sku,
            max_seller_share_pct=item.max_seller_share_pct,
            margin_floor_price=item.margin_floor_price,
            auto_apply=item.auto_apply,
            freight_band_opt=item.freight_band_opt,
            skip_when_winning=item.skip_when_winning,
            excluded_promo_types=item.excluded_promo_types,
            notes=item.notes,
            updated_by=body.updated_by,
        )
        out.append(await _enrich_cap(session, row))
    await session.commit()
    return out


@router.delete("/caps/{mlb_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_cap(
    mlb_id: str,
    session: AsyncSession = Depends(db_session),
) -> None:
    repo = MLPromoCapRepository(session)
    deleted = await repo.delete(mlb_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"no cap for mlb_id={mlb_id}")
    await session.commit()


# ---------------------------------------------------------------------------
# COSTS
# ---------------------------------------------------------------------------
def _snapshot_out(row: Any) -> CostSnapshotOut:
    """Build a CostSnapshotOut and attach the DIFAL constant so the
    frontend can mirror the margin formula without an extra config call.
    """
    from tiny_mirror.config import settings

    out = CostSnapshotOut.model_validate(row)
    out.difal_pct = Decimal(str(settings.margin_difal_pct))
    return out


@router.get("/costs/{sku}", response_model=list[CostSnapshotOut])
async def get_costs(
    sku: str,
    session: AsyncSession = Depends(db_session),
) -> list[CostSnapshotOut]:
    repo = MLCostsSnapshotRepository(session)
    rows = await repo.get_by_sku(sku)
    return [_snapshot_out(r) for r in rows]


@router.post("/costs/refresh/{sku}", response_model=list[CostSnapshotOut])
async def refresh_costs(
    sku: str,
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> list[CostSnapshotOut]:
    listings = MLListingRepository(session)
    mlb_ids = await listings.get_active_mlb_ids_for_sku(sku)
    if not mlb_ids:
        raise HTTPException(status_code=404, detail=f"no active MLB for sku={sku}")
    for mlb in mlb_ids:
        await service.refresh_costs_for_mlb(session, mlb)
    await session.commit()
    repo = MLCostsSnapshotRepository(session)
    rows = await repo.get_by_sku(sku)
    return [_snapshot_out(r) for r in rows]


@router.post("/costs/refresh-all", response_model=dict[str, int])
async def refresh_all_costs(
    request: Request,
    session: AsyncSession = Depends(db_session),
) -> dict[str, int]:
    """Atualiza snapshots de custo via bulk endpoint do GAS.

    Antes: loop com 1 GET ``?action=cost`` por MLB ativo no ml_listings.
    Para ~370 MLBs isso levava ~25 min e estourava o curl timeout.

    Agora: 1 GET ``?action=costs_all`` que devolve todo o dump da planilha
    de uma vez (~15-30s independente de N). Implementação compartilhada
    com ``/caps/recompute?refresh_costs_first=true``.

    Idempotente. Cron diário ``ml-costs-refresh-daily`` continua hitting
    esse endpoint — apenas o motor mudou.

    Mantém shape de resposta ``{total, ok, errors}`` por compat com o
    shell wrapper ``refresh-costs.sh``.
    """
    from tiny_mirror.config import settings
    from tiny_mirror.services.cost_refresh_service import (
        CostRefreshError,
        refresh_all_from_bulk,
    )
    from tiny_mirror.services.gas_client import GASClient

    if not settings.gas_base_url or not settings.gas_token:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="GAS base URL / token not configured in env",
        )
    gas = GASClient(
        http=request.app.state.http_client,
        base_url=settings.gas_base_url,
        token=settings.gas_token,
        timeout_seconds=settings.gas_http_timeout_seconds,
    )
    try:
        stats = await refresh_all_from_bulk(session, gas)
    except CostRefreshError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"GAS bulk refresh failed: {exc}",
        ) from exc
    return {
        "total": stats["received"],
        "ok": stats["upserted"],
        "errors": stats["skipped_no_data"] + stats["skipped_invalid_id"],
    }


# ---------------------------------------------------------------------------
# CATALOG STATUS — refresh price_to_win / buy-box snapshot for every MLB
# ---------------------------------------------------------------------------
@router.post("/catalog-status/refresh-all")
async def refresh_catalog_status_all(
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, int]:
    """Iterate every active MLB and refresh `ml_catalog_status`.

    Calls /items/{MLB}/price_to_win for each. Items returning 404 are
    persisted with `catalog_listing=false` (so the engine knows they are
    NOT in a catalog listing — different from "never fetched").

    Heavy operation (~3-5 min depending on MLB count); the daily cron is
    the normal trigger. Use this endpoint manually after rebuilding the
    table or for testing.
    """
    from tiny_mirror.services.catalog_status_sync_service import CatalogStatusSyncService

    sync = CatalogStatusSyncService(
        token_service=service._token_service,
        http_client=service._http,
    )
    stats = await sync.refresh_all(session)
    return stats


# ---------------------------------------------------------------------------
# CAPS RECOMPUTE — derive max_seller_share_pct from costs + 10% margin target
# ---------------------------------------------------------------------------
@router.get("/audit")
async def audit_shown_promotions(
    sample: int = Query(default=40, description="max phantom rows returned in the sample list"),
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    """Cross-check EVERY promotion the UI shows against ML LIVE.

    Shown universe = Inscritas (``status='started'``, minus coupons) + Disponíveis
    (real-campaign ``candidate``/``pending``: DEAL/LIGHTNING/SELLER_CAMPAIGN/DOD/
    UNHEALTHY_STOCK). For each listing we re-fetch ML's live eligible promos and
    flag a *phantom* = a mirror row ML no longer returns (``absent``) or whose
    status diverged (``status_mismatch``). Read-only: no writes, no deletes — it
    only VERIFIES the mirror sweep is keeping up. Drives the daily phantom-audit
    cron, which alerts when ``phantom_count > 0``.
    """
    import asyncio

    shown_sql = text(
        """
        SELECT mlb_id, sku, promotion_type, promotion_id, status
        FROM ml_promotions
        WHERE (status='started' AND promotion_type <> 'SELLER_COUPON_CAMPAIGN')
           OR (status IN ('candidate','pending')
               AND promotion_type IN
                   ('DEAL','LIGHTNING','SELLER_CAMPAIGN','DOD','UNHEALTHY_STOCK'))
        """
    )
    rows = (await session.execute(shown_sql)).all()
    by_mlb: dict[str, list[tuple[Any, Any, Any, Any]]] = {}
    for mlb, sku, ptype, pid, st in rows:
        by_mlb.setdefault(mlb, []).append((sku, ptype, pid, st))

    phantoms: list[dict[str, Any]] = []
    counters = {"checked": 0, "valid": 0, "fetch_errors": 0}
    sem = asyncio.Semaphore(6)

    def _match(
        live: list[dict[str, Any]], ptype: str, pid: Any, want_started: bool
    ) -> tuple[str, Any]:
        for p in live:
            if (p.get("type") or "").upper() != ptype:
                continue
            if pid and p.get("id") != pid:
                continue
            st = (p.get("status") or "").lower()
            if want_started:
                return ("ok" if st == "started" else "status_mismatch", st)
            return ("ok" if st in ("candidate", "pending") else "status_mismatch", st)
        return ("absent", None)

    async def _check(mlb: str) -> None:
        async with sem:
            try:
                live = await service.fetch_eligible_promos(mlb)
            except Exception:
                counters["fetch_errors"] += 1
                return
            for sku, ptype, pid, st in by_mlb[mlb]:
                verdict, livest = _match(live, (ptype or "").upper(), pid, st == "started")
                counters["checked"] += 1
                if verdict == "ok":
                    counters["valid"] += 1
                else:
                    phantoms.append(
                        {
                            "mlb_id": mlb,
                            "sku": sku,
                            "promotion_type": ptype,
                            "promotion_id": pid,
                            "mirror_status": st,
                            "verdict": verdict,
                            "live_status": livest,
                        }
                    )

    await asyncio.gather(*(_check(m) for m in by_mlb))
    phantoms.sort(key=lambda p: str(p["mlb_id"]))
    logger.info(
        "promo audit done",
        shown=len(rows),
        anuncios=len(by_mlb),
        valid=counters["valid"],
        phantom_count=len(phantoms),
        fetch_errors=counters["fetch_errors"],
    )
    return {
        "shown": len(rows),
        "anuncios": len(by_mlb),
        "checked": counters["checked"],
        "valid": counters["valid"],
        "phantom_count": len(phantoms),
        "fetch_errors": counters["fetch_errors"],
        "phantoms": phantoms[:sample],
    }


@router.get("/ml-live/{mlb_id}")
async def ml_live(
    mlb_id: str,
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    """Read-only: estado AO VIVO no ML do anúncio — ``item.price`` (preço de venda
    AGORA, reflete a promo ativa) + ``item.base_price`` (cheio) + promos elegíveis.
    Serve pra confirmar que o valor da TELA bate com o ML."""
    try:
        item = await service.fetch_item_price(mlb_id)
    except Exception as exc:  # pragma: no cover — rede
        logger.warning("ml_live.item_failed", mlb_id=mlb_id, error=str(exc))
        item = {}
    try:
        promos_raw = await service.fetch_eligible_promos(mlb_id)
    except Exception as exc:  # pragma: no cover — rede
        logger.warning("ml_live.promos_failed", mlb_id=mlb_id, error=str(exc))
        promos_raw = []
    return {
        "mlb_id": mlb_id,
        "item": item,
        "promos": [
            {
                "type": p.get("type"),
                "id": p.get("id"),
                "status": p.get("status"),
                "ref_id": p.get("ref_id"),
                "price": p.get("price"),
                "name": p.get("name"),
            }
            for p in promos_raw
        ],
    }


@router.post("/caps/recompute")
async def recompute_caps(
    refresh_costs_first: bool = Query(
        default=False,
        description=(
            "when true, pulls a fresh bulk dump of all costs from the GAS "
            "Web App (single HTTP call) before recomputing"
        ),
    ),
    use_active_promos: bool = Query(
        default=True,
        description=(
            "when true (default), fetch each MLB's live promos from ML and "
            "anchor the cap to any STARTED promo. When false, only the "
            "fallback sheet+10% margin logic runs (no ML API calls)."
        ),
    ),
    request: Request = None,  # type: ignore[assignment]
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    """Recompute every ``ml_promo_caps`` row using each MLB's CURRENT
    STARTED promo as the baseline. Falls back to ``sheet_discount_pct``
    (or ``DEFAULT_CAP_PCT=30``) clipped by ``MIN_MARGIN_PCT=10`` when an
    MLB has no live promo. With ``refresh_costs_first=true`` the bulk
    GAS endpoint is called once to refresh all snapshots first.
    """
    from tiny_mirror.config import settings
    from tiny_mirror.services.cap_recompute_service import recompute_all_caps
    from tiny_mirror.services.cost_refresh_service import (
        CostRefreshError,
        refresh_all_from_bulk,
    )
    from tiny_mirror.services.gas_client import GASClient

    refresh_stats: dict[str, int] | None = None
    if refresh_costs_first:
        if not settings.gas_base_url or not settings.gas_token:
            raise HTTPException(
                status_code=503,
                detail="GAS base URL / token not configured in env",
            )
        http_client = request.app.state.http_client
        gas = GASClient(
            http=http_client,
            base_url=settings.gas_base_url,
            token=settings.gas_token,
            timeout_seconds=settings.gas_http_timeout_seconds,
        )
        try:
            refresh_stats = await refresh_all_from_bulk(session, gas)
        except CostRefreshError as exc:
            raise HTTPException(status_code=502, detail=f"GAS bulk refresh failed: {exc}") from exc

    stats = await recompute_all_caps(session, service=service if use_active_promos else None)
    if refresh_stats is not None:
        stats = {"refresh": refresh_stats, **stats}
    return stats


# ---------------------------------------------------------------------------
# PROFITABILITY — real-time margin math (no ML API call)
# ---------------------------------------------------------------------------
@router.get("/profitability/{sku}")
async def profitability(
    sku: str,
    price: Annotated[Decimal | None, Query(gt=0, description="margin AT this price")] = None,
    max_discount_pct: Annotated[
        Decimal | None,
        Query(gt=0, le=100, description="margin at list_price * (1 - pct/100)"),
    ] = None,
    min_margin_pct: Annotated[
        Decimal | None,
        Query(ge=0, le=100, description="lowest price keeping at least this margin %"),
    ] = None,
    meli_banca_pct: Annotated[
        Decimal,
        Query(ge=0, le=100, description="ML co-pay % on SMART/DEAL (0 for PRICE_DISCOUNT)"),
    ] = Decimal(0),
    session: AsyncSession = Depends(db_session),
) -> dict[str, Any]:
    """Real-time margin breakdown for a SKU, using the cost snapshot from
    the spreadsheet (base_cost + commission_pct + freight_bands) plus the
    sheet-wide DIFAL constant.

    Pick exactly one of: ``price``, ``max_discount_pct``, ``min_margin_pct``.

    Returns per-MLB result, since costs and freight bands are stored per
    MLB even when SKU has variants — the snapshot can drift (e.g. only
    one variant has cost data yet).
    """
    from tiny_mirror.services.pricing_service import (
        PricingDataError,
        margin_at_price,
        target_price_for_max_discount_pct,
        target_price_for_min_margin_pct,
    )

    provided = sum(x is not None for x in (price, max_discount_pct, min_margin_pct))
    if provided != 1:
        raise HTTPException(
            status_code=400,
            detail="provide exactly one of: price, max_discount_pct, min_margin_pct",
        )

    repo = MLCostsSnapshotRepository(session)
    snapshots = await repo.get_by_sku(sku)
    if not snapshots:
        raise HTTPException(status_code=404, detail=f"no cost snapshot for sku={sku}")

    out: list[dict[str, Any]] = []
    for snap in snapshots:
        if snap.base_cost is None or snap.commission_pct is None or snap.list_price is None:
            out.append(
                {
                    "mlb_id": snap.mlb_id,
                    "error": "snapshot missing base_cost/commission_pct/list_price",
                    "fetch_error": snap.fetch_error,
                }
            )
            continue
        try:
            if price is not None:
                effective_price = price
            elif max_discount_pct is not None:
                effective_price = target_price_for_max_discount_pct(
                    list_price=snap.list_price,
                    max_discount_pct=max_discount_pct,
                )
            else:
                assert min_margin_pct is not None
                effective_price = target_price_for_min_margin_pct(
                    base_cost=snap.base_cost,
                    commission_pct=snap.commission_pct,
                    freight_bands=snap.freight_bands,
                    min_margin_pct=min_margin_pct,
                    list_price=snap.list_price,
                    meli_banca_pct=meli_banca_pct,
                )
            breakdown = margin_at_price(
                price=effective_price,
                base_cost=snap.base_cost,
                commission_pct=snap.commission_pct,
                freight_bands=snap.freight_bands,
                list_price=snap.list_price,
                meli_banca_pct=meli_banca_pct,
            )
        except PricingDataError as exc:
            out.append({"mlb_id": snap.mlb_id, "error": str(exc)})
            continue

        out.append(
            {
                "mlb_id": snap.mlb_id,
                "list_price": float(snap.list_price),
                "discount_pct_vs_list": (
                    float(((snap.list_price - breakdown.price) / snap.list_price) * 100)
                    if snap.list_price > 0
                    else None
                ),
                **breakdown.as_dict(),
            }
        )

    return {"sku": sku, "results": out}


# ---------------------------------------------------------------------------
# ELIGIBLE — live ML API pass-through
# ---------------------------------------------------------------------------
@router.get("/eligible/{sku}")
async def list_eligible(
    sku: str,
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    listings = MLListingRepository(session)
    mlb_ids = await listings.get_active_mlb_ids_for_sku(sku)
    if not mlb_ids:
        raise HTTPException(status_code=404, detail=f"no active MLB for sku={sku}")
    out: dict[str, Any] = {}
    for mlb in mlb_ids:
        out[mlb] = await service.fetch_eligible_promos(mlb)
    return {"sku": sku, "mlbs": out}


# ---------------------------------------------------------------------------
# EVALUATE — run algorithm (always dry-run in this phase)
# ---------------------------------------------------------------------------
@router.post("/evaluate/{sku}", response_model=list[EvaluateOut])
async def evaluate_sku(
    sku: str,
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> list[EvaluateOut]:
    results = await service.evaluate_sku(session, sku, dry_run=True, actor="manual")
    await session.commit()
    return [
        EvaluateOut(
            sku=sku,
            mlb_id=r["mlb_id"],
            action=r["decision"].action,
            reason=r["decision"].reason,
            dry_run=r["dry_run"],
            floor_price=r["decision"].floor_price,
            current_price=r["decision"].current_price,
            current_total_pct=r["decision"].current_total_pct,
            current_promo_type=r["decision"].current_promo_type,
            target_price=r["decision"].target_price,
            target_total_pct=r["decision"].target_total_pct,
            target_seller_pct=r["decision"].target_seller_pct,
            target_promo_type=r["decision"].target_promo_type,
            target_promo_name=r["decision"].target_promo_name,
            floor_violated=r["decision"].floor_violated,
            freight_opt_net_gain=r["decision"].freight_opt.net_gain
            if r["decision"].freight_opt
            else None,
        )
        for r in results
    ]


@router.post("/evaluate-all", response_model=dict[str, int])
async def evaluate_all(
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, int]:
    """Iterate every SKU with at least one auto_apply=true cap and evaluate.
    Returns count summary."""
    repo = MLPromoCapRepository(session)
    rows, _ = await repo.list_all(only_auto=True, limit=2000)
    summary: dict[str, int] = {}
    # Caps are now per-MLB; dedupe SKUs so we don't call evaluate_sku N times.
    skus_seen: set[str] = set()
    for cap in rows:
        if cap.sku in skus_seen:
            continue
        skus_seen.add(cap.sku)
        results = await service.evaluate_sku(session, cap.sku, dry_run=True, actor="cron")
        for r in results:
            summary[r["decision"].action] = summary.get(r["decision"].action, 0) + 1
    await session.commit()
    return summary


@router.post("/analyze-all")
async def analyze_all(
    limit: int = Query(default=2000, ge=1, le=5000),
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    """Full dry-run analysis across every SKU that has a cap.

    Side-effects-free:
      - reads caps + cost snapshots from Postgres (no GAS call)
      - calls ML /seller-promotions per MLB (live read, no write)
      - returns aggregate stats + elapsed time + per-action breakdown
      - DOES NOT write ml_promo_actions, ml_promo_alerts, or anything to ML

    Use this to forecast "if I turned auto_apply on tomorrow, how many
    promos would be activated, how long would it take, what would break".
    """
    import time as _time

    repo = MLPromoCapRepository(session)
    rows, total = await repo.list_all(only_auto=None, limit=limit)
    started_at = _time.monotonic()
    action_counts: dict[str, int] = {}
    promo_type_counts: dict[str, int] = {}
    catalog_status_counts: dict[str, int] = {}
    visit_share_counts: dict[str, int] = {}
    skus_with_results = 0
    mlbs_evaluated = 0
    total_eligible_candidates = 0
    activate_examples: list[dict[str, Any]] = []
    create_examples: list[dict[str, Any]] = []
    still_losing_examples: list[dict[str, Any]] = []
    keep_with_violation = 0
    still_losing_count = 0
    skus_with_zero_cap = 0
    errors: list[dict[str, Any]] = []

    # Caps are per-MLB now; dedupe SKUs and use a SKU-level "zero" check
    # (every MLB of the SKU has cap=0 => the whole SKU is skipped).
    by_sku: dict[str, list[Any]] = {}
    for cap in rows:
        by_sku.setdefault(cap.sku, []).append(cap)

    for sku, sku_caps in by_sku.items():
        if all(c.max_seller_share_pct == 0 for c in sku_caps):
            skus_with_zero_cap += 1
            action_counts["skip_zero_cap"] = action_counts.get("skip_zero_cap", 0) + 1
            continue
        try:
            results = await service.analyze_sku_dry(session, sku)
        except Exception as exc:  # pragma: no cover — surface error in report
            errors.append({"sku": sku, "error": str(exc)[:200]})
            continue
        if results:
            skus_with_results += 1
        sku_eligible_count = 0
        for r in results:
            mlbs_evaluated += 1
            sku_eligible_count += int(r.get("eligible_candidates_in_cap", 0))
            dec = r["decision"]
            action_counts[dec.action] = action_counts.get(dec.action, 0) + 1
            if dec.target_promo_type:
                promo_type_counts[dec.target_promo_type] = (
                    promo_type_counts.get(dec.target_promo_type, 0) + 1
                )
            if dec.action == "activate_candidate" and len(activate_examples) < 5:
                activate_examples.append(
                    {
                        "sku": sku,
                        "mlb_id": r["mlb_id"],
                        "from_total_pct": dec.current_total_pct,
                        "to_total_pct": dec.target_total_pct,
                        "target_price": dec.target_price,
                        "promo_type": dec.target_promo_type,
                        "promo_name": dec.target_promo_name,
                        "reason": dec.reason,
                    }
                )
            if dec.action == "create_price_discount" and len(create_examples) < 5:
                create_examples.append(
                    {
                        "sku": sku,
                        "mlb_id": r["mlb_id"],
                        "target_total_pct": dec.target_total_pct,
                        "target_price": dec.target_price,
                        "reason": dec.reason,
                    }
                )
            if dec.action == "keep" and dec.floor_violated:
                keep_with_violation += 1
            # Catalog-aware stats
            if dec.catalog_status:
                catalog_status_counts[dec.catalog_status] = (
                    catalog_status_counts.get(dec.catalog_status, 0) + 1
                )
            if dec.visit_share:
                visit_share_counts[dec.visit_share] = visit_share_counts.get(dec.visit_share, 0) + 1
            if dec.still_losing:
                still_losing_count += 1
                if len(still_losing_examples) < 5:
                    still_losing_examples.append(
                        {
                            "sku": sku,
                            "mlb_id": r["mlb_id"],
                            "our_price": dec.target_price or dec.current_price,
                            "price_to_win": dec.price_to_win,
                            "floor_price": dec.floor_price,
                            "reason": dec.reason,
                        }
                    )
        total_eligible_candidates += sku_eligible_count

    elapsed = _time.monotonic() - started_at
    would_activate = action_counts.get("activate_candidate", 0) + action_counts.get(
        "create_price_discount", 0
    )

    return {
        "elapsed_seconds": round(elapsed, 2),
        "caps_total_in_db": total,
        "caps_iterated": len(rows),
        "skus_with_zero_cap_skipped": skus_with_zero_cap,
        "skus_with_at_least_one_mlb": skus_with_results,
        "mlbs_evaluated": mlbs_evaluated,
        "would_activate_total": would_activate,
        "total_eligible_candidates_in_cap": total_eligible_candidates,
        "action_counts": action_counts,
        "promo_type_counts_when_acting": promo_type_counts,
        "catalog_status_counts": catalog_status_counts,
        "visit_share_counts": visit_share_counts,
        "still_losing_count": still_losing_count,
        "keep_with_floor_violation": keep_with_violation,
        "errors": errors,
        "activate_examples": activate_examples,
        "create_examples": create_examples,
        "still_losing_examples": still_losing_examples,
    }


# ---------------------------------------------------------------------------
# LOG
# ---------------------------------------------------------------------------
@router.get("/log", response_model=list[ActionOut])
async def list_actions(
    sku: str | None = Query(default=None),
    action: str | None = Query(default=None),
    include_dry_run: bool = Query(default=True),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(db_session),
) -> list[ActionOut]:
    repo = MLPromoActionRepository(session)
    rows, _ = await repo.list_all(
        sku=sku,
        action=action,
        include_dry_run=include_dry_run,
        limit=limit,
        offset=offset,
    )
    return [ActionOut.model_validate(r) for r in rows]


# ---------------------------------------------------------------------------
# ALERTS
# ---------------------------------------------------------------------------
@router.get("/alerts", response_model=list[AlertOut])
async def list_alerts(
    acknowledged: bool = Query(default=False),
    kind: str | None = Query(default=None),
    sku: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(db_session),
) -> list[AlertOut]:
    repo = MLPromoAlertRepository(session)
    rows, _ = await repo.list_all(
        acknowledged=acknowledged,
        kind=kind,
        sku=sku,
        limit=limit,
        offset=offset,
    )
    return [AlertOut.model_validate(r) for r in rows]


@router.post("/alerts/{alert_id}/ack", status_code=status.HTTP_204_NO_CONTENT)
async def acknowledge_alert(
    alert_id: int,
    by: str | None = Query(default=None),
    session: AsyncSession = Depends(db_session),
) -> None:
    repo = MLPromoAlertRepository(session)
    ok = await repo.acknowledge(alert_id, by=by)
    if not ok:
        raise HTTPException(status_code=404, detail=f"alert {alert_id} not found or already acked")
    await session.commit()


# ---------------------------------------------------------------------------
# DECISIONS (operator approval queue)
# ---------------------------------------------------------------------------
@router.get("/config")
async def get_config() -> dict[str, Any]:
    """Read-only snapshot of every promotion feature flag.

    Surfaced to the UI so it can render a 'simulação vs execução' banner
    above the Decisões table. **Read-only by contract** — flipping
    flags requires editing the VPS env (ML_PROMO_APPLY_ENABLED) and a
    service restart. The approve/reject/ignore handlers re-read flag
    state on every request and never trust the caller.
    """
    return {
        "flags": feature_flags.public_state(),
        # Convenience top-level alias so the UI can read one field instead
        # of digging into flags["ml_promo_apply"].
        "execute_enabled": feature_flags.is_enabled("ml_promo_apply"),
    }


@router.post("/decisions/generate")
async def generate_decisions(
    only_sku: str | None = Query(default=None, description="restrict to a single SKU"),
    limit_skus: int | None = Query(default=None, ge=1, le=2000),
    refresh_active_prices: bool = Query(
        default=False,
        description=(
            "também atualiza o preço das linhas 'started' já existentes com o "
            "valor vivo do ML (resync completo, corrige preços defasados)"
        ),
    ),
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    """Enumerate eligible candidate promos per MLB and insert one PENDING
    decision per (mlb_id, promo_key). Idempotent — re-running this skips
    decisions already in the queue (pending / approved / rejected).

    Pass ``refresh_active_prices=true`` for a full resync that also refreshes the
    cached price of existing active promos to match ML live."""
    return await service.generate_pending_decisions(
        session,
        only_sku=only_sku,
        limit_skus=limit_skus,
        refresh_active_prices=refresh_active_prices,
    )


@router.post("/decisions/bulk-act")
async def bulk_act_decisions(
    body: BulkDecideIn,
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    """Bulk-ignore or bulk-reject pending decisions matching a filter.

    Defaults to dry-run; the operator-facing UI calls this twice
    (preview, then commit). Approve is not supported here — bulk
    approve would skip the per-row target/cap/floor revalidation that
    the single-row endpoint enforces, and promotions cost money.
    """
    started = time.monotonic()
    _bind_op("bulk_act", actor=body.decided_by)
    logger.info(
        "promo_op.start",
        bulk_action=body.action,
        dry_run=body.dry_run,
        promo_types=body.promo_types,
        skus=body.skus,
        min_delta_pct=body.min_delta_pct,
        max_delta_pct=body.max_delta_pct,
    )
    result = await service.bulk_decide_pending(
        session,
        action=body.action,
        promo_types=body.promo_types,
        min_delta_pct=body.min_delta_pct,
        max_delta_pct=body.max_delta_pct,
        skus=body.skus,
        dry_run=body.dry_run,
        decided_by=body.decided_by,
        notes=body.notes,
    )
    _done(
        "bulk_preview" if body.dry_run else "bulk_committed",
        bulk_action=body.action,
        matched=result.get("matched") if isinstance(result, dict) else None,
        affected=result.get("affected") if isinstance(result, dict) else None,
        elapsed_ms=round((time.monotonic() - started) * 1000),
    )
    return result


@router.post("/decisions/expire-stale")
async def expire_stale_decisions(
    price_drift_pct: float | None = Query(default=None, ge=0.0, le=100.0),
    cap_drift_pct: float | None = Query(default=None, ge=0.0, le=100.0),
    floor_drift_pct: float | None = Query(default=None, ge=0.0, le=100.0),
    age_days: int | None = Query(default=None, ge=0, le=365),
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    """Flip pending decisions to status='expired' when inputs moved.

    Same logic the daily cron uses; exposed here so the operator can
    re-sweep on demand right after the cap recompute. Each threshold
    falls back to the Settings default when omitted, so a no-arg call
    matches what runs at 05:00.
    """
    return await service.expire_stale_decisions(
        session,
        price_drift_pct=price_drift_pct,
        cap_drift_pct=cap_drift_pct,
        floor_drift_pct=floor_drift_pct,
        age_days=age_days,
    )


@router.get("/decisions", response_model=list[DecisionOut])
async def list_decisions(
    status_: Annotated[str | None, Query(alias="status")] = "pending",
    sku: str | None = Query(default=None),
    constraint_used: str | None = Query(default=None),
    exclude_types: Annotated[list[str] | None, Query()] = None,
    exclude_active: bool = Query(
        default=False,
        description=(
            "exclui decisões de MLBs que já têm promoção ATIVA (started) — "
            "usado pela view 'sem promoção' pra mostrar só anúncios sem promo rodando"
        ),
    ),
    limit: int = Query(default=200, ge=1, le=2000),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(db_session),
) -> list[DecisionOut]:
    repo = MLPromoDecisionRepository(session)
    # "all" (ou vazio) = sem filtro de status — usado pela view "Em andamento",
    # que cruza por constraint_used (ex.: "started" = campanha já ativa no ML).
    status_filter = None if status_ in (None, "all", "") else status_
    rows, _ = await repo.list_(
        status=status_filter,
        sku=sku,
        constraint_used=constraint_used,
        exclude_promo_types=exclude_types,
        exclude_active=exclude_active,
        limit=limit,
        offset=offset,
    )
    return [DecisionOut.model_validate(r) for r in rows]


# Co-participação CONDICIONAL: o ML define o preço dinamicamente (o `price` é só
# o piso). NÃO mostrar como desconto fixo.
_DYNAMIC_PROMO_TYPES = frozenset({"SMART", "PRICE_MATCHING", "MARKETPLACE_CAMPAIGN"})


@router.get("/promotions/active", response_model=list[DecisionOut])
async def list_active_promotions(
    session: AsyncSession = Depends(db_session),
) -> list[DecisionOut]:
    """Inscritas (promoções ATIVAS) servidas do espelho AS-IS ``ml_promotions``
    (Etapa 2). Mesma forma do ``/decisions?constraint_used=started`` — FATO do
    ML, sem preço inventado. Co-participação condicional (SMART/PRICE_MATCHING)
    vem marcada ``is_dynamic`` pra UI mostrar 'preço dinâmico' em vez do piso."""
    rows = (
        (
            await session.execute(
                text(
                    "SELECT p.id, p.mlb_id, p.sku, p.promo_key, p.promotion_id, "
                    "  p.promotion_type, p.name, p.price, p.original_price, "
                    "  p.meli_percentage, p.start_date, p.finish_date, p.first_seen_at, "
                    "  c.max_seller_share_pct AS cap_pct, c.margin_floor_price AS floor_price "
                    "FROM ml_promotions p "
                    "LEFT JOIN ml_promo_caps c ON c.mlb_id = p.mlb_id "
                    "WHERE p.status = 'started' "
                    "ORDER BY p.sku NULLS LAST, p.mlb_id, p.price"
                )
            )
        )
        .mappings()
        .all()
    )
    out: list[DecisionOut] = []
    for r in rows:
        price = r["price"]
        orig = r["original_price"]
        meli = r["meli_percentage"]
        is_dynamic = r["promotion_type"] in _DYNAMIC_PROMO_TYPES
        total_pct: Decimal | None = None
        if price is not None and orig and orig > 0:
            total_pct = ((orig - price) / orig * Decimal(100)).quantize(Decimal("0.01"))
        seller_pct = (
            (total_pct - meli).quantize(Decimal("0.01"))
            if total_pct is not None and meli is not None
            else None
        )
        out.append(
            DecisionOut(
                id=r["id"],
                mlb_id=r["mlb_id"],
                sku=r["sku"] or r["mlb_id"],
                promo_key=r["promo_key"],
                promo_id=r["promotion_id"],
                promo_type=r["promotion_type"],
                promo_name=r["name"],
                decision_kind="already_active",
                target_price=price,
                target_total_pct=total_pct,
                target_seller_pct=seller_pct,
                meli_percentage=meli,
                constraint_used="started",
                list_price=orig,
                cap_pct=r["cap_pct"],
                floor_price=r["floor_price"],
                reason=("Preço dinâmico — o ML define" if is_dynamic else "Ativa no ML"),
                status="ignored",
                promo_start_date=r["start_date"],
                promo_finish_date=r["finish_date"],
                created_at=r["first_seen_at"],
                decided_at=None,
                decided_by=None,
                notes=None,
                is_dynamic=is_dynamic,
            )
        )
    return out


@router.get("/promotions/available", response_model=list[DecisionOut])
async def list_available_promotions(
    session: AsyncSession = Depends(db_session),
) -> list[DecisionOut]:
    """Disponíveis (promoções que o ML OFERECE mas ainda não estão ativas)
    servidas do espelho AS-IS ``ml_promotions`` (Etapa 3). FATO do ML — mostra
    LIGHTNING/DEAL/etc. exatamente como o ML lista, sem o motor de decisão
    esconder/ignorar/inventar. Mesma forma ``DecisionOut`` que a UI já consome.

    ``decision_kind`` direciona a ação na UI: ``ml_managed`` (co-participação,
    o ML define o preço → botão Ativar) vs ``mirror_offer`` (vendedor escolhe o
    preço dentro da faixa → botão Entrar)."""
    rows = (
        (
            await session.execute(
                text(
                    "SELECT p.id, p.mlb_id, p.sku, p.promo_key, p.promotion_id, "
                    "  p.promotion_type, p.name, p.price, p.original_price, p.suggested_price, "
                    "  p.min_price, p.max_price, p.meli_percentage, p.start_date, p.finish_date, "
                    "  p.first_seen_at, p.stock, c.max_seller_share_pct AS cap_pct, "
                    "  c.margin_floor_price AS floor_price "
                    "FROM ml_promotions p "
                    "LEFT JOIN ml_promo_caps c ON c.mlb_id = p.mlb_id "
                    "WHERE p.status <> 'started' "
                    # PRICE_DISCOUNT candidate é só "você PODE criar um desconto" (o
                    # ML retorna pra quase todo anúncio) — não é promoção que o ML
                    # oferece, então não polui Disponíveis. Criar desconto tem fluxo
                    # próprio; PRICE_DISCOUNT já ATIVO (started) aparece em Inscritas.
                    "AND NOT (p.promotion_type = 'PRICE_DISCOUNT' AND p.status = 'candidate') "
                    # Co-participação (SMART/PRICE_MATCHING/MARKETPLACE_CAMPAIGN/BANK)
                    # CANDIDATE = CONVITE perpétuo que o ML estende a quase todo
                    # anúncio: o ML define o preço e auto-gerencia (auto-inicia). Não
                    # é promoção acionável de verdade — clicar "Ativar" dá "Candidate
                    # not valid". Some das Disponíveis; quando ATIVA (started) aparece
                    # nas Inscritas normalmente.
                    "AND NOT (p.promotion_type IN "
                    "  ('SMART','PRICE_MATCHING','MARKETPLACE_CAMPAIGN','BANK') "
                    "  AND p.status = 'candidate') "
                    "ORDER BY p.sku NULLS LAST, p.mlb_id, p.promotion_type"
                )
            )
        )
        .mappings()
        .all()
    )
    out: list[DecisionOut] = []
    for r in rows:
        ptype = r["promotion_type"]
        is_dynamic = ptype in _DYNAMIC_PROMO_TYPES
        orig = r["original_price"]
        meli = r["meli_percentage"]
        # Preço a exibir/inscrever: o ofertado concreto (LIGHTNING traz o preço da
        # relâmpago em ``price``); senão o sugerido do ML (DEAL/PRICE_DISCOUNT vêm
        # com price=0); senão o piso da faixa. Co-participação é só piso (dinâmico).
        target: Decimal | None = next(
            (v for v in (r["price"], r["suggested_price"], r["min_price"]) if v and v > 0),
            None,
        )
        total_pct: Decimal | None = None
        if target is not None and orig and orig > 0:
            total_pct = ((orig - target) / orig * Decimal(100)).quantize(Decimal("0.01"))
        seller_pct = (
            (total_pct - meli).quantize(Decimal("0.01"))
            if total_pct is not None and meli is not None
            else None
        )
        # LIGHTNING/DOD trazem a faixa de unidades em ``stock`` {min,max} — a UI usa
        # pra pedir a quantidade reservada na inscrição.
        st = r["stock"] if isinstance(r["stock"], dict) else {}
        stock_min = st.get("min")
        stock_max = st.get("max")
        out.append(
            DecisionOut(
                id=r["id"],
                mlb_id=r["mlb_id"],
                sku=r["sku"] or r["mlb_id"],
                promo_key=r["promo_key"],
                promo_id=r["promotion_id"],
                promo_type=ptype,
                promo_name=r["name"],
                decision_kind="ml_managed" if is_dynamic else "mirror_offer",
                target_price=target,
                target_total_pct=total_pct,
                target_seller_pct=seller_pct,
                meli_percentage=meli,
                constraint_used="ml_priced" if is_dynamic else "mirror",
                list_price=orig,
                cap_pct=r["cap_pct"],
                floor_price=r["floor_price"],
                min_price=r["min_price"],
                max_price=r["max_price"],
                stock_min=int(stock_min) if stock_min is not None else None,
                stock_max=int(stock_max) if stock_max is not None else None,
                reason=("Preço dinâmico — o ML define" if is_dynamic else "Disponível no ML"),
                status="ignored" if is_dynamic else "pending",
                promo_start_date=r["start_date"],
                promo_finish_date=r["finish_date"],
                created_at=r["first_seen_at"],
                decided_at=None,
                decided_by=None,
                notes=None,
                is_dynamic=is_dynamic,
            )
        )
    return out


class RecCandidateIn(BaseModel):
    mlb_id: str
    sku: str
    promo_type: str
    target_price: float | None = None


class RecOut(BaseModel):
    mlb_id: str
    promo_type: str
    action: str  # enter | skip | neutral
    suggested_total_pct: float | None = None
    confidence: str
    n_neighbors: int
    why: str


@router.post("/recommendations", response_model=list[RecOut])
async def recommendations(
    body: list[RecCandidateIn],
    session: AsyncSession = Depends(db_session),
) -> list[RecOut]:
    """Recomendação por SIMILARIDADE — aprende com as decisões do usuário (Stage 5).
    Recebe os candidatos VISÍVEIS na tela e devolve, pros que têm vizinhos parecidos
    o bastante, "entrar a X% / pular" + confiança + porquê. Tipos sem dados o
    suficiente não voltam (a UI fica calada — sem fingir confiança)."""
    samples = await load_samples(session)
    # Pré-filtro: só monta o contexto (caro) pra tipos com massa mínima.
    from collections import Counter

    from tiny_mirror.services.promo_learning_service import _MIN_NEIGHBORS

    by_type: Counter[str] = Counter(s.promo_type for s in samples)
    learnable = {t for t, n in by_type.items() if n >= _MIN_NEIGHBORS}

    out: list[RecOut] = []
    for c in body[:200]:
        if c.promo_type not in learnable:
            continue
        ctx = await _build_decision_context(
            session,
            mlb_id=c.mlb_id,
            sku=c.sku,
            price_after=c.target_price,
            promo_type=c.promo_type,
        )
        rec = recommend(ctx, c.promo_type, samples)
        if rec is None:
            continue
        out.append(
            RecOut(
                mlb_id=c.mlb_id,
                promo_type=c.promo_type,
                action=rec.action,
                suggested_total_pct=rec.suggested_total_pct,
                confidence=rec.confidence,
                n_neighbors=rec.n_neighbors,
                why=rec.why,
            )
        )
    return out


class NoPromoOut(BaseModel):
    mlb_id: str
    sku: str | None = None
    title: str | None = None
    thumbnail: str | None = None
    logistic_type: str | None = None
    catalog_listing: bool | None = None
    catalog_status: str | None = None
    winner_price: float | None = None
    price_to_win: float | None = None
    current_price: float | None = None


@router.get("/no-decisions", response_model=list[NoPromoOut])
async def list_no_decisions(
    limit: int = Query(default=2000, ge=1, le=5000),
    session: AsyncSession = Depends(db_session),
) -> list[NoPromoOut]:
    """Anúncios ATIVOS ELEGÍVEIS a promoção, mas sem nenhuma rodando nem oferta —
    'livres' pra criar uma promoção de vendedor. Fonte = espelho AS-IS
    ``ml_promotions`` (não mais o motor de decisão).

    Elegibilidade: o anúncio só entra se o ML retornou o PRICE_DISCOUNT candidate
    pra ele (sinal de 'você pode criar um desconto'). Anúncio NÃO elegível não tem
    NENHUMA linha no espelho — não deve aparecer como 'livre'. Ignora cupons (não
    são promoção de preço)."""
    rows = (
        (
            await session.execute(
                text(
                    "SELECT l.mlb_id, l.sku, l.title, l.thumbnail, l.logistic_type, "
                    "c.catalog_listing, c.status AS catalog_status, c.winner_price, "
                    "c.price_to_win, c.current_price "
                    "FROM ml_listings l LEFT JOIN ml_catalog_status c ON c.mlb_id = l.mlb_id "
                    "WHERE l.status = 'active' "
                    # Elegível: tem o PRICE_DISCOUNT candidate que o ML oferece.
                    "AND EXISTS (SELECT 1 FROM ml_promotions p WHERE p.mlb_id = l.mlb_id "
                    "  AND p.promotion_type = 'PRICE_DISCOUNT' AND p.status = 'candidate') "
                    # Sem nenhuma promoção REAL (ativa ou oferta de campanha).
                    "AND NOT EXISTS (SELECT 1 FROM ml_promotions p WHERE p.mlb_id = l.mlb_id "
                    "  AND p.promotion_type <> 'SELLER_COUPON_CAMPAIGN' "
                    "  AND NOT (p.promotion_type = 'PRICE_DISCOUNT' AND p.status = 'candidate')) "
                    "ORDER BY l.sku NULLS LAST, l.mlb_id LIMIT :lim"
                ),
                {"lim": limit},
            )
        )
        .mappings()
        .all()
    )

    def _f(v: Any) -> float | None:
        return float(v) if v is not None else None

    return [
        NoPromoOut(
            mlb_id=r["mlb_id"],
            sku=r["sku"],
            title=r["title"],
            thumbnail=r["thumbnail"],
            logistic_type=r["logistic_type"],
            catalog_listing=r["catalog_listing"],
            catalog_status=r["catalog_status"],
            winner_price=_f(r["winner_price"]),
            price_to_win=_f(r["price_to_win"]),
            current_price=_f(r["current_price"]),
        )
        for r in rows
    ]


async def _apply_target_override(
    repo: MLPromoDecisionRepository,
    decision_id: int,
    override_price: Decimal,
) -> tuple[dict[str, Decimal], str | None]:
    """Validate a target_price override and return the recomputed pct
    fields plus an optional warning string.

    Limites HARD (todos retornam 422):

    - ``cap_pct``: o cap do ML pro share do seller. Viola → erro.
    - ``floor_price``: o nosso piso de margem. Override que coloca o
      preço abaixo do piso é recusado *categoricamente*. Antes era
      SOFT (warning + permite); foi endurecido em 2026-05-29 quando o
      executor Phase 5 entrou em produção. ML POST não pode acontecer
      abaixo do piso, então a única forma segura de bloquear é não
      deixar o operador submeter o valor — o executor sempre lê o
      ``target_price`` da própria linha.
    - ``floor_price IS NULL``: quando a linha não tem piso (custos não
      carregaram no momento da geração) o operador NÃO pode forçar
      preço para baixo — não temos como verificar a margem. Override
      para cima continua permitido (mais conservador = mais margem).
      Pra forçar pra baixo o operador precisa regenerar com custos
      frescos.

    O retorno mantém a assinatura ``(updates, warning)`` por
    compatibilidade mas ``warning`` é sempre ``None`` agora — todo
    risco vira erro.
    """
    row = await repo.get(decision_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"decision {decision_id} not found")
    if row.list_price is None or row.list_price <= 0:
        raise HTTPException(
            status_code=422,
            detail="decision has no list_price snapshot; override not supported",
        )

    if override_price <= 0:
        raise HTTPException(status_code=422, detail="target_price must be > 0")

    list_price = Decimal(row.list_price)

    # Promoção nunca aumenta preço: target tem que ser ≤ list_price.
    # Tolerância de 0.01 pra arredondamento. Acima disso recusa porque
    # significa que o operador está tentando subir o preço (desconto
    # negativo), o que não faz sentido como promo.
    if override_price > list_price + Decimal("0.01"):
        raise HTTPException(
            status_code=422,
            detail=(
                f"target_price R$ {override_price} > preço atual R$ {list_price} "
                f"(promoção não pode aumentar o preço)"
            ),
        )

    meli_pct = Decimal(row.meli_percentage or 0)
    new_total_pct = ((list_price - override_price) / list_price * 100).quantize(Decimal("0.01"))
    new_seller_pct = (new_total_pct - meli_pct).quantize(Decimal("0.01"))

    # CAP + piso de margem são SOFT na aprovação MANUAL (2026-06-05): o operador
    # decide. O cap do canal só será exigido na aprovação AUTOMÁTICA (quando a
    # automação de promoções entrar). Margem negativa não bloqueia aqui — o front
    # exige dupla confirmação. Tudo vira aviso, registrado nas notas pra auditoria.
    soft: list[str] = []
    if row.cap_pct is not None and new_seller_pct > row.cap_pct + Decimal("0.01"):
        soft.append(
            f"seller {new_seller_pct}% > cap ML {row.cap_pct}% (cap ignorado na aprovação manual)"
        )
    if row.floor_price is not None and override_price + Decimal("0.005") < row.floor_price:
        soft.append(
            f"preço R$ {override_price} abaixo do piso R$ {row.floor_price} (margem em risco)"
        )

    return (
        {
            "target_price": override_price.quantize(Decimal("0.01")),
            "target_total_pct": new_total_pct,
            "target_seller_pct": new_seller_pct,
        },
        "; ".join(soft) or None,
    )


@router.post("/decisions/{decision_id}/approve", response_model=DecisionOut)
async def approve_decision(
    decision_id: int,
    body: DecisionDecideIn,
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> DecisionOut:
    # Re-read the apply flag on every call so the operator can toggle
    # ML_PROMO_APPLY_ENABLED without a redeploy. With the flag OFF the
    # apply branch is skipped entirely and the row stays
    # ml_apply_status='skipped' so the audit trail records that the
    # engine deliberately did not contact ML.
    apply_enabled = feature_flags.is_enabled("ml_promo_apply")
    started = time.monotonic()
    _bind_op(
        "approve",
        decision_id=decision_id,
        actor=body.decided_by,
        apply_mode="live" if apply_enabled else "simulation",
    )
    repo = MLPromoDecisionRepository(session)
    override: dict[str, Decimal] = {}
    notes = body.notes
    if body.target_price is not None:
        override, warning = await _apply_target_override(repo, decision_id, body.target_price)
        if warning:
            # Anexa o aviso de piso violado nas notas pra audit. Se o
            # operador já passou um `notes`, preserva no início.
            notes = f"{notes}\n{warning}".strip() if notes else warning
    # Fetch the pending row first to get sku/mlb_id for context snapshot.
    pre = await repo.get(decision_id)
    ctx: dict[str, Any] = {}
    if pre is not None:
        _bind_op(
            "approve",
            mlb_id=pre.mlb_id,
            sku=pre.sku,
            promo_type=pre.promo_type,
            promo_id=pre.promo_id,
        )
        ctx = await _build_decision_context(
            session,
            mlb_id=pre.mlb_id,
            sku=pre.sku,
            price_after=float(body.target_price or pre.target_price or 0) or None,
            list_price=float(pre.list_price or 0) or None,
            floor_price=float(pre.floor_price or 0) or None,
        )
    logger.info(
        "promo_op.start",
        target_price=_fnum(
            body.target_price
            if body.target_price is not None
            else (pre.target_price if pre else None)
        ),
        list_price=_fnum(pre.list_price if pre else None),
        floor_price=_fnum(pre.floor_price if pre else None),
        units=body.units,
        had_override=body.target_price is not None,
    )
    row = await repo.decide(
        decision_id,
        status="approved",
        by=body.decided_by,
        notes=notes,
        stock_chosen=body.units,
        decision_context=ctx or None,
        **override,
    )
    if row is None:
        logger.warning(
            "promo_op.done",
            result="not_found",
            elapsed_ms=round((time.monotonic() - started) * 1000),
        )
        raise HTTPException(
            status_code=404,
            detail=f"decision {decision_id} not found or already decided",
        )
    await session.commit()
    if apply_enabled:
        # Send to ML. Errors are non-fatal: the operator already approved,
        # the DB commit happened, and the failure is recorded on the row
        # for the UI to surface a 'Reenviar' button. We never raise here.
        try:
            apply_result = await service.apply_decision_to_ml(decision=row)
        except Exception as exc:  # pragma: no cover — defensive belt
            logger.exception("promo_apply unexpected error", decision_id=decision_id)
            apply_result = {
                "status": "failed",
                "status_code": None,
                "response": f"unhandled: {exc!s}"[:2000],
            }
        updated = await repo.record_apply_result(
            decision_id,
            status=apply_result["status"],
            status_code=apply_result["status_code"],
            response=apply_result["response"],
        )
        await session.commit()
        if updated is not None:
            row = updated
    else:
        # Flag OFF: stamp 'skipped' so a future flag flip doesn't make
        # this look like a never-attempted approval. Only set on the
        # first transition; idempotent.
        apply_result = {"status": "skipped", "status_code": None, "response": "flag OFF"}
        if row.ml_apply_status is None:
            updated = await repo.record_apply_result(
                decision_id,
                status="skipped",
                status_code=None,
                response="ML_PROMO_APPLY_ENABLED was OFF at approve time",
            )
            await session.commit()
            if updated is not None:
                row = updated
    ml_status = apply_result["status"]
    _done(
        "approved",
        ml_apply_status=ml_status,
        ml_status_code=apply_result["status_code"],
        elapsed_ms=round((time.monotonic() - started) * 1000),
        level="warning" if ml_status == "failed" else "info",
    )
    return DecisionOut.model_validate(row)


@router.post("/decisions/{decision_id}/retry-ml", response_model=DecisionOut)
async def retry_decision_ml(
    decision_id: int,
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> DecisionOut:
    """Re-attempt the ML POST for a previously-approved decision.

    Only acts when the operator status is already 'approved' — we
    never resurrect a rejected/ignored/expired row through this lane.
    Honours the flag: if ML_PROMO_APPLY_ENABLED is OFF the call is
    refused with 409 so the operator doesn't think they fired
    something when they didn't.
    """
    started = time.monotonic()
    _bind_op("retry_ml", decision_id=decision_id, apply_mode="live")
    if not feature_flags.is_enabled("ml_promo_apply"):
        _done("refused_flag_off", level="warning")
        raise HTTPException(
            status_code=409,
            detail="ML_PROMO_APPLY_ENABLED is OFF; nothing was sent",
        )
    repo = MLPromoDecisionRepository(session)
    row = await repo.get(decision_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"decision {decision_id} not found")
    if row.status != "approved":
        _done("refused_wrong_status", level="warning", row_status=row.status)
        raise HTTPException(
            status_code=409,
            detail=f"decision is {row.status}, only 'approved' rows can be retried",
        )
    _bind_op(
        "retry_ml", mlb_id=row.mlb_id, sku=row.sku, promo_type=row.promo_type, promo_id=row.promo_id
    )
    logger.info("promo_op.start", target_price=_fnum(row.target_price))
    try:
        apply_result = await service.apply_decision_to_ml(decision=row)
    except Exception as exc:  # pragma: no cover
        logger.exception("promo_apply retry unexpected error", decision_id=decision_id)
        apply_result = {
            "status": "failed",
            "status_code": None,
            "response": f"unhandled: {exc!s}"[:2000],
        }
    updated = await repo.record_apply_result(
        decision_id,
        status=apply_result["status"],
        status_code=apply_result["status_code"],
        response=apply_result["response"],
    )
    await session.commit()
    _done(
        "retried",
        ml_apply_status=apply_result["status"],
        ml_status_code=apply_result["status_code"],
        elapsed_ms=round((time.monotonic() - started) * 1000),
        level="warning" if apply_result["status"] == "failed" else "info",
    )
    return DecisionOut.model_validate(updated or row)


@router.post("/decisions/{decision_id}/reject", response_model=DecisionOut)
async def reject_decision(
    decision_id: int,
    body: DecisionDecideIn,
    session: AsyncSession = Depends(db_session),
) -> DecisionOut:
    _bind_op("reject", decision_id=decision_id, actor=body.decided_by)
    repo = MLPromoDecisionRepository(session)
    pre = await repo.get(decision_id)
    ctx: dict[str, Any] = {}
    if pre is not None:
        _bind_op(
            "reject",
            mlb_id=pre.mlb_id,
            sku=pre.sku,
            promo_type=pre.promo_type,
            promo_id=pre.promo_id,
        )
        ctx = await _build_decision_context(
            session,
            mlb_id=pre.mlb_id,
            sku=pre.sku,
            price_after=float(pre.target_price or 0) or None,
            list_price=float(pre.list_price or 0) or None,
            floor_price=float(pre.floor_price or 0) or None,
        )
    logger.info("promo_op.start")
    row = await repo.decide(
        decision_id,
        status="rejected",
        by=body.decided_by,
        notes=body.notes,
        decision_context=ctx or None,
    )
    if row is None:
        _done("not_found", level="warning")
        raise HTTPException(
            status_code=404,
            detail=f"decision {decision_id} not found or already decided",
        )
    await session.commit()
    _done("rejected")
    return DecisionOut.model_validate(row)


@router.post("/decisions/{decision_id}/ignore", response_model=DecisionOut)
async def ignore_decision(
    decision_id: int,
    body: DecisionDecideIn,
    session: AsyncSession = Depends(db_session),
) -> DecisionOut:
    """Skip without committing yes/no. Same dedupe semantics as reject —
    the row stays out of the pending queue and the cron will not
    re-prompt for it."""
    _bind_op("ignore", decision_id=decision_id, actor=body.decided_by)
    repo = MLPromoDecisionRepository(session)
    pre = await repo.get(decision_id)
    ctx: dict[str, Any] = {}
    if pre is not None:
        _bind_op(
            "ignore",
            mlb_id=pre.mlb_id,
            sku=pre.sku,
            promo_type=pre.promo_type,
            promo_id=pre.promo_id,
        )
        ctx = await _build_decision_context(
            session,
            mlb_id=pre.mlb_id,
            sku=pre.sku,
            price_after=float(pre.target_price or 0) or None,
            list_price=float(pre.list_price or 0) or None,
            floor_price=float(pre.floor_price or 0) or None,
        )
    logger.info("promo_op.start")
    row = await repo.decide(
        decision_id,
        status="ignored",
        by=body.decided_by,
        notes=body.notes,
        decision_context=ctx or None,
    )
    if row is None:
        _done("not_found", level="warning")
        raise HTTPException(
            status_code=404,
            detail=f"decision {decision_id} not found or already decided",
        )
    await session.commit()
    _done("ignored")
    return DecisionOut.model_validate(row)


@router.post("/decisions/{decision_id}/undo", response_model=DecisionOut)
async def undo_decision(
    decision_id: int,
    session: AsyncSession = Depends(db_session),
) -> DecisionOut:
    """Reverter uma decisão terminal (approved/rejected/ignored) de
    volta para pending. Mantém ``decided_at`` / ``decided_by`` como
    audit da última ação — o operador volta a ter o item na fila
    pendente e pode revisar/aprovar/rejeitar de novo.
    """
    _bind_op("undo", decision_id=decision_id)
    repo = MLPromoDecisionRepository(session)
    row = await repo.revert_to_pending(decision_id)
    if row is None:
        _done("not_found", level="warning")
        raise HTTPException(
            status_code=404,
            detail=f"decision {decision_id} not found or already pending",
        )
    _bind_op(
        "undo", mlb_id=row.mlb_id, sku=row.sku, promo_type=row.promo_type, promo_id=row.promo_id
    )
    await session.commit()
    _done("reverted", prev_status=row.status)
    return DecisionOut.model_validate(row)


class CreatePriceDiscountIn(BaseModel):
    mlb_id: str
    deal_price: Decimal = Field(gt=0)
    decided_by: str | None = None
    # Como a ação foi disparada (ex.: 'single' | 'bulk') — capturado no contexto
    # de automação pra distinguir criação individual de criação em massa.
    source: str | None = None
    # Vigência do desconto — formato LOCAL "YYYY-MM-DDTHH:MM:SS" (o ML usa só a
    # data). Opcional: o serviço usa hoje → +30d quando ausente.
    start_date: str | None = None
    finish_date: str | None = None


class SellerCampaignOut(BaseModel):
    """Campanha em que o vendedor pode inscrever anúncios em massa."""

    id: str
    type: str  # SELLER_CAMPAIGN | DEAL
    sub_type: str | None = None
    name: str | None = None
    status: str | None = None
    start_date: str | None = None
    finish_date: str | None = None


class CampaignCandidateOut(BaseModel):
    """Anúncio ELEGÍVEL a entrar numa campanha + faixa de preço permitida."""

    mlb_id: str
    sku: str | None = None
    title: str | None = None
    thumbnail: str | None = None
    logistic_type: str | None = None
    warehouse_available: int | None = None
    full_available: int | None = None
    full_in_transit: int | None = None
    original_price: float | None = None
    min_price: float | None = None  # menor preço permitido (maior desconto)
    max_price: float | None = None  # maior preço permitido (menor desconto)
    suggested_price: float | None = None


class EnrollCampaignItemIn(BaseModel):
    mlb_id: str
    promotion_id: str
    promotion_type: str  # SELLER_CAMPAIGN | DEAL
    deal_price: Decimal = Field(gt=0)
    decided_by: str | None = None
    source: str | None = None  # 'single' | 'bulk'


@router.post("/create-price-discount")
async def create_price_discount_direct(
    body: CreatePriceDiscountIn,
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    """Cria promoção PRICE_DISCOUNT diretamente no ML para um MLB.

    Aprovação MANUAL: o piso de margem (CAP) NÃO bloqueia (2026-06-05) — o
    operador decide, e o front exige dupla confirmação quando a margem fica
    negativa. O CAP só será exigido na aprovação automática.
    """
    from tiny_mirror.infrastructure.orm.models import MLListingORM, MLPromoCapORM

    started = time.monotonic()
    cap = await session.get(MLPromoCapORM, body.mlb_id)
    _bind_op(
        "create_price_discount",
        mlb_id=body.mlb_id,
        sku=cap.sku if cap else None,
        promo_type="PRICE_DISCOUNT",
        actor=body.decided_by,
        apply_mode="live",
    )
    logger.info(
        "promo_op.start",
        deal_price=_fnum(body.deal_price),
        floor_price=_fnum(cap.margin_floor_price if cap else None),
    )
    # Anúncio pausado/encerrado não pode receber promoção: ela não é exibida a
    # ninguém e o generate (que alimenta "Inscritas") nem varre anúncio inativo,
    # então a promo ficaria órfã. Bloqueia aqui — é a fonte de verdade.
    listing = await session.get(MLListingORM, body.mlb_id)
    if listing is None or listing.status != "active":
        _done(
            "refused_inactive_listing",
            level="warning",
            listing_status=listing.status if listing else None,
        )
        raise HTTPException(
            status_code=422,
            detail=(
                "O anúncio não está ativo no Mercado Livre "
                f"({listing.status if listing else 'não encontrado'}). "
                "Promoção só pode ser criada em anúncio ativo — reative o anúncio primeiro."
            ),
        )
    result = await service.create_price_discount(
        mlb_id=body.mlb_id,
        deal_price=float(body.deal_price),
        start_date=body.start_date,
        finish_date=body.finish_date,
    )
    sc = result.get("status_code")
    if sc is not None and sc >= 400:
        _done(
            "ml_rejected",
            level="warning",
            ml_status_code=sc,
            elapsed_ms=round((time.monotonic() - started) * 1000),
        )
        raise HTTPException(status_code=sc if sc < 600 else 502, detail=result.get("response"))

    # Log para auditoria e futura automação
    sku = cap.sku if cap else body.mlb_id
    ctx = await _build_decision_context(
        session,
        mlb_id=body.mlb_id,
        sku=sku,
        price_after=float(body.deal_price),
        floor_price=float(cap.margin_floor_price) if cap and cap.margin_floor_price else None,
        promo_type="PRICE_DISCOUNT",
        source=body.source or "single",
    )
    action_repo = MLPromoActionRepository(session)
    await action_repo.log(
        sku=sku,
        mlb_id=body.mlb_id,
        action="direct_create_price_discount",
        promo_type="PRICE_DISCOUNT",
        price_after=body.deal_price,
        reason="criação manual de PRICE_DISCOUNT pelo operador",
        ml_response=result,
        dry_run=False,
        decided_by=body.decided_by,
        context=ctx,
    )
    # Grava a linha 'started' no espelho na hora pra a promoção aparecer em
    # "Inscritas" já — o generate diário pula anúncios pausados e só roda 1x/dia,
    # então sem isso a promoção recém-criada ficava invisível (e indeletável).
    resp = result.get("response") if isinstance(result.get("response"), dict) else {}
    list_price = _to_dec((resp or {}).get("original_price"))
    if list_price is None:
        snap = await MLCostsSnapshotRepository(session).get(body.mlb_id)
        list_price = snap.list_price if snap else None
    await MLPromoDecisionRepository(session).upsert_started(
        mlb_id=body.mlb_id,
        sku=sku,
        promo_type="PRICE_DISCOUNT",
        target_price=body.deal_price,
        list_price=list_price,
        cap_pct=cap.max_seller_share_pct if cap else None,
        floor_price=cap.margin_floor_price if cap else None,
        promo_start_date=_parse_iso_dt(body.start_date),
        promo_finish_date=_parse_iso_dt(body.finish_date),
        reason="PRICE_DISCOUNT criada manualmente pelo operador",
    )
    await session.commit()
    _done("created", ml_status_code=sc, elapsed_ms=round((time.monotonic() - started) * 1000))
    return result


@router.get("/seller-campaigns", response_model=list[SellerCampaignOut])
async def list_seller_campaigns(
    service: MLPromotionService = Depends(_service_dep),
) -> list[SellerCampaignOut]:
    """Campanhas entráveis (SELLER_CAMPAIGN + DEAL) pra inscrição em massa."""
    raw = await service.list_seller_campaigns()
    return [
        SellerCampaignOut(
            id=c["id"],
            type=c.get("type", ""),
            sub_type=c.get("sub_type"),
            name=c.get("name"),
            status=c.get("status"),
            start_date=c.get("start_date"),
            finish_date=c.get("finish_date"),
        )
        for c in raw
    ]


@router.get(
    "/seller-campaigns/{promotion_id}/candidates",
    response_model=list[CampaignCandidateOut],
)
async def list_campaign_candidates(
    promotion_id: str,
    promotion_type: str = Query(...),
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> list[CampaignCandidateOut]:
    """Anúncios elegíveis a entrar numa campanha, com a faixa de preço por item.
    Enriquece com sku/título/imagem do espelho pra busca + UI."""
    cands = await service.list_campaign_candidates(promotion_id, promotion_type)
    mlb_ids = [c["id"] for c in cands if c.get("id")]
    meta: dict[str, Any] = {}
    stock_by_sku: dict[str, dict[str, int]] = {}
    if mlb_ids:
        rows = (
            await session.execute(
                text(
                    "SELECT mlb_id, sku, title, thumbnail, logistic_type, available_quantity "
                    "FROM ml_listings WHERE mlb_id = ANY(:ids)"
                ),
                {"ids": mlb_ids},
            )
        ).mappings()
        meta = {r["mlb_id"]: r for r in rows}
        stock_by_sku = await _stock_by_skus(session, [m["sku"] for m in meta.values() if m["sku"]])

    def _f(v: Any) -> float | None:
        return float(v) if v is not None else None

    out: list[CampaignCandidateOut] = []
    for c in cands:
        mlb = c.get("id")
        if not mlb:
            continue
        lo_raw = _f(c.get("min_discounted_price"))
        hi_raw = _f(c.get("max_discounted_price"))
        # ML pode nomear min/max ao contrário por tipo — normaliza pra lo<=hi.
        lo = hi = None
        if lo_raw is not None and hi_raw is not None:
            lo, hi = min(lo_raw, hi_raw), max(lo_raw, hi_raw)
        elif lo_raw is not None:
            lo = hi = lo_raw
        elif hi_raw is not None:
            lo = hi = hi_raw
        m = meta.get(mlb) or {}
        m_sku = m.get("sku")
        st = stock_by_sku.get(m_sku) if m_sku else None
        out.append(
            CampaignCandidateOut(
                mlb_id=mlb,
                sku=m.get("sku"),
                title=m.get("title"),
                thumbnail=m.get("thumbnail"),
                logistic_type=m.get("logistic_type"),
                warehouse_available=st["warehouse_available"] if st else None,
                # FULL efetivo = available do anúncio + in_transfer interno do ML.
                full_available=(m.get("available_quantity") or 0)
                + (st["full_in_transfer"] if st else 0),
                full_in_transit=st["pending_full"] if st else None,
                original_price=_f(c.get("original_price")),
                min_price=lo,
                max_price=hi,
                suggested_price=_f(c.get("suggested_discounted_price")),
            )
        )
    return out


# Mantém referência forte às migrações em voo (asyncio.create_task pode ser
# coletado pelo GC se ninguém guardar a Task).
_migration_tasks: set[asyncio.Task[None]] = set()


async def _run_campaign_migration(
    op_id: str,
    target: str,
    items: list[tuple[str, float]],
    service: MLPromotionService,
) -> None:
    """Inscreve os anúncios candidatos na campanha destino — DIRETO e CONCORRENTE
    (sem fila throttled). Cada inscrição é um POST /seller-promotions/items/{MLB}.
    Progresso em Redis ``migrate:{op_id}`` (campos total/done/failed/status)."""
    redis = get_redis()
    key = f"migrate:{op_id}"
    # Concorrência menor reduz o "No candidates found for item" — erro TRANSITÓRIO do
    # ML (lista o item como candidato mas, sob carga/concorrência ou antes de assentar,
    # não acha o candidato no enroll). NÃO é preço (item em-faixa também falhava).
    sem = asyncio.Semaphore(4)

    async def _one(mlb_id: str, price: float) -> None:
        async with sem:
            ok = False
            detail: str | None = None
            for attempt in range(3):  # retry: deixa o ML assentar o candidato
                try:
                    r = await service.modify_promotion(
                        mlb_id=mlb_id,
                        deal_price=price,
                        promotion_id=target,
                        promotion_type="SELLER_CAMPAIGN",
                    )
                    sc = r.get("status_code")
                    ok = sc is None or int(sc) < 400
                    detail = None if ok else str(r.get("response"))[:200]
                except Exception as exc:  # uma falha não derruba a migração
                    ok = False
                    detail = str(exc)[:200]
                if ok:
                    break
                await asyncio.sleep(1.5 * (attempt + 1))  # backoff
            if not ok:
                logger.warning("promo.migrate_item_failed", mlb_id=mlb_id, detail=detail)
            if ok:
                # Marca a linha do espelho como started+enrolled_at (igual ao enroll
                # único): o ML deixa o anúncio 'pending' (campanha futura), e a UI trata
                # 'pending' como Disponíveis ("Entrar"). Sem isto, o anúncio migrado
                # continuava aparecendo como "Entrar". enrolled_at protege do sync.
                try:
                    async with AsyncSessionLocal() as s:
                        await s.execute(
                            text(
                                "UPDATE ml_promotions SET status='started', enrolled_at=now(), "
                                "price=:price, updated_at=now() "
                                "WHERE mlb_id=:m AND promotion_id=:t"
                            ),
                            {"price": price, "m": mlb_id, "t": target},
                        )
                        await s.commit()
                except Exception as exc:  # falha no espelho não invalida o enroll no ML
                    logger.warning("promo.migrate_mirror_failed", mlb_id=mlb_id, error=str(exc))
            await redis.hincrby(key, "done" if ok else "failed", 1)  # type: ignore[misc]

    try:
        await asyncio.gather(*[_one(m, p) for m, p in items])
    finally:
        await redis.hset(key, "status", "done")  # type: ignore[misc]
        await redis.expire(key, 7200)
    logger.info("promo.migrate_campaign_done", op_id=op_id, target=target, count=len(items))


class MigrateCampaignIn(BaseModel):
    source_promotion_id: str
    target_promotion_id: str
    dry_run: bool = True
    decided_by: str | None = None


@router.post("/promotions/migrate-campaign")
async def migrate_campaign(
    body: MigrateCampaignIn,
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    """Migra os anúncios de uma campanha SELLER (origem) para outra (destino) NO
    MESMO PREÇO. NÃO cria campanha — o ML só deixa criar SELLER_CAMPAIGN no painel.

    Migrável = anúncio que é CANDIDATO do destino E tem preço inscrito na origem
    (espelho). Os que já estão pending/started no destino contam como FEITOS. Inscreve
    DIRETO e CONCORRENTE (sem fila throttled): cada um é um POST imediato. Progresso em
    ``migrate:{op_id}`` no Redis — faça poll em GET migrate-campaign/{migration_id}.

    dry_run=true (padrão) só conta {total, done, to_migrate}; dry_run=false inscreve."""
    rows = (
        await session.execute(
            text(
                "SELECT mlb_id, price FROM ml_promotions WHERE promotion_id = :src "
                "AND status = 'started' AND price IS NOT NULL AND price > 0"
            ),
            {"src": body.source_promotion_id},
        )
    ).all()
    src_price = {r[0]: float(r[1]) for r in rows}

    # Destino: candidatos (a inscrever) + pending (já feitos). Paginação por cursor.
    cand = await service.list_campaign_candidates(
        body.target_promotion_id, "SELLER_CAMPAIGN", status="candidate"
    )
    pend = await service.list_campaign_candidates(
        body.target_promotion_id, "SELLER_CAMPAIGN", status="pending"
    )
    cand_ids = {str(c["id"]) for c in cand if c.get("id")}
    pend_ids = {str(p["id"]) for p in pend if p.get("id")}
    to_migrate_all = [(m, src_price[m]) for m in cand_ids if m in src_price]

    # Bloqueia quem está numa promoção SMART ATIVA (co-participação do ML): o ML não
    # deixa empilhar uma SELLER_CAMPAIGN por cima e devolve "No candidates found for
    # item" no enroll (verificado — os únicos que recusam têm SMART started). Voltam a
    # ser migráveis quando o SMART acabar. Espelho tem o dado, então é só uma query.
    blocked: set[str] = set()
    if to_migrate_all:
        res = await session.execute(
            text(
                "SELECT DISTINCT mlb_id FROM ml_promotions "
                "WHERE mlb_id = ANY(:m) AND promotion_type = 'SMART' AND status = 'started'"
            ),
            {"m": [m for m, _ in to_migrate_all]},
        )
        blocked = {row[0] for row in res}
    to_migrate = [(m, p) for m, p in to_migrate_all if m not in blocked]

    already_ids = pend_ids & set(src_price)
    already = len(already_ids)
    total = already + len(to_migrate)

    if body.dry_run:
        return {
            "dry_run": True,
            "total": total,
            "done": already,
            "to_migrate": len(to_migrate),
            "blocked_smart": len(blocked),
            "no_source_price": len(cand_ids - set(src_price)),
            "source": body.source_promotion_id,
            "target": body.target_promotion_id,
        }

    # Os que JÁ estão inscritos no destino aparecem 'pending' no ML → a UI os mostra
    # como "Entrar". Reconcilia o espelho deles para started+enrolled_at agora, pra
    # saírem de Disponíveis e irem pra Inscritas (os novos são tratados no bg task).
    if already_ids:
        await session.execute(
            text(
                "UPDATE ml_promotions SET status='started', enrolled_at=now(), updated_at=now() "
                "WHERE promotion_id=:t AND mlb_id = ANY(:ids) AND status <> 'started'"
            ),
            {"t": body.target_promotion_id, "ids": list(already_ids)},
        )
        await session.commit()

    op_id = structlog.contextvars.get_contextvars().get("op_id") or uuid.uuid4().hex[:12]
    redis = get_redis()
    key = f"migrate:{op_id}"
    await redis.hset(  # type: ignore[misc]
        key,
        mapping={
            "total": total,
            "done": already,
            "failed": 0,
            "status": "running",
            "target": body.target_promotion_id,
        },
    )
    await redis.expire(key, 7200)
    task = asyncio.create_task(
        _run_campaign_migration(op_id, body.target_promotion_id, to_migrate, service)
    )
    _migration_tasks.add(task)
    task.add_done_callback(_migration_tasks.discard)
    logger.info(
        "promo.migrate_campaign_started",
        source=body.source_promotion_id,
        target=body.target_promotion_id,
        total=total,
        already=already,
        to_migrate=len(to_migrate),
        decided_by=body.decided_by,
        op_id=op_id,
    )
    return {
        "dry_run": False,
        "migration_id": op_id,
        "total": total,
        "done": already,
        "to_migrate": len(to_migrate),
    }


@router.get("/promotions/migrate-campaign/{op_id}")
async def migrate_campaign_status(op_id: str) -> dict[str, Any]:
    """Progresso de uma migração em voo (Redis ``migrate:{op_id}``)."""
    data = await get_redis().hgetall(f"migrate:{op_id}")  # type: ignore[misc]
    if not data:
        return {"found": False}
    total = int(data.get("total", 0) or 0)
    done = int(data.get("done", 0) or 0)
    return {
        "found": True,
        "total": total,
        "done": done,
        "failed": int(data.get("failed", 0) or 0),
        "status": data.get("status", "running"),
        "pct": round(100 * done / total) if total else 100,
    }


@router.post("/enroll-campaign-item")
async def enroll_campaign_item(
    body: EnrollCampaignItemIn,
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    """Inscreve UM anúncio numa campanha (SELLER_CAMPAIGN/DEAL) a um preço. O
    front chama em loop (com pacing) pra inscrição em massa — reusa o mesmo
    funil de _ml_write (com retry de rate-limit) do create-price-discount."""
    from tiny_mirror.infrastructure.orm.models import MLListingORM, MLPromoCapORM

    started = time.monotonic()
    cap = await session.get(MLPromoCapORM, body.mlb_id)
    sku = cap.sku if cap else body.mlb_id
    _bind_op(
        "enroll_campaign_item",
        mlb_id=body.mlb_id,
        sku=sku,
        promo_type=body.promotion_type,
        actor=body.decided_by,
        apply_mode="live",
    )
    logger.info("promo_op.start", deal_price=_fnum(body.deal_price), promotion_id=body.promotion_id)

    listing = await session.get(MLListingORM, body.mlb_id)
    if listing is None or listing.status != "active":
        _done(
            "refused_inactive_listing",
            level="warning",
            listing_status=listing.status if listing else None,
        )
        raise HTTPException(
            status_code=422,
            detail=(
                f"O anúncio não está ativo no Mercado Livre "
                f"({listing.status if listing else 'não encontrado'}). "
                "Inscrição só em anúncio ativo."
            ),
        )

    result = await service.modify_promotion(
        mlb_id=body.mlb_id,
        deal_price=float(body.deal_price),
        promotion_id=body.promotion_id,
        promotion_type=body.promotion_type,
    )
    sc = result.get("status_code")
    if sc is not None and sc >= 400:
        _done(
            "ml_rejected",
            level="warning",
            ml_status_code=sc,
            elapsed_ms=round((time.monotonic() - started) * 1000),
        )
        raise HTTPException(status_code=sc if sc < 600 else 502, detail=result.get("response"))

    ctx = await _build_decision_context(
        session,
        mlb_id=body.mlb_id,
        sku=sku,
        price_after=float(body.deal_price),
        floor_price=float(cap.margin_floor_price) if cap and cap.margin_floor_price else None,
        promo_type=body.promotion_type,
        source=body.source or "bulk",
    )
    await MLPromoActionRepository(session).log(
        sku=sku,
        mlb_id=body.mlb_id,
        action="enroll_campaign_item",
        promo_type=body.promotion_type,
        price_after=body.deal_price,
        reason=f"inscrição na campanha {body.promotion_id} pelo operador",
        ml_response=result,
        dry_run=False,
        decided_by=body.decided_by,
        context=ctx,
    )
    resp = result.get("response") if isinstance(result.get("response"), dict) else {}
    list_price = _to_dec((resp or {}).get("original_price"))
    if list_price is None:
        snap = await MLCostsSnapshotRepository(session).get(body.mlb_id)
        list_price = snap.list_price if snap else None
    await MLPromoDecisionRepository(session).upsert_started(
        mlb_id=body.mlb_id,
        sku=sku,
        promo_type=body.promotion_type,
        target_price=body.deal_price,
        promo_id=body.promotion_id,
        promo_key=body.promotion_id,
        list_price=list_price,
        cap_pct=cap.max_seller_share_pct if cap else None,
        floor_price=cap.margin_floor_price if cap else None,
        reason=f"inscrito na campanha {body.promotion_id} pelo operador",
    )
    await session.commit()
    _done("enrolled", ml_status_code=sc, elapsed_ms=round((time.monotonic() - started) * 1000))
    return result


class EnrollOfferGenericIn(BaseModel):
    mlb_id: str
    promo_type: str  # DEAL | SELLER_CAMPAIGN | DOD | LIGHTNING | PRICE_DISCOUNT
    promo_id: str | None = None  # ausente p/ DOD e p/ criar PRICE_DISCOUNT
    deal_price: Decimal = Field(gt=0)
    stock: int | None = None  # OBRIGATÓRIO p/ LIGHTNING (unidades reservadas)
    decided_by: str | None = None
    source: str | None = None


@router.post("/promotions/enroll")
async def enroll_offer_generic(
    body: EnrollOfferGenericIn,
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    """Entrar numa oferta do espelho (Disponíveis) — genérico por tipo. Reusa o
    executor ``apply_decision_to_ml`` (mesmos corpos JÁ validados do approve do
    motor), então cobre os tipos que o ``enroll-campaign-item`` não cobria:
    DOD (sem promotion_id) e LIGHTNING (com ``stock`` obrigatório), além de
    DEAL/SELLER_CAMPAIGN/PRICE_DISCOUNT. Escreve DIRETO no ML."""
    from types import SimpleNamespace

    from tiny_mirror.infrastructure.orm.models import MLListingORM, MLPromoCapORM

    started = time.monotonic()
    pt = body.promo_type.upper()
    cap = await session.get(MLPromoCapORM, body.mlb_id)
    sku = cap.sku if cap else body.mlb_id
    _bind_op(
        "enroll_offer",
        mlb_id=body.mlb_id,
        sku=sku,
        promo_type=pt,
        promo_id=body.promo_id,
        actor=body.decided_by,
        apply_mode="live",
    )
    logger.info("promo_op.start", deal_price=_fnum(body.deal_price), stock=body.stock)

    if pt == "LIGHTNING" and not body.stock:
        _done("missing_stock", level="warning")
        raise HTTPException(status_code=422, detail="LIGHTNING exige 'stock' (unidades).")

    listing = await session.get(MLListingORM, body.mlb_id)
    if listing is None or listing.status != "active":
        _done("refused_inactive_listing", level="warning")
        raise HTTPException(
            status_code=422,
            detail=(
                f"O anúncio não está ativo no Mercado Livre "
                f"({listing.status if listing else 'não encontrado'})."
            ),
        )

    decision_kind = (
        "create_price_discount"
        if (pt == "PRICE_DISCOUNT" and not body.promo_id)
        else "would_activate"
    )
    ns = SimpleNamespace(
        promo_type=pt,
        decision_kind=decision_kind,
        mlb_id=body.mlb_id,
        sku=sku,
        target_price=body.deal_price,
        promo_id=body.promo_id,
        stock_chosen=body.stock,
        floor_price=None,
        list_price=None,
        meli_percentage=None,
        target_total_pct=None,
    )
    result = await service.apply_decision_to_ml(decision=ns)
    status = result.get("status")
    sc = result.get("status_code")
    if status == "skipped":
        _done("unsupported_type", level="warning")
        raise HTTPException(status_code=422, detail=result.get("response"))
    if status != "ok":
        _done("ml_rejected", level="warning", ml_status_code=sc)
        raise HTTPException(
            status_code=sc if sc and sc < 600 else 502, detail=result.get("response")
        )

    # Snapshot de features (estoque/catálogo/vendas/margem) → vira exemplo de
    # treino do recomendador. Best-effort.
    ctx = await _build_decision_context(
        session,
        mlb_id=body.mlb_id,
        sku=sku,
        price_after=float(body.deal_price),
        promo_type=pt,
        source="promo_enter",
    )
    await MLPromoActionRepository(session).log(
        sku=sku,
        mlb_id=body.mlb_id,
        action="enroll_offer",
        promo_type=pt,
        price_after=body.deal_price,
        reason=f"entrou na promoção {pt} ({body.promo_id or 'sem id'}) pelo operador",
        ml_response=result,
        dry_run=False,
        decided_by=body.decided_by,
        context=ctx,
    )
    await MLPromoDecisionRepository(session).upsert_started(
        mlb_id=body.mlb_id,
        sku=sku,
        promo_type=pt,
        target_price=body.deal_price,
        promo_id=body.promo_id,
        promo_key=body.promo_id or pt,
        list_price=None,
        cap_pct=cap.max_seller_share_pct if cap else None,
        floor_price=cap.margin_floor_price if cap else None,
        reason="inscrito pelo operador",
    )
    # Commita o IMPORTANTE (log + started) ANTES do sync — assim um sync que falhe
    # não desfaz a auditoria nem transforma um enroll bem-sucedido em 500.
    await session.commit()
    # AUTORIDADE: o ML deu 201, então ESTAMOS inscritos — e o NOSSO banco manda
    # nisso, não o eligible do ML (que demora/flapa e segue dizendo 'candidate').
    if body.promo_id:
        # Caso comum (a promo já existia como candidate no espelho): só marca a
        # linha como started + PREÇO + enrolled_at. NÃO re-sincroniza o anúncio
        # inteiro — era isso que rebaixava promoções VIZINHAS quando o eligible
        # flapava e fazia TODAS as ativas SUMIREM da tela. Fixar o preço aqui evita
        # o "R$ 0,00" (DEAL vem com price=0 no eligible). O sweep horário reconcilia
        # o resto; enrolled_at protege esta linha de ser rebaixada/apagada.
        try:
            params: dict[str, Any] = {"m": body.mlb_id, "p": body.promo_id}
            set_price = ""
            if body.deal_price is not None:
                set_price = ", price=:price"
                params["price"] = body.deal_price
            await session.execute(
                text(
                    "UPDATE ml_promotions SET status='started', enrolled_at=now(), "
                    f"updated_at=now(){set_price} WHERE mlb_id=:m AND promotion_id=:p"
                ),
                params,
            )
            await session.commit()
            logger.info("enroll_offer.marked_started", mlb_id=body.mlb_id, promo_id=body.promo_id)
        except Exception as exc:  # pragma: no cover — rede
            try:
                await session.rollback()
            except Exception:
                pass
            logger.warning("enroll_offer.mark_started_failed", mlb_id=body.mlb_id, error=str(exc))
    else:
        # PRICE_DISCOUNT criado agora (sem promotion_id) → não há linha pra marcar;
        # precisa do sync pra materializar a nova promo no espelho.
        import asyncio

        for attempt in range(3):
            try:
                await PromotionMirrorService(service).sync_mlb(session, body.mlb_id, sku)
                break
            except Exception as exc:  # pragma: no cover — rede
                try:
                    await session.rollback()
                except Exception:
                    pass
                logger.warning(
                    "enroll_offer.mirror_sync_failed",
                    mlb_id=body.mlb_id,
                    attempt=attempt + 1,
                    error=str(exc),
                )
                if attempt < 2:
                    await asyncio.sleep(1.0)
    _done("enrolled", ml_status_code=sc, elapsed_ms=round((time.monotonic() - started) * 1000))
    return result


class RepriceIn(BaseModel):
    mlb_id: str
    new_price: Decimal = Field(gt=0)
    decided_by: str | None = None


@router.post("/reprice")
async def reprice_listing(
    body: RepriceIn,
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    """Sai da promoção atual e cria uma nova PRICE_DISCOUNT ao new_price.

    Fluxo atômico do ponto de vista do operador:
    1. DELETE /seller-promotions/items/{mlb_id} — remove promo ativa
    2. POST  /seller-promotions/items/{mlb_id} — entra com novo preço

    Aprovação MANUAL: o piso (CAP) NÃO bloqueia (2026-06-05) — decisão do
    operador, com dupla confirmação no front p/ margem negativa. Devolve ambos
    os resultados para o frontend mostrar o diagnóstico se algo falhar.
    """
    from tiny_mirror.infrastructure.orm.models import MLPromoCapORM

    started = time.monotonic()
    cap = await session.get(MLPromoCapORM, body.mlb_id)
    _bind_op(
        "reprice",
        mlb_id=body.mlb_id,
        sku=cap.sku if cap else None,
        promo_type="PRICE_DISCOUNT",
        actor=body.decided_by,
        apply_mode="live",
    )
    logger.info(
        "promo_op.start",
        new_price=_fnum(body.new_price),
        floor_price=_fnum(cap.margin_floor_price if cap else None),
    )

    exit_result = await service.exit_promotion(mlb_id=body.mlb_id)
    exit_sc = exit_result.get("status_code")
    # Toleramos 404 (não estava em promo) e 200/204 como sucesso.
    if exit_sc is not None and exit_sc >= 400 and exit_sc != 404:
        _done(
            "ml_rejected",
            level="warning",
            step="exit",
            ml_status_code=exit_sc,
            elapsed_ms=round((time.monotonic() - started) * 1000),
        )
        raise HTTPException(
            status_code=exit_sc if exit_sc < 600 else 502,
            detail={"step": "exit", "response": exit_result.get("response")},
        )

    enter_result = await service.create_price_discount(
        mlb_id=body.mlb_id, deal_price=float(body.new_price)
    )
    enter_sc = enter_result.get("status_code")
    if enter_sc is not None and enter_sc >= 400:
        _done(
            "ml_rejected",
            level="warning",
            step="enter",
            exit_ok=True,
            ml_status_code=enter_sc,
            elapsed_ms=round((time.monotonic() - started) * 1000),
        )
        raise HTTPException(
            status_code=enter_sc if enter_sc < 600 else 502,
            detail={"step": "enter", "exit_ok": True, "response": enter_result.get("response")},
        )

    # Log para auditoria e futura automação
    sku = cap.sku if cap else body.mlb_id
    ctx = await _build_decision_context(
        session,
        mlb_id=body.mlb_id,
        sku=sku,
        price_after=float(body.new_price),
        floor_price=float(cap.margin_floor_price) if cap and cap.margin_floor_price else None,
    )
    action_repo = MLPromoActionRepository(session)
    await action_repo.log(
        sku=sku,
        mlb_id=body.mlb_id,
        action="reprice",
        promo_type="PRICE_DISCOUNT",
        price_after=body.new_price,
        reason="subir margem: saiu da promo e reentrou com preço mais alto",
        ml_response={"exit": exit_result, "enter": enter_result},
        dry_run=False,
        decided_by=body.decided_by,
        context=ctx,
    )
    await session.commit()
    _done(
        "repriced",
        exit_status_code=exit_sc,
        enter_status_code=enter_sc,
        elapsed_ms=round((time.monotonic() - started) * 1000),
    )
    return {"exit": exit_result, "enter": enter_result}


class ExitPromoIn(BaseModel):
    mlb_id: str
    promo_type: str | None = None
    promo_id: str | None = None
    decided_by: str | None = None
    source: str | None = None  # 'single' | 'bulk' — capturado no contexto


@router.post("/exit-promotion")
async def exit_promotion_endpoint(
    body: ExitPromoIn,
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    """Sai da promoção ativa (DELETE) — o preço volta ao cheio. Usado quando a
    sugestão de margem é o próprio preço cheio (sozinho no catálogo, ou o
    concorrente mais barato está acima do nosso cheio).

    Campanhas de co-participação (SMART/PRICE_MATCHING/MARKETPLACE_CAMPAIGN)
    precisam de ``promotion_id`` + ``offer_id`` no DELETE — para esses tipos
    roteamos por ``exit_offer``, que resolve esses ids ao vivo."""
    from tiny_mirror.infrastructure.orm.models import MLPromoCapORM

    started = time.monotonic()
    cap = await session.get(MLPromoCapORM, body.mlb_id)
    sku = cap.sku if cap else body.mlb_id
    _bind_op(
        "exit_promotion",
        mlb_id=body.mlb_id,
        sku=sku,
        promo_type=body.promo_type,
        promo_id=body.promo_id,
        actor=body.decided_by,
        apply_mode="live",
    )
    logger.info(
        "promo_op.start",
        co_participation=bool(
            body.promo_type and body.promo_type.upper() in CO_PARTICIPATION_TYPES
        ),
    )
    if body.promo_type and body.promo_type.upper() in CO_PARTICIPATION_TYPES:
        result = await service.exit_offer(
            mlb_id=body.mlb_id, promotion_type=body.promo_type, promotion_id=body.promo_id
        )
    else:
        # Doc ML: o DELETE de DEAL/SELLER_CAMPAIGN exige promotion_id (senão
        # 400 "Promotion id is required"). exit_promotion só inclui o param
        # quando truthy, então é seguro passar sempre o promo_id da oferta.
        result = await service.exit_promotion(
            mlb_id=body.mlb_id, promotion_type=body.promo_type, promotion_id=body.promo_id
        )
    sc = result.get("status_code")
    resp = result.get("response")
    no_offers = isinstance(resp, dict) and "no offers" in str(resp.get("message", "")).lower()
    delete_ok = (sc is not None and sc in (200, 201, 204)) or sc == 404 or no_offers
    if not delete_ok:
        _done(
            "ml_rejected",
            level="warning",
            ml_status_code=sc,
            elapsed_ms=round((time.monotonic() - started) * 1000),
        )
        raise HTTPException(
            status_code=sc if sc and sc < 600 else 502,
            detail={"step": "exit", "response": resp},
        )

    # GARANTE A SAÍDA DE VERDADE. O ML processa enroll/exit de forma ASSÍNCRONA: um
    # exit logo após um enroll pode ser ULTRAPASSADO (o ML aplica o enroll por
    # último) e o anúncio acaba INSCRITO de novo, mesmo o DELETE tendo dado 200 —
    # aí a tela diz "fora" e o ML fica "dentro" (bug relatado: "desinscrever rápido
    # não funciona"). Então confirmamos lendo o ML e RE-DELETAMOS se reaparecer
    # inscrito (DELETE é idempotente), até 3 leituras SEGUIDAS confirmarem que saiu
    # (estabilidade que ultrapassa o enroll atrasado). Flap por lag → re-deletar é
    # inofensivo. Backstop: o sweep horário reconcilia se algo escapar.
    import asyncio

    pt = (body.promo_type or "PRICE_DISCOUNT").upper()

    def _still_in(promos: list[dict[str, Any]]) -> bool:
        for p in promos:
            if (p.get("type") or "").upper() != pt:
                continue
            if body.promo_id and p.get("id") not in (body.promo_id, None):
                continue
            st = (p.get("status") or "").lower()
            if st == "started" or str(p.get("ref_id") or "").startswith("OFFER-"):
                return True
        return False

    clean = 0
    removed = False
    for _ in range(8):
        await asyncio.sleep(2.0)
        try:
            promos = await service.fetch_eligible_promos(body.mlb_id)
        except Exception:  # pragma: no cover — rede
            continue
        if _still_in(promos):
            clean = 0
            await service.exit_promotion(
                mlb_id=body.mlb_id, promotion_type=body.promo_type, promotion_id=body.promo_id
            )
        else:
            clean += 1
            if clean >= 3:
                removed = True
                break
    if not removed:
        logger.warning("promo.exit_not_effective", mlb_id=body.mlb_id, ml_status_code=sc)
        _done(
            "exit_not_effective",
            level="warning",
            ml_status_code=sc,
            elapsed_ms=round((time.monotonic() - started) * 1000),
        )
        raise HTTPException(
            status_code=409,
            detail=(
                "Não foi possível remover a promoção: o Mercado Livre segue mostrando "
                "o anúncio inscrito após várias tentativas. Tente de novo em instantes."
            ),
        )
    # Saída CONFIRMADA no ML (3 leituras seguidas) → sucesso.

    # Snapshot de contexto pra automação: o "porquê" da saída (ex.: ganhando o
    # catálogo, margem baixa, estoque acabando) fica capturado pra aprender.
    ctx = await _build_decision_context(
        session, mlb_id=body.mlb_id, sku=sku, promo_type=body.promo_type, source=body.source
    )
    action_repo = MLPromoActionRepository(session)
    await action_repo.log(
        sku=sku,
        mlb_id=body.mlb_id,
        action="exit_promotion",
        promo_type=body.promo_type or "PRICE_DISCOUNT",
        reason="saiu da promoção (preço volta ao cheio)",
        ml_response={"exit": result},
        dry_run=False,
        decided_by=body.decided_by,
        context=ctx,
    )
    # Tira a linha 'started' do espelho imediatamente pra ela sair de 'Inscritas'
    # antes do re-sync diário (a escrita no ML não toca no nosso espelho).
    await MLPromoDecisionRepository(session).expire_started(
        mlb_id=body.mlb_id, promo_id=body.promo_id, promo_type=body.promo_type
    )
    # NOSSO banco passa a mandar: limpa o marcador de inscrição e rebaixa o status
    # pra a promo sair de Inscritas na hora (sem isso, o enrolled_at preservaria
    # 'started' no sync e ela ficaria presa em Inscritas mesmo após sair).
    if body.promo_id:
        await session.execute(
            text(
                "UPDATE ml_promotions SET enrolled_at=NULL, status='candidate', "
                "updated_at=now() WHERE mlb_id=:m AND promotion_id=:p"
            ),
            {"m": body.mlb_id, "p": body.promo_id},
        )
    await session.commit()
    _done("exited", ml_status_code=sc, elapsed_ms=round((time.monotonic() - started) * 1000))
    return result


class EnrollOfferIn(BaseModel):
    mlb_id: str
    promo_type: str = "SMART"
    promo_id: str | None = None
    decided_by: str | None = None


@router.post("/activate-smart")
async def activate_smart_endpoint(
    body: EnrollOfferIn,
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    """Ativa (aceita) uma campanha de co-participação para um anúncio — SMART,
    PRICE_MATCHING ou MARKETPLACE_CAMPAIGN. O Mercado Livre define o preço; o
    vendedor só aceita. Resolve o ``offer_id`` ao vivo e faz o POST de inscrição.

    Escreve DIRETO no ML (irreversível a não ser saindo da campanha). A UI só
    chama isto após confirmação explícita do operador."""
    from tiny_mirror.infrastructure.orm.models import MLPromoCapORM

    started = time.monotonic()
    pt = (body.promo_type or "SMART").upper()
    cap = await session.get(MLPromoCapORM, body.mlb_id)
    sku = cap.sku if cap else body.mlb_id
    _bind_op(
        "activate_smart",
        mlb_id=body.mlb_id,
        sku=sku,
        promo_type=pt,
        promo_id=body.promo_id,
        actor=body.decided_by,
        apply_mode="live",
    )
    logger.info("promo_op.start")
    if pt not in CO_PARTICIPATION_TYPES:
        _done("refused_wrong_type", level="warning")
        raise HTTPException(
            status_code=422,
            detail={
                "step": "validate",
                "response": (
                    f"{pt} não é campanha de co-participação; use os endpoints de "
                    "preço (create-price-discount/modify-promotion) para esse tipo"
                ),
            },
        )
    result = await service.enroll_offer(
        mlb_id=body.mlb_id, promotion_type=pt, promotion_id=body.promo_id
    )
    # Sincroniza o espelho deste anúncio com o estado AS-IS do ML — vale pra
    # QUALQUER desfecho: inscreveu (vai pra Inscritas), já estava ativa, ou a
    # oferta sumiu (o ML iniciou/retirou). Assim a UI se corrige e o operador
    # para de ver "Ativar" numa oferta que não é mais candidata. Best-effort.
    try:
        await PromotionMirrorService(service).sync_mlb(session, body.mlb_id, sku)
    except Exception as exc:  # pragma: no cover — rede
        # Rollback: se o sync abortou a transação, a sessão não pode ficar poluída
        # pro log abaixo (senão um sync que falha derruba um enroll que DEU CERTO).
        try:
            await session.rollback()
        except Exception:
            pass
        logger.warning("activate_smart.mirror_sync_failed", mlb_id=body.mlb_id, error=str(exc))
    sc = result.get("status_code")
    if sc is None or sc >= 400:
        _done(
            "ml_rejected",
            level="warning",
            ml_status_code=sc,
            elapsed_ms=round((time.monotonic() - started) * 1000),
        )
        raise HTTPException(
            status_code=sc if (sc is not None and sc < 600) else 502,
            detail={
                "step": "enroll",
                "response": result.get("response"),
                "sent_body": result.get("sent_body"),
            },
        )
    ctx = await _build_decision_context(session, mlb_id=body.mlb_id, sku=sku, promo_type=pt)
    action_repo = MLPromoActionRepository(session)
    await action_repo.log(
        sku=sku,
        mlb_id=body.mlb_id,
        action="enroll_offer",
        promo_type=pt,
        reason=f"ativou campanha de co-participação {pt} (preço definido pelo ML)",
        ml_response={"enroll": result},
        dry_run=False,
        decided_by=body.decided_by,
        context=ctx,
    )
    await session.commit()
    _done("enrolled", ml_status_code=sc, elapsed_ms=round((time.monotonic() - started) * 1000))
    return result


class ModifyPromoIn(BaseModel):
    mlb_id: str
    new_price: Decimal = Field(gt=0)
    promo_id: str | None = None
    promo_type: str = "PRICE_DISCOUNT"
    current_price: Decimal | None = None
    decided_by: str | None = None


async def _settle_eligible(
    service: MLPromotionService,
    mlb_id: str,
    promo_id: str | None,
    *,
    started: bool,
    price: float | None = None,
    require_present: bool = False,
    tries: int = 12,
    sleep_s: float = 3.0,
) -> bool:
    """Espera o ML ASSENTAR a promo no estado desejado (``started`` ou fora) e,
    opcionalmente, no ``price`` alvo. ``require_present=True`` exige que o item
    esteja LISTADO (ex.: re-sugerido como candidate após um exit, antes de reentrar
    — senão o reenter dispara com o item ausente e falha). O ML processa enroll/
    exit/edit de forma ASSÍNCRONA — operar back-to-back sem isto causa
    ``NOT_FOUND_CANDIDATE_OR_OFFER`` ou corrida (reentrar antes do exit assentar)."""
    import asyncio

    for _ in range(tries):
        await asyncio.sleep(sleep_s)
        try:
            promos = await service.fetch_eligible_promos(mlb_id)
        except Exception:  # pragma: no cover — rede
            continue
        match = next((p for p in promos if promo_id and p.get("id") == promo_id), None)
        if require_present and match is None:
            continue
        is_started = match is not None and (match.get("status") or "").lower() == "started"
        if is_started != started:
            continue
        if price is not None:
            pp = match.get("price") if match is not None else None
            if not (pp and abs(float(pp) - price) < 0.02):
                continue
        return True
    return False


@router.post("/modify-promotion")
async def modify_promotion_endpoint(
    body: ModifyPromoIn,
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    """Altera o preço de uma promoção JÁ inscrita.

    Dois caminhos, escolhidos pelo sentido da mudança (vs ``current_price``):

    - **Baixar/igual** → ``POST`` in-place, preservando ``promotion_id``/tipo (o
      ML aceita baixar o preço de uma oferta inscrita sem sair dela).
    - **Subir acima do teto atual** → o ML não deixa subir in-place; então
      fazemos a **re-inscrição automática**: ``DELETE`` (sai da oferta) seguido
      de ``POST`` reentrando no novo preço. Para PRICE_DISCOUNT reentra como
      desconto de vendedor; para campanhas reentra com o ``promotion_id`` (se o
      novo preço exceder o teto da campanha, o ML devolve erro e repassamos).

    Nota (doc ML): ofertas LIGHTNING já iniciadas não podem ser removidas — a
    re-inscrição falhará e o erro do ML volta pra UI.
    """
    from tiny_mirror.infrastructure.orm.models import MLPromoCapORM

    started = time.monotonic()
    promo_type = body.promo_type
    cap = await session.get(MLPromoCapORM, body.mlb_id)
    sku = cap.sku if cap else body.mlb_id
    is_raise = body.current_price is not None and body.new_price > body.current_price + Decimal(
        "0.005"
    )
    _bind_op(
        "modify_promotion",
        mlb_id=body.mlb_id,
        sku=sku,
        promo_type=promo_type,
        promo_id=body.promo_id,
        actor=body.decided_by,
        apply_mode="live",
    )
    logger.info(
        "promo_op.start",
        new_price=_fnum(body.new_price),
        current_price=_fnum(body.current_price),
        is_raise=is_raise,
    )

    # Guard: só DEAL/SELLER_CAMPAIGN/PRICE_DISCOUNT permitem alterar o preço. O
    # resto (co-participação SMART/PRICE_MATCHING/MARKETPLACE_CAMPAIGN — preço do
    # ML; cupom — desconto fixo; DOD/LIGHTNING — sem edição) só dá pra sair.
    promo_type_u = (promo_type or "").upper()
    if promo_type_u not in PRICE_EDITABLE_TYPES:
        _done("refused_not_editable", level="warning")
        raise HTTPException(
            status_code=422,
            detail={
                "step": "validate",
                "response": (
                    f"{promo_type_u} não permite alterar o preço (regra do ML) — "
                    "use 'sair da promoção' para desativar."
                ),
            },
        )

    # PRICE_DISCOUNT não tem edição in-place (PUT); só DEAL/SELLER_CAMPAIGN têm.
    # Então: DEAL/SELLER_CAMPAIGN baixando → PUT; PRICE_DISCOUNT (qualquer
    # direção) e DEAL/SELLER_CAMPAIGN subindo → sair + reentrar.
    inplace_edit = promo_type_u in EDITABLE_INPLACE_TYPES and not is_raise

    if inplace_edit:
        # DEAL/SELLER_CAMPAIGN baixando/igual: edita IN-PLACE (PUT), sem sair.
        # Garante que a oferta JÁ assentou no ML (senão o PUT dá NOT_FOUND, porque o
        # ML ainda vê o item como 'candidate', não 'started').
        await _settle_eligible(service, body.mlb_id, body.promo_id, started=True, tries=6)
        result = await service.edit_promotion_price(
            mlb_id=body.mlb_id,
            deal_price=float(body.new_price),
            promotion_id=body.promo_id,
            promotion_type=promo_type,
        )
        sc = result.get("status_code")
        # Ainda assentando (NOT_FOUND) → espera mais e retenta uma vez.
        if sc == 400 and "NOT_FOUND" in str(result.get("response")):
            await _settle_eligible(service, body.mlb_id, body.promo_id, started=True)
            result = await service.edit_promotion_price(
                mlb_id=body.mlb_id,
                deal_price=float(body.new_price),
                promotion_id=body.promo_id,
                promotion_type=promo_type,
            )
            sc = result.get("status_code")
        if sc is not None and sc >= 400:
            _done(
                "ml_rejected",
                level="warning",
                step="modify",
                ml_status_code=sc,
                elapsed_ms=round((time.monotonic() - started) * 1000),
            )
            raise HTTPException(
                status_code=sc if sc < 600 else 502,
                detail={"step": "modify", "response": result.get("response")},
            )
        # Confirma que o ML assentou no NOVO preço (best-effort — o espelho é
        # autoritativo p/ a tela; isto só dá tempo do ML refletir).
        await _settle_eligible(
            service, body.mlb_id, body.promo_id, started=True, price=float(body.new_price)
        )
        action = "modify_promotion"
        reason = "alterar promoção inscrita: baixou o preço in-place"
        ml_response: dict[str, Any] = result
    else:
        # Sair + reentrar. Vale para: DEAL/SELLER_CAMPAIGN SUBINDO (o ML não
        # deixa subir in-place) e PRICE_DISCOUNT em QUALQUER direção (não tem
        # PUT — a única forma de alterar é remover e recriar).
        exit_result = await service.exit_promotion(
            mlb_id=body.mlb_id, promotion_type=promo_type, promotion_id=body.promo_id
        )
        exit_sc = exit_result.get("status_code")
        if exit_sc is not None and exit_sc >= 400 and exit_sc != 404:
            _done(
                "ml_rejected",
                level="warning",
                step="exit",
                ml_status_code=exit_sc,
                elapsed_ms=round((time.monotonic() - started) * 1000),
            )
            raise HTTPException(
                status_code=exit_sc if exit_sc < 600 else 502,
                detail={"step": "exit", "response": exit_result.get("response")},
            )
        # ESPERA o exit ASSENTAR e o item ser RE-SUGERIDO como candidate antes de
        # reentrar. O ML é assíncrono: reentrar back-to-back é ultrapassado pela saída
        # (corrida → preço antigo), e reentrar com o item ainda AUSENTE (o ML não
        # re-sugeriu a oferta ainda) falha. require_present espera o candidate voltar.
        # (Campanha gerida pelo ML pode demorar a re-sugerir → cai na fila de
        # re-inscrição abaixo, que é o fallback já existente.)
        await _settle_eligible(
            service, body.mlb_id, body.promo_id, started=False, require_present=True, tries=15
        )
        # Reentra: PRICE_DISCOUNT (sem campanha) cria desconto de vendedor;
        # campanha reentra com o promotion_id (POST = nova inscrição).
        if promo_type == "PRICE_DISCOUNT" or body.promo_id is None:
            enter_result = await service.create_price_discount(
                mlb_id=body.mlb_id, deal_price=float(body.new_price)
            )
        else:
            enter_result = await service.modify_promotion(
                mlb_id=body.mlb_id,
                deal_price=float(body.new_price),
                promotion_id=body.promo_id,
                promotion_type=promo_type,
            )
        enter_sc = enter_result.get("status_code")
        if enter_sc is not None and enter_sc >= 400:
            # RISCO da abordagem sair+reentrar: o DELETE deu certo (o anúncio
            # JÁ voltou ao preço cheio) mas a reinscrição falhou. Para campanhas
            # geridas pelo ML, depois do exit o item pode levar um tempo até ser
            # RE-SUGERIDO como candidato — então a reinscrição imediata pode
            # devolver "não elegível/oferta inexistente". Estado resultante: SEM
            # promoção, a preço cheio. Logado num evento próprio pra a gente
            # decidir o que fazer (retry com backoff? alerta? fila?).
            logger.warning(
                "promo.resubscribe_failed",
                note=(
                    "exit OK mas reinscrição falhou — anúncio voltou ao preço CHEIO, "
                    "sem promoção. Campanha pode demorar a ser re-sugerida pelo ML."
                ),
                step="reenter",
                exit_ok=True,
                ml_status_code=enter_sc,
                new_price=_fnum(body.new_price),
            )
            # Campanha (DEAL/SELLER_CAMPAIGN) subindo: o atraso de re-sugestão do
            # ML é esperado. Em vez de deixar o anúncio a preço cheio, enfileira
            # a re-inscrição — o poller reentra assim que a oferta reaparecer.
            can_queue = (
                is_raise and body.promo_id is not None and promo_type_u in (EDITABLE_INPLACE_TYPES)
            )
            if can_queue:
                now = datetime.now(UTC)
                deadline = now + timedelta(hours=settings.ml_promo_resubscribe_deadline_hours)
                interval = max(1, settings.ml_promo_resubscribe_poll_interval_seconds)
                max_attempts = max(
                    1,
                    int(settings.ml_promo_resubscribe_deadline_hours * 3600 / interval) + 5,
                )
                op_id = structlog.contextvars.get_contextvars().get("op_id")
                job = await MLPromoResubscribeRepository(session).enqueue(
                    mlb_id=body.mlb_id,
                    sku=sku,
                    promo_type=promo_type_u,
                    target_price=body.new_price,
                    deadline=deadline,
                    promo_id=body.promo_id,
                    max_attempts=max_attempts,
                    op_id=op_id,
                    decided_by=body.decided_by,
                    last_error=str(enter_result.get("response"))[:500],
                    last_status_code=enter_sc,
                )
                await MLPromoActionRepository(session).log(
                    sku=sku,
                    mlb_id=body.mlb_id,
                    action="resubscribe_scheduled",
                    promo_type=promo_type_u,
                    promo_id=body.promo_id,
                    price_after=body.new_price,
                    reason=(
                        "subir o preço exige sair + reentrar; o ML ainda não re-sugeriu "
                        "a oferta — re-inscrição agendada na fila"
                    ),
                    ml_response={"exit": exit_result, "enter": enter_result},
                    decided_by=body.decided_by,
                )
                # O exit deu certo: o anúncio voltou ao preço cheio, sem promoção.
                # Tira a linha 'started' do espelho pra não mostrar promoção que
                # já saiu — a fila recoloca quando reentrar.
                await MLPromoDecisionRepository(session).expire_started(
                    mlb_id=body.mlb_id, promo_id=body.promo_id, promo_type=promo_type_u
                )
                await session.commit()
                _done(
                    "resubscribe_scheduled",
                    step="reenter",
                    exit_ok=True,
                    ml_status_code=enter_sc,
                    resubscribe_job_id=job.id,
                    elapsed_ms=round((time.monotonic() - started) * 1000),
                )
                return {
                    "exit": exit_result,
                    "enter": enter_result,
                    "resubscribe_scheduled": True,
                    "resubscribe_job_id": job.id,
                    "message": (
                        "Saiu da promoção, mas o Mercado Livre ainda não re-sugeriu a "
                        "oferta com o novo preço. A re-inscrição foi agendada e vai "
                        "entrar automaticamente assim que a promoção reaparecer."
                    ),
                }
            _done(
                "ml_rejected",
                level="warning",
                step="reenter",
                exit_ok=True,
                ml_status_code=enter_sc,
                elapsed_ms=round((time.monotonic() - started) * 1000),
            )
            raise HTTPException(
                status_code=enter_sc if enter_sc < 600 else 502,
                detail={
                    "step": "reenter",
                    "exit_ok": True,
                    "response": enter_result.get("response"),
                },
            )
        action = "resubscribe_promotion"
        reason = "alterar promoção inscrita: re-inscrição automática (sair + reentrar) — " + (
            "subiu acima do teto" if is_raise else "PRICE_DISCOUNT não tem edição in-place"
        )
        ml_response = {"exit": exit_result, "enter": enter_result}

    ctx = await _build_decision_context(
        session,
        mlb_id=body.mlb_id,
        sku=sku,
        price_before=_fnum(body.current_price),
        price_after=float(body.new_price),
        floor_price=float(cap.margin_floor_price) if cap and cap.margin_floor_price else None,
        promo_type=promo_type,
    )
    action_repo = MLPromoActionRepository(session)
    await action_repo.log(
        sku=sku,
        mlb_id=body.mlb_id,
        action=action,
        promo_type=promo_type,
        price_after=body.new_price,
        reason=reason,
        ml_response=ml_response,
        dry_run=False,
        decided_by=body.decided_by,
        context=ctx,
    )
    resubscribed = not inplace_edit
    # Reflete o novo preço na linha 'started' em cache para a UI atualizar ANTES
    # do re-sync diário (a escrita no ML não toca no nosso espelho).
    dec_repo = MLPromoDecisionRepository(session)
    await dec_repo.update_started_price(
        mlb_id=body.mlb_id,
        new_price=body.new_price,
        promo_id=body.promo_id,
        promo_type=promo_type,
    )
    # A aba "Inscritas" (/active) lê o ESPELHO ml_promotions, não a tabela de
    # decisões — então o Alterar tem que escrever o preço novo AQUI também, senão a
    # tela continua mostrando o valor velho. (price=None só p/ co-participação, que
    # é "Dinâmico"; aí não mexe no preço.)
    if body.promo_id and body.new_price is not None:
        await session.execute(
            text(
                "UPDATE ml_promotions SET price=:price, status='started', "
                "enrolled_at=now(), updated_at=now() WHERE mlb_id=:m AND promotion_id=:p"
            ),
            {"m": body.mlb_id, "p": body.promo_id, "price": body.new_price},
        )
    if resubscribed:
        # Visibilidade no dock: registra a re-inscrição imediata como concluída.
        await MLPromoResubscribeRepository(session).record_completed(
            mlb_id=body.mlb_id,
            sku=sku,
            promo_type=promo_type_u,
            promo_id=body.promo_id,
            target_price=body.new_price,
            op_id=structlog.contextvars.get_contextvars().get("op_id"),
            decided_by=body.decided_by,
        )
    await session.commit()
    _done(
        "modified", resubscribed=resubscribed, elapsed_ms=round((time.monotonic() - started) * 1000)
    )
    return {"resubscribed": resubscribed, **ml_response}


def _serialize_resub_job(job: Any) -> dict[str, Any]:
    return {
        "id": job.id,
        "mlb_id": job.mlb_id,
        "sku": job.sku,
        "promo_type": job.promo_type,
        "promo_id": job.promo_id,
        "target_price": _fnum(job.target_price),
        "status": job.status,
        "attempts": job.attempts,
        "max_attempts": job.max_attempts,
        "next_attempt_at": job.next_attempt_at.isoformat() if job.next_attempt_at else None,
        "deadline": job.deadline.isoformat() if job.deadline else None,
        "last_error": job.last_error,
        "last_status_code": job.last_status_code,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "resolved_at": job.resolved_at.isoformat() if job.resolved_at else None,
    }


@router.get("/resubscribe-jobs")
async def list_resubscribe_jobs(
    status_filter: str | None = Query(
        None, alias="status", description="pending|done|failed|cancelled"
    ),
    mlb_id: str | None = Query(None),
    limit: int = Query(200, ge=1, le=500),
    session: AsyncSession = Depends(db_session),
) -> list[dict[str, Any]]:
    """Fila de re-inscrição automática (raise = sair + reentrar). A UI usa pra
    mostrar quais anúncios estão aguardando o ML re-sugerir a oferta."""
    jobs = await MLPromoResubscribeRepository(session).list_(
        status=status_filter, mlb_id=mlb_id, limit=limit
    )
    return [_serialize_resub_job(j) for j in jobs]


@router.post("/resubscribe-jobs/{job_id}/cancel")
async def cancel_resubscribe_job(
    job_id: int,
    session: AsyncSession = Depends(db_session),
) -> dict[str, Any]:
    """Cancela um job de re-inscrição pendente (o operador desistiu de subir o
    preço). Não mexe no ML — o anúncio fica como está (preço cheio)."""
    repo = MLPromoResubscribeRepository(session)
    job = await repo.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job de re-inscrição não encontrado.")
    if job.status == "pending":
        await repo.cancel(job)
        await session.commit()
    return _serialize_resub_job(job)


@router.get("/catalog-competitors")
async def catalog_competitors(
    mlb_id: str = Query(..., description="MLB do anúncio de catálogo"),
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    """Concorrência do catálogo + sugestão de preço pra recuperar margem.

    Quando ganhamos a buy box, a ML não revela o 2º colocado via price_to_win.
    Aqui buscamos ``GET /products/{cpid}/items`` (todos os vendedores do
    catálogo) e sugerimos um preço: sozinho -> preço cheio; com concorrentes ->
    logo abaixo do mais barato (recupera margem mantendo competitividade),
    limitado pelo piso de margem e pelo preço cheio. Avisa quando está apertado
    (concorrente <= nosso preço, ou empate no 1º): ganhando, sem folga pra subir.
    """
    from tiny_mirror.config import settings
    from tiny_mirror.infrastructure.orm.models import (
        MLCatalogStatusORM,
        MLCostsSnapshotORM,
        MLListingORM,
        MLPromoCapORM,
    )
    from tiny_mirror.services.pricing_service import margin_at_price

    cat = await session.get(MLCatalogStatusORM, mlb_id)
    if not cat or not cat.catalog_product_id:
        return {"mlb_id": mlb_id, "is_catalog": False}

    cap = await session.get(MLPromoCapORM, mlb_id)
    snap = await session.get(MLCostsSnapshotORM, mlb_id)
    listing = await session.get(MLListingORM, mlb_id)

    full_price: float | None = None
    if listing and listing.price is not None:
        full_price = float(listing.price)
    elif snap and snap.list_price is not None:
        full_price = float(snap.list_price)
    current = float(cat.current_price) if cat.current_price is not None else None
    floor = float(cap.margin_floor_price) if cap and cap.margin_floor_price is not None else None

    our_id: int | None = None
    try:
        our_id = int(settings.ml_user_id) if settings.ml_user_id else None
    except ValueError:
        our_id = None

    raw = await service.fetch_catalog_competitors(cat.catalog_product_id)
    competitors: list[dict[str, Any]] = []
    comp_prices: list[float] = []
    for c in raw:
        price = c.get("price")
        if price is None:
            continue
        is_ours = our_id is not None and c.get("seller_id") == our_id
        if not is_ours and (c.get("condition") or "new") == "new":
            comp_prices.append(float(price))
        competitors.append(
            {
                "item_id": c.get("item_id"),
                "price": float(price),
                "free_shipping": c.get("free_shipping"),
                "logistic_type": c.get("logistic_type"),
                "is_ours": is_ours,
            }
        )
    competitors.sort(key=lambda x: x["price"])
    comp_prices.sort()
    cheapest = comp_prices[0] if comp_prices else None

    tied = cat.status == "sharing_first_place" or (cat.competitors_sharing_first_place or 0) > 0
    winning = cat.status in ("winning", "sharing_first_place")

    # Sugestão de preço. Quatro casos:
    #   alone    — sozinhos no catálogo: sobe ao preço cheio (margem máx, sem risco).
    #   undercut — somos os mais baratos: sobe pra logo abaixo do concorrente
    #              mais barato (recupera o máximo ainda ficando o melhor preço).
    #   boost    — ganhando ACIMA do concorrente mais barato (vitória por
    #              reputação/fulfillment): subida CONSERVADORA (metade do caminho
    #              até o cheio), marcada como estimativa — não dá pra saber o teto.
    #   tight    — empate no 1º ou sem folga: não sugere subir.
    suggested: float | None
    if tied:
        kind = "tight"
        suggested = current
    elif not comp_prices:
        kind = "alone"
        suggested = full_price
    elif current is not None and cheapest is not None and (cheapest - 0.01) > current:
        kind = "undercut"
        cand = round(cheapest - 0.01, 2)
        suggested = min(cand, full_price) if full_price is not None else cand
    elif winning and full_price is not None and current is not None and full_price > current + 0.01:
        kind = "boost"
        suggested = round(current + (full_price - current) * 0.5, 2)
    else:
        kind = "tight"
        suggested = current
    if full_price is not None and suggested is not None and suggested > full_price:
        suggested = full_price
    if floor is not None and suggested is not None and suggested < floor:
        suggested = floor
    if suggested is not None:
        suggested = round(suggested, 2)

    warning = (
        "tied" if tied else "estimate" if kind == "boost" else "tight" if kind == "tight" else None
    )
    can_raise = bool(
        winning
        and not tied
        and suggested is not None
        and current is not None
        and suggested > current + 0.01
    )

    def _margin(price: float | None) -> dict[str, float] | None:
        if (
            price is None
            or not snap
            or snap.base_cost is None
            or snap.commission_pct is None
            or not snap.freight_bands
        ):
            return None
        try:
            b = margin_at_price(
                price=price,
                base_cost=snap.base_cost,
                commission_pct=snap.commission_pct,
                freight_bands=snap.freight_bands,
            )
            return {"pct": float(b.margin_pct), "value": float(b.margin_value)}
        except Exception:  # pragma: no cover — dados de custo incompletos
            return None

    action = (
        "exit"
        if full_price is not None and suggested is not None and suggested >= full_price - 0.005
        else "reprice"
    )

    return {
        "mlb_id": mlb_id,
        "is_catalog": True,
        "catalog_product_id": cat.catalog_product_id,
        "status": cat.status,
        "current_price": current,
        "full_price": full_price,
        "floor_price": floor,
        "competitors": competitors,
        "n_competitors": len(comp_prices),
        "cheapest_competitor": cheapest,
        "suggested_price": suggested,
        "suggestion_kind": kind,
        "can_raise": can_raise,
        "warning": warning,
        "margin_now": _margin(current),
        "margin_suggested": _margin(suggested),
        "action": action,
    }


# ── Tendência robusta (sem ML) ────────────────────────────────────────────────
# Dois ruídos atrapalham um momentum simples (taxa 30d / taxa 90d):
#   1. Volume baixo: 2 vendas em 90d viram "+50% subindo forte" — é só acaso.
#   2. Pico num único dia: uma venda absurda num dia (lançamento/promo) infla a
#      base 90d e faz o resto parecer "caindo forte", ou — se o pico for recente
#      — "subindo forte". É um outlier, não tendência.
# Tratamento: (a) só calculamos tendência com volume mínimo e vendas em dias
# suficientes; (b) winsorizamos cada dia num teto robusto (3x a mediana dos dias
# com venda) antes de comparar — o pico vira um dia normal e a curva fala.
_TREND_MIN_UNITS = 10  # < isso em 90d: amostra fraca demais p/ tendência
_TREND_MIN_ACTIVE_DAYS = 5  # vendas em < 5 dias distintos: idem


def _robust_momentum(daily: list[int]) -> float:
    """``daily`` = 90 valores (mais antigo -> mais recente). Retorna um momentum
    em torno de 1.0 (estável). Pipeline robusto, sem ML:

      1. Gate de volume: < 10 un OU < 5 dias com venda -> 1.0 (amostra fraca).
      2. Winsoriza cada dia num teto (3x a mediana dos dias com venda) -> uma
         venda absurda num único dia vira um dia normal e não distorce a base.
      3. Agrega em 13 semanas (suaviza o agrupamento dia-a-dia, que faz a janela
         de 30d pegar/perder "clusters" e gerar falso sinal).
      4. raw = ritmo das 4 semanas recentes / média semanal.
      5. Encolhe ``raw`` para 1.0 conforme a tendência é INCONSISTENTE: confiança
         = concordância de sinal dos slopes par-a-par das semanas (ruído ~0.5 de
         concordância -> confiança 0; tendência limpa -> ~1). Aplicada ao
         quadrado (variância) -> ruído some, tendência real passa intacta."""
    total = sum(daily)
    active = sum(1 for x in daily if x > 0)
    if total < _TREND_MIN_UNITS or active < _TREND_MIN_ACTIVE_DAYS:
        return 1.0  # baixo giro -> estável (honesto: não dá pra inferir)
    nonzero = sorted(x for x in daily if x > 0)
    cap = max(nonzero[len(nonzero) // 2] * 3, 2)  # teto robusto: neutraliza o pico
    w = [min(x, cap) for x in daily]
    weeks = [sum(w[i : i + 7]) for i in range(0, 90, 7)]  # 13 sem, antigo -> recente
    base = sum(weeks) / len(weeks)
    if base <= 0:
        return 1.0
    raw = (sum(weeks[-4:]) / 4) / base
    slopes = [weeks[j] - weeks[i] for i in range(len(weeks)) for j in range(i + 1, len(weeks))]
    nz = [s for s in slopes if s != 0]
    if nz:
        agree = max(sum(s > 0 for s in nz), sum(s < 0 for s in nz)) / len(nz)
        conf = max(0.0, (agree - 0.5) * 2)
    else:
        conf = 0.0
    return round(1.0 + (raw - 1.0) * conf**2, 2)


@router.get("/trends", response_model=dict[str, float | None])
async def list_trends(session: AsyncSession = Depends(db_session)) -> dict[str, float | None]:
    """Retorna ``{baseSku: momentum}`` — demanda por SKU base, só Mercado
    Livre (ml_sales_daily), janela de 90 dias.

    momentum robusto (ver ``_robust_momentum``): taxa diária dos últimos 30d vs.
    a média 90d, sobre a série winsorizada (resiste a pico de um dia) e só quando
    há volume mínimo (>= 10 un em >= 5 dias) — senão 1.0 (estável). Agrupa todos
    os anúncios do SKU base (kit ``NU-X`` cai em ``X``; combos ``COM-…`` ficam
    como estão). Ausente quando não houve venda em 90d."""
    from datetime import date as _date
    from datetime import timedelta as _td

    rows = await session.execute(
        text(
            """
            SELECT regexp_replace(sku, '^[0-9]+U-', '') AS base_sku,
                   sale_date, SUM(qty)::int AS qty
            FROM ml_sales_daily
            WHERE sale_date >= CURRENT_DATE - INTERVAL '89 days'
              AND sku IS NOT NULL AND sku <> ''
            GROUP BY 1, sale_date
            """
        )
    )
    start = _date.today() - _td(days=89)
    series: dict[str, list[int]] = {}
    for base_sku, sale_date, qty in rows.all():
        arr = series.setdefault(base_sku, [0] * 90)
        idx = (sale_date - start).days
        if 0 <= idx < 90:
            arr[idx] += int(qty)
    return {base_sku: _robust_momentum(daily) for base_sku, daily in series.items()}


@router.get("/sales-daily")
async def sales_daily(
    sku: str = Query(..., description="SKU do produto"),
    days: int = Query(default=30, ge=1, le=120),
    session: AsyncSession = Depends(db_session),
) -> list[dict[str, Any]]:
    """Série diária de vendas (sale_buckets) dos últimos ``days`` dias para um
    SKU, incluindo o dia atual e preenchendo dias sem venda com 0. Usado pelo
    gráfico de vendas na aba Promoções."""
    rows = await session.execute(
        text(
            """
            SELECT d::date AS day, COALESCE(s.qty, 0)::int AS qty
            FROM generate_series(
                CURRENT_DATE - ((:days - 1) * INTERVAL '1 day'),
                CURRENT_DATE,
                INTERVAL '1 day'
            ) d
            LEFT JOIN (
                SELECT bucket_date, SUM(quantity_sold) AS qty
                FROM sale_buckets
                WHERE sku = :sku
                  AND bucket_date >= CURRENT_DATE - ((:days - 1) * INTERVAL '1 day')
                GROUP BY bucket_date
            ) s ON s.bucket_date = d::date
            ORDER BY day
            """
        ),
        {"sku": sku, "days": days},
    )
    return [{"date": r[0].isoformat(), "qty": int(r[1])} for r in rows.all()]


@router.post("/sync/ml-sales")
async def sync_ml_sales(
    days: int = Query(default=90, ge=1, le=180),
    request: Request = None,  # type: ignore[assignment]
) -> dict[str, Any]:
    """Reconstrói ml_sales_daily (vendas por anúncio, só ML) dos últimos
    ``days`` dias, buscando da ML Orders API. Síncrono (~1-2 min p/ 90d)."""
    from tiny_mirror.config import settings
    from tiny_mirror.services.ml_sales_sync_service import MLSalesSyncService

    ml_token_service = getattr(request.app.state, "ml_token_service", None)
    if ml_token_service is None:
        raise HTTPException(status_code=503, detail="ML token service not configured")
    svc = MLSalesSyncService(
        token_service=ml_token_service,
        http_client=request.app.state.http_client,
        ml_user_id=settings.ml_user_id,
    )
    return await svc.backfill(days=days)


@router.post("/calibrate-flex")
async def calibrate_flex(
    days: int = Query(default=90, ge=1, le=180),
    max_shipments: int = Query(default=3000, ge=100, le=12000),
    request: Request = None,  # type: ignore[assignment]
) -> dict[str, Any]:
    """Dispara a recalibração das taxas Flex (comissão + frete por anúncio dos
    pedidos reais) em background. Pesado (~10-15 min, puxa pedidos + fretes), por
    isso roda detached e devolve na hora. O job semanal faz o mesmo."""
    import asyncio

    from tiny_mirror.config import settings
    from tiny_mirror.services.flex_fee_calibration_service import FlexFeeCalibrationService

    ml_token_service = getattr(request.app.state, "ml_token_service", None)
    if ml_token_service is None:
        raise HTTPException(status_code=503, detail="ML token service not configured")
    svc = FlexFeeCalibrationService(
        token_service=ml_token_service,
        http_client=request.app.state.http_client,
        ml_user_id=settings.ml_user_id,
    )

    async def _run() -> None:
        try:
            await svc.recalibrate(days=days, max_shipments=max_shipments)
        except Exception as exc:  # pragma: no cover — background safety
            logger.error("calibrate-flex background run failed", error=str(exc))

    # Keep a reference on app.state so the task isn't garbage-collected mid-run.
    request.app.state.flex_calib_task = asyncio.create_task(_run())
    return {"status": "started", "days": days, "max_shipments": max_shipments}


@router.get("/sales-daily-all")
async def sales_daily_all(
    days: int = Query(default=90, ge=1, le=180),
    session: AsyncSession = Depends(db_session),
) -> dict[str, list[int]]:
    """Série diária de vendas (Mercado Livre, ml_sales_daily) dos últimos
    ``days`` dias, agrupada por **SKU base** (soma todos os anúncios do SKU;
    kit ``NU-X`` cai em ``X``). ``{baseSku: [qty_antigo … hoje]}``. Alimenta o
    sparkline de vendas no header do card."""
    from datetime import date, timedelta

    rows = await session.execute(
        text(
            """
            SELECT regexp_replace(sku, '^[0-9]+U-', '') AS base_sku, sale_date, SUM(qty)::int AS qty
            FROM ml_sales_daily
            WHERE sale_date >= CURRENT_DATE - ((:days - 1) * INTERVAL '1 day')
              AND sku IS NOT NULL AND sku <> ''
            GROUP BY 1, sale_date
            """
        ),
        {"days": days},
    )
    start = date.today() - timedelta(days=days - 1)
    out: dict[str, list[int]] = {}
    for base_sku, bdate, qty in rows.all():
        arr = out.setdefault(base_sku, [0] * days)
        idx = (bdate - start).days
        if 0 <= idx < days:
            arr[idx] += int(qty)
    return out


@router.get("/sales-daily-mlb")
async def sales_daily_mlb(
    days: int = Query(default=90, ge=1, le=180),
    session: AsyncSession = Depends(db_session),
) -> dict[str, list[int]]:
    """Série diária de vendas (Mercado Livre) por **anúncio (MLB)** dos
    últimos ``days`` dias. ``{mlb_id: [qty_antigo … hoje]}``. Alimenta o
    gráfico de vendas por anúncio na visão expandida."""
    from datetime import date, timedelta

    rows = await session.execute(
        text(
            """
            SELECT mlb_id, sale_date, qty
            FROM ml_sales_daily
            WHERE sale_date >= CURRENT_DATE - ((:days - 1) * INTERVAL '1 day')
            """
        ),
        {"days": days},
    )
    start = date.today() - timedelta(days=days - 1)
    out: dict[str, list[int]] = {}
    for mlb, bdate, qty in rows.all():
        arr = out.setdefault(mlb, [0] * days)
        idx = (bdate - start).days
        if 0 <= idx < days:
            arr[idx] += int(qty)
    return out
