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

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.api.dependencies import db_session
from tiny_mirror.infrastructure.repositories.ml_listing_repository import (
    MLListingRepository,
)
from tiny_mirror.infrastructure.repositories.ml_promo_repository import (
    MLCostsSnapshotRepository,
    MLPromoActionRepository,
    MLPromoAlertRepository,
    MLPromoCapRepository,
    MLPromoDecisionRepository,
)
from tiny_mirror.services.ml_promotion_service import MLPromotionService

router = APIRouter()


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
    freight_band_opt: bool
    skip_when_winning: bool
    excluded_promo_types: list[str]
    notes: str | None
    updated_by: str | None
    updated_at: datetime
    # Joined from ml_listings (the MLB's listing row) — type + status.
    logistic_type: str | None = None
    listing_status: str | None = None
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
    reason: str
    status: str
    created_at: datetime
    decided_at: datetime | None
    decided_by: str | None
    notes: str | None


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
# CAPS
# ---------------------------------------------------------------------------
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
        out.commission_pct = snap.commission_pct
        out.freight_bands = snap.freight_bands
        out.sheet_promo_price = snap.sheet_promo_price

        floor_price = cap.margin_floor_price or snap.sheet_promo_price
        if (
            floor_price is not None
            and snap.base_cost is not None
            and snap.commission_pct is not None
            and snap.freight_bands
        ):
            try:
                breakdown = margin_at_price(
                    price=floor_price,
                    base_cost=snap.base_cost,
                    commission_pct=snap.commission_pct,
                    freight_bands=snap.freight_bands,
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
        out.has_active_listing = listing.status == "active"
    else:
        out.has_active_listing = False

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
@router.post("/decisions/generate")
async def generate_decisions(
    only_sku: str | None = Query(default=None, description="restrict to a single SKU"),
    limit_skus: int | None = Query(default=None, ge=1, le=2000),
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, Any]:
    """Enumerate eligible candidate promos per MLB and insert one PENDING
    decision per (mlb_id, promo_key). Idempotent — re-running this skips
    decisions already in the queue (pending / approved / rejected)."""
    return await service.generate_pending_decisions(
        session, only_sku=only_sku, limit_skus=limit_skus
    )


@router.get("/decisions", response_model=list[DecisionOut])
async def list_decisions(
    status_: Annotated[str | None, Query(alias="status")] = "pending",
    sku: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(db_session),
) -> list[DecisionOut]:
    repo = MLPromoDecisionRepository(session)
    rows, _ = await repo.list_(status=status_, sku=sku, limit=limit, offset=offset)
    return [DecisionOut.model_validate(r) for r in rows]


async def _apply_target_override(
    repo: MLPromoDecisionRepository,
    decision_id: int,
    override_price: Decimal,
) -> tuple[dict[str, Decimal], str | None]:
    """Validate a target_price override and return the recomputed pct
    fields plus an optional warning string.

    Limites diferentes:
    - **cap_pct** (limite do ML pra share do seller) é HARD: viola → 422.
    - **floor_price** (nosso piso de margem) é SOFT: o operador pode
      aprovar mesmo abaixo, mas devolvemos uma string de aviso que o
      caller anexa nas `notes` da linha pra audit ("margem em risco").

    Raises HTTPException(422) só quando o override viola o cap do ML
    (ou o preço é inválido). O piso só gera warning.
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

    # HARD: cap_pct é o teto da parte do seller (sem co-pay do ML). Pra
    # promoções como SMART/DEAL, ML usa esse cap pra definir se o seller
    # está jogando dentro da regra do canal. Override que viola é
    # bloqueado.
    if row.cap_pct is not None and new_seller_pct > row.cap_pct + Decimal("0.01"):
        raise HTTPException(
            status_code=422,
            detail=(
                f"seller {new_seller_pct}% > cap ML {row.cap_pct}% "
                f"(target_price R$ {override_price} ultrapassa o cap do canal)"
            ),
        )

    # SOFT: piso de margem é nosso, não do ML. Operador pode forçar
    # abaixo (ex: pra match preço de concorrente, ou pra giro). Volta
    # uma warning string pra ser anexada na audit; o status code fica 200.
    warning: str | None = None
    if row.floor_price is not None and override_price + Decimal("0.005") < row.floor_price:
        warning = (
            f"[forçado abaixo do piso] target R$ {override_price} < "
            f"piso R$ {row.floor_price} — margem em risco"
        )

    return (
        {
            "target_price": override_price.quantize(Decimal("0.01")),
            "target_total_pct": new_total_pct,
            "target_seller_pct": new_seller_pct,
        },
        warning,
    )


@router.post("/decisions/{decision_id}/approve", response_model=DecisionOut)
async def approve_decision(
    decision_id: int,
    body: DecisionDecideIn,
    session: AsyncSession = Depends(db_session),
) -> DecisionOut:
    repo = MLPromoDecisionRepository(session)
    override: dict[str, Decimal] = {}
    notes = body.notes
    if body.target_price is not None:
        override, warning = await _apply_target_override(repo, decision_id, body.target_price)
        if warning:
            # Anexa o aviso de piso violado nas notas pra audit. Se o
            # operador já passou um `notes`, preserva no início.
            notes = f"{notes}\n{warning}".strip() if notes else warning
    row = await repo.decide(
        decision_id,
        status="approved",
        by=body.decided_by,
        notes=notes,
        **override,
    )
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"decision {decision_id} not found or already decided",
        )
    await session.commit()
    return DecisionOut.model_validate(row)


@router.post("/decisions/{decision_id}/reject", response_model=DecisionOut)
async def reject_decision(
    decision_id: int,
    body: DecisionDecideIn,
    session: AsyncSession = Depends(db_session),
) -> DecisionOut:
    repo = MLPromoDecisionRepository(session)
    row = await repo.decide(decision_id, status="rejected", by=body.decided_by, notes=body.notes)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"decision {decision_id} not found or already decided",
        )
    await session.commit()
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
    repo = MLPromoDecisionRepository(session)
    row = await repo.decide(decision_id, status="ignored", by=body.decided_by, notes=body.notes)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"decision {decision_id} not found or already decided",
        )
    await session.commit()
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
    repo = MLPromoDecisionRepository(session)
    row = await repo.revert_to_pending(decision_id)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"decision {decision_id} not found or already pending",
        )
    await session.commit()
    return DecisionOut.model_validate(row)


@router.get("/trends", response_model=dict[str, float | None])
async def list_trends(session: AsyncSession = Depends(db_session)) -> dict[str, float | None]:
    """Retorna ``{sku: momentum_15v30}`` para todos os SKUs com vendas.

    Inclui SKUs base (próprios em mv_coverage) e também kits/combos
    cujo componente único tem momentum — assim a aba Decisões consegue
    mostrar tendência mesmo pros SKUs como `10U-FOO` ou `COM-BAR` que
    a view não rastreia diretamente. Pra kits com múltiplos componentes
    (`COM-X`), o momentum sai do componente principal.

    momentum_15v30: ratio (sold_15d/15) / daily_rate. <0.8 caindo,
    0.8-1.2 estável, ≥1.2 subindo. NULL quando não há baseline (SKU
    sem vendas no mês). View mv_coverage atualiza a cada 15 min via
    cron de REFRESH MATERIALIZED VIEW.
    """
    # SKUs base — direto da view.
    base_q = await session.execute(
        text("SELECT sku, momentum_15v30 FROM mv_coverage WHERE momentum_15v30 IS NOT NULL")
    )
    out: dict[str, float | None] = {sku: float(mom) for sku, mom in base_q.all()}

    # Kits/variantes: pega o momentum do primeiro componente da composição.
    # Pra kit puro (1 componente) é o próprio base; pra combo (N), a query
    # ainda devolve um deles — útil como sinal aproximado da demanda.
    kit_q = await session.execute(
        text(
            """
            SELECT DISTINCT ON (p.sku) p.sku, m.momentum_15v30
            FROM product_kit_components kc
            JOIN products p ON p.tiny_id = kc.kit_product_tiny_id
            JOIN mv_coverage m ON m.sku = kc.component_sku
            WHERE m.momentum_15v30 IS NOT NULL
            ORDER BY p.sku, kc.id
            """
        )
    )
    for kit_sku, mom in kit_q.all():
        # Não sobrescreve um SKU que já tem momentum próprio (improvável,
        # mas defensivo — kit puro tem sempre o mesmo valor que o base).
        if kit_sku not in out:
            out[kit_sku] = float(mom)
    return out
