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
)
from tiny_mirror.services.ml_promotion_service import MLPromotionService

router = APIRouter()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
class CapIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    sku: str
    max_seller_share_pct: Decimal = Field(..., gt=0, le=100)
    margin_floor_price: Decimal | None = Field(default=None, ge=0)
    auto_apply: bool | None = None
    freight_band_opt: bool | None = None
    excluded_promo_types: list[str] | None = None
    notes: str | None = None


class CapsBulkIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    items: list[CapIn]
    updated_by: str | None = None


class CapMLBOut(BaseModel):
    """Per-MLB detail exposed inside CapOut.mlbs[] for the collapsible row."""

    model_config = ConfigDict(from_attributes=True)
    mlb_id: str
    logistic_type: str | None = None  # fulfillment / cross_docking / etc.
    listing_status: str | None = None  # active / paused / closed
    # From ml_costs_snapshot.
    base_cost: Decimal | None = None
    commission_pct: Decimal | None = None
    list_price: Decimal | None = None
    sheet_promo_price: Decimal | None = None
    freight_bands: Any | None = None
    margin_at_floor_value: Decimal | None = None
    margin_at_floor_pct: Decimal | None = None
    # From ml_catalog_status (price_to_win).
    catalog_status: str | None = None
    visit_share: str | None = None
    current_price: Decimal | None = None
    price_to_win: Decimal | None = None
    winner_price: Decimal | None = None
    competitors_sharing_first_place: int | None = None


class CapOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    sku: str
    max_seller_share_pct: Decimal
    margin_floor_price: Decimal | None
    auto_apply: bool
    freight_band_opt: bool
    excluded_promo_types: list[str]
    notes: str | None
    updated_by: str | None
    updated_at: datetime
    # Joined from ml_costs_snapshot (latest active one) so the dashboard
    # table can show profitability without a per-SKU round-trip.
    list_price: Decimal | None = None
    margin_at_floor_value: Decimal | None = None
    margin_at_floor_pct: Decimal | None = None
    # Full pricing inputs so the dashboard can recompute margin live
    # while the operator drags the cap slider in the table.
    base_cost: Decimal | None = None
    commission_pct: Decimal | None = None
    difal_pct: Decimal | None = None
    freight_bands: Any | None = None
    # Joined from ml_catalog_status (worst-case MLB) so the dashboard can
    # show the buy-box context without an extra round-trip.
    catalog_listing: bool | None = None
    catalog_status: str | None = None
    visit_share: str | None = None
    current_price: Decimal | None = None
    price_to_win: Decimal | None = None
    winner_price: Decimal | None = None
    competitors_sharing_first_place: int | None = None
    # True when the SKU has at least one active MLB in ml_listings.
    # False = the cap is orphan (snapshot exists from a past listing, but
    # the listing is gone). The dashboard renders this as "Sem anúncio"
    # instead of a misleading "—".
    has_active_listing: bool | None = None
    # Full per-MLB detail (one row per active MLB). Populated only by
    # endpoints that include catalog data (single-SKU and list_caps);
    # default empty for backward compat with consumers that ignore it.
    mlbs: list[CapMLBOut] = Field(default_factory=list)


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
    """Attach list_price + margin-at-floor to a cap row.

    The margin uses the same pricing formula as the recompute job so
    numbers shown in the dashboard match the engine exactly. Snapshot
    lookup is by SKU; if multiple MLBs exist we use the most expensive
    base_cost (the worst-case MLB the cap has to protect).
    """
    from tiny_mirror.config import settings
    from tiny_mirror.services.pricing_service import PricingDataError, margin_at_price

    snap_repo = MLCostsSnapshotRepository(session)
    snaps = await snap_repo.get_by_sku(cap.sku)

    out = CapOut.model_validate(cap)
    if not snaps:
        return out

    # Pick the worst-case MLB (highest base_cost) — the same conservative
    # choice cap_recompute_service makes.
    usable = [
        s
        for s in snaps
        if s.base_cost is not None
        and s.commission_pct is not None
        and s.list_price is not None
        and s.freight_bands
    ]
    if not usable:
        return out
    snap = max(usable, key=lambda s: s.base_cost or Decimal(0))
    out.list_price = snap.list_price
    out.base_cost = snap.base_cost
    out.commission_pct = snap.commission_pct
    out.freight_bands = snap.freight_bands
    out.difal_pct = Decimal(str(settings.margin_difal_pct))

    floor_price = cap.margin_floor_price
    if floor_price is None:
        floor_price = snap.sheet_promo_price
    if floor_price is None or snap.base_cost is None or snap.commission_pct is None:
        return out
    try:
        breakdown = margin_at_price(
            price=floor_price,
            base_cost=snap.base_cost,
            commission_pct=snap.commission_pct,
            freight_bands=snap.freight_bands,
        )
    except PricingDataError:
        return out
    out.margin_at_floor_value = breakdown.margin_value
    out.margin_at_floor_pct = breakdown.margin_pct

    # Catalog context (buy-box). Pick the WORST-CASE MLB — competing /
    # losing beats winning so the dashboard surfaces problems first.
    from sqlalchemy import select

    from tiny_mirror.infrastructure.orm.models import MLCatalogStatusORM

    cat_rows = (
        (await session.execute(select(MLCatalogStatusORM).where(MLCatalogStatusORM.sku == cap.sku)))
        .scalars()
        .all()
    )
    if cat_rows:
        rank = {
            "competing": 0,
            "losing": 0,
            "sharing_first_place": 1,
            "winning": 2,
            "not_listed": 3,
            "unknown": 4,
        }
        cat = min(cat_rows, key=lambda r: rank.get(r.status or "unknown", 5))
        out.catalog_listing = bool(cat.catalog_listing)
        out.catalog_status = cat.status
        out.visit_share = cat.visit_share
        out.current_price = cat.current_price
        out.price_to_win = cat.price_to_win
        out.winner_price = cat.winner_price
        out.competitors_sharing_first_place = cat.competitors_sharing_first_place

    # has_active_listing: distinguishes "no MLB at all" (orphan cap) from
    # "MLB exists but ML returned not_listed". Without this the UI shows
    # "—" for both, which made the operator chase 107 phantom caps.
    from tiny_mirror.infrastructure.orm.models import MLListingORM

    listings_rows = (
        (
            await session.execute(
                select(MLListingORM).where(
                    MLListingORM.sku == cap.sku, MLListingORM.status == "active"
                )
            )
        )
        .scalars()
        .all()
    )
    out.has_active_listing = bool(listings_rows)

    # Per-MLB detail for the collapsible row. Build a {mlb_id -> snap/cat}
    # lookup so a single iteration over active listings can join everything.
    snaps_by_mlb = {s.mlb_id: s for s in snaps}
    cats_by_mlb = {c.mlb_id: c for c in cat_rows} if cat_rows else {}

    mlb_rows: list[CapMLBOut] = []
    for lst in listings_rows:
        s = snaps_by_mlb.get(lst.mlb_id)
        c = cats_by_mlb.get(lst.mlb_id)
        margin_at_floor_value: Decimal | None = None
        margin_at_floor_pct: Decimal | None = None
        floor_for_margin = cap.margin_floor_price or (s.sheet_promo_price if s else None)
        if (
            s is not None
            and s.base_cost is not None
            and s.commission_pct is not None
            and s.freight_bands
            and floor_for_margin is not None
        ):
            try:
                br = margin_at_price(
                    price=floor_for_margin,
                    base_cost=s.base_cost,
                    commission_pct=s.commission_pct,
                    freight_bands=s.freight_bands,
                )
                margin_at_floor_value = br.margin_value
                margin_at_floor_pct = br.margin_pct
            except PricingDataError:
                pass

        mlb_rows.append(
            CapMLBOut(
                mlb_id=lst.mlb_id,
                logistic_type=lst.logistic_type,
                listing_status=lst.status,
                base_cost=s.base_cost if s else None,
                commission_pct=s.commission_pct if s else None,
                list_price=s.list_price if s else None,
                sheet_promo_price=s.sheet_promo_price if s else None,
                freight_bands=s.freight_bands if s else None,
                margin_at_floor_value=margin_at_floor_value,
                margin_at_floor_pct=margin_at_floor_pct,
                catalog_status=c.status if c else None,
                visit_share=c.visit_share if c else None,
                current_price=c.current_price if c else None,
                price_to_win=c.price_to_win if c else None,
                winner_price=c.winner_price if c else None,
                competitors_sharing_first_place=(c.competitors_sharing_first_place if c else None),
            )
        )
    out.mlbs = mlb_rows
    return out


@router.get("/caps", response_model=list[CapOut])
async def list_caps(
    only_auto: Annotated[bool | None, Query(description="filter by auto_apply")] = None,
    include_orphans: Annotated[
        bool,
        Query(
            description=(
                "Include caps without any active MLB in ml_listings. "
                "False (default) hides them — they cannot be acted on."
            ),
        ),
    ] = False,
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(db_session),
) -> list[CapOut]:
    repo = MLPromoCapRepository(session)
    rows, _ = await repo.list_all(only_auto=only_auto, limit=limit, offset=offset)
    enriched = [await _enrich_cap(session, r) for r in rows]
    if not include_orphans:
        enriched = [c for c in enriched if c.has_active_listing]
    return enriched


@router.get("/caps/{sku}", response_model=CapOut)
async def get_cap(
    sku: str,
    session: AsyncSession = Depends(db_session),
) -> CapOut:
    repo = MLPromoCapRepository(session)
    row = await repo.get(sku)
    if row is None:
        raise HTTPException(status_code=404, detail=f"no cap for sku={sku}")
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
            sku=item.sku,
            max_seller_share_pct=item.max_seller_share_pct,
            margin_floor_price=item.margin_floor_price,
            auto_apply=item.auto_apply,
            freight_band_opt=item.freight_band_opt,
            excluded_promo_types=item.excluded_promo_types,
            notes=item.notes,
            updated_by=body.updated_by,
        )
        out.append(CapOut.model_validate(row))
    await session.commit()
    return out


@router.delete("/caps/{sku}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_cap(
    sku: str,
    session: AsyncSession = Depends(db_session),
) -> None:
    repo = MLPromoCapRepository(session)
    deleted = await repo.delete(sku)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"no cap for sku={sku}")
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
    session: AsyncSession = Depends(db_session),
    service: MLPromotionService = Depends(_service_dep),
) -> dict[str, int]:
    """Itera por todos os MLBs ativos e refresca o snapshot do GAS endpoint.

    Idempotente; commit por batches de 25 pra não bloquear conexão.
    Usado pelo cron diário ml-costs-refresh-daily.
    """
    listings = MLListingRepository(session)
    pairs = await listings.get_all_active_mlb_ids()
    ok = 0
    errors = 0
    for i, (mlb_id, _sku) in enumerate(pairs):
        try:
            result = await service.refresh_costs_for_mlb(session, mlb_id)
            if result is None or (isinstance(result, dict) and result.get("error")):
                errors += 1
            else:
                ok += 1
        except Exception:
            errors += 1
        # Commit em batches de 25 para liberar locks
        if (i + 1) % 25 == 0:
            await session.commit()
    await session.commit()
    return {"total": len(pairs), "ok": ok, "errors": errors}


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
    """Iterate every SKU with auto_apply=true and run evaluate. Returns count summary."""
    repo = MLPromoCapRepository(session)
    rows, _ = await repo.list_all(only_auto=True, limit=1000)
    summary: dict[str, int] = {}
    for cap in rows:
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

    for cap in rows:
        if cap.max_seller_share_pct == 0:
            skus_with_zero_cap += 1
            action_counts["skip_zero_cap"] = action_counts.get("skip_zero_cap", 0) + 1
            continue
        try:
            results = await service.analyze_sku_dry(session, cap.sku)
        except Exception as exc:  # pragma: no cover — surface error in report
            errors.append({"sku": cap.sku, "error": str(exc)[:200]})
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
                        "sku": cap.sku,
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
                        "sku": cap.sku,
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
                            "sku": cap.sku,
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
