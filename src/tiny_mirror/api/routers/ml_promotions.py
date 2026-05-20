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
@router.get("/caps", response_model=list[CapOut])
async def list_caps(
    only_auto: Annotated[bool | None, Query(description="filter by auto_apply")] = None,
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(db_session),
) -> list[CapOut]:
    repo = MLPromoCapRepository(session)
    rows, _ = await repo.list_all(only_auto=only_auto, limit=limit, offset=offset)
    return [CapOut.model_validate(r) for r in rows]


@router.get("/caps/{sku}", response_model=CapOut)
async def get_cap(
    sku: str,
    session: AsyncSession = Depends(db_session),
) -> CapOut:
    repo = MLPromoCapRepository(session)
    row = await repo.get(sku)
    if row is None:
        raise HTTPException(status_code=404, detail=f"no cap for sku={sku}")
    return CapOut.model_validate(row)


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
@router.get("/costs/{sku}", response_model=list[CostSnapshotOut])
async def get_costs(
    sku: str,
    session: AsyncSession = Depends(db_session),
) -> list[CostSnapshotOut]:
    repo = MLCostsSnapshotRepository(session)
    rows = await repo.get_by_sku(sku)
    return [CostSnapshotOut.model_validate(r) for r in rows]


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
    return [CostSnapshotOut.model_validate(r) for r in rows]


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
