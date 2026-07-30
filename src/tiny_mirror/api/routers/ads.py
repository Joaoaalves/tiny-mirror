"""Read-only ADS metrics routes (Mercado Ads / Product Ads bridge).

Built for the fleet's ads analyst (Dante) — spec in the OpenClaw
workspace (`docs/pedido-erp-ads.md`): campaigns, per-campaign daily
series, per-ad metrics and the account daily summary, all behind the
same X-API-Key as every other route. Strictly GET; nothing here can
mutate campaigns on Mercado Livre.

Every response carries ``collected_at`` because ads metrics mature
retroactively (attribution window) — consumers must qualify numbers by
collection time.
"""

from __future__ import annotations

from collections.abc import Awaitable
from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, status

from tiny_mirror.services.ml_ads_service import (
    MLAdsService,
    MLAdsUpstreamError,
    validate_window,
)

router = APIRouter()

DEFAULT_WINDOW_DAYS = 30


def _service_dep(request: Request) -> MLAdsService:
    service: MLAdsService | None = getattr(request.app.state, "ml_ads_service", None)
    if service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="ML ads service not configured (ML_CLIENT_ID missing)",
        )
    return service


def _resolve_window(date_from: date | None, date_to: date | None) -> tuple[date, date]:
    """Default = the last 30 days ending today; validate ML's 90-day cap."""
    resolved_to = date_to or date.today()
    resolved_from = date_from or resolved_to - timedelta(days=DEFAULT_WINDOW_DAYS)
    try:
        validate_window(resolved_from, resolved_to)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        ) from exc
    return resolved_from, resolved_to


async def _proxy(coro: Awaitable[dict[str, Any]]) -> dict[str, Any]:
    try:
        return await coro
    except MLAdsUpstreamError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=exc.message,
        ) from exc


@router.get("/campaigns")
async def list_campaigns(
    request: Request,
    date_from: date | None = Query(None, alias="from"),
    date_to: date | None = Query(None, alias="to"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """Product Ads campaigns with window metrics (id, name, status, budget,
    strategy, acos/roas targets + clicks/prints/cost/acos/amounts)."""
    resolved_from, resolved_to = _resolve_window(date_from, date_to)
    service = _service_dep(request)
    return await _proxy(
        service.list_campaigns(resolved_from, resolved_to, limit=limit, offset=offset)
    )


@router.get("/campaigns/{campaign_id}/metrics")
async def campaign_metrics(
    campaign_id: int,
    request: Request,
    date_from: date | None = Query(None, alias="from"),
    date_to: date | None = Query(None, alias="to"),
    granularity: str = Query("day"),
) -> dict[str, Any]:
    """Daily series for one campaign (plus impression-share extras)."""
    if granularity != "day":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="only granularity=day is supported",
        )
    resolved_from, resolved_to = _resolve_window(date_from, date_to)
    service = _service_dep(request)
    return await _proxy(service.campaign_daily_metrics(campaign_id, resolved_from, resolved_to))


@router.get("/items/metrics")
async def items_metrics(
    request: Request,
    date_from: date | None = Query(None, alias="from"),
    date_to: date | None = Query(None, alias="to"),
    campaign_id: int | None = Query(None, alias="campaignId"),
    item_ids: str | None = Query(
        None,
        alias="itemIds",
        description="Comma-separated MLBs (resolved via ad_groups filters[item_ids])",
    ),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """Per-ad (ad group) metrics — where a bleeding SKU shows up.

    Traditional listings expose the MLB in ``ad_group_external_id``;
    catalog listings expose the catalog product id there instead.
    """
    resolved_from, resolved_to = _resolve_window(date_from, date_to)
    service = _service_dep(request)
    ids = [i.strip() for i in item_ids.split(",") if i.strip()] if item_ids else None
    return await _proxy(
        service.items_metrics(
            resolved_from,
            resolved_to,
            campaign_id=campaign_id,
            item_ids=ids,
            limit=limit,
            offset=offset,
        )
    )


@router.get("/summary/daily")
async def summary_daily(
    request: Request,
    date_from: date | None = Query(None, alias="from"),
    date_to: date | None = Query(None, alias="to"),
) -> dict[str, Any]:
    """Account-wide daily aggregate (investment, attributed revenue, ACOS)."""
    resolved_from, resolved_to = _resolve_window(date_from, date_to)
    service = _service_dep(request)
    return await _proxy(service.daily_summary(resolved_from, resolved_to))
