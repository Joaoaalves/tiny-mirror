"""Live equivalence check: batch caps enrichment == the old per-row enrichment.

``GET /ml-promotions/caps`` used to run ~8 queries per cap (``_enrich_cap`` per
row). It now pre-loads everything in a fixed 7 queries (``_enrich_caps``). The
response must not change, so this test keeps a verbatim copy of the old per-row
function (``_legacy_enrich_cap``) and compares both over EVERY cap row plus the
synthesized capless rows the promoções board asks for, field by field.

It also pins ``GET /caps/summary`` to ``len(GET /caps)`` / ``sum(auto_apply)``.

Read-only: one REPEATABLE READ, READ ONLY transaction (both sides see the same
snapshot even while the syncs write), rolled back at the end. Safe to run
against production.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.api.routers.ml_promotions import (
    CapOut,
    _effective_fees,
    _enrich_caps,
    _stock_by_skus,
    caps_summary,
    list_caps,
)
from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import MLListingORM, MLPromoCapORM
from tiny_mirror.infrastructure.repositories.ml_promo_repository import (
    MLCostsSnapshotRepository,
)

pytestmark = pytest.mark.e2e


async def _legacy_enrich_cap(session: AsyncSession, cap: Any) -> CapOut:
    """Verbatim copy of the per-row ``_enrich_cap`` before the batch rewrite."""
    from tiny_mirror.config import settings
    from tiny_mirror.infrastructure.orm.models import MLCatalogStatusORM
    from tiny_mirror.services.pricing_service import PricingDataError, margin_at_price

    snap_repo = MLCostsSnapshotRepository(session)
    snap = await snap_repo.get(cap.mlb_id)

    out = CapOut.model_validate(cap)
    out.difal_pct = Decimal(str(settings.margin_difal_pct))

    if snap is not None:
        out.list_price = snap.list_price
        out.base_cost = snap.base_cost
        out.sheet_promo_price = snap.sheet_promo_price

    eff_commission_pct, eff_freight_bands, eff_commission_bands = await _effective_fees(
        session, cap.mlb_id, snap
    )
    out.commission_pct = eff_commission_pct
    out.freight_bands = eff_freight_bands
    out.commission_bands = eff_commission_bands

    out.manual_status = (
        await session.execute(
            text("SELECT manual_status FROM products WHERE sku = :sku LIMIT 1"),
            {"sku": cap.sku},
        )
    ).scalar_one_or_none()

    if out.base_cost is None:
        tiny_cost = (
            await session.execute(
                text(
                    "SELECT NULLIF(prices->>'cost_price', '')::numeric FROM products "
                    "WHERE sku = :sku AND NULLIF(prices->>'cost_price', '') IS NOT NULL "
                    "LIMIT 1"
                ),
                {"sku": cap.sku},
            )
        ).scalar_one_or_none()
        if tiny_cost is not None:
            out.base_cost = Decimal(str(tiny_cost))

    floor_price = cap.margin_floor_price or (snap.sheet_promo_price if snap is not None else None)
    if (
        floor_price is not None
        and out.base_cost is not None
        and eff_commission_pct is not None
        and eff_freight_bands
    ):
        try:
            breakdown = margin_at_price(
                price=floor_price,
                base_cost=out.base_cost,
                commission_pct=eff_commission_pct,
                freight_bands=eff_freight_bands,
                commission_bands=eff_commission_bands,
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
        out.warehouse_available = st["warehouse_available"]
        out.full_in_transit = st["pending_full"]
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


@pytest_asyncio.fixture
async def ro_session(live_db: None) -> AsyncIterator[AsyncSession]:
    async with AsyncSessionLocal() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        try:
            yield session
        finally:
            await session.rollback()


async def _all_inputs(session: AsyncSession) -> list[Any]:
    """Every cap row + one synthetic capless cap per active/paused listing without
    a cap — the full population ``/caps?include_orphans&include_capless`` enriches."""
    caps: list[Any] = list(
        (await session.execute(select(MLPromoCapORM).order_by(MLPromoCapORM.sku))).scalars().all()
    )
    have = {c.mlb_id for c in caps}
    listings = (
        (
            await session.execute(
                select(MLListingORM).where(MLListingORM.status.in_(("active", "paused")))
            )
        )
        .scalars()
        .all()
    )
    now = datetime.now(UTC)
    for lst in listings:
        if lst.mlb_id in have:
            continue
        caps.append(
            CapOut(
                mlb_id=lst.mlb_id,
                sku=lst.sku or lst.mlb_id,
                max_seller_share_pct=None,
                margin_floor_price=None,
                auto_apply=False,
                freight_band_opt=False,
                skip_when_winning=False,
                excluded_promo_types=[],
                notes=None,
                updated_by=None,
                updated_at=now,
            )
        )
    return caps


@pytest.mark.asyncio
async def test_batch_enrichment_matches_legacy_per_row(ro_session: AsyncSession) -> None:
    inputs = await _all_inputs(ro_session)
    if not inputs:
        pytest.skip("no caps/listings in this database")
    legacy = [(await _legacy_enrich_cap(ro_session, c)).model_dump() for c in inputs]
    batch = [c.model_dump() for c in await _enrich_caps(ro_session, inputs)]
    assert len(batch) == len(legacy)
    mismatches = [
        (old["mlb_id"], k, old[k], new[k])
        for old, new in zip(legacy, batch, strict=True)
        for k in old
        if old[k] != new[k]
    ]
    assert mismatches == []


@pytest.mark.asyncio
@pytest.mark.parametrize("include_orphans", [False, True])
async def test_summary_matches_caps_listing(
    ro_session: AsyncSession, include_orphans: bool
) -> None:
    caps = await list_caps(
        only_auto=None,
        include_orphans=include_orphans,
        include_capless=False,
        limit=2000,
        offset=0,
        session=ro_session,
    )
    summary = await caps_summary(include_orphans=include_orphans, session=ro_session)
    assert summary.total == len(caps)
    assert summary.auto_apply == sum(1 for c in caps if c.auto_apply)
