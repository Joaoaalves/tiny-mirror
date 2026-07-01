"""Repository for the Estoque Full feature.

Two responsibilities:

1. **Read the per-MLB metrics** that every tab needs (one row per FULFILLMENT
   listing): FL stock = the listing's own ``available_quantity`` at ML (units apt
   to sell for THAT anúncio — a kit reports its own sellable count, not the SKU
   pool), galpão + coverage classification (``mv_coverage``, SKU-level), per-MLB
   30d sales/ritmo, 90d revenue (for the ABC curve), product age and the current
   active promotion. Stock/sales/promo are per MLB; galpão/status are SKU-level.

2. **Persist the workflow state** — ``ml_fl_tracking`` (+ snapshots),
   ``ml_fl_tracking_events`` (timeline/annotations) and ``ml_fl_dismissals``
   (ignore/remove). Nothing here writes to Tiny or ML.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.orm.models import (
    MLFlDismissalORM,
    MLFlTrackingEventORM,
    MLFlTrackingORM,
)

# One row per fulfillment MLB with everything the tabs need. ABC is computed in
# the service (needs the full-universe ranking); this only supplies rev_90d.
_METRICS_SQL = text(
    """
    WITH fl AS (
        SELECT mlb_id, sku, title, permalink, available_quantity
        FROM ml_listings
        WHERE logistic_type = 'fulfillment' AND sku IS NOT NULL
    ),
    sales30 AS (
        SELECT mlb_id, SUM(qty)::int AS sold_30d
        FROM ml_sales_daily
        WHERE sale_date >= CURRENT_DATE - INTERVAL '30 days'
          AND sale_date <  CURRENT_DATE
        GROUP BY mlb_id
    ),
    rev90 AS (
        SELECT mlb_id, COALESCE(SUM(revenue), 0)::numeric AS rev_90d
        FROM ml_sales_daily
        WHERE sale_date >= CURRENT_DATE - INTERVAL '90 days'
          AND sale_date <  CURRENT_DATE
        GROUP BY mlb_id
    ),
    promo AS (
        SELECT DISTINCT ON (mlb_id)
            mlb_id, price, original_price, promotion_type,
            seller_percentage, meli_percentage
        FROM ml_promotions
        WHERE status = 'started'
        ORDER BY mlb_id, price ASC
    )
    SELECT
        fl.mlb_id,
        fl.sku,
        fl.title,
        fl.permalink,
        -- Estoque FL POR ANÚNCIO: available_quantity do próprio anúncio no ML
        -- (o que ele tem apto a vender). NÃO usar mv_coverage.stock_full_ml, que
        -- é o pool agregado por SKU em unidades (SKU base + combos + kits somados)
        -- e infla listagens de kit (ex.: kit mostrava 813 em vez de 18 reais).
        COALESCE(fl.available_quantity, 0) AS stock_full,
        COALESCE(mc.stock_galpao, 0)    AS stock_galpao,
        mc.status_base                  AS status_base,
        COALESCE(s.sold_30d, 0)         AS sold_30d,
        COALESCE(r.rev_90d, 0)          AS rev_90d,
        p.created_at                    AS product_created_at,
        pr.price                        AS promo_price,
        pr.original_price               AS promo_original,
        pr.promotion_type               AS promo_type,
        pr.seller_percentage            AS promo_seller_pct,
        pr.meli_percentage              AS promo_meli_pct
    FROM fl
    LEFT JOIN mv_coverage mc ON mc.sku = fl.sku
    LEFT JOIN sales30 s      ON s.mlb_id = fl.mlb_id
    LEFT JOIN rev90 r        ON r.mlb_id = fl.mlb_id
    LEFT JOIN products p     ON p.sku = fl.sku
    LEFT JOIN promo pr       ON pr.mlb_id = fl.mlb_id
    """
)

# Per-MLB average daily sales between two dates (for "resultado desde…" columns).
_AVG_SINCE_SQL = text(
    """
    SELECT COALESCE(SUM(qty), 0)::int AS units
    FROM ml_sales_daily
    WHERE mlb_id = :mlb_id
      AND sale_date >= :since
      AND sale_date <  CURRENT_DATE
    """
)


class MLFlTrackingRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # ── metrics ────────────────────────────────────────────────────────────
    async def fetch_metrics(self) -> list[dict[str, Any]]:
        """One row per fulfillment MLB with all live metrics."""
        rows = await self._session.execute(_METRICS_SQL)
        return [dict(m) for m in rows.mappings().all()]

    async def units_sold_since(self, mlb_id: str, since: datetime) -> int:
        r = await self._session.execute(_AVG_SINCE_SQL, {"mlb_id": mlb_id, "since": since.date()})
        return int(r.scalar_one() or 0)

    # ── tracking ───────────────────────────────────────────────────────────
    async def get_tracking(self, tracking_id: int) -> MLFlTrackingORM | None:
        return await self._session.get(MLFlTrackingORM, tracking_id)

    async def get_active_by_mlb(self, mlb_id: str) -> MLFlTrackingORM | None:
        r = await self._session.execute(
            select(MLFlTrackingORM).where(
                MLFlTrackingORM.mlb_id == mlb_id,
                MLFlTrackingORM.status == "tracking",
            )
        )
        return r.scalar_one_or_none()

    async def list_tracking(self, status: str) -> list[MLFlTrackingORM]:
        r = await self._session.execute(
            select(MLFlTrackingORM)
            .where(MLFlTrackingORM.status == status)
            .order_by(MLFlTrackingORM.moved_at.desc())
        )
        return list(r.scalars().all())

    async def create_tracking(self, **values: Any) -> MLFlTrackingORM:
        row = MLFlTrackingORM(**values)
        self._session.add(row)
        await self._session.flush()
        return row

    async def delete_tracking(self, tracking_id: int) -> bool:
        row = await self.get_tracking(tracking_id)
        if row is None:
            return False
        await self._session.delete(row)
        await self._session.flush()
        return True

    # ── events ─────────────────────────────────────────────────────────────
    async def add_event(
        self,
        tracking_id: int,
        *,
        event_type: str,
        author: str | None = None,
        note: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> MLFlTrackingEventORM:
        ev = MLFlTrackingEventORM(
            tracking_id=tracking_id,
            event_type=event_type,
            author=author,
            note=note,
            payload=payload,
        )
        self._session.add(ev)
        await self._session.flush()
        return ev

    async def list_events(self, tracking_id: int) -> list[MLFlTrackingEventORM]:
        r = await self._session.execute(
            select(MLFlTrackingEventORM)
            .where(MLFlTrackingEventORM.tracking_id == tracking_id)
            .order_by(MLFlTrackingEventORM.created_at.asc())
        )
        return list(r.scalars().all())

    async def list_events_for(
        self, tracking_ids: list[int]
    ) -> dict[int, list[MLFlTrackingEventORM]]:
        """Events for many trackings at once (avoids N+1 on the list endpoint)."""
        if not tracking_ids:
            return {}
        r = await self._session.execute(
            select(MLFlTrackingEventORM)
            .where(MLFlTrackingEventORM.tracking_id.in_(tracking_ids))
            .order_by(MLFlTrackingEventORM.created_at.asc())
        )
        out: dict[int, list[MLFlTrackingEventORM]] = {}
        for ev in r.scalars().all():
            out.setdefault(ev.tracking_id, []).append(ev)
        return out

    # ── dismissals ─────────────────────────────────────────────────────────
    async def upsert_dismissal(
        self,
        mlb_id: str,
        *,
        kind: str,
        sku: str | None,
        created_by: str | None,
        ignore_days: int,
        now: datetime,
    ) -> MLFlDismissalORM:
        until = now + timedelta(days=ignore_days) if kind == "ignore" else None
        stmt = (
            pg_insert(MLFlDismissalORM)
            .values(
                mlb_id=mlb_id,
                sku=sku,
                kind=kind,
                until=until,
                created_by=created_by,
            )
            .on_conflict_do_update(
                index_elements=["mlb_id"],
                set_={
                    "kind": kind,
                    "until": until,
                    "sku": sku,
                    "created_by": created_by,
                    "created_at": func.now(),
                },
            )
            .returning(MLFlDismissalORM)
        )
        r = await self._session.execute(stmt)
        await self._session.flush()
        return r.scalar_one()

    async def delete_dismissal(self, mlb_id: str) -> bool:
        row = await self._session.get(MLFlDismissalORM, mlb_id)
        if row is None:
            return False
        await self._session.delete(row)
        await self._session.flush()
        return True

    async def list_dismissals(self) -> list[MLFlDismissalORM]:
        r = await self._session.execute(select(MLFlDismissalORM))
        return list(r.scalars().all())

    async def active_tracking_mlbs(self) -> set[str]:
        r = await self._session.execute(
            select(MLFlTrackingORM.mlb_id).where(MLFlTrackingORM.status == "tracking")
        )
        return set(r.scalars().all())
