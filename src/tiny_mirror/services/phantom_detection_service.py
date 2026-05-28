"""Daily cron: detects 'phantom' products in the Tiny catalog.

A phantom is a Tiny product created automatically (usually triggered by
an ML order whose listing's SELLER_SKU doesn't map to any cataloged
product). The operator later excludes (situacao='E'), but more orders
keep arriving on the same SKU and the cycle repeats — each detection
absorbs stock from a product that doesn't really exist and distorts
the FL inventory.

Detection criterion (need ANY of):
  - SKU has >= 2 products with situacao='E' (excluded) — multiple duplicates
    is a strong phantom signal even if our DB has no orders for the SKU.
  - SKU has >= 1 excluded AND >= 1 order_item from Mercado Livre — phantom
    that already absorbed sales.

  We deliberately do NOT require `orders_ml_count >= 1`: Tiny sometimes
  creates phantoms via channels whose order_items don't reach our DB (kit
  explosions, third-party integrations), so any SKU with multiple excluded
  copies is suspicious regardless of our local order trail.

Per phantom SKU, we record:
  - active vs excluded counts
  - units drained on ML
  - first/last sale dates
  - forensic snapshot (descriptions, latest orders) for the operator to
    investigate which ML listing originated the phantoms

Read-only against external systems. Only writes to phantom_products_log.
"""

from __future__ import annotations

from typing import Any

import structlog
from sqlalchemy import text

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.repositories.phantom_products_log_repository import (
    PhantomProductsLogRepository,
)
from tiny_mirror.infrastructure.repositories.sync_log_repository import SyncLogRepository

logger = structlog.get_logger(__name__)


class PhantomDetectionService:
    async def run_detection(self, sync_log_id: int) -> None:
        """Scan products + orders for phantom SKUs and write one row per SKU.

        A single pass — all queries are cheap (indexed joins). Counts as one
        processed item per phantom detected; failures inside a single SKU don't
        abort the run.
        """
        logger.info("Phantom detection job started", sync_log_id=sync_log_id)

        candidates = await self._load_candidates()
        logger.info("Phantom candidates found", count=len(candidates))

        recorded = 0
        failed = 0
        for cand in candidates:
            try:
                await self._record_one(sync_log_id, cand)
                recorded += 1
            except Exception as exc:
                failed += 1
                logger.warning(
                    "Phantom record failed, continuing",
                    sku=cand.get("sku"),
                    error=str(exc),
                )

        async with AsyncSessionLocal() as session:
            sync_logs = SyncLogRepository(session)
            for _ in range(recorded):
                await sync_logs.increment_processed(sync_log_id)
            for _ in range(failed):
                await sync_logs.increment_failed(sync_log_id)
            await sync_logs.try_finalize(sync_log_id)

        logger.info(
            "Phantom detection completed",
            sync_log_id=sync_log_id,
            recorded=recorded,
            failed=failed,
            total=len(candidates),
        )

    # ------------------------------------------------------------------
    async def _load_candidates(self) -> list[dict[str, Any]]:
        """Return one row per SKU flagged as a phantom candidate.

        A SKU is flagged when it has at least one excluded duplicate AND
        either (a) >=2 excluded duplicates total — repeated phantom creation
        is itself the signal — or (b) at least one Mercado Livre order_item
        that absorbed sales on the SKU. Single isolated exclusions (typo
        fixes, manual cleanup) are ignored.

        Skips test SKUs and empty SKUs. Active tiny_id is the first non-excluded
        copy (A preferred over I). Falls back to a LEFT JOIN on order_counts so
        SKUs with no orders in our DB still come through.
        """
        sql = text(
            """
            WITH order_counts AS (
                SELECT oi.product_sku,
                       COUNT(DISTINCT oi.order_tiny_id) FILTER (
                           WHERE o.ecommerce_name LIKE 'Mercado Livre%'
                       ) AS orders_ml_count,
                       SUM(oi.quantity) FILTER (
                           WHERE o.ecommerce_name LIKE 'Mercado Livre%'
                       )::int AS units_ml,
                       MIN(o.order_date) FILTER (
                           WHERE o.ecommerce_name LIKE 'Mercado Livre%'
                       ) AS first_sale,
                       MAX(o.order_date) FILTER (
                           WHERE o.ecommerce_name LIKE 'Mercado Livre%'
                       ) AS last_sale
                FROM order_items oi
                JOIN orders o ON o.tiny_id = oi.order_tiny_id
                WHERE oi.product_sku <> ''
                GROUP BY oi.product_sku
            ),
            -- Stock drain over 30d aggregated across every tiny_id sharing the SKU.
            -- This is the ground truth for "stock is being absorbed" — kit
            -- sales decrement components in Tiny but never write to our
            -- order_items, so order_counts misses them entirely.
            stock_drain AS (
                SELECT
                    product_sku,
                    SUM(GREATEST(first_bal - last_bal, 0))::int AS units_drained,
                    MIN(first_date) AS first_seen,
                    MAX(last_date)  AS last_seen
                FROM (
                    SELECT DISTINCT
                        product_sku, product_tiny_id, deposit_name,
                        FIRST_VALUE(balance) OVER w_asc  AS first_bal,
                        FIRST_VALUE(balance) OVER w_desc AS last_bal,
                        FIRST_VALUE(snapshot_date) OVER w_asc  AS first_date,
                        FIRST_VALUE(snapshot_date) OVER w_desc AS last_date
                    FROM stock_history
                    WHERE snapshot_date >= CURRENT_DATE - INTERVAL '30 days'
                    WINDOW
                        w_asc  AS (PARTITION BY product_sku, product_tiny_id, deposit_name
                                   ORDER BY snapshot_date ASC),
                        w_desc AS (PARTITION BY product_sku, product_tiny_id, deposit_name
                                   ORDER BY snapshot_date DESC)
                ) d
                GROUP BY product_sku
            ),
            sku_summary AS (
                SELECT
                    p.sku,
                    array_agg(p.tiny_id) FILTER (WHERE p.situation = 'E') AS excluded_ids,
                    -- prefer active over inactive when picking the "real" one
                    (array_agg(p.tiny_id ORDER BY
                        CASE p.situation WHEN 'A' THEN 0 WHEN 'I' THEN 1 ELSE 2 END
                    ) FILTER (WHERE p.situation IN ('A', 'I')))[1] AS active_id
                FROM products p
                WHERE p.sku IS NOT NULL AND p.sku <> ''
                  AND p.sku NOT LIKE 'SKU-TEST%'
                GROUP BY p.sku
            )
            SELECT
                s.sku,
                s.active_id,
                COALESCE(s.excluded_ids, ARRAY[]::bigint[]) AS excluded_ids,
                COALESCE(oc.orders_ml_count, 0) AS orders_ml_count,
                -- Effective units = max(order_items count, stock drain over 30d).
                -- Order items undercounts when the SKU only ships as a kit
                -- component; stock drain catches the latent drain too.
                GREATEST(
                    COALESCE(oc.units_ml, 0),
                    COALESCE(sd.units_drained, 0)
                ) AS units_ml,
                COALESCE(oc.first_sale, sd.first_seen) AS first_sale,
                COALESCE(oc.last_sale,  sd.last_seen)  AS last_sale
            FROM sku_summary s
            LEFT JOIN order_counts oc ON oc.product_sku = s.sku
            LEFT JOIN stock_drain  sd ON sd.product_sku = s.sku
            WHERE s.excluded_ids IS NOT NULL
              AND array_length(s.excluded_ids, 1) >= 1
              AND (
                   array_length(s.excluded_ids, 1) >= 2
                   OR COALESCE(oc.orders_ml_count, 0) >= 1
                   OR COALESCE(sd.units_drained, 0)  >= 1
              )
            ORDER BY
                -- critical (no active) first, then highest exclusion count, then most units sold
                (s.active_id IS NULL) DESC,
                array_length(s.excluded_ids, 1) DESC,
                GREATEST(COALESCE(oc.units_ml, 0), COALESCE(sd.units_drained, 0)) DESC,
                s.sku;
            """
        )
        async with AsyncSessionLocal() as session:
            result = await session.execute(sql)
            rows = result.all()
        return [
            {
                "sku": r[0],
                "active_id": int(r[1]) if r[1] is not None else None,
                "excluded_ids": [int(x) for x in (r[2] or [])],
                "orders_ml_count": int(r[3]),
                "units_ml": int(r[4]),
                "first_sale": r[5],
                "last_sale": r[6],
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    async def _record_one(self, sync_log_id: int, cand: dict[str, Any]) -> None:
        """Build forensic payload for one phantom + persist."""
        investigation = await self._build_investigation(cand)
        async with AsyncSessionLocal() as session:
            repo = PhantomProductsLogRepository(session)
            await repo.record(
                detection_run_id=sync_log_id,
                sku=cand["sku"],
                product_active_tiny_id=cand["active_id"],
                excluded_tiny_ids=cand["excluded_ids"],
                orders_ml_count=cand["orders_ml_count"],
                units_ml=cand["units_ml"],
                first_sale_date=cand["first_sale"],
                last_sale_date=cand["last_sale"],
                investigation_payload=investigation,
            )

    # ------------------------------------------------------------------
    async def _build_investigation(self, cand: dict[str, Any]) -> dict[str, Any]:
        """Capture context: descriptions of all products with this SKU + sample
        of recent ML orders that hit it. Resilient: per-query failures populate
        an *_error key but don't abort the whole record.
        """
        sku = cand["sku"]
        out: dict[str, Any] = {
            "sku": sku,
            "severity": "critical" if cand["active_id"] is None else "normal",
        }
        async with AsyncSessionLocal() as session:
            try:
                products_result = await session.execute(
                    text(
                        """
                        SELECT tiny_id, situation, description, type,
                               COALESCE(created_at_tiny, created_at)::date::text AS created
                        FROM products
                        WHERE sku = :sku
                        ORDER BY situation, tiny_id;
                        """
                    ),
                    {"sku": sku},
                )
                out["products_in_tiny"] = [
                    {
                        "tiny_id": int(r[0]),
                        "situation": r[1],
                        "description": r[2],
                        "type": r[3],
                        "created": r[4],
                    }
                    for r in products_result.all()
                ]
            except Exception as exc:
                out["products_in_tiny_error"] = str(exc)

            try:
                orders_result = await session.execute(
                    text(
                        """
                        SELECT o.tiny_id, o.ecommerce_order_number, o.order_date::text,
                               o.ecommerce_name, oi.quantity::int, o.situation
                        FROM order_items oi
                        JOIN orders o ON o.tiny_id = oi.order_tiny_id
                        WHERE oi.product_sku = :sku
                          AND o.ecommerce_name LIKE 'Mercado Livre%'
                        ORDER BY o.order_date DESC
                        LIMIT 20;
                        """
                    ),
                    {"sku": sku},
                )
                out["recent_ml_orders"] = [
                    {
                        "tiny_id": int(r[0]),
                        "ecommerce_order_number": r[1],
                        "order_date": r[2],
                        "ecommerce_name": r[3],
                        "quantity": int(r[4]),
                        "situation": int(r[5]) if r[5] is not None else None,
                    }
                    for r in orders_result.all()
                ]
            except Exception as exc:
                out["recent_ml_orders_error"] = str(exc)

            # Stock history snapshot across every tiny_id sharing this SKU.
            # Kit-sale phantoms don't show up in order_items (the order line is
            # the parent kit), but Tiny still decrements the component balance
            # — so stock_history is the ground truth for "this SKU is draining".
            try:
                stock_result = await session.execute(
                    text(
                        """
                        SELECT product_tiny_id, snapshot_date::text, deposit_name, balance
                        FROM stock_history
                        WHERE product_sku = :sku
                          AND snapshot_date >= CURRENT_DATE - INTERVAL '30 days'
                        ORDER BY snapshot_date DESC, product_tiny_id, deposit_name
                        LIMIT 400;
                        """
                    ),
                    {"sku": sku},
                )
                rows = stock_result.all()
                out["stock_history"] = [
                    {
                        "tiny_id": int(r[0]),
                        "snapshot_date": r[1],
                        "deposit_name": r[2],
                        "balance": int(r[3]),
                    }
                    for r in rows
                ]
                # Net drain over the window: sum of (earliest - latest) per
                # (tiny_id, deposit). Positive = stock left the catalog.
                drain_result = await session.execute(
                    text(
                        """
                        WITH ranked AS (
                            SELECT product_tiny_id, deposit_name, balance, snapshot_date,
                                   FIRST_VALUE(balance) OVER (
                                       PARTITION BY product_tiny_id, deposit_name
                                       ORDER BY snapshot_date ASC
                                   ) AS first_bal,
                                   FIRST_VALUE(balance) OVER (
                                       PARTITION BY product_tiny_id, deposit_name
                                       ORDER BY snapshot_date DESC
                                   ) AS last_bal
                            FROM stock_history
                            WHERE product_sku = :sku
                              AND snapshot_date >= CURRENT_DATE - INTERVAL '30 days'
                        )
                        SELECT COALESCE(SUM(GREATEST(first_bal - last_bal, 0)), 0)::int
                        FROM (SELECT DISTINCT product_tiny_id, deposit_name, first_bal, last_bal FROM ranked) d;
                        """
                    ),
                    {"sku": sku},
                )
                out["units_drained_stock_30d"] = int(drain_result.scalar_one() or 0)
            except Exception as exc:
                out["stock_history_error"] = str(exc)

        return out
