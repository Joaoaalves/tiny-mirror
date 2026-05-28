"""Daily cron: detects 'phantom' products in the Tiny catalog.

A phantom is a Tiny product created automatically (usually triggered by
an ML order whose listing's SELLER_SKU doesn't map to any cataloged
product). The operator later excludes (situacao='E'), but more orders
keep arriving on the same SKU and the cycle repeats — each detection
absorbs stock from a product that doesn't really exist and distorts
the FL inventory.

Detection criterion (need ANY of):
  - SKU has >= 2 products with situacao='E' (excluded) — multiple duplicates
    is a strong phantom signal on its own.
  - SKU has >= 1 excluded AND >= 1 ML invoice line in ``invoice_items`` —
    phantom that already absorbed real sales.

Source-of-truth for ML sales is ``invoice_items`` (mirrors the ``itens``
array of ``GET /notas/{id}``), populated by the incremental NF sync plus
the ``backfill_invoice_items.py`` script. ``order_items`` is intentionally
NOT consulted: it only carries the parent kit SKU, so kit-component
phantoms would be invisible.

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

        # Single-pass cron: no fan-out, so flip 'running' → 'completed'
        # synchronously. try_finalize requires metadata.total_enqueued and
        # would silently leak the sync_log until the stale watchdog kicked in.
        async with AsyncSessionLocal() as session:
            await SyncLogRepository(session).update_sync_log_complete(
                sync_log_id, items_processed=recorded, items_failed=failed
            )

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
        # invoice_items is the ground truth for ML sales by SKU. order_items
        # only carries the parent kit SKU, so kit-component sales (e.g.
        # CAMP-CNJ-FACPEG inside KIT-FACAPEG-ESCV-GARR) are invisible there.
        # invoice_items mirrors GET /notas/{id}.itens which records the
        # actual decremented SKU per line. We aggregate across every tiny_id
        # sharing the SKU (so phantom duplicates that absorbed sales also
        # count) and filter on the originating invoice's ecommerce channel.
        sql = text(
            """
            WITH invoice_sales AS (
                SELECT
                    ii.product_sku,
                    count(DISTINCT ii.invoice_tiny_id) AS invoices_ml_count,
                    SUM(ii.quantity)::int AS units_ml,
                    MIN(inv.issue_date)   AS first_sale,
                    MAX(inv.issue_date)   AS last_sale
                FROM invoice_items ii
                JOIN invoices inv ON inv.tiny_id = ii.invoice_tiny_id
                WHERE ii.product_sku <> ''
                  AND inv.ecommerce IS NOT NULL
                  AND inv.ecommerce->>'nome' LIKE 'Mercado Livre%'
                GROUP BY ii.product_sku
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
                COALESCE(isales.invoices_ml_count, 0) AS orders_ml_count,
                COALESCE(isales.units_ml, 0)          AS units_ml,
                isales.first_sale AS first_sale,
                isales.last_sale  AS last_sale
            FROM sku_summary s
            LEFT JOIN invoice_sales isales ON isales.product_sku = s.sku
            WHERE s.excluded_ids IS NOT NULL
              AND array_length(s.excluded_ids, 1) >= 1
              AND (
                   array_length(s.excluded_ids, 1) >= 2
                   OR COALESCE(isales.invoices_ml_count, 0) >= 1
              )
            ORDER BY
                -- critical (no active) first, then highest exclusion count, then most units sold
                (s.active_id IS NULL) DESC,
                array_length(s.excluded_ids, 1) DESC,
                COALESCE(isales.units_ml, 0) DESC,
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

            # Recent ML invoice lines that touched the SKU. This pulls from
            # invoice_items (ground truth) — kit components show up here even
            # though order_items only stores the parent kit SKU.
            try:
                invoices_result = await session.execute(
                    text(
                        """
                        SELECT inv.tiny_id, inv.number, inv.issue_date::text,
                               inv.ecommerce->>'nome' AS ecommerce_name,
                               inv.ecommerce->>'numeroPedidoEcommerce' AS ecom_order_number,
                               ii.quantity::int, ii.product_tiny_id,
                               inv.status
                        FROM invoice_items ii
                        JOIN invoices inv ON inv.tiny_id = ii.invoice_tiny_id
                        WHERE ii.product_sku = :sku
                          AND inv.ecommerce IS NOT NULL
                          AND inv.ecommerce->>'nome' LIKE 'Mercado Livre%'
                        ORDER BY inv.issue_date DESC, inv.tiny_id DESC
                        LIMIT 50;
                        """
                    ),
                    {"sku": sku},
                )
                out["recent_ml_orders"] = [
                    {
                        # Field names match the dashboard's OrderRow type so the
                        # existing modal table renders without changes. The
                        # numeric tiny_id here is the *invoice* tiny_id.
                        "tiny_id": int(r[0]),
                        "invoice_number": r[1],
                        "order_date": r[2],
                        "ecommerce_name": r[3],
                        "ecommerce_order_number": r[4],
                        "quantity": int(r[5]),
                        "product_tiny_id": int(r[6]) if r[6] is not None else None,
                        "situation": None,  # invoice status (str) ≠ order situation (int)
                        "invoice_status": r[7],
                    }
                    for r in invoices_result.all()
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
