"""Add in_transfer to stock_deposits + extend mv_coverage with FL breakdown.

ML's Inventory API returns three relevant numbers per inventory:

  - ``available_quantity`` — units ready to sell from FL
  - ``not_available_detail[status=transfer].quantity`` — units physically at
    FL but being moved between ML internal warehouses; they re-enter the
    available pool within hours
  - ``total`` — sum of the two (everything physically at FL)

Until now the ml_fl_stock cron persisted only ``available_quantity`` to
``stock_deposits.available``. Units in ML internal transfer were invisible
to every downstream view, under-counting effective FL stock for any SKU
in active warehouse rebalancing (sample: MXCR-CHAVETIQT-100, LIJN06055,
HALQ54888, VWHK09361 — all had non-zero ``transfer`` qty when probed).

Changes:
  1. ``stock_deposits.in_transfer`` (numeric default 0). Only the
     'Full Mercado Livre' row gets non-zero values; populated by the
     ML cron from ``not_available_detail[status=transfer].quantity``.
  2. ``mv_coverage`` rebuilt verbatim from the live definition (commit
     08baaa1 era) with two surgical changes:
       - ``stock_full_ml`` now = SUM(available + in_transfer) for the FL
         deposit (the *effective* stock at FL, which is what coverage
         math + send_to_fl should use).
       - new column ``stock_fl_in_transfer`` exposes the breakdown so
         the stats page can show "X disponível + Y em transferência".

The CASCADE on the DROP is required because other materialised views and
foreign data wrappers may reference mv_coverage; the alembic-managed views
should be recreated here too, but as of 2026-05-29 the dependency graph
is empty (verified via pg_depend).

Revision ID: fl_in_transfer
Revises: partial_reception
Create Date: 2026-05-29
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "fl_in_transfer"
down_revision = "partial_reception"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "stock_deposits",
        sa.Column(
            "in_transfer",
            sa.Numeric(10, 2),
            nullable=False,
            server_default="0",
            comment=(
                "Units physically at this deposit but locked while ML "
                "moves them between internal warehouses. Only populated "
                "for the 'Full Mercado Livre' row, sourced from "
                "GET /inventories/{id}/stock/fulfillment "
                "not_available_detail[status=transfer]. Counted as "
                "effective FL stock by mv_coverage (the move always "
                "finishes within hours and the units never leave FL)."
            ),
        ),
    )

    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage CASCADE")
    op.execute(_MV_COVERAGE_SQL)
    op.execute("CREATE UNIQUE INDEX mv_coverage_sku ON mv_coverage (sku)")
    op.execute("REFRESH MATERIALIZED VIEW mv_coverage")


def downgrade() -> None:
    # Restore the pre-migration view (without in_transfer / stock_fl_in_transfer)
    # and drop the column. Operators rolling back further should consult
    # 20260514_mv_coverage.py for the older shape.
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage CASCADE")
    op.execute(_MV_COVERAGE_SQL_PREV)
    op.execute("CREATE UNIQUE INDEX mv_coverage_sku ON mv_coverage (sku)")
    op.execute("REFRESH MATERIALIZED VIEW mv_coverage")
    op.drop_column("stock_deposits", "in_transfer")


# ---------------------------------------------------------------------------
# mv_coverage SQL — NEW (with in_transfer rolled into stock_full_ml and a
# breakdown column exposed as stock_fl_in_transfer).
# ---------------------------------------------------------------------------
_MV_COVERAGE_SQL = r"""
CREATE MATERIALIZED VIEW mv_coverage AS
WITH non_kit AS (
    SELECT p.tiny_id, p.sku, p.description,
        COALESCE(pfx_sup.supplier_name, NULLIF(sup.supplier_name, '')) AS supplier_name,
        sup.supplier_code,
        COALESCE(fl.has_fl, false) AS has_fl_listing
    FROM products p
    LEFT JOIN LATERAL (
        SELECT (s.value ->> 'nome')                         AS supplier_name,
               (s.value ->> 'codigoProdutoNoFornecedor')    AS supplier_code
        FROM jsonb_array_elements(COALESCE(p.suppliers, '[]'::jsonb)) s
        LIMIT 1
    ) sup ON true
    LEFT JOIN LATERAL (
        SELECT sps.supplier_name
        FROM sku_prefix_supplier sps
        WHERE p.sku LIKE sps.prefix || '%'
        ORDER BY length(sps.prefix) DESC
        LIMIT 1
    ) pfx_sup ON true
    LEFT JOIN LATERAL (
        SELECT true AS has_fl
        FROM ml_listings mll
        WHERE mll.sku = p.sku AND mll.logistic_type = 'fulfillment'
        LIMIT 1
    ) fl ON true
    WHERE p.situation = 'A'
      AND p.type NOT IN ('K','V')
      AND p.sku NOT LIKE 'KIT-%'
      AND p.sku NOT LIKE 'COM-%'
      AND p.sku NOT LIKE 'XU-%'
      AND p.sku !~ '^[0-9]'
), stock_dep AS (
    SELECT product_tiny_id,
        -- Sum of all unignored deposits' available qty (legacy column).
        GREATEST(SUM(available), 0)::int AS stock_total,
        GREATEST(SUM(available) FILTER (
            WHERE deposit_name ILIKE '%Galpão%'
        ), 0)::int AS stock_galpao,
        -- *Effective* FL stock: available + units in ML internal transfer.
        -- The latter are physically at FL, just being relocated between
        -- ML's own warehouses, and become available again within hours —
        -- they belong in coverage math and in send_to_fl gating.
        GREATEST(SUM(available + in_transfer) FILTER (
            WHERE deposit_name ILIKE '%Full Mercado Livre%'
        ), 0)::int AS stock_full_ml,
        -- Breakdown for the UI: how much of stock_full_ml is in transfer.
        GREATEST(SUM(in_transfer) FILTER (
            WHERE deposit_name ILIKE '%Full Mercado Livre%'
        ), 0)::int AS stock_fl_in_transfer,
        GREATEST(SUM(available) FILTER (
            WHERE deposit_name ILIKE '%A Caminho%'
        ), 0)::int AS stock_chegando
    FROM stock_deposits
    WHERE NOT ignore
    GROUP BY product_tiny_id
), pending AS (
    SELECT product_sku AS sku,
        SUM(quantity - quantity_received)::int AS pending_qty
    FROM fulfillment_transfers
    WHERE status = 'pending'
    GROUP BY product_sku
), d7 AS (
    SELECT sku, SUM(quantity_sold)::int AS sold_7d
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '7 days'
      AND bucket_date <  CURRENT_DATE
    GROUP BY sku
), d15 AS (
    SELECT sku, SUM(quantity_sold)::int AS sold_15d
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '15 days'
      AND bucket_date <  CURRENT_DATE
    GROUP BY sku
), d30 AS (
    SELECT sku,
        SUM(quantity_sold)::int                                            AS sold_30d,
        SUM(quantity_sold) FILTER (WHERE NOT is_kit_expansion)::int        AS sold_30d_direct,
        SUM(total_revenue) FILTER (WHERE NOT is_kit_expansion)             AS rev_30d
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '30 days'
      AND bucket_date <  CURRENT_DATE
    GROUP BY sku
), d30_fl AS (
    SELECT sku, SUM(quantity_sold)::int AS sold_30d_fl
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '30 days'
      AND bucket_date <  CURRENT_DATE
      AND ecommerce_name = 'Mercado Livre FULL'
    GROUP BY sku
), d60 AS (
    SELECT sku, SUM(quantity_sold)::int AS sold_30_60d
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '60 days'
      AND bucket_date <  CURRENT_DATE - INTERVAL '30 days'
    GROUP BY sku
), d90 AS (
    SELECT sku, SUM(quantity_sold)::int AS sold_60_90d
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '90 days'
      AND bucket_date <  CURRENT_DATE - INTERVAL '60 days'
    GROUP BY sku
), dprior AS (
    SELECT sku,
        SUM(quantity_sold) FILTER (WHERE NOT is_kit_expansion)::int AS sold_prior_direct,
        SUM(total_revenue) FILTER (WHERE NOT is_kit_expansion)      AS rev_prior
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '60 days'
      AND bucket_date <  CURRENT_DATE - INTERVAL '30 days'
    GROUP BY sku
), dbase AS (
    SELECT sku,
        SUM(quantity_sold) FILTER (WHERE NOT is_kit_expansion)::int AS sold_base_direct,
        SUM(total_revenue) FILTER (WHERE NOT is_kit_expansion)      AS rev_base
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '90 days'
      AND bucket_date <  CURRENT_DATE - INTERVAL '60 days'
    GROUP BY sku
), metrics AS (
    SELECT p.sku, p.description, p.supplier_name, p.supplier_code, p.has_fl_listing,
        COALESCE(s.stock_total, 0)            AS stock_total,
        COALESCE(s.stock_galpao, 0)           AS stock_galpao,
        COALESCE(s.stock_full_ml, 0)          AS stock_full_ml,
        COALESCE(s.stock_fl_in_transfer, 0)   AS stock_fl_in_transfer,
        COALESCE(s.stock_chegando, 0)         AS stock_chegando,
        COALESCE(pend.pending_qty, 0)         AS pending_full_qty,
        (COALESCE(s.stock_galpao, 0) + COALESCE(s.stock_full_ml, 0)) AS physical_stock,
        COALESCE(d7.sold_7d, 0)               AS sold_7d,
        COALESCE(d15.sold_15d, 0)             AS sold_15d,
        COALESCE(d30.sold_30d, 0)             AS sold_30d,
        COALESCE(d30.sold_30d_direct, 0)      AS sold_30d_direct,
        COALESCE(d30_fl.sold_30d_fl, 0)       AS sold_30d_fl,
        COALESCE(d60.sold_30_60d, 0)          AS sold_30_60d,
        COALESCE(d90.sold_60_90d, 0)          AS sold_60_90d,
        CASE WHEN COALESCE(d30.sold_30d, 0) > 0
             THEN ROUND((d30.sold_30d)::numeric / 30.0, 2)
             ELSE 0 END                       AS daily_rate,
        CASE WHEN COALESCE(d30_fl.sold_30d_fl, 0) > 0
             THEN ROUND((d30_fl.sold_30d_fl)::numeric / 30.0, 2)
             ELSE 0 END                       AS daily_rate_fl,
        CASE WHEN COALESCE(d30.sold_30d, 0) > 0
             THEN ROUND(COALESCE(s.stock_total, 0)::numeric / (d30.sold_30d::numeric / 30.0), 1)
             ELSE NULL END                    AS coverage_days,
        CASE WHEN COALESCE(d30.sold_30d_direct, 0) > 0
             THEN ROUND(d30.rev_30d / d30.sold_30d_direct::numeric, 2)
             ELSE NULL END                    AS avg_price_30d,
        CASE WHEN COALESCE(dprior.sold_prior_direct, 0) > 0
             THEN ROUND(dprior.rev_prior / dprior.sold_prior_direct::numeric, 2)
             ELSE NULL END                    AS avg_price_prior,
        CASE WHEN COALESCE(dbase.sold_base_direct, 0) > 0
             THEN ROUND(dbase.rev_base / dbase.sold_base_direct::numeric, 2)
             ELSE NULL END                    AS avg_price_base
    FROM non_kit p
    LEFT JOIN stock_dep s     ON s.product_tiny_id = p.tiny_id
    LEFT JOIN pending pend     ON pend.sku = p.sku
    LEFT JOIN d7              ON d7.sku   = p.sku
    LEFT JOIN d15             ON d15.sku  = p.sku
    LEFT JOIN d30             ON d30.sku  = p.sku
    LEFT JOIN d30_fl          ON d30_fl.sku = p.sku
    LEFT JOIN d60             ON d60.sku  = p.sku
    LEFT JOIN d90             ON d90.sku  = p.sku
    LEFT JOIN dprior          ON dprior.sku = p.sku
    LEFT JOIN dbase           ON dbase.sku  = p.sku
), classified AS (
    SELECT m.*,
        CASE WHEN m.daily_rate > 0
             THEN ROUND((m.sold_7d::numeric / 7.0) / m.daily_rate, 2)
             ELSE NULL END AS momentum_7v30,
        CASE WHEN m.daily_rate > 0
             THEN ROUND((m.sold_15d::numeric / 15.0) / m.daily_rate, 2)
             ELSE NULL END AS momentum_15v30,
        CASE WHEN m.daily_rate > 0
             THEN ROUND(m.physical_stock::numeric / m.daily_rate, 1)
             ELSE NULL END AS physical_coverage_days,
        CASE
            WHEN m.stock_total > 0 AND m.sold_30d = 0 AND m.sold_30_60d = 0 AND m.sold_60_90d = 0 THEN 'zombie'
            WHEN m.stock_total = 0 AND m.sold_30d = 0 AND m.sold_30_60d = 0 AND m.sold_60_90d = 0 THEN 'discontinue'
            WHEN m.stock_total = 0 AND m.sold_30d = 0 AND m.sold_30_60d < 3 AND m.sold_60_90d < 5 THEN 'discontinue'
            WHEN m.sold_30d_direct >= 3 AND m.avg_price_30d IS NOT NULL
                 AND (
                       (m.avg_price_prior IS NOT NULL AND m.avg_price_30d < m.avg_price_prior * 0.85)
                    OR (m.avg_price_prior IS NULL AND m.avg_price_base IS NOT NULL AND m.avg_price_30d < m.avg_price_base * 0.85)
                 )
                 AND (m.avg_price_base IS NULL OR m.avg_price_30d < m.avg_price_base * 0.85) THEN 'clearance'
            WHEN m.stock_total = 0 AND m.sold_30d_direct > 0 THEN 'rupture'
            WHEN m.stock_total = 0 AND m.sold_30d = 0 AND m.sold_30_60d >= 3 THEN 'rupture'
            WHEN m.stock_total = 0 AND m.sold_30d = 0 AND m.sold_30_60d = 0 AND m.sold_60_90d >= 5 THEN 'rupture'
            WHEN m.stock_total > 0 AND m.sold_30d = 0 AND (m.sold_30_60d > 0 OR m.sold_60_90d > 0) THEN 'slow'
            WHEN m.coverage_days >= 90 AND m.sold_30d >= 1 AND m.daily_rate <= 0.2 THEN 'slow'
            WHEN m.sold_30d > 0 AND m.sold_30_60d >= 5
                 AND m.sold_30d::numeric < m.sold_30_60d::numeric * 0.5
                 AND (m.stock_galpao + m.stock_full_ml) > m.sold_30d THEN 'declining'
            ELSE 'pending'
        END AS status_base,
        -- FL coverage now naturally includes in_transfer because
        -- stock_full_ml already does. pending_full_qty is added on top
        -- because units already despatched from galpão WILL land at FL
        -- shortly even if ML hasn't confirmed yet.
        CASE WHEN m.has_fl_listing AND m.daily_rate_fl > 0
             THEN ROUND((m.stock_full_ml + m.pending_full_qty)::numeric / m.daily_rate_fl, 1)
             ELSE NULL END AS coverage_days_fl,
        CASE WHEN m.has_fl_listing AND m.daily_rate_fl > 0
             THEN GREATEST(0, CEIL(15.0 * m.daily_rate_fl)::int - m.stock_full_ml - m.pending_full_qty)
             ELSE NULL END AS send_to_fl
    FROM metrics m
)
SELECT
    sku, description, supplier_name, supplier_code, has_fl_listing,
    stock_total, stock_galpao,
    stock_full_ml,                  -- effective: available + in_transfer
    stock_fl_in_transfer,           -- breakdown
    stock_chegando, pending_full_qty,
    physical_stock,
    sold_7d, sold_15d, sold_30d, sold_30d_direct, sold_30d_fl, sold_30_60d, sold_60_90d,
    daily_rate, daily_rate_fl,
    coverage_days,
    avg_price_30d, avg_price_prior, avg_price_base,
    momentum_7v30, momentum_15v30,
    physical_coverage_days,
    status_base,
    coverage_days_fl, send_to_fl
FROM classified;
"""

# ---------------------------------------------------------------------------
# mv_coverage SQL — PREVIOUS shape (without in_transfer / stock_fl_in_transfer).
# Used by the downgrade path. Kept verbatim from pg_matviews on 2026-05-29.
# ---------------------------------------------------------------------------
_MV_COVERAGE_SQL_PREV = r"""
CREATE MATERIALIZED VIEW mv_coverage AS
WITH non_kit AS (
    SELECT p.tiny_id, p.sku, p.description,
        COALESCE(pfx_sup.supplier_name, NULLIF(sup.supplier_name, '')) AS supplier_name,
        sup.supplier_code,
        COALESCE(fl.has_fl, false) AS has_fl_listing
    FROM products p
    LEFT JOIN LATERAL (
        SELECT (s.value ->> 'nome') AS supplier_name,
               (s.value ->> 'codigoProdutoNoFornecedor') AS supplier_code
        FROM jsonb_array_elements(COALESCE(p.suppliers, '[]'::jsonb)) s
        LIMIT 1
    ) sup ON true
    LEFT JOIN LATERAL (
        SELECT sps.supplier_name FROM sku_prefix_supplier sps
        WHERE p.sku LIKE sps.prefix || '%' ORDER BY length(sps.prefix) DESC LIMIT 1
    ) pfx_sup ON true
    LEFT JOIN LATERAL (
        SELECT true AS has_fl FROM ml_listings mll
        WHERE mll.sku = p.sku AND mll.logistic_type = 'fulfillment' LIMIT 1
    ) fl ON true
    WHERE p.situation = 'A' AND p.type NOT IN ('K','V')
      AND p.sku NOT LIKE 'KIT-%' AND p.sku NOT LIKE 'COM-%'
      AND p.sku NOT LIKE 'XU-%'  AND p.sku !~ '^[0-9]'
), stock_dep AS (
    SELECT product_tiny_id,
        GREATEST(SUM(available), 0)::int AS stock_total,
        GREATEST(SUM(available) FILTER (WHERE deposit_name ILIKE '%Galpão%'), 0)::int AS stock_galpao,
        GREATEST(SUM(available) FILTER (WHERE deposit_name ILIKE '%Full Mercado Livre%'), 0)::int AS stock_full_ml,
        GREATEST(SUM(available) FILTER (WHERE deposit_name ILIKE '%A Caminho%'), 0)::int AS stock_chegando
    FROM stock_deposits WHERE NOT ignore GROUP BY product_tiny_id
), pending AS (
    SELECT product_sku AS sku, SUM(quantity)::int AS pending_qty
    FROM fulfillment_transfers WHERE status = 'pending' GROUP BY product_sku
), d7 AS (
    SELECT sku, SUM(quantity_sold)::int AS sold_7d FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '7 days' AND bucket_date < CURRENT_DATE GROUP BY sku
), d15 AS (
    SELECT sku, SUM(quantity_sold)::int AS sold_15d FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '15 days' AND bucket_date < CURRENT_DATE GROUP BY sku
), d30 AS (
    SELECT sku,
        SUM(quantity_sold)::int AS sold_30d,
        SUM(quantity_sold) FILTER (WHERE NOT is_kit_expansion)::int AS sold_30d_direct,
        SUM(total_revenue) FILTER (WHERE NOT is_kit_expansion) AS rev_30d
    FROM sale_buckets WHERE bucket_date >= CURRENT_DATE - INTERVAL '30 days' AND bucket_date < CURRENT_DATE GROUP BY sku
), d30_fl AS (
    SELECT sku, SUM(quantity_sold)::int AS sold_30d_fl FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '30 days' AND bucket_date < CURRENT_DATE
      AND ecommerce_name = 'Mercado Livre FULL' GROUP BY sku
), d60 AS (
    SELECT sku, SUM(quantity_sold)::int AS sold_30_60d FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '60 days' AND bucket_date < CURRENT_DATE - INTERVAL '30 days' GROUP BY sku
), d90 AS (
    SELECT sku, SUM(quantity_sold)::int AS sold_60_90d FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '90 days' AND bucket_date < CURRENT_DATE - INTERVAL '60 days' GROUP BY sku
), dprior AS (
    SELECT sku,
        SUM(quantity_sold) FILTER (WHERE NOT is_kit_expansion)::int AS sold_prior_direct,
        SUM(total_revenue) FILTER (WHERE NOT is_kit_expansion) AS rev_prior
    FROM sale_buckets WHERE bucket_date >= CURRENT_DATE - INTERVAL '60 days' AND bucket_date < CURRENT_DATE - INTERVAL '30 days' GROUP BY sku
), dbase AS (
    SELECT sku,
        SUM(quantity_sold) FILTER (WHERE NOT is_kit_expansion)::int AS sold_base_direct,
        SUM(total_revenue) FILTER (WHERE NOT is_kit_expansion) AS rev_base
    FROM sale_buckets WHERE bucket_date >= CURRENT_DATE - INTERVAL '90 days' AND bucket_date < CURRENT_DATE - INTERVAL '60 days' GROUP BY sku
), metrics AS (
    SELECT p.sku, p.description, p.supplier_name, p.supplier_code, p.has_fl_listing,
        COALESCE(s.stock_total, 0) AS stock_total,
        COALESCE(s.stock_galpao, 0) AS stock_galpao,
        COALESCE(s.stock_full_ml, 0) AS stock_full_ml,
        COALESCE(s.stock_chegando, 0) AS stock_chegando,
        COALESCE(pend.pending_qty, 0) AS pending_full_qty,
        (COALESCE(s.stock_galpao, 0) + COALESCE(s.stock_full_ml, 0)) AS physical_stock,
        COALESCE(d7.sold_7d, 0) AS sold_7d,
        COALESCE(d15.sold_15d, 0) AS sold_15d,
        COALESCE(d30.sold_30d, 0) AS sold_30d,
        COALESCE(d30.sold_30d_direct, 0) AS sold_30d_direct,
        COALESCE(d30_fl.sold_30d_fl, 0) AS sold_30d_fl,
        COALESCE(d60.sold_30_60d, 0) AS sold_30_60d,
        COALESCE(d90.sold_60_90d, 0) AS sold_60_90d,
        CASE WHEN COALESCE(d30.sold_30d, 0) > 0 THEN ROUND(d30.sold_30d::numeric / 30.0, 2) ELSE 0 END AS daily_rate,
        CASE WHEN COALESCE(d30_fl.sold_30d_fl, 0) > 0 THEN ROUND(d30_fl.sold_30d_fl::numeric / 30.0, 2) ELSE 0 END AS daily_rate_fl,
        CASE WHEN COALESCE(d30.sold_30d, 0) > 0 THEN ROUND(COALESCE(s.stock_total, 0)::numeric / (d30.sold_30d::numeric / 30.0), 1) ELSE NULL END AS coverage_days,
        CASE WHEN COALESCE(d30.sold_30d_direct, 0) > 0 THEN ROUND(d30.rev_30d / d30.sold_30d_direct::numeric, 2) ELSE NULL END AS avg_price_30d,
        CASE WHEN COALESCE(dprior.sold_prior_direct, 0) > 0 THEN ROUND(dprior.rev_prior / dprior.sold_prior_direct::numeric, 2) ELSE NULL END AS avg_price_prior,
        CASE WHEN COALESCE(dbase.sold_base_direct, 0) > 0 THEN ROUND(dbase.rev_base / dbase.sold_base_direct::numeric, 2) ELSE NULL END AS avg_price_base
    FROM non_kit p
    LEFT JOIN stock_dep s ON s.product_tiny_id = p.tiny_id
    LEFT JOIN pending pend ON pend.sku = p.sku
    LEFT JOIN d7 ON d7.sku = p.sku LEFT JOIN d15 ON d15.sku = p.sku
    LEFT JOIN d30 ON d30.sku = p.sku LEFT JOIN d30_fl ON d30_fl.sku = p.sku
    LEFT JOIN d60 ON d60.sku = p.sku LEFT JOIN d90 ON d90.sku = p.sku
    LEFT JOIN dprior ON dprior.sku = p.sku LEFT JOIN dbase ON dbase.sku = p.sku
), classified AS (
    SELECT m.*,
        CASE WHEN m.daily_rate > 0 THEN ROUND((m.sold_7d::numeric / 7.0) / m.daily_rate, 2) ELSE NULL END AS momentum_7v30,
        CASE WHEN m.daily_rate > 0 THEN ROUND((m.sold_15d::numeric / 15.0) / m.daily_rate, 2) ELSE NULL END AS momentum_15v30,
        CASE WHEN m.daily_rate > 0 THEN ROUND(m.physical_stock::numeric / m.daily_rate, 1) ELSE NULL END AS physical_coverage_days,
        CASE
            WHEN m.stock_total > 0 AND m.sold_30d = 0 AND m.sold_30_60d = 0 AND m.sold_60_90d = 0 THEN 'zombie'
            WHEN m.stock_total = 0 AND m.sold_30d = 0 AND m.sold_30_60d = 0 AND m.sold_60_90d = 0 THEN 'discontinue'
            WHEN m.stock_total = 0 AND m.sold_30d = 0 AND m.sold_30_60d < 3 AND m.sold_60_90d < 5 THEN 'discontinue'
            WHEN m.sold_30d_direct >= 3 AND m.avg_price_30d IS NOT NULL
                 AND ((m.avg_price_prior IS NOT NULL AND m.avg_price_30d < m.avg_price_prior * 0.85)
                   OR (m.avg_price_prior IS NULL AND m.avg_price_base IS NOT NULL AND m.avg_price_30d < m.avg_price_base * 0.85))
                 AND (m.avg_price_base IS NULL OR m.avg_price_30d < m.avg_price_base * 0.85) THEN 'clearance'
            WHEN m.stock_total = 0 AND m.sold_30d_direct > 0 THEN 'rupture'
            WHEN m.stock_total = 0 AND m.sold_30d = 0 AND m.sold_30_60d >= 3 THEN 'rupture'
            WHEN m.stock_total = 0 AND m.sold_30d = 0 AND m.sold_30_60d = 0 AND m.sold_60_90d >= 5 THEN 'rupture'
            WHEN m.stock_total > 0 AND m.sold_30d = 0 AND (m.sold_30_60d > 0 OR m.sold_60_90d > 0) THEN 'slow'
            WHEN m.coverage_days >= 90 AND m.sold_30d >= 1 AND m.daily_rate <= 0.2 THEN 'slow'
            WHEN m.sold_30d > 0 AND m.sold_30_60d >= 5
                 AND m.sold_30d::numeric < m.sold_30_60d::numeric * 0.5
                 AND (m.stock_galpao + m.stock_full_ml) > m.sold_30d THEN 'declining'
            ELSE 'pending'
        END AS status_base,
        CASE WHEN m.has_fl_listing AND m.daily_rate_fl > 0 THEN ROUND((m.stock_full_ml + m.pending_full_qty)::numeric / m.daily_rate_fl, 1) ELSE NULL END AS coverage_days_fl,
        CASE WHEN m.has_fl_listing AND m.daily_rate_fl > 0 THEN GREATEST(0, CEIL(15.0 * m.daily_rate_fl)::int - m.stock_full_ml - m.pending_full_qty) ELSE NULL END AS send_to_fl
    FROM metrics m
)
SELECT sku, description, supplier_name, supplier_code, has_fl_listing,
    stock_total, stock_galpao, stock_full_ml, stock_chegando, pending_full_qty,
    physical_stock,
    sold_7d, sold_15d, sold_30d, sold_30d_direct, sold_30d_fl, sold_30_60d, sold_60_90d,
    daily_rate, daily_rate_fl, coverage_days,
    avg_price_30d, avg_price_prior, avg_price_base,
    momentum_7v30, momentum_15v30, physical_coverage_days,
    status_base, coverage_days_fl, send_to_fl
FROM classified;
"""
