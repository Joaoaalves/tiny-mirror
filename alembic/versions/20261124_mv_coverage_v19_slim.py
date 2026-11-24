"""mv_coverage v19 — drop the columns nothing reads any more.

The dashboard cleanup of 24/11/2026 removed the Queima and Estoque Full pages
and the OpenClaw suggestion scripts (tiny-suggest-daily / tiny-copiar) that
were the last readers of has_fl_listing, physical_stock, sold_7d,
sold_30d_direct, avg_price_base, momentum_7v30, physical_coverage_days and
status_base. Those outputs go, and with them the d7/dbase sale_buckets scans
and the status_base classification. Every remaining column keeps the exact
v18 formula (has_fl_listing and sold_30d_direct stay as inputs of
coverage_days_fl/send_to_fl and avg_price_30d).

mv_coverage_fl_kits is untouched.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

from alembic import op

revision = "mv_coverage_v19_slim"
down_revision = "drop_estoque_full"
branch_labels = None
depends_on = None


def upgrade() -> None:
    _recreate(
        _CREATE_MV_COVERAGE, "v19 — slimmed: only columns with live readers; v18 math unchanged"
    )


def downgrade() -> None:
    v18 = _load_v18()
    _recreate(
        v18._CREATE_MV_COVERAGE,
        "v18 — Galpão / Full Mercado Livre / A Caminho counted regardless of "
        "stock_deposits.ignore (Tiny desconsiderar); v17 FL math unchanged",
    )


def _recreate(create_sql: str, comment: str) -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage;")
    op.execute(create_sql)
    op.execute("CREATE UNIQUE INDEX mv_coverage_sku ON mv_coverage (sku);")
    op.execute("GRANT SELECT ON mv_coverage TO tiny_readonly;")
    op.execute(f"COMMENT ON MATERIALIZED VIEW mv_coverage IS '{comment}';")


def _load_v18() -> Any:
    path = Path(__file__).with_name("20260911_mv_coverage_v18_deposit_ignore.py")
    spec = importlib.util.spec_from_file_location("mv_coverage_v18", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_DEPOSIT_ROW_FILTER = """
    WHERE NOT ignore
       OR deposit_name ILIKE '%Galpão%'
       OR deposit_name ILIKE '%Full Mercado Livre%'
       OR deposit_name ILIKE '%A Caminho%'
""".strip("\n")
_CREATE_MV_COVERAGE = (
    """
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
        GREATEST(SUM(available), 0)::int AS stock_total,
        GREATEST(SUM(available) FILTER (
            WHERE deposit_name ILIKE '%Galpão%'
        ), 0)::int AS stock_galpao,
        GREATEST(SUM(available + in_transfer) FILTER (
            WHERE deposit_name ILIKE '%Full Mercado Livre%'
        ), 0)::int AS stock_full_ml,
        GREATEST(SUM(in_transfer) FILTER (
            WHERE deposit_name ILIKE '%Full Mercado Livre%'
        ), 0)::int AS stock_fl_in_transfer,
        GREATEST(SUM(available) FILTER (
            WHERE deposit_name ILIKE '%A Caminho%'
        ), 0)::int AS stock_chegando
    FROM stock_deposits
"""
    + _DEPOSIT_ROW_FILTER
    + """
    GROUP BY product_tiny_id
), pending AS (
    SELECT product_sku AS sku,
        SUM(quantity - quantity_received)::int AS pending_qty
    FROM fulfillment_transfers
    WHERE status = 'pending'
    GROUP BY product_sku
), d15 AS (
    SELECT sku, SUM(quantity_sold)::int AS sold_15d
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '15 days'
      AND bucket_date <  CURRENT_DATE
    GROUP BY sku
), d30 AS (
    SELECT sku,
        SUM(quantity_sold)::int AS sold_30d,
        SUM(quantity_sold) FILTER (WHERE NOT is_kit_expansion)::int AS sold_30d_direct,
        SUM(total_revenue)  FILTER (WHERE NOT is_kit_expansion)      AS rev_30d
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '30 days'
      AND bucket_date <  CURRENT_DATE
    GROUP BY sku
), d30_fl AS (
    SELECT sku,
        SUM(quantity_sold) FILTER (WHERE NOT is_kit_expansion)::int AS sold_30d_fl
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
), metrics AS (
    SELECT p.sku, p.description, p.supplier_name, p.supplier_code, p.has_fl_listing,
        COALESCE(s.stock_total, 0)            AS stock_total,
        COALESCE(s.stock_galpao, 0)           AS stock_galpao,
        COALESCE(s.stock_full_ml, 0)          AS stock_full_ml,
        COALESCE(s.stock_fl_in_transfer, 0)   AS stock_fl_in_transfer,
        COALESCE(s.stock_chegando, 0)         AS stock_chegando,
        COALESCE(pend.pending_qty, 0)         AS pending_full_qty,
        COALESCE(d15.sold_15d, 0)             AS sold_15d,
        COALESCE(d30.sold_30d, 0)             AS sold_30d,
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
             ELSE NULL END                    AS avg_price_prior
    FROM non_kit p
    LEFT JOIN stock_dep s     ON s.product_tiny_id = p.tiny_id
    LEFT JOIN pending pend     ON pend.sku = p.sku
    LEFT JOIN d15             ON d15.sku  = p.sku
    LEFT JOIN d30             ON d30.sku  = p.sku
    LEFT JOIN d30_fl          ON d30_fl.sku = p.sku
    LEFT JOIN d60             ON d60.sku  = p.sku
    LEFT JOIN d90             ON d90.sku  = p.sku
    LEFT JOIN dprior          ON dprior.sku = p.sku
), classified AS (
    SELECT m.*,
        CASE WHEN m.daily_rate > 0
             THEN ROUND((m.sold_15d::numeric / 15.0) / m.daily_rate, 2)
             ELSE NULL END AS momentum_15v30,
        -- v17: effective_fl = stock_full_ml + the Entrada-pendente slice ML's
        -- Inventory API omits. stock_fl_in_transfer is ML's Em-transferência
        -- (already inside stock_full_ml); pending_full_qty is our drained
        -- ledger (= ML "a caminho" per SKU). The difference, clamped at 0, is
        -- the units already committed/in-route but not yet reflected by ML's
        -- in_transfer. Counting it stops re-suggesting already-sent SKUs.
        CASE WHEN m.has_fl_listing AND m.daily_rate_fl > 0
             THEN ROUND(
                    (m.stock_full_ml + GREATEST(0, m.pending_full_qty - m.stock_fl_in_transfer))::numeric
                    / m.daily_rate_fl, 1)
             ELSE NULL END AS coverage_days_fl,
        CASE WHEN m.has_fl_listing AND m.daily_rate_fl > 0
             THEN GREATEST(0, CEIL(15.0 * m.daily_rate_fl)::int
                    - (m.stock_full_ml + GREATEST(0, m.pending_full_qty - m.stock_fl_in_transfer)))
             ELSE NULL END AS send_to_fl
    FROM metrics m
)
SELECT
    sku, description, supplier_name, supplier_code,
    stock_total, stock_galpao,
    stock_full_ml,                  -- effective: available + in_transfer
    stock_fl_in_transfer,           -- breakdown of stock_full_ml
    stock_chegando, pending_full_qty,
    sold_15d, sold_30d, sold_30d_fl, sold_30_60d, sold_60_90d,
    daily_rate, daily_rate_fl,
    coverage_days,
    avg_price_30d, avg_price_prior,
    momentum_15v30,
    coverage_days_fl,
    send_to_fl
FROM classified
WITH DATA;
"""
)
