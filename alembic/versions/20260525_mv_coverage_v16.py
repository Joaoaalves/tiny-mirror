"""mv_coverage v16: channel-aware FL metrics.

`coverage_days_fl` and `send_to_fl` were computed off the cross-channel
`daily_rate` (sales across Mercado Livre, ML FULL, Amazon, Magalu, Shopee,
TikTok, Nuvemshop, etc). That inflates the FULL-restock signal: a SKU
that sells heavily on Amazon but lightly on ML FULL was getting flagged
for aggressive FL restock — the Telegram message for `RTA-GAV6-P`
showed "1184 vend 30d / send 185 un" when the real ML FULL number was
860 / 30d with the correct send_to_fl ≈ 80.

This migration adds a FULL-only sales aggregate (`d30_fl` CTE) and
rewires the two FL columns to use it. Cross-channel metrics
(`sold_30d`, `coverage_days`, status classifications) stay unchanged
because they're correct for their purpose: judging the SKU as a whole.

Changes vs v15:
- New CTE ``d30_fl`` sums sale_buckets restricted to
  ``ecommerce_name = 'Mercado Livre FULL'`` (kit-expanded rows included
  — the expansion bucket carries the same ecommerce_name as its source).
- New metric column ``sold_30d_fl`` (int).
- New metric column ``daily_rate_fl`` (numeric).
- ``coverage_days_fl`` and ``send_to_fl`` now divide by / ceil
  against ``daily_rate_fl`` instead of cross-channel ``daily_rate``.

Revises: mv_coverage_v15
"""

from __future__ import annotations

from alembic import op

revision = "mv_coverage_v16"
down_revision = "promo_decisions_v1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage;")
    op.execute(_CREATE_VIEW_V16)
    op.execute("CREATE UNIQUE INDEX mv_coverage_sku ON mv_coverage (sku);")
    op.execute("GRANT SELECT ON mv_coverage TO tiny_readonly;")
    op.execute(
        "COMMENT ON MATERIALIZED VIEW mv_coverage IS "
        "'v16 — FL coverage_days + send_to_fl now use FL-only daily rate';"
    )


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage;")


_CREATE_VIEW_V16 = """
CREATE MATERIALIZED VIEW mv_coverage AS
WITH non_kit AS (
    SELECT
        p.tiny_id, p.sku, p.description,
        COALESCE(pfx_sup.supplier_name, NULLIF(sup.supplier_name, '')) AS supplier_name,
        sup.supplier_code,
        COALESCE(fl.has_fl, FALSE) AS has_fl_listing
    FROM products p
    LEFT JOIN LATERAL (
        SELECT s->>'nome'                            AS supplier_name,
               s->>'codigoProdutoNoFornecedor'       AS supplier_code
        FROM jsonb_array_elements(COALESCE(p.suppliers, '[]'::jsonb)) AS s
        LIMIT 1
    ) sup ON TRUE
    LEFT JOIN LATERAL (
        SELECT sps.supplier_name
        FROM sku_prefix_supplier sps
        WHERE p.sku LIKE sps.prefix || '%'
        ORDER BY length(sps.prefix) DESC
        LIMIT 1
    ) pfx_sup ON TRUE
    LEFT JOIN LATERAL (
        SELECT TRUE AS has_fl
        FROM ml_listings mll
        WHERE mll.sku = p.sku
          AND mll.logistic_type = 'fulfillment'
        LIMIT 1
    ) fl ON TRUE
    WHERE p.situation = 'A'
      AND p.type NOT IN ('K', 'V')
      AND p.sku NOT LIKE 'KIT-%'
      AND p.sku NOT LIKE 'COM-%'
      AND p.sku NOT LIKE 'XU-%'
      AND p.sku !~ '^[0-9]'
),
stock_dep AS (
    SELECT product_tiny_id,
        GREATEST(SUM(available), 0)::int AS stock_total,
        GREATEST(SUM(available) FILTER (WHERE deposit_name ILIKE '%Galpão%'),          0)::int AS stock_galpao,
        GREATEST(SUM(available) FILTER (WHERE deposit_name ILIKE '%Full Mercado Livre%'), 0)::int AS stock_full_ml,
        GREATEST(SUM(available) FILTER (WHERE deposit_name ILIKE '%A Caminho%'),       0)::int AS stock_chegando
    FROM stock_deposits
    WHERE NOT ignore
    GROUP BY product_tiny_id
),
pending AS (
    SELECT product_sku AS sku, SUM(quantity)::int AS pending_qty
    FROM fulfillment_transfers
    WHERE status = 'pending'
    GROUP BY product_sku
),
d7 AS (
    SELECT sku, SUM(quantity_sold)::int AS sold_7d
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '7 days'
      AND bucket_date <  CURRENT_DATE
    GROUP BY sku
),
d15 AS (
    SELECT sku, SUM(quantity_sold)::int AS sold_15d
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '15 days'
      AND bucket_date <  CURRENT_DATE
    GROUP BY sku
),
d30 AS (
    SELECT sku,
        SUM(quantity_sold)::int                                                     AS sold_30d,
        SUM(quantity_sold) FILTER (WHERE NOT is_kit_expansion)::int                 AS sold_30d_direct,
        SUM(total_revenue)  FILTER (WHERE NOT is_kit_expansion)                     AS rev_30d
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '30 days'
      AND bucket_date <  CURRENT_DATE
    GROUP BY sku
),
-- v16: FL-only 30d. Kit-expanded rows are tagged with the *kit order's*
-- ecommerce_name (filtering by 'Mercado Livre FULL' keeps the expanded
-- components for FULL kit sales and drops Amazon/Magalu/etc. expansions).
d30_fl AS (
    SELECT sku, SUM(quantity_sold)::int AS sold_30d_fl
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '30 days'
      AND bucket_date <  CURRENT_DATE
      AND ecommerce_name = 'Mercado Livre FULL'
    GROUP BY sku
),
d60 AS (
    SELECT sku, SUM(quantity_sold)::int AS sold_30_60d
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '60 days'
      AND bucket_date <  CURRENT_DATE - INTERVAL '30 days'
    GROUP BY sku
),
d90 AS (
    SELECT sku, SUM(quantity_sold)::int AS sold_60_90d
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '90 days'
      AND bucket_date <  CURRENT_DATE - INTERVAL '60 days'
    GROUP BY sku
),
dprior AS (
    SELECT sku,
        SUM(quantity_sold) FILTER (WHERE NOT is_kit_expansion)::int AS sold_prior_direct,
        SUM(total_revenue)  FILTER (WHERE NOT is_kit_expansion)     AS rev_prior
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '60 days'
      AND bucket_date <  CURRENT_DATE - INTERVAL '30 days'
    GROUP BY sku
),
dbase AS (
    SELECT sku,
        SUM(quantity_sold) FILTER (WHERE NOT is_kit_expansion)::int AS sold_base_direct,
        SUM(total_revenue)  FILTER (WHERE NOT is_kit_expansion)     AS rev_base
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '90 days'
      AND bucket_date <  CURRENT_DATE - INTERVAL '60 days'
    GROUP BY sku
),
metrics AS (
    SELECT
        p.sku, p.description, p.supplier_name, p.supplier_code,
        p.has_fl_listing,
        COALESCE(s.stock_total,    0) AS stock_total,
        COALESCE(s.stock_galpao,   0) AS stock_galpao,
        COALESCE(s.stock_full_ml,  0) AS stock_full_ml,
        COALESCE(s.stock_chegando, 0) AS stock_chegando,
        COALESCE(pend.pending_qty, 0) AS pending_full_qty,
        (COALESCE(s.stock_galpao, 0) + COALESCE(s.stock_full_ml, 0)) AS physical_stock,
        COALESCE(d7.sold_7d,            0) AS sold_7d,
        COALESCE(d15.sold_15d,          0) AS sold_15d,
        COALESCE(d30.sold_30d,          0) AS sold_30d,
        COALESCE(d30.sold_30d_direct,   0) AS sold_30d_direct,
        COALESCE(d30_fl.sold_30d_fl,    0) AS sold_30d_fl,
        COALESCE(d60.sold_30_60d,       0) AS sold_30_60d,
        COALESCE(d90.sold_60_90d,       0) AS sold_60_90d,
        CASE WHEN COALESCE(d30.sold_30d, 0) > 0
            THEN ROUND(d30.sold_30d / 30.0, 2) ELSE 0 END                                          AS daily_rate,
        CASE WHEN COALESCE(d30_fl.sold_30d_fl, 0) > 0
            THEN ROUND(d30_fl.sold_30d_fl / 30.0, 2) ELSE 0 END                                    AS daily_rate_fl,
        CASE WHEN COALESCE(d30.sold_30d, 0) > 0
            THEN ROUND(COALESCE(s.stock_total, 0) / (d30.sold_30d / 30.0), 1) END                  AS coverage_days,
        CASE WHEN COALESCE(d30.sold_30d_direct,    0) > 0
            THEN ROUND(d30.rev_30d    / d30.sold_30d_direct,    2) END                             AS avg_price_30d,
        CASE WHEN COALESCE(dprior.sold_prior_direct, 0) > 0
            THEN ROUND(dprior.rev_prior / dprior.sold_prior_direct, 2) END                         AS avg_price_prior,
        CASE WHEN COALESCE(dbase.sold_base_direct,  0) > 0
            THEN ROUND(dbase.rev_base  / dbase.sold_base_direct,  2) END                           AS avg_price_base
    FROM non_kit p
    LEFT JOIN stock_dep s   ON s.product_tiny_id = p.tiny_id
    LEFT JOIN pending pend  ON pend.sku = p.sku
    LEFT JOIN d7            ON d7.sku    = p.sku
    LEFT JOIN d15           ON d15.sku   = p.sku
    LEFT JOIN d30           ON d30.sku   = p.sku
    LEFT JOIN d30_fl        ON d30_fl.sku = p.sku
    LEFT JOIN d60           ON d60.sku   = p.sku
    LEFT JOIN d90           ON d90.sku   = p.sku
    LEFT JOIN dprior        ON dprior.sku = p.sku
    LEFT JOIN dbase         ON dbase.sku  = p.sku
),
classified AS (
    SELECT *,
        -- Kept for backwards compat; no consumer reads these anymore.
        CASE WHEN daily_rate > 0
            THEN ROUND((sold_7d / 7.0) / daily_rate, 2)
        END AS momentum_7v30,

        CASE WHEN daily_rate > 0
            THEN ROUND((sold_15d / 15.0) / daily_rate, 2)
        END AS momentum_15v30,

        CASE WHEN daily_rate > 0
            THEN ROUND(physical_stock::numeric / daily_rate, 1)
        END AS physical_coverage_days,

        CASE
            WHEN stock_total > 0
              AND sold_30d = 0 AND sold_30_60d = 0 AND sold_60_90d = 0
            THEN 'zombie'

            WHEN stock_total = 0
              AND sold_30d = 0 AND sold_30_60d = 0 AND sold_60_90d = 0
            THEN 'discontinue'

            WHEN stock_total = 0 AND sold_30d = 0
              AND sold_30_60d < 3 AND sold_60_90d < 5
            THEN 'discontinue'

            WHEN sold_30d_direct >= 3
              AND avg_price_30d IS NOT NULL
              AND (
                (avg_price_prior IS NOT NULL AND avg_price_30d < avg_price_prior * 0.85)
                OR (avg_price_prior IS NULL AND avg_price_base IS NOT NULL
                    AND avg_price_30d < avg_price_base * 0.85)
              )
              AND (avg_price_base IS NULL OR avg_price_30d < avg_price_base * 0.85)
            THEN 'clearance'

            WHEN stock_total = 0 AND sold_30d_direct > 0
            THEN 'rupture'

            WHEN stock_total = 0 AND sold_30d = 0 AND sold_30_60d >= 3
            THEN 'rupture'

            WHEN stock_total = 0 AND sold_30d = 0
              AND sold_30_60d = 0 AND sold_60_90d >= 5
            THEN 'rupture'

            WHEN stock_total > 0 AND sold_30d = 0
              AND (sold_30_60d > 0 OR sold_60_90d > 0)
            THEN 'slow'

            WHEN coverage_days >= 90 AND sold_30d >= 1 AND daily_rate <= 0.2
            THEN 'slow'

            WHEN sold_30d > 0
              AND sold_30_60d >= 5
              AND sold_30d < sold_30_60d * 0.5
              AND (stock_galpao + stock_full_ml) > sold_30d
            THEN 'declining'

            ELSE 'pending'
        END AS status_base,

        -- v16: FL-specific metrics use daily_rate_fl (FULL-only sales)
        -- so the restock signal isn't inflated by Amazon/Magalu/etc.
        CASE WHEN has_fl_listing AND daily_rate_fl > 0
            THEN ROUND((stock_full_ml + pending_full_qty)::numeric / daily_rate_fl, 1)
        END AS coverage_days_fl,

        CASE WHEN has_fl_listing AND daily_rate_fl > 0
            THEN GREATEST(0, CEIL(15.0 * daily_rate_fl)::int - stock_full_ml - pending_full_qty)
        END AS send_to_fl

    FROM metrics
)
SELECT * FROM classified
WITH DATA;
"""
