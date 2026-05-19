"""mv_coverage v13: short-term velocity + physical stock + declining status.

Adds the smallest set of columns that meaningfully sharpens the queima /
reposição / FL analyses without changing semantics for existing consumers:

- ``sold_7d`` — last 7 days of sales (rolling, includes kit expansion like
  ``sold_30d``). Captures collapse / spike that the 30d window smooths over.

- ``momentum_7v30`` — ratio (sold_7d/7) / daily_rate. <0.5 = collapse last
  week; >1.5 = recent acceleration. NULL when daily_rate=0.

- ``physical_stock`` — ``stock_galpao + stock_full_ml`` (no A Caminho). This
  is what's actually vendable today; used as the base for queima and for the
  declining status. Same expression was inlined in the Telegram script — now
  it's a column so script and any future consumer share one definition.

- ``physical_coverage_days`` — physical_stock / daily_rate. Cobertura real,
  sem inflar com PO em trânsito.

- ``status_base`` gains one new value ``declining``: SKUs with a meaningful
  velocity drop (momentum_7v30 < 0.5) that still have stock for more than
  30d at current rate. Without this they sit in ``pending`` and only the
  queima query catches them via a Python-side decay filter, which means any
  other consumer treats them like any other healthy SKU.

Window of 7d is the shortest that smooths daily noise enough to trust.
Threshold 0.5 chosen so it triggers on a true halving of velocity, not on
weekly seasonality. Both can be tuned without changing column shapes.

Revises: ml_promo_v1
"""

from __future__ import annotations

from alembic import op

revision = "mv_coverage_v13"
down_revision = "ml_promo_v1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage;")
    op.execute(_CREATE_VIEW_V13)
    op.execute("CREATE UNIQUE INDEX mv_coverage_sku ON mv_coverage (sku);")
    op.execute("GRANT SELECT ON mv_coverage TO tiny_readonly;")
    op.execute(
        "COMMENT ON MATERIALIZED VIEW mv_coverage IS "
        "'v13 — adds sold_7d, momentum_7v30, physical_stock, "
        "physical_coverage_days, status_base=declining';"
    )


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage;")
    op.execute(_CREATE_VIEW_V12)
    op.execute("CREATE UNIQUE INDEX mv_coverage_sku ON mv_coverage (sku);")
    op.execute("GRANT SELECT ON mv_coverage TO tiny_readonly;")
    op.execute(
        "COMMENT ON MATERIALIZED VIEW mv_coverage IS "
        "'v12 — adds pending_full_qty from fulfillment_transfers';"
    )


_CREATE_VIEW_V13 = """
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
        -- physical_stock = vendável agora (galpão + FL).
        -- Não inclui A Caminho; usado por queima e pelo status declining.
        (COALESCE(s.stock_galpao, 0) + COALESCE(s.stock_full_ml, 0)) AS physical_stock,
        COALESCE(d7.sold_7d,           0) AS sold_7d,
        COALESCE(d30.sold_30d,         0) AS sold_30d,
        COALESCE(d30.sold_30d_direct,  0) AS sold_30d_direct,
        COALESCE(d60.sold_30_60d,      0) AS sold_30_60d,
        COALESCE(d90.sold_60_90d,      0) AS sold_60_90d,
        CASE WHEN COALESCE(d30.sold_30d, 0) > 0
            THEN ROUND(d30.sold_30d / 30.0, 2) ELSE 0 END                                          AS daily_rate,
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
    LEFT JOIN d30           ON d30.sku   = p.sku
    LEFT JOIN d60           ON d60.sku   = p.sku
    LEFT JOIN d90           ON d90.sku   = p.sku
    LEFT JOIN dprior        ON dprior.sku = p.sku
    LEFT JOIN dbase         ON dbase.sku  = p.sku
),
classified AS (
    SELECT *,
        -- momentum_7v30: velocidade dos últimos 7 dias / velocidade 30d.
        -- <0.5 = colapso recente (semana atual rodando à metade ou menos do mês).
        -- >1.5 = aceleração recente. NULL quando não há baseline 30d.
        CASE WHEN daily_rate > 0
            THEN ROUND((sold_7d / 7.0) / daily_rate, 2)
        END AS momentum_7v30,

        -- Cobertura física: estoque vendável / velocidade 30d.
        -- Sem A Caminho. É o que o queima usa.
        CASE WHEN daily_rate > 0
            THEN ROUND(physical_stock::numeric / daily_rate, 1)
        END AS physical_coverage_days,

        CASE
            -- ZOMBIE: has stock, zero sales in 90d
            WHEN stock_total > 0
              AND sold_30d = 0 AND sold_30_60d = 0 AND sold_60_90d = 0
            THEN 'zombie'

            -- DISCONTINUE (base): no stock, zero sales in 90d
            WHEN stock_total = 0
              AND sold_30d = 0 AND sold_30_60d = 0 AND sold_60_90d = 0
            THEN 'discontinue'

            -- DISCONTINUE (extended): no stock AND sales were too low to justify reorder
            WHEN stock_total = 0 AND sold_30d = 0
              AND sold_30_60d < 3 AND sold_60_90d < 5
            THEN 'discontinue'

            -- CLEARANCE: selling at a significant discount vs historical prices (>= 15% below).
            WHEN sold_30d_direct >= 3
              AND avg_price_30d IS NOT NULL
              AND (
                (avg_price_prior IS NOT NULL AND avg_price_30d < avg_price_prior * 0.85)
                OR (avg_price_prior IS NULL AND avg_price_base IS NOT NULL
                    AND avg_price_30d < avg_price_base * 0.85)
              )
              AND (avg_price_base IS NULL OR avg_price_30d < avg_price_base * 0.85)
            THEN 'clearance'

            -- RUPTURE (active): has direct sales but no stock
            WHEN stock_total = 0 AND sold_30d_direct > 0
            THEN 'rupture'

            -- RUPTURE: out of stock, meaningful demand 30-60d ago
            WHEN stock_total = 0 AND sold_30d = 0 AND sold_30_60d >= 3
            THEN 'rupture'

            -- RUPTURE (extended): stockout 60+ days, strong demand 60-90d ago
            WHEN stock_total = 0 AND sold_30d = 0
              AND sold_30_60d = 0 AND sold_60_90d >= 5
            THEN 'rupture'

            -- SLOW: has stock but stopped selling in last 30d (sold in past 90d)
            WHEN stock_total > 0 AND sold_30d = 0
              AND (sold_30_60d > 0 OR sold_60_90d > 0)
            THEN 'slow'

            -- SLOW: very low velocity with excess stock
            WHEN coverage_days >= 90 AND sold_30d >= 1 AND daily_rate <= 0.2
            THEN 'slow'

            -- DECLINING (v13): SKU ainda vende, mas velocidade da última semana caiu
            -- pra metade ou menos do baseline 30d, com estoque físico para
            -- 30+ dias na taxa atual. Mascarava em 'pending' antes.
            -- Sinal: produto vai virar queima se a tendência continuar.
            WHEN sold_30d > 0
              AND (sold_7d / 7.0) < (sold_30d / 30.0) * 0.5
              AND (stock_galpao + stock_full_ml) > sold_30d
              AND sold_30_60d >= 5
            THEN 'declining'

            ELSE 'pending'
        END AS status_base,

        -- Effective FL stock = confirmed (ML) + pending transfers in transit
        CASE WHEN has_fl_listing AND daily_rate > 0
            THEN ROUND((stock_full_ml + pending_full_qty)::numeric / daily_rate, 1)
        END AS coverage_days_fl,

        CASE WHEN has_fl_listing AND daily_rate > 0
            THEN GREATEST(0, CEIL(15.0 * daily_rate)::int - stock_full_ml - pending_full_qty)
        END AS send_to_fl

    FROM metrics
)
SELECT * FROM classified
WITH DATA;
"""


# Kept verbatim from 20260514_fulfillment_transfers.py so downgrade restores
# the exact v12 view.
_CREATE_VIEW_V12 = """
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
        COALESCE(d30.sold_30d,        0) AS sold_30d,
        COALESCE(d30.sold_30d_direct, 0) AS sold_30d_direct,
        COALESCE(d60.sold_30_60d, 0) AS sold_30_60d,
        COALESCE(d90.sold_60_90d, 0) AS sold_60_90d,
        CASE WHEN COALESCE(d30.sold_30d, 0) > 0
            THEN ROUND(d30.sold_30d / 30.0, 2) ELSE 0 END                                          AS daily_rate,
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
    LEFT JOIN d30           ON d30.sku   = p.sku
    LEFT JOIN d60           ON d60.sku   = p.sku
    LEFT JOIN d90           ON d90.sku   = p.sku
    LEFT JOIN dprior        ON dprior.sku = p.sku
    LEFT JOIN dbase         ON dbase.sku  = p.sku
),
classified AS (
    SELECT *,
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
            ELSE 'pending'
        END AS status_base,

        CASE WHEN has_fl_listing AND daily_rate > 0
            THEN ROUND((stock_full_ml + pending_full_qty)::numeric / daily_rate, 1)
        END AS coverage_days_fl,

        CASE WHEN has_fl_listing AND daily_rate > 0
            THEN GREATEST(0, CEIL(15.0 * daily_rate)::int - stock_full_ml - pending_full_qty)
        END AS send_to_fl

    FROM metrics
)
SELECT * FROM classified
WITH DATA;
"""
