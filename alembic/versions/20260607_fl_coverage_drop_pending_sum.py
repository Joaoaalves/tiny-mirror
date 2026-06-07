"""mv_coverage / mv_coverage_fl_kits — stop adding pending_full_qty into
the FL coverage / send_to_fl / effective_total formulas.

Why
---
ML's Inventory API already reflects sent-and-received units in
``stock_full_ml = available + in_transfer``. Adding our own
``pending_full_qty`` on top double-counts the same physical units once
ML acknowledges the inbound (which it does within a few days for almost
every transfer).

2026-06-07 audit on BUB-ASPR-NAS-ESTJ (transfer #279, 100u):
- ML's ``in_transfer = 88`` already covers most of the inbound
- Our ``pending_outstanding = 85`` is the same physical pool
- Old formula would show 98 + 85 = 183 effective; reality is ~100

New rule
--------
- ``stock_full_ml`` (= available + in_transfer) is the source of truth
  for "what's at the FL right now".
- ``pending_full_qty`` stays in the view as **audit/info** — visible
  to operators but no longer summed into coverage math.
- A transfer that ML hasn't acknowledged yet shows up as a temporary
  discrepancy in the audit field; it'll be reconciled once the
  TRANSFER_DELIVERY event fires and reception scan credits qty_received.

mv_coverage_fl_kits gets the same fix AND has its ``stock_full_ml``
brought in line with mv_coverage (= available + in_transfer instead of
available alone) so the two views agree about what "FL stock" means.
"""

from __future__ import annotations

from alembic import op

revision = "fl_coverage_drop_pending"
down_revision = "mv_coverage_fl_kits_v1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage;")
    op.execute(_CREATE_MV_COVERAGE)
    op.execute("CREATE UNIQUE INDEX mv_coverage_sku ON mv_coverage (sku);")
    op.execute("GRANT SELECT ON mv_coverage TO tiny_readonly;")
    op.execute(
        "COMMENT ON MATERIALIZED VIEW mv_coverage IS "
        "'v14 — drops pending_full_qty from coverage_days_fl + send_to_fl "
        "to avoid double-counting against ML in_transfer';"
    )

    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage_fl_kits;")
    op.execute(_CREATE_MV_COVERAGE_FL_KITS)
    op.execute("CREATE UNIQUE INDEX mv_coverage_fl_kits_sku ON mv_coverage_fl_kits (sku);")
    op.execute("GRANT SELECT ON mv_coverage_fl_kits TO tiny_readonly;")
    op.execute(
        "COMMENT ON MATERIALIZED VIEW mv_coverage_fl_kits IS "
        "'v2 — stock_full_ml = avail+in_transfer (matches mv_coverage); "
        "effective_total no longer adds pending_full_qty';"
    )


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage;")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage_fl_kits;")


# Identical structure to 20260529_fl_in_transfer.py up to (and including)
# the classified CTE; the only diff is the two CASE branches at the end
# that no longer add pending_full_qty. Kept verbatim so the downgrade and
# the diff are obvious to reviewers.
_CREATE_MV_COVERAGE = r"""
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
        -- v14 change: stock_full_ml already includes ML's in_transfer
        -- bucket, which on inspection is the same physical pool as our
        -- pending_full_qty once ML acknowledges the inbound. Summing
        -- both double-counts. coverage_days_fl and send_to_fl now use
        -- the ML number alone; pending_full_qty stays in the view for
        -- audit / display purposes only.
        CASE WHEN m.has_fl_listing AND m.daily_rate_fl > 0
             THEN ROUND(m.stock_full_ml::numeric / m.daily_rate_fl, 1)
             ELSE NULL END AS coverage_days_fl,
        CASE WHEN m.has_fl_listing AND m.daily_rate_fl > 0
             THEN GREATEST(0, CEIL(15.0 * m.daily_rate_fl)::int - m.stock_full_ml)
             ELSE NULL END AS send_to_fl
    FROM metrics m
)
SELECT
    sku, description, supplier_name, supplier_code, has_fl_listing,
    stock_total, stock_galpao,
    stock_full_ml,                  -- effective: available + in_transfer
    stock_fl_in_transfer,           -- breakdown of stock_full_ml
    stock_chegando, pending_full_qty,
    physical_stock,
    sold_7d, sold_15d, sold_30d, sold_30d_direct, sold_30d_fl, sold_30_60d, sold_60_90d,
    daily_rate, daily_rate_fl,
    coverage_days,
    avg_price_30d, avg_price_prior, avg_price_base,
    momentum_7v30, momentum_15v30,
    physical_coverage_days,
    status_base,
    coverage_days_fl,
    send_to_fl
FROM classified
WITH DATA;
"""


_CREATE_MV_COVERAGE_FL_KITS = """
CREATE MATERIALIZED VIEW mv_coverage_fl_kits AS
WITH stock_dep AS (
    SELECT product_tiny_id,
        GREATEST(SUM(available) FILTER (
            WHERE deposit_name ILIKE '%Galpão%'
        ), 0)::int AS stock_galpao,
        -- v2: stock_full_ml matches mv_coverage's definition.
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
    WHERE NOT ignore
    GROUP BY product_tiny_id
),
pending AS (
    SELECT product_sku AS sku,
        SUM(quantity - quantity_received)::int AS pending_qty
    FROM fulfillment_transfers
    WHERE status = 'pending'
    GROUP BY product_sku
),
d30 AS (
    SELECT sku, SUM(quantity_sold) FILTER (WHERE NOT is_kit_expansion)::int AS sold_30d
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '30 days'
      AND bucket_date <  CURRENT_DATE
    GROUP BY sku
),
d30_fl AS (
    SELECT sku, SUM(quantity_sold) FILTER (WHERE NOT is_kit_expansion)::int AS sold_30d_fl
    FROM sale_buckets
    WHERE bucket_date >= CURRENT_DATE - INTERVAL '30 days'
      AND bucket_date <  CURRENT_DATE
      AND ecommerce_name = 'Mercado Livre FULL'
    GROUP BY sku
),
fl_listings AS (
    SELECT
        sku,
        STRING_AGG(DISTINCT mlb_id, ',' ORDER BY mlb_id) AS mlb_ids,
        STRING_AGG(DISTINCT inventory_id, ',' ORDER BY inventory_id)
            FILTER (WHERE inventory_id IS NOT NULL) AS inventory_ids
    FROM ml_listings
    WHERE logistic_type = 'fulfillment' AND status = 'active'
    GROUP BY sku
)
SELECT
    p.tiny_id,
    p.sku,
    p.description,
    flk.mlb_ids,
    flk.inventory_ids,
    COALESCE(s.stock_galpao,         0) AS stock_galpao,
    COALESCE(s.stock_full_ml,        0) AS stock_full_ml,
    COALESCE(s.stock_fl_in_transfer, 0) AS stock_fl_in_transfer,
    COALESCE(s.stock_chegando,       0) AS stock_chegando,
    COALESCE(pend.pending_qty,       0) AS pending_full_qty,
    -- v2: effective_total no longer adds pending_full_qty (double-count vs ML).
    (COALESCE(s.stock_galpao, 0) + COALESCE(s.stock_full_ml, 0)) AS effective_total,
    COALESCE(d30.sold_30d,           0) AS sold_30d,
    COALESCE(d30_fl.sold_30d_fl,     0) AS sold_30d_fl
FROM products p
JOIN fl_listings flk    ON flk.sku = p.sku
LEFT JOIN stock_dep s   ON s.product_tiny_id = p.tiny_id
LEFT JOIN pending pend  ON pend.sku = p.sku
LEFT JOIN d30           ON d30.sku  = p.sku
LEFT JOIN d30_fl        ON d30_fl.sku = p.sku
WHERE p.type = 'K'
  AND p.situation = 'A'
WITH DATA;
"""
