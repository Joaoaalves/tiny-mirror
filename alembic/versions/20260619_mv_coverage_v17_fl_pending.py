"""mv_coverage / mv_coverage_fl_kits — re-introduce the "a caminho do Full"
slice that ML's Inventory API does NOT expose, into the FL coverage /
send_to_fl / effective_total formulas.

Why (corrects the 2026-06-07 premise)
-------------------------------------
2026-06-07 (``fl_coverage_drop_pending``) removed ``pending_full_qty`` from
the FL coverage math, on the premise that ML's ``in_transfer`` already
covers the same physical pool as our pending ledger. A 2026-06-19 audit
(ML stock report vs ``stock_deposits`` vs ``fulfillment_transfers``) proved
that premise FALSE:

- ML's panel "a caminho do Full" = ``Entrada pendente`` + ``Em transferência``.
- ML's Inventory API ``in_transfer`` (= our ``stock_fl_in_transfer``, the
  ``in_transfer`` slice of ``stock_full_ml``) matches ONLY the
  ``Em transferência`` column — verified exact on multiple non-kit SKUs
  (SLF-CNJ-PORCOPO-PR 49=49, SOZ-APOI-BLKPIANO 20=20, EVA-AIMP 17=17).
- The much larger ``Entrada pendente`` slice (1508 of 1780 units at audit
  time) is invisible to ``stock_full_ml``. So units already committed/sent
  to Full kept being suggested for re-send — the exact bug the operator hit.

New rule
--------
``effective_fl = stock_full_ml + GREATEST(0, pending_full_qty - stock_fl_in_transfer)``

- ``stock_full_ml`` (= available + in_transfer) still counts ML's available
  + Em-transferência.
- ``GREATEST(0, pending_full_qty - stock_fl_in_transfer)`` adds back ONLY the
  Entrada-pendente slice (our drained ledger minus the Em-transferência that
  ML already reports), clamped at 0 so it never double-counts nor goes
  negative under kit-aggregation skew.
- ``coverage_days_fl`` and ``send_to_fl`` use ``effective_fl``; a SKU whose
  inbound is already on the way drops out of the send suggestion.

Depends on the 2026-06-19 phantom drain: ``fulfillment_transfers.pending``
was reconciled down to ML's real "a caminho" per SKU, so ``pending_full_qty``
is now faithful and safe to feed back into coverage.
"""

from __future__ import annotations

from alembic import op

revision = "mv_coverage_v17_fl_pending"
down_revision = "ml_promotions_mirror"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage;")
    op.execute(_CREATE_MV_COVERAGE)
    op.execute("CREATE UNIQUE INDEX mv_coverage_sku ON mv_coverage (sku);")
    op.execute("GRANT SELECT ON mv_coverage TO tiny_readonly;")
    op.execute(
        "COMMENT ON MATERIALIZED VIEW mv_coverage IS "
        "'v17 — coverage_days_fl + send_to_fl add back the Entrada-pendente "
        "slice (pending_full_qty - in_transfer) ML API omits';"
    )

    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage_fl_kits;")
    op.execute(_CREATE_MV_COVERAGE_FL_KITS)
    op.execute("CREATE UNIQUE INDEX mv_coverage_fl_kits_sku ON mv_coverage_fl_kits (sku);")
    op.execute("GRANT SELECT ON mv_coverage_fl_kits TO tiny_readonly;")
    op.execute(
        "COMMENT ON MATERIALIZED VIEW mv_coverage_fl_kits IS "
        "'v3 — effective_total adds back the Entrada-pendente slice "
        "(pending_full_qty - in_transfer) ML API omits';"
    )


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage;")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage_fl_kits;")


# Body identical to 20260607_fl_coverage_drop_pending_sum.py except the two
# CASE branches (coverage_days_fl, send_to_fl) now use effective_fl. Kept
# verbatim otherwise so the diff is obvious to reviewers.
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
    -- v3: effective_total adds back the Entrada-pendente slice ML's API omits
    -- (pending_full_qty - in_transfer, clamped at 0), matching mv_coverage.
    (COALESCE(s.stock_galpao, 0) + COALESCE(s.stock_full_ml, 0)
     + GREATEST(0, COALESCE(pend.pending_qty, 0) - COALESCE(s.stock_fl_in_transfer, 0))) AS effective_total,
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
