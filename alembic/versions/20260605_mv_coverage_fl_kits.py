"""mv_coverage_fl_kits — parallel view exposing FL kits.

mv_coverage's ``non_kit`` CTE excludes ``products.type='K'`` because the
classifier logic (zombie / clearance / rupture / etc.) is calibrated for
base SKUs; running it over kits gives misleading statuses (a kit's
sale_buckets are dual-tagged with the kit row + the kit-expanded
component rows, which the cross-channel daily_rate already accounts
for at the component level).

The downside is that mission-control's stats page has no row for a
kit listing — operators couldn't see how many units of a kit they had
in Tiny galpão vs ML FL vs in-transit to FL. The 2026-06-05 SLF-KITDISPLPDEN-PR
investigation surfaced this exact gap.

This view ships a *focused* per-kit summary with only the columns that
make sense at the kit level:
- the basic stock picture (galpão, full ML, in-transfer, sent-to-full)
- direct FL sales for the kit (no expansion: a kit row in
  sale_buckets has its OWN quantity_sold for the kit itself; the
  components are tracked separately via is_kit_expansion=true rows)

It is NOT a drop-in replacement for mv_coverage — no classification,
no daily rate, no coverage-days math. Operators who need that view
the components.

Trigger to refresh: same daily REFRESH cycle as mv_coverage.
"""

from __future__ import annotations

from alembic import op

revision = "mv_coverage_fl_kits_v1"
down_revision = "decision_lightning_stock"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage_fl_kits;")
    op.execute(_CREATE_VIEW)
    op.execute("CREATE UNIQUE INDEX mv_coverage_fl_kits_sku " "ON mv_coverage_fl_kits (sku);")
    op.execute("GRANT SELECT ON mv_coverage_fl_kits TO tiny_readonly;")
    op.execute(
        "COMMENT ON MATERIALIZED VIEW mv_coverage_fl_kits IS "
        "'v1 — focused per-kit stock + FL sales summary; companion to mv_coverage';"
    )


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage_fl_kits;")


_CREATE_VIEW = """
CREATE MATERIALIZED VIEW mv_coverage_fl_kits AS
WITH stock_dep AS (
    SELECT product_tiny_id,
        GREATEST(SUM(available) FILTER (WHERE deposit_name ILIKE '%Galpão%'),               0)::int  AS stock_galpao,
        GREATEST(SUM(available) FILTER (WHERE deposit_name ILIKE '%Full Mercado Livre%'),   0)::int  AS stock_full_ml,
        GREATEST(SUM(in_transfer) FILTER (WHERE deposit_name ILIKE '%Full Mercado Livre%'), 0)::int  AS stock_fl_in_transfer,
        GREATEST(SUM(available) FILTER (WHERE deposit_name ILIKE '%A Caminho%'),            0)::int  AS stock_chegando
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
-- Collapse multiple FL listings of the SAME kit into one row. A kit
-- with two MLBs (e.g. listed twice on the marketplace) would otherwise
-- yield 2 rows and break the unique index. STRING_AGG keeps every
-- mlb_id / inventory_id visible to the UI as a comma-separated list.
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
    (COALESCE(s.stock_galpao, 0) + COALESCE(s.stock_full_ml, 0)
     + COALESCE(s.stock_fl_in_transfer, 0) + COALESCE(pend.pending_qty, 0)
    ) AS effective_total,
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
