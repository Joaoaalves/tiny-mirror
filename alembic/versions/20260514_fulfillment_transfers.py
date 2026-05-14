"""Create fulfillment_transfers table and upgrade mv_coverage to v12.

Changes:
- New table: fulfillment_transfers — tracks units sent from Galpão to Full ML
  via the POST /fulfillment/transfer API.  status=pending until ML
  INBOUND_RECEPTION confirms arrival.
- mv_coverage v12: adds pending_full_qty CTE that sums pending transfers per
  SKU, then uses (stock_full_ml + pending_full_qty) as the effective Full ML
  stock so coverage_days_fl and send_to_fl don't double-send while units are
  in transit.

Revision ID: fulfillment_transfers_v1
Revises: mv_coverage_v11
Create Date: 2026-05-14
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "fulfillment_transfers_v1"
down_revision = "mv_coverage_v11"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "fulfillment_transfers",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("product_tiny_id", sa.BigInteger(), nullable=False),
        sa.Column("product_sku", sa.String(100), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("cost_per_unit", sa.Numeric(12, 2), nullable=False),
        sa.Column(
            "transferred_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'received', 'cancelled')",
            name="valid_fulfillment_transfer_status",
        ),
        sa.ForeignKeyConstraint(
            ["product_tiny_id"],
            ["products.tiny_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        comment=(
            "Tracks units transferred from Galpão to Full ML via Tiny API. "
            "status=pending until ML INBOUND_RECEPTION confirms arrival. "
            "Used to compute effective Full ML stock in mv_coverage so we "
            "don't double-send while transfers are in transit."
        ),
    )
    op.create_index(
        "ix_fulfillment_transfers_product_sku", "fulfillment_transfers", ["product_sku"]
    )
    op.create_index("ix_fulfillment_transfers_status", "fulfillment_transfers", ["status"])
    op.create_index(
        "ix_fulfillment_transfers_transferred_at", "fulfillment_transfers", ["transferred_at"]
    )
    op.execute("GRANT SELECT ON fulfillment_transfers TO tiny_readonly;")
    op.execute("GRANT INSERT, UPDATE ON fulfillment_transfers TO tiny_mirror;")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE fulfillment_transfers_id_seq TO tiny_mirror;")

    # Upgrade mv_coverage to v12
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage;")
    op.execute(_CREATE_VIEW_V12)
    op.execute("CREATE UNIQUE INDEX mv_coverage_sku ON mv_coverage (sku);")
    op.execute("GRANT SELECT ON mv_coverage TO tiny_readonly;")
    op.execute(
        "COMMENT ON MATERIALIZED VIEW mv_coverage IS "
        "'schema_version=12; consumers: coverage-v3.sql, route.ts; "
        "refresh: CONCURRENTLY after stock/order sync; "
        "pending_full_qty added — includes in-transit transfers in effective FL stock';"
    )


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage;")
    op.drop_index("ix_fulfillment_transfers_transferred_at", "fulfillment_transfers")
    op.drop_index("ix_fulfillment_transfers_status", "fulfillment_transfers")
    op.drop_index("ix_fulfillment_transfers_product_sku", "fulfillment_transfers")
    op.drop_table("fulfillment_transfers")

    # Restore mv_coverage v11
    op.execute(_CREATE_VIEW_V11)
    op.execute("CREATE UNIQUE INDEX mv_coverage_sku ON mv_coverage (sku);")
    op.execute("GRANT SELECT ON mv_coverage TO tiny_readonly;")
    op.execute(
        "COMMENT ON MATERIALIZED VIEW mv_coverage IS "
        "'schema_version=11; consumers: coverage-v3.sql, route.ts; "
        "refresh: CONCURRENTLY after stock/order sync; "
        "coverage_days_fl and send_to_fl added (15-day FL target)';"
    )


# ---------------------------------------------------------------------------
# mv_coverage v12 — adds pending_full_qty from fulfillment_transfers
# ---------------------------------------------------------------------------
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

            ELSE 'pending'
        END AS status_base,

        -- Effective FL stock = confirmed (ML) + pending transfers in transit
        -- NULL when product has no FL listing or no sales data
        CASE WHEN has_fl_listing AND daily_rate > 0
            THEN ROUND((stock_full_ml + pending_full_qty)::numeric / daily_rate, 1)
        END AS coverage_days_fl,

        -- Units to send to reach 15-day target, accounting for pending transfers
        -- 0 means effective FL stock already covers the target; NULL = not applicable
        CASE WHEN has_fl_listing AND daily_rate > 0
            THEN GREATEST(0, CEIL(15.0 * daily_rate)::int - stock_full_ml - pending_full_qty)
        END AS send_to_fl

    FROM metrics
)
SELECT * FROM classified
WITH DATA;
"""

# ---------------------------------------------------------------------------
# mv_coverage v11 (for downgrade)
# ---------------------------------------------------------------------------
_CREATE_VIEW_V11 = """
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
            THEN ROUND(stock_full_ml::numeric / daily_rate, 1)
        END AS coverage_days_fl,
        CASE WHEN has_fl_listing AND daily_rate > 0
            THEN GREATEST(0, CEIL(15.0 * daily_rate)::int - stock_full_ml)
        END AS send_to_fl
    FROM metrics
)
SELECT * FROM classified
WITH DATA;
"""
