"""Add sku_prefix_supplier table and update mv_coverage to use prefix fallback.

When a product has no supplier set in Tiny, mv_coverage resolves the
supplier name from this table by matching the first SKU segment (before
the first dash). Longest-known-prefix wins; if no match, supplier_name
stays NULL.

Consumers of mv_coverage need no changes — supplier_name is already a
column they read; it just becomes non-NULL for previously unresolved rows.

Revision ID: sku_prefix_supplier_v1
Revises: mv_coverage_v4
Create Date: 2026-05-06
"""

from __future__ import annotations

from alembic import op

revision = "sku_prefix_supplier_v1"
down_revision = "mv_coverage_v4"
branch_labels = None
depends_on = None

# ---------------------------------------------------------------------------
# Initial seed — one row per known SKU prefix. supplier_name must match
# exactly the value used in products.suppliers JSONB so grouping is stable.
# ---------------------------------------------------------------------------
_INITIAL_DATA = [
    ("BRG",  "BURIGOTTO S/A IND. E COM."),
    ("BUB",  "MOAS INDUSTRIA E COMERCIO IMPORTACAO E EXPORTACAO LTDA"),
    ("CAMP", "CAMPINEIRA UTILIDADES LT"),
    ("DEL",  "DELO INDUSTRIA E COMERCIO LTDA"),
    ("EMB",  "M.N.Plast- Embalagens Ltda."),
    ("EVA",  "MAXTATAME COMERCIO EIRELLI - EPP"),
    ("GALX", "SULD DISTRIBUIDORA DE PRODUTOS ELETRICOS LTDA"),
    ("JAV",  "GERMAN COM. DE UTIL. DOMESTICAS LTDA EPP"),
    ("MAST", "GRUPO MASTER COMERCIO IMPORTACAO E EXPORTACAO LTDA"),
    ("MNPL", "M.N.Plast- Embalagens Ltda."),
    ("MX",   "MAX EBERHARDT UTILIDADES DOMESTICAS, COMERCIO, IMPORTACAO"),
    ("MXCR", "MAXCRIL INDUSTRIA DE MATERIAIS PARA ESCRITORIO LTDA"),
    ("NIT",  "NITRONPLAST INDUSTRIA E COMERCIO LTDA"),
    ("OUR",  "OUROLUX COMERCIAL LTDA"),
    ("POL",  "POLIBRAS MINAS PLASTICOS LTDA (CD)"),
    ("PRE",  "DEOFORT COMERCIO DE PRODUTOS DE HIGIENE E LIMPEZA LTDA"),
    ("RON",  "RONI GRILL INDÚSTRIA E COMÉRCIO  DE TÁBUAS ARTESANAIS EIRELI"),
    ("RTA",  "RT AT MANU ORGANIZACAO E ARMAZENAMENTO LTDA"),
    ("SLF",  "Artefatos de Madeira Stolf LTDA"),
    ("SOZ",  "SOUZA PRODUTOS DE MADEIRA LTDA"),
    ("UNI",  "UNINJET IND. E COM. DE PLASTICOS LTDA"),
]

_CREATE_TABLE = """
CREATE TABLE sku_prefix_supplier (
    prefix        TEXT PRIMARY KEY,
    supplier_name TEXT NOT NULL,
    notes         TEXT
);
COMMENT ON TABLE sku_prefix_supplier IS
  'Fallback supplier resolution by SKU prefix (first segment before dash). '
  'Used in mv_coverage when products.suppliers JSONB has no supplier set. '
  'After changes here, run: REFRESH MATERIALIZED VIEW CONCURRENTLY mv_coverage';
GRANT SELECT ON sku_prefix_supplier TO tiny_readonly;
"""

_DROP_TABLE = "DROP TABLE IF EXISTS sku_prefix_supplier;"

# mv_coverage with prefix fallback — drop & recreate
_DROP_VIEW = """
DROP MATERIALIZED VIEW IF EXISTS mv_coverage;
"""

_CREATE_VIEW = """
CREATE MATERIALIZED VIEW mv_coverage AS
WITH non_kit AS (
    SELECT
        p.tiny_id, p.sku, p.description,
        COALESCE(NULLIF(sup.supplier_name, ''), psp.supplier_name) AS supplier_name,
        sup.supplier_code
    FROM products p
    LEFT JOIN LATERAL (
        SELECT s->>'nome'                      AS supplier_name,
               s->>'codigoProdutoNoFornecedor' AS supplier_code
        FROM jsonb_array_elements(COALESCE(p.suppliers, '[]'::jsonb)) AS s
        LIMIT 1
    ) sup ON TRUE
    LEFT JOIN sku_prefix_supplier psp
           ON psp.prefix = split_part(p.sku, '-', 1)
    WHERE p.situation = 'A'
      AND p.type NOT IN ('K')
      AND p.sku NOT LIKE 'KIT-%'
      AND p.sku NOT LIKE 'COM-%'
      AND p.sku NOT LIKE 'XU-%'
      AND p.sku !~ '^[0-9]'
),
stock_dep AS (
    SELECT product_tiny_id,
        GREATEST(SUM(available), 0)::int AS stock_total,
        GREATEST(SUM(available) FILTER (WHERE deposit_name ILIKE '%Galpão%'),             0)::int AS stock_galpao,
        GREATEST(SUM(available) FILTER (WHERE deposit_name ILIKE '%Full Mercado Livre%'), 0)::int AS stock_full_ml,
        GREATEST(SUM(available) FILTER (WHERE deposit_name ILIKE '%A Caminho%'),          0)::int AS stock_chegando
    FROM stock_deposits WHERE NOT ignore
    GROUP BY product_tiny_id
),
d30 AS (
    SELECT sku,
        SUM(quantity_sold)::int                                         AS sold_30d,
        SUM(quantity_sold) FILTER (WHERE NOT is_kit_expansion)::int     AS sold_30d_direct,
        SUM(total_revenue)  FILTER (WHERE NOT is_kit_expansion)         AS rev_30d
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
        COALESCE(s.stock_total,    0) AS stock_total,
        COALESCE(s.stock_galpao,   0) AS stock_galpao,
        COALESCE(s.stock_full_ml,  0) AS stock_full_ml,
        COALESCE(s.stock_chegando, 0) AS stock_chegando,
        COALESCE(d30.sold_30d,    0) AS sold_30d,
        COALESCE(d60.sold_30_60d, 0) AS sold_30_60d,
        COALESCE(d90.sold_60_90d, 0) AS sold_60_90d,
        CASE WHEN COALESCE(d30.sold_30d, 0) > 0
            THEN ROUND(d30.sold_30d / 30.0, 2) ELSE 0 END AS daily_rate,
        CASE WHEN COALESCE(d30.sold_30d, 0) > 0
            THEN ROUND(COALESCE(s.stock_total, 0) / (d30.sold_30d / 30.0), 1) END AS coverage_days,
        CASE WHEN COALESCE(d30.sold_30d_direct, 0) > 0
            THEN ROUND(d30.rev_30d / d30.sold_30d_direct, 2) END AS avg_price_30d,
        CASE WHEN COALESCE(dprior.sold_prior_direct, 0) > 0
            THEN ROUND(dprior.rev_prior / dprior.sold_prior_direct, 2) END AS avg_price_prior,
        CASE WHEN COALESCE(dbase.sold_base_direct, 0) > 0
            THEN ROUND(dbase.rev_base / dbase.sold_base_direct, 2) END AS avg_price_base
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
            WHEN stock_total > 0 AND sold_30d = 0 AND sold_30_60d = 0 AND sold_60_90d = 0 THEN 'zombie'
            WHEN sold_30d = 0 AND sold_30_60d = 0 AND sold_60_90d = 0 AND stock_total = 0 THEN 'discontinue'
            WHEN avg_price_prior IS NOT NULL AND avg_price_30d IS NOT NULL
              AND avg_price_30d < avg_price_prior * 0.85
              AND (avg_price_base IS NULL OR avg_price_30d < avg_price_base * 0.85)        THEN 'clearance'
            WHEN stock_total = 0 AND sold_30d = 0 AND sold_30_60d > 0                     THEN 'rupture'
            ELSE 'pending'
        END AS status_base
    FROM metrics
)
SELECT * FROM classified
WITH DATA;

CREATE UNIQUE INDEX mv_coverage_sku ON mv_coverage (sku);
GRANT SELECT ON mv_coverage TO tiny_readonly;
COMMENT ON MATERIALIZED VIEW mv_coverage IS
  'schema_version=4; consumers: coverage-v3.sql, route.ts; '
  'refresh: CONCURRENTLY after stock/order sync';
"""


def upgrade() -> None:
    op.execute(_CREATE_TABLE)

    rows = ", ".join(
        f"('{prefix}', '{name}')"
        for prefix, name in _INITIAL_DATA
    )
    op.execute(f"INSERT INTO sku_prefix_supplier (prefix, supplier_name) VALUES {rows};")

    op.execute(_DROP_VIEW)
    op.execute(_CREATE_VIEW)


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_coverage;")
    op.execute(_DROP_TABLE)
    # Restore previous mv_coverage without prefix fallback would require
    # re-running the prior migration — simplest is to re-run upgrade of mv_coverage_v4.
