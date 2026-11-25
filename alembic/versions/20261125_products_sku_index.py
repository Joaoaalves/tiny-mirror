"""Full (non-partial) btree index on products(sku).

The only sku index is the partial ``uq_products_sku ... WHERE situation IN
('A','I')``. Lookups by SKU that do not repeat that predicate — the caps
enrichment (``SELECT manual_status FROM products WHERE sku = :sku``), the
Tiny-cost fallback, and the dashboard's financeiro/precificação queries that
sort ``(situation = 'A') DESC`` to include the ``E`` rows — cannot use it and
seq-scan the table (≈188k seq scans / 370M tuples read on 2026-11-25).

Built with ``CREATE INDEX CONCURRENTLY`` so the deploy never takes the write
lock on ``products`` (the Tiny product sync keeps writing during the build).
CONCURRENTLY cannot run inside a transaction, hence the autocommit block. A
failed concurrent build leaves an INVALID index behind; ``IF NOT EXISTS``
would then silently keep it, so upgrade drops an invalid leftover first.
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision = "products_sku_index"
down_revision = "mv_coverage_v19_slim"
branch_labels = None
depends_on = None

INDEX_NAME = "ix_products_sku"

_INVALID_LEFTOVER_SQL = (
    "SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
    "WHERE c.relname = :name AND NOT i.indisvalid"
)
CREATE_SQL = f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {INDEX_NAME} ON products (sku)"
DROP_SQL = f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX_NAME}"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        bind = op.get_bind()
        if bind.execute(text(_INVALID_LEFTOVER_SQL), {"name": INDEX_NAME}).first():
            op.execute(DROP_SQL)
        op.execute(CREATE_SQL)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(DROP_SQL)
