"""Add ml_fl_stock to sync_logs valid_sync_type constraint.

Enables the every-15-min ML-only Full stock refresh job, which writes
its sync_log rows with sync_type='ml_fl_stock'.

Revision ID: ml_fl_stock_sync_type
Revises: fl_snapshot_with_galpao
Create Date: 2026-05-27
"""

from __future__ import annotations

from alembic import op

revision = "ml_fl_stock_sync_type"
down_revision = "fl_snapshot_with_galpao"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE sync_logs DROP CONSTRAINT IF EXISTS ck_sync_logs_valid_sync_type")
    op.execute(
        "ALTER TABLE sync_logs ADD CONSTRAINT ck_sync_logs_valid_sync_type CHECK ("
        " sync_type = ANY (ARRAY["
        "   'products', 'orders', 'stock', 'sale_buckets',"
        "   'token_rotation', 'mercadolivre_stock', 'invoices',"
        "   'stock_history', 'purchase_orders', 'ml_listings',"
        "   'ml_fl_stock'"
        " ])"
        ")"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE sync_logs DROP CONSTRAINT IF EXISTS ck_sync_logs_valid_sync_type")
    op.execute(
        "ALTER TABLE sync_logs ADD CONSTRAINT ck_sync_logs_valid_sync_type CHECK ("
        " sync_type = ANY (ARRAY["
        "   'products', 'orders', 'stock', 'sale_buckets',"
        "   'token_rotation', 'mercadolivre_stock', 'invoices',"
        "   'stock_history', 'purchase_orders', 'ml_listings'"
        " ])"
        ")"
    )
