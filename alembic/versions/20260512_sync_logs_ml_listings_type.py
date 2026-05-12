"""Add ml_listings to sync_logs valid_sync_type constraint.

Revision ID: sync_logs_ml_listings_type
Revises: ml_listings
Create Date: 2026-05-12
"""

from __future__ import annotations

from alembic import op

revision = "sync_logs_ml_listings_type"
down_revision = "ml_listings"
branch_labels = None
depends_on = None


def upgrade() -> None:
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


def downgrade() -> None:
    op.execute("ALTER TABLE sync_logs DROP CONSTRAINT IF EXISTS ck_sync_logs_valid_sync_type")
    op.execute(
        "ALTER TABLE sync_logs ADD CONSTRAINT ck_sync_logs_valid_sync_type CHECK ("
        " sync_type = ANY (ARRAY["
        "   'products', 'orders', 'stock', 'sale_buckets',"
        "   'token_rotation', 'mercadolivre_stock', 'invoices',"
        "   'stock_history', 'purchase_orders'"
        " ])"
        ")"
    )
