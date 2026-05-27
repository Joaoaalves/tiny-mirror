"""Create phantom_products_log table + add 'phantom_detection' to sync_type CHECK.

Daily audit trail of phantom products detected — SKUs where the Tiny
catalog has >=1 excluded duplicate AND ML orders kept arriving on that
SKU (= the listing's SELLER_SKU points to nothing real, Tiny auto-creates
a new product per order, operator later excludes them).

Each detection run writes one row per phantom SKU. Multiple runs over
time let the operator see the trend (is the bleed getting worse or
slowing down after they fix listings on the ML side?).

Revision ID: phantom_products_log
Revises: products_sku_partial_unique
Create Date: 2026-05-27
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "phantom_products_log"
down_revision = "products_sku_partial_unique"
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
        "   'ml_fl_stock', 'fl_stock_correction', 'phantom_detection'"
        " ])"
        ")"
    )

    op.create_table(
        "phantom_products_log",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("detection_run_id", sa.BigInteger, nullable=False),
        sa.Column("sku", sa.String(100), nullable=False),
        sa.Column(
            "product_active_tiny_id",
            sa.BigInteger,
            nullable=True,
            comment=(
                "tiny_id of the 'real' active/inactive product with this SKU. "
                "NULL when the catalog has zero non-excluded copies (critical)."
            ),
        ),
        sa.Column("num_excluded", sa.Integer, nullable=False),
        sa.Column(
            "excluded_tiny_ids",
            postgresql.ARRAY(sa.BigInteger),
            nullable=False,
            server_default="{}",
        ),
        sa.Column("orders_ml_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("units_ml", sa.Integer, nullable=False, server_default="0"),
        sa.Column("first_sale_date", sa.Date, nullable=True),
        sa.Column("last_sale_date", sa.Date, nullable=True),
        sa.Column(
            "investigation_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            comment=(
                "Forensic snapshot per phantom: descriptions of active+excluded "
                "products, recent ML orders that hit this SKU, suggested action."
            ),
        ),
        sa.Column(
            "detected_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        comment=(
            "Audit trail of phantom products (Tiny SKUs with excluded duplicates "
            "absorbing ML orders). One row per (run, sku). Append-only — never "
            "delete; the trend across runs is the value."
        ),
    )
    op.create_index("ix_phantom_log_sku", "phantom_products_log", ["sku"])
    op.create_index("ix_phantom_log_run", "phantom_products_log", ["detection_run_id"])
    op.create_index("ix_phantom_log_detected_at", "phantom_products_log", ["detected_at"])


def downgrade() -> None:
    op.drop_index("ix_phantom_log_detected_at", table_name="phantom_products_log")
    op.drop_index("ix_phantom_log_run", table_name="phantom_products_log")
    op.drop_index("ix_phantom_log_sku", table_name="phantom_products_log")
    op.drop_table("phantom_products_log")
    op.execute("ALTER TABLE sync_logs DROP CONSTRAINT IF EXISTS ck_sync_logs_valid_sync_type")
    op.execute(
        "ALTER TABLE sync_logs ADD CONSTRAINT ck_sync_logs_valid_sync_type CHECK ("
        " sync_type = ANY (ARRAY["
        "   'products', 'orders', 'stock', 'sale_buckets',"
        "   'token_rotation', 'mercadolivre_stock', 'invoices',"
        "   'stock_history', 'purchase_orders', 'ml_listings',"
        "   'ml_fl_stock', 'fl_stock_correction'"
        " ])"
        ")"
    )
