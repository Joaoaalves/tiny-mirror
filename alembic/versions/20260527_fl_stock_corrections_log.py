"""Create fl_stock_corrections_log table + add 'fl_stock_correction' to sync_type CHECK.

Audit + investigation trail for the hourly cron that corrects the
'Full Mercado Livre' deposit in Tiny based on our DB (= ML truth).
Each mismatch detection writes a row regardless of whether the
correction succeeded — gives full forensic visibility.

Revision ID: fl_stock_corrections_log
Revises: ml_fl_stock_sync_type
Create Date: 2026-05-27
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "fl_stock_corrections_log"
down_revision = "ml_fl_stock_sync_type"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add to sync_type CHECK constraint
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

    # Audit + investigation log
    op.create_table(
        "fl_stock_corrections_log",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("product_tiny_id", sa.BigInteger, nullable=False),
        sa.Column("sku", sa.String(100), nullable=False),
        sa.Column("tiny_saldo_before", sa.Integer, nullable=False),
        sa.Column("ml_qty", sa.Integer, nullable=False),
        sa.Column("delta", sa.Integer, nullable=False),
        sa.Column(
            "correction_applied", sa.Boolean, nullable=False, server_default=sa.text("false")
        ),
        sa.Column("tiny_id_lancamento", sa.BigInteger, nullable=True),
        sa.Column("tiny_saldo_after", sa.Integer, nullable=True),
        sa.Column("http_status", sa.Integer, nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column(
            "investigation_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            comment=(
                "Snapshot for forensic analysis: tiny estoque response (all deposits "
                "+ saldo/reservado/disponivel), recent orders affecting the SKU, "
                "recent fulfillment_transfers, recent stock_history. Captured BEFORE "
                "the correction POST so we can later prove what state the SKU was in."
            ),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        comment=(
            "Audit trail of FL stock corrections: every mismatch detected by the "
            "hourly cron, whether or not the correction succeeded. Append-only — "
            "never delete rows. Investigation payload preserves enough context to "
            "diagnose recurring drift causes (Hypothesis 1 = NFs not cancelled, "
            "Hypothesis 2 = phantom products — see docs/03)."
        ),
    )
    op.create_index(
        "ix_fl_corrections_log_sku",
        "fl_stock_corrections_log",
        ["sku"],
    )
    op.create_index(
        "ix_fl_corrections_log_created_at",
        "fl_stock_corrections_log",
        ["created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_fl_corrections_log_created_at", table_name="fl_stock_corrections_log")
    op.drop_index("ix_fl_corrections_log_sku", table_name="fl_stock_corrections_log")
    op.drop_table("fl_stock_corrections_log")
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
