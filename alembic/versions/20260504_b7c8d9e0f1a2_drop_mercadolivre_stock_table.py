"""Drop mercadolivre_stock table.

The Full ML stock now lives inline in ``stock_deposits`` (the per-product
stock sync overwrites the unreliable Tiny "Full Mercado Livre" row with
the authoritative quantity from the ML API). The dedicated table is
no longer needed.

The ``sync_logs.valid_sync_type`` CHECK constraint is intentionally left
alone — historical rows with ``sync_type='mercadolivre_stock'`` (from
the brief life of the dedicated cron) would violate a tighter
constraint, and tolerating the extra value is harmless.

The ``ml_oauth_tokens`` table also stays — it is still required for ML
auth.

Revision ID: b7c8d9e0f1a2
Revises: a1b2c3d4e5f6
Create Date: 2026-05-04
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "b7c8d9e0f1a2"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_mercadolivre_stock_logistic_type", table_name="mercadolivre_stock")
    op.drop_index("ix_mercadolivre_stock_sku", table_name="mercadolivre_stock")
    op.drop_table("mercadolivre_stock")


def downgrade() -> None:
    op.create_table(
        "mercadolivre_stock",
        sa.Column("sku", sa.String(100), nullable=False),
        sa.Column("mlb_id", sa.String(50), nullable=False),
        sa.Column(
            "available_quantity",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("logistic_type", sa.String(50), nullable=False),
        sa.Column(
            "status",
            sa.String(50),
            nullable=False,
            server_default=sa.text("'active'"),
        ),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("sku", "mlb_id"),
    )
    op.create_index("ix_mercadolivre_stock_sku", "mercadolivre_stock", ["sku"])
    op.create_index("ix_mercadolivre_stock_logistic_type", "mercadolivre_stock", ["logistic_type"])
