"""Add ml_oauth_tokens, mercadolivre_stock tables; expand sync_type constraint.

Revision ID: a1b2c3d4e5f6
Revises: d8a3f1c0e2a7
Create Date: 2026-05-04

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "a1b2c3d4e5f6"
down_revision = "d8a3f1c0e2a7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ml_oauth_tokens",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("access_token", sa.Text(), nullable=False),
        sa.Column("refresh_token", sa.Text(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("refresh_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        comment=(
            "Stores the single active OAuth2 token for Mercado Livre API. "
            "Always contains exactly one row."
        ),
    )

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
        comment=(
            "Per-listing stock levels from the ML API. "
            "Sum available_quantity WHERE logistic_type='fulfillment' per SKU for Full ML stock."
        ),
    )
    op.create_index("ix_mercadolivre_stock_sku", "mercadolivre_stock", ["sku"])
    op.create_index(
        "ix_mercadolivre_stock_logistic_type", "mercadolivre_stock", ["logistic_type"]
    )

    # Expand sync_logs.sync_type to include 'mercadolivre_stock'.
    # Pass type_="check" so the naming convention prefix (ck_<table>_) applies.
    op.drop_constraint("valid_sync_type", "sync_logs", type_="check")
    op.create_check_constraint(
        "valid_sync_type",
        "sync_logs",
        "sync_type IN ('products', 'orders', 'stock', 'sale_buckets', 'token_rotation', "
        "'mercadolivre_stock')",
    )


def downgrade() -> None:
    op.drop_constraint("valid_sync_type", "sync_logs", type_="check")
    op.create_check_constraint(
        "valid_sync_type",
        "sync_logs",
        "sync_type IN ('products', 'orders', 'stock', 'sale_buckets', 'token_rotation')",
    )

    op.drop_index("ix_mercadolivre_stock_logistic_type", table_name="mercadolivre_stock")
    op.drop_index("ix_mercadolivre_stock_sku", table_name="mercadolivre_stock")
    op.drop_table("mercadolivre_stock")
    op.drop_table("ml_oauth_tokens")
