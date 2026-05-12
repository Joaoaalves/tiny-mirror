"""Add ml_listings and ml_listing_variations tables.

ml_listings — one row per active ML listing (MLB ID), populated by the
  daily ml_listings sync. Stores the seller SKU, logistic type, and
  item-level inventory_id so stock sync can look up MLB IDs from the DB
  instead of calling the ML search API per product.

ml_listing_variations — one row per variation on a listing that has
  variations. Tracks per-variation inventory_id, which is required for
  items where inventory is managed at the variation level (item-level
  inventory_id is null in those cases).

Revision ID: ml_listings
Revises: stock_history_purchase_orders
Create Date: 2026-05-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "ml_listings"
down_revision = "stock_history_purchase_orders"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ml_listings",
        sa.Column("mlb_id", sa.String(50), primary_key=True),
        sa.Column("sku", sa.String(100), nullable=True),
        sa.Column("logistic_type", sa.String(50), nullable=True),
        sa.Column("status", sa.String(50), nullable=True),
        sa.Column("inventory_id", sa.String(50), nullable=True),
        sa.Column("has_variations", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column(
            "synced_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_ml_listings_sku", "ml_listings", ["sku"])
    op.create_index("ix_ml_listings_logistic_type", "ml_listings", ["logistic_type"])

    op.create_table(
        "ml_listing_variations",
        sa.Column(
            "mlb_id",
            sa.String(50),
            sa.ForeignKey("ml_listings.mlb_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("variation_id", sa.BigInteger(), nullable=False),
        sa.Column("inventory_id", sa.String(50), nullable=True),
        sa.PrimaryKeyConstraint("mlb_id", "variation_id"),
    )
    op.create_index(
        "ix_ml_listing_variations_inventory_id", "ml_listing_variations", ["inventory_id"]
    )


def downgrade() -> None:
    op.drop_table("ml_listing_variations")
    op.drop_table("ml_listings")
