"""Per-MLB calibrated ML fees for Flex (non-fulfillment) listings.

The spreadsheet commission and the generic freight bands are correct for
fulfillment but wrong for Flex/self_service/xd_drop_off listings (ML charges a
different effective commission and the seller freight depends on the R$79
free-shipping band, not the price-tier bands we apply). This table holds what
ML *actually* charged per Flex listing, derived from settled orders.

Revision ID: ml_flex_fee_calibration
Revises: fl_coverage_drop_pending
Create Date: 2026-06-08
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "ml_flex_fee_calibration"
down_revision = "fl_coverage_drop_pending"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ml_flex_fee_calibration",
        sa.Column("mlb_id", sa.String(length=20), primary_key=True),
        sa.Column("sku", sa.String(length=100), nullable=True),
        sa.Column("n_sales", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("real_comm_pct", sa.Numeric(5, 2), nullable=True),
        sa.Column("freight_per_unit_lt79", sa.Numeric(8, 2), nullable=True),
        sa.Column("freight_per_unit_ge79", sa.Numeric(8, 2), nullable=True),
        sa.Column("n_freight_lt79", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("n_freight_ge79", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )


def downgrade() -> None:
    op.drop_table("ml_flex_fee_calibration")
