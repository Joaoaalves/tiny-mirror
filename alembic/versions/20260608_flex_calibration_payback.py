"""Add ML freight-payback (subsidy) columns to ml_flex_fee_calibration.

The seller freight we already store is the NET cost (what we pay). For an
informative UI we also surface how much ML subsidises the shipping (the
"payback" = senders.save), per R$79 band.

Revision ID: flex_calibration_payback
Revises: ml_flex_fee_calibration
Create Date: 2026-06-08
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "flex_calibration_payback"
down_revision = "ml_flex_fee_calibration"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ml_flex_fee_calibration",
        sa.Column("payback_per_unit_lt79", sa.Numeric(8, 2), nullable=True),
    )
    op.add_column(
        "ml_flex_fee_calibration",
        sa.Column("payback_per_unit_ge79", sa.Numeric(8, 2), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("ml_flex_fee_calibration", "payback_per_unit_ge79")
    op.drop_column("ml_flex_fee_calibration", "payback_per_unit_lt79")
