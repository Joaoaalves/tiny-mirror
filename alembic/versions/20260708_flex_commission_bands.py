"""Add nominal ML commission schedule (price-banded) to ml_flex_fee_calibration.

The historical median commission (``real_comm_pct``) is a single flat rate and
can't reproduce ML's per-price fee schedule: many categories drop the % a few
points in a mid-price band (e.g. gold_pro 17% → 14% between ~R$150-500 → 17%).
Mercado Turbo reads the nominal schedule from ``/sites/MLB/listing_prices`` — we
store it here as ``commission_bands`` (JSONB list of ``{min, max, pct}``) so the
margin engine can look commission up by band and match Turbo exactly. Flex only;
fulfillment is never touched.

Revision ID: flex_commission_bands
Revises: ml_sales_full_qty
Create Date: 2026-07-08
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "flex_commission_bands"
down_revision = "ml_sales_full_qty"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ml_flex_fee_calibration",
        sa.Column("commission_bands", JSONB, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("ml_flex_fee_calibration", "commission_bands")
