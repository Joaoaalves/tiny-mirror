"""Add price-banded freight schedule to ml_flex_fee_calibration.

The flat per-listing freight (shipping_options quote at the CURRENT price) is only
right in the price bracket the listing sells at today: ML's seller freight is a
function of (dimensions, price bracket) — e.g. a 56L box costs the seller R$11,75
under R$79 and R$48,55 above. Mercado Turbo projects it per simulated price via
ML's freight calculator (`/users/{uid}/shipping_options/free?dimensions=&item_price=`,
verified exact on 7/7 probes). We store that schedule here as ``freight_bands``
(JSONB list of ``{min, max, cost}``) so the margin engine prices freight at the
PROMO price, not today's price. Flex only; fulfillment untouched.

Revision ID: flex_freight_bands
Revises: flex_commission_bands
Create Date: 2026-07-08
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "flex_freight_bands"
down_revision = "flex_commission_bands"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ml_flex_fee_calibration",
        sa.Column("freight_bands", JSONB, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("ml_flex_fee_calibration", "freight_bands")
