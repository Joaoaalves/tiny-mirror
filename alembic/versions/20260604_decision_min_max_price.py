"""Add min_price / max_price to ml_promo_decisions.

For INTERVAL promo types (DEAL/DOD/LIGHTNING/PRICE_DISCOUNT/SELLER_CAMPAIGN)
ML publishes an allowed price range [min_discounted_price, max_discounted_price].
We persist it so the operator sees the range when editing the target price.
NULL for fixed-price types (SMART / fixed_percentage / price_only) where ML
pins the price and there's no seller choice.

Revision ID: decision_min_max_price
Revises: ml_listings_linked
Create Date: 2026-06-04
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "decision_min_max_price"
down_revision = "ml_listings_linked"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("ml_promo_decisions", sa.Column("min_price", sa.Numeric(12, 2), nullable=True))
    op.add_column("ml_promo_decisions", sa.Column("max_price", sa.Numeric(12, 2), nullable=True))


def downgrade() -> None:
    op.drop_column("ml_promo_decisions", "max_price")
    op.drop_column("ml_promo_decisions", "min_price")
