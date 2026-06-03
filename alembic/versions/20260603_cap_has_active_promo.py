"""Add has_active_promo to ml_promo_caps.

Set during the daily cap recompute: True when ML's seller-promotions API
returns a STARTED promo for the MLB at recompute time, False otherwise.
Gives the promo dashboard an authoritative, current "has an active promo
on ML" signal per anúncio (the old proxy — cumulative `started` decisions
— never expired, so it counted ended promos as active).

Revision ID: cap_has_active_promo
Revises: ml_listings_thumb_permalink
Create Date: 2026-06-03
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "cap_has_active_promo"
down_revision = "ml_listings_thumb_permalink"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ml_promo_caps",
        sa.Column(
            "has_active_promo",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )


def downgrade() -> None:
    op.drop_column("ml_promo_caps", "has_active_promo")
