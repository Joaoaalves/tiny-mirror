"""Add promo_id + api_promo_type to ml_panel_promos (enroll panel-only campaigns).

Each panel row's action button carries the campaign's promoId + promotionType;
mapped to the API's promotion_type they let us enroll a listing in a campaign
the seller-promotions API doesn't list for it (panel-only).

Revision ID: panel_promo_id
Revises: ml_panel_promos
Create Date: 2026-07-10
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "panel_promo_id"
down_revision = "ml_panel_promos"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("ml_panel_promos", sa.Column("promo_id", sa.String(40), nullable=True))
    op.add_column("ml_panel_promos", sa.Column("api_promo_type", sa.String(40), nullable=True))


def downgrade() -> None:
    op.drop_column("ml_panel_promos", "api_promo_type")
    op.drop_column("ml_panel_promos", "promo_id")
