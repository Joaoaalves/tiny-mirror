"""Add thumbnail + permalink to ml_listings.

Both are captured from the ML items API during the daily ml_listings
sync. ``thumbnail`` (secure URL) lets the promo dashboard show a product
image; ``permalink`` makes the MLB id a direct link to the listing.

Revision ID: ml_listings_thumb_permalink
Revises: decision_finish_date
Create Date: 2026-06-03
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "ml_listings_thumb_permalink"
down_revision = "decision_finish_date"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("ml_listings", sa.Column("thumbnail", sa.Text(), nullable=True))
    op.add_column("ml_listings", sa.Column("permalink", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("ml_listings", "permalink")
    op.drop_column("ml_listings", "thumbnail")
