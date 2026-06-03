"""Add linked_mlb_ids to ml_listings.

Captures ML's ``item_relations`` (the catalog↔traditional linkage). When a
catalog listing and a traditional listing are LINKED, each carries the
other's MLB id here (with a shared user_product_id + stock_relation), so a
promo applied to one applies to both. When NOT linked, the array is empty.
Lets the promo dashboard act only on the catalog listing for linked pairs
and flag the traditional as managed-via-catalog.

Revision ID: ml_listings_linked
Revises: ml_prices_display
Create Date: 2026-06-03
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "ml_listings_linked"
down_revision = "ml_prices_display"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ml_listings",
        sa.Column("linked_mlb_ids", JSONB(), nullable=False, server_default=sa.text("'[]'::jsonb")),
    )


def downgrade() -> None:
    op.drop_column("ml_listings", "linked_mlb_ids")
