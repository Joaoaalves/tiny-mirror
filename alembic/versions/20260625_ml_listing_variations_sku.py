"""ml_listing_variations: per-variation sku + available_quantity.

Variation listings (e.g. POL-PAST-A45DIVNEON, one listing with 4 colours)
carry ``sku=NULL`` at the item level — the per-variation seller SKU is exposed
by ML *only* via ``GET /user-products/{user_product_id}`` (the item-level and
variation-level ``attributes`` omit ``SELLER_SKU``). The ml_listings sync now
fetches it and stores it here, alongside the variation's available_quantity,
so variation children surface in the FL reposição table — they were invisible
because that query filters ``ml_listings.sku <> ''`` and the parent row's sku
is null.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "ml_listing_variations_sku"
down_revision = "mv_coverage_v17_fl_pending"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ml_listing_variations",
        sa.Column("sku", sa.String(length=100), nullable=True),
    )
    op.add_column(
        "ml_listing_variations",
        sa.Column("available_quantity", sa.Integer(), nullable=True),
    )
    op.create_index(
        "ix_ml_listing_variations_sku",
        "ml_listing_variations",
        ["sku"],
    )


def downgrade() -> None:
    op.drop_index("ix_ml_listing_variations_sku", table_name="ml_listing_variations")
    op.drop_column("ml_listing_variations", "available_quantity")
    op.drop_column("ml_listing_variations", "sku")
