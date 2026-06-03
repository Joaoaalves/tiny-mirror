"""Add ML-sourced display prices.

Per the operator: the *product price* shown on the dashboard must come
from Mercado Livre, not the planilha (cost/commission/freight stay on the
planilha for margin math). Two new columns:

- ``ml_listings.price`` — the listing's full price on ML (item.price),
  captured each ml_listings sync. The "preço cheio" / "De".
- ``ml_promo_caps.active_promo_price`` — the lowest STARTED promo price
  on the MLB, set at recompute when has_active_promo. The real current
  selling price ("Por") the customer sees.

Revision ID: ml_prices_display
Revises: cap_has_active_promo
Create Date: 2026-06-03
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "ml_prices_display"
down_revision = "cap_has_active_promo"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("ml_listings", sa.Column("price", sa.Numeric(12, 2), nullable=True))
    op.add_column(
        "ml_promo_caps",
        sa.Column("active_promo_price", sa.Numeric(12, 2), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("ml_promo_caps", "active_promo_price")
    op.drop_column("ml_listings", "price")
