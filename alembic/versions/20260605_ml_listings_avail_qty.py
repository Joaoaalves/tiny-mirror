"""Add available_quantity (estoque) to ml_listings.

O sync de anúncios já guarda o status (active/paused/closed); agora também
persiste o estoque disponível (item.available_quantity). O ML pausa o anúncio
quando zera o estoque, mas isso pode demorar — o estoque dá o sinal direto de
vendável e ajuda a esconder anúncios mortos sem esperar o ML pausar.

Revision ID: ml_listings_avail_qty
Revises: ml_sales_daily
Create Date: 2026-06-05
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "ml_listings_avail_qty"
down_revision = "ml_sales_daily"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("ml_listings", sa.Column("available_quantity", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("ml_listings", "available_quantity")
