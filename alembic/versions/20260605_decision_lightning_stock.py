"""Add lightning stock fields to ml_promo_decisions.

Ofertas Relâmpago (LIGHTNING) — e Oferta do Dia (DOD) — exigem reservar um
estoque para a oferta, dentro de uma faixa [min, max] que o ML publica em
``offer.stock``. Persistimos a faixa (stock_min/stock_max) e a quantidade
escolhida pelo operador na aprovação (stock_chosen), que vai no POST de
ativação.

Revision ID: decision_lightning_stock
Revises: ml_listings_avail_qty
Create Date: 2026-06-05
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "decision_lightning_stock"
down_revision = "ml_listings_avail_qty"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("ml_promo_decisions", sa.Column("stock_min", sa.Integer(), nullable=True))
    op.add_column("ml_promo_decisions", sa.Column("stock_max", sa.Integer(), nullable=True))
    op.add_column("ml_promo_decisions", sa.Column("stock_chosen", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("ml_promo_decisions", "stock_chosen")
    op.drop_column("ml_promo_decisions", "stock_max")
    op.drop_column("ml_promo_decisions", "stock_min")
