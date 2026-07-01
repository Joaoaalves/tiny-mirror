"""Add revenue to ml_sales_daily — receita diária por anúncio (MLB).

``ml_sales_daily`` já guarda ``qty`` por (mlb_id, sale_date), mas não a receita.
A Curva ABC (Pareto 80/15/5) do Estoque Full precisa ranquear anúncios pela
receita FULL real — não por um proxy qty*preco. Somamos ``unit_price * quantity``
de cada ``order_item`` na ML Orders API (o preço efetivamente cobrado, já com a
promo aplicada). Coluna nasce ``0`` e é preenchida no próximo backfill.

Revision ID: ml_sales_revenue
Revises: resub_strict_promo_id
Create Date: 2026-07-01
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "ml_sales_revenue"
down_revision = "resub_strict_promo_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ml_sales_daily",
        sa.Column(
            "revenue",
            sa.Numeric(12, 2),
            nullable=False,
            server_default=sa.text("0"),
            comment=(
                "Receita diária do anúncio no Mercado Livre (soma de "
                "unit_price*quantity dos order_items, preço já com promo). "
                "Base da Curva ABC do Estoque Full. Preenchida a partir do "
                "próximo backfill; linhas antigas ficam 0 até um backfill >=90d."
            ),
        ),
    )


def downgrade() -> None:
    op.drop_column("ml_sales_daily", "revenue")
