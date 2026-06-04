"""Add ml_sales_daily — vendas por anúncio (MLB) por dia, só Mercado Livre.

Os pedidos sincronizados do Tiny só guardam o SKU, não o MLB. A divisão de
vendas por anúncio (que é o que importa pra promoções) vem da ML Orders API
(``/orders/search``), que traz ``order_items[].item.id`` (MLB) + seller_sku +
quantity + date_created. Agregamos por (mlb_id, sale_date). Só Mercado Livre
por construção (é a API do ML). Alimenta demanda + gráficos por SKU base e
por anúncio.

Revision ID: ml_sales_daily
Revises: decision_min_max_price
Create Date: 2026-06-04
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "ml_sales_daily"
down_revision = "decision_min_max_price"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ml_sales_daily",
        sa.Column("mlb_id", sa.String(20), primary_key=True),
        sa.Column("sale_date", sa.Date(), primary_key=True),
        sa.Column("sku", sa.String(100), nullable=True),
        sa.Column("qty", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        comment=(
            "Vendas diárias por anúncio (MLB) no Mercado Livre, da ML Orders API. "
            "Fonte da demanda e dos gráficos de vendas (por SKU base e por anúncio)."
        ),
    )
    op.create_index("ix_ml_sales_daily_sku", "ml_sales_daily", ["sku"])
    op.create_index("ix_ml_sales_daily_date", "ml_sales_daily", ["sale_date"])


def downgrade() -> None:
    op.drop_table("ml_sales_daily")
