"""Add full_qty/full_revenue to ml_sales_daily — vendas despachadas por Full.

O "Vendas 30 dias" do painel Full do ML conta só unidades **despachadas pelo
Full** (``shipment.logistic_type='fulfillment'``), não Flex/self_service. O
``logistic_type`` não vem no pedido, então o sync busca o shipment e separa a
parcela Full. ``qty``/``revenue`` seguem sendo o total (todos os canais);
``full_qty``/``full_revenue`` são só a parte Full. Estoque Full usa a Full.

Revision ID: ml_sales_full_qty
Revises: ml_fl_tracking
Create Date: 2026-07-01
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "ml_sales_full_qty"
down_revision = "ml_fl_tracking"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ml_sales_daily",
        sa.Column(
            "full_qty",
            sa.Integer,
            nullable=False,
            server_default=sa.text("0"),
            comment="Unidades despachadas por Full (logistic_type=fulfillment). Subconjunto de qty.",
        ),
    )
    op.add_column(
        "ml_sales_daily",
        sa.Column(
            "full_revenue",
            sa.Numeric(12, 2),
            nullable=False,
            server_default=sa.text("0"),
            comment="Receita das unidades Full. Subconjunto de revenue.",
        ),
    )


def downgrade() -> None:
    op.drop_column("ml_sales_daily", "full_revenue")
    op.drop_column("ml_sales_daily", "full_qty")
