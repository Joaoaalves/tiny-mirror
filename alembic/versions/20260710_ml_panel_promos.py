"""ml_panel_promos — panel-truth promotion values scraped from the ML seller panel.

The official seller-promotions API serves stale/absent data for CANDIDATE
promotions (suggested prices, SMART percentages, whole campaigns missing),
while the seller panel (/anuncios/lista/promos) always shows the current
truth — including the SMART tariff reduction ("Reduzimos R$ X das suas
tarifas") and the full cost detail per promotion. We scrape that page's
embedded JSON hourly (cookie session) and overlay these values on the
board's candidates. Started promotions stay on the API (already exact).

Revision ID: ml_panel_promos
Revises: flex_freight_bands
Create Date: 2026-07-10
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "ml_panel_promos"
down_revision = "flex_freight_bands"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ml_panel_promos",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("mlb_id", sa.String(20), nullable=False),
        sa.Column("promo_name", sa.Text, nullable=False),
        sa.Column("badge", sa.Text, nullable=True),
        sa.Column("status_chip", sa.String(30), nullable=True),
        sa.Column("vigencia", sa.Text, nullable=True),
        sa.Column("discount_value", sa.Numeric(12, 2), nullable=True),
        sa.Column("discount_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("final_price", sa.Numeric(12, 2), nullable=True),
        sa.Column("you_receive", sa.Numeric(12, 2), nullable=True),
        sa.Column("sale_fee", sa.Numeric(12, 2), nullable=True),
        sa.Column("shipping_cost", sa.Numeric(12, 2), nullable=True),
        sa.Column("listing_type_label", sa.Text, nullable=True),
        sa.Column("meli_reduction", sa.Numeric(12, 2), nullable=True),
        sa.Column("is_suggested", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("is_coupon", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("action_label", sa.Text, nullable=True),
        sa.Column(
            "scraped_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("mlb_id", "promo_name", name="uq_ml_panel_promos_mlb_name"),
    )
    op.create_index("ix_ml_panel_promos_mlb", "ml_panel_promos", ["mlb_id"])
    op.create_index("ix_ml_panel_promos_scraped", "ml_panel_promos", ["scraped_at"])


def downgrade() -> None:
    op.drop_table("ml_panel_promos")
