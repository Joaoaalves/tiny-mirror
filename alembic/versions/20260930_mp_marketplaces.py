"""mp_orders + mp_listings — other marketplaces; v_order_datetime covers them.

DASH requests: exact order time beyond ML (Tiny orders carry only a date,
verified on v2 and v3) and the active listings per channel. Amazon (SP-API)
and Shopee are the channels with credentials today (OpenClaw owns them and
exports a read-only copy). Tiny links by ``ecommerce_order_number`` =
Amazon order id / Shopee order_sn.

v_order_datetime keeps its columns (tiny_id, order_datetime, source) and now
unions ML (earliest order of the pack) with mp_orders (source = channel).

Revision ID: mp_marketplaces
Revises: ml_claims_shipment_link
Create Date: 2026-09-30
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "mp_marketplaces"
down_revision = "ml_claims_shipment_link"
branch_labels = None
depends_on = None

_ML_ONLY = """
        SELECT o.tiny_id,
               m.order_datetime,
               'mercadolivre'::text AS source
        FROM orders o
        JOIN (
            SELECT tiny_ref, min(date_created) AS order_datetime
            FROM ml_orders
            GROUP BY tiny_ref
        ) m ON m.tiny_ref = o.ecommerce_order_number
"""

_MARKETPLACES = """
        UNION ALL
        SELECT o.tiny_id, mp.created_at AS order_datetime, mp.channel::text AS source
        FROM orders o
        JOIN mp_orders mp ON mp.order_id = o.ecommerce_order_number
"""


def upgrade() -> None:
    op.create_table(
        "mp_orders",
        sa.Column("channel", sa.String(20), primary_key=True),
        sa.Column("order_id", sa.String(40), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_updated", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(40), nullable=True),
        sa.Column("fulfilled_by", sa.String(40), nullable=True),
        sa.Column("total_amount", sa.Numeric(12, 2), nullable=True),
        sa.Column("currency", sa.String(5), nullable=True),
        sa.Column("raw", JSONB, nullable=False),
        sa.Column(
            "synced_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        comment=(
            "Pedidos de outros marketplaces (amazon, shopee) com hora exata. Liga no "
            "Tiny por order_id = orders.ecommerce_order_number."
        ),
    )
    op.create_index("ix_mp_orders_created_at", "mp_orders", ["created_at"])
    op.create_index("ix_mp_orders_order_id", "mp_orders", ["order_id"])
    op.create_table(
        "mp_listings",
        sa.Column("channel", sa.String(20), primary_key=True),
        sa.Column("listing_id", sa.String(80), primary_key=True),
        sa.Column("variation_id", sa.String(40), primary_key=True),
        sa.Column("sku", sa.String(100), nullable=True),
        sa.Column("external_id", sa.String(40), nullable=True),
        sa.Column("title", sa.Text, nullable=True),
        sa.Column("status", sa.String(80), nullable=True),
        sa.Column("is_active", sa.Boolean, nullable=False),
        sa.Column("price", sa.Numeric(12, 2), nullable=True),
        sa.Column("stock", sa.Integer, nullable=True),
        sa.Column("url", sa.Text, nullable=True),
        sa.Column("raw", JSONB, nullable=False),
        sa.Column(
            "synced_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        comment=(
            "Anúncios de outros marketplaces (amazon, shopee), snapshot atual por canal. "
            "is_active = vendável agora."
        ),
    )
    op.create_index("ix_mp_listings_sku", "mp_listings", ["sku"])
    op.execute("CREATE OR REPLACE VIEW v_order_datetime AS" + _ML_ONLY + _MARKETPLACES)


def downgrade() -> None:
    op.execute("CREATE OR REPLACE VIEW v_order_datetime AS" + _ML_ONLY)
    op.drop_table("mp_listings")
    op.drop_table("mp_orders")
