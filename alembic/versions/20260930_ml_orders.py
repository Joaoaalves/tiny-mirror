"""ml_orders — per-order Mercado Livre data the Tiny mirror does not have.

DASH requests (2026-09-30): the Tiny order has only a DATE (no time field in
the v3 detail), and no ML commission or seller freight. The ML Orders API
has all of it:

- ml_orders: every ML order (all statuses) with the exact date_created,
  pack_id, buyer id, cancel_detail. ``tiny_ref`` = pack_id if present, else
  order_id = Tiny ``orders.ecommerce_order_number`` (verified 12/12 live).
- ml_order_items: MLB, SKU, qty, unit price, sale_fee (commission PER UNIT).
- ml_shipments: seller freight (senders.cost), ML discount to the seller
  (senders.save), buyer freight, list cost — per shipment, because one
  shipment can serve several orders of a pack.
- v_order_datetime: Tiny order -> exact order time (earliest ML order of
  the pack). Only ML orders; other channels have no time source.

New tables are owned by tiny_mirror, so tiny_readonly gets SELECT through
the default privileges; the view grant is explicit.

Revision ID: ml_orders
Revises: mv_coverage_v18_deposit_ignore
Create Date: 2026-09-30
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "ml_orders"
down_revision = "mv_coverage_v18_deposit_ignore"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ml_orders",
        sa.Column("order_id", sa.BigInteger, primary_key=True, autoincrement=False),
        sa.Column("pack_id", sa.BigInteger, nullable=True),
        sa.Column("tiny_ref", sa.String(30), nullable=False),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("date_created", sa.DateTime(timezone=True), nullable=False),
        sa.Column("date_closed", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_updated", sa.DateTime(timezone=True), nullable=True),
        sa.Column("buyer_id", sa.BigInteger, nullable=True),
        sa.Column("total_amount", sa.Numeric(12, 2), nullable=True),
        sa.Column("paid_amount", sa.Numeric(12, 2), nullable=True),
        sa.Column("shipping_id", sa.BigInteger, nullable=True),
        sa.Column("cancel_detail", JSONB, nullable=True),
        sa.Column("tags", JSONB, nullable=True),
        sa.Column(
            "synced_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        comment=(
            "Pedidos do Mercado Livre (todos os status), da ML Orders API. "
            "date_created tem hora exata (o Tiny só guarda a data). Liga no Tiny "
            "por tiny_ref = orders.ecommerce_order_number."
        ),
    )
    op.create_index("ix_ml_orders_tiny_ref", "ml_orders", ["tiny_ref"])
    op.create_index("ix_ml_orders_date_created", "ml_orders", ["date_created"])
    op.create_index("ix_ml_orders_shipping_id", "ml_orders", ["shipping_id"])

    op.create_table(
        "ml_order_items",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "order_id",
            sa.BigInteger,
            sa.ForeignKey("ml_orders.order_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("mlb_id", sa.String(20), nullable=False),
        sa.Column("variation_id", sa.BigInteger, nullable=True),
        sa.Column("seller_sku", sa.String(100), nullable=True),
        sa.Column("quantity", sa.Integer, nullable=False),
        sa.Column("unit_price", sa.Numeric(12, 2), nullable=True),
        sa.Column("sale_fee", sa.Numeric(12, 2), nullable=True),
        sa.Column("listing_type_id", sa.String(30), nullable=True),
        comment=(
            "Itens dos pedidos do ML. sale_fee é a comissão do ML POR UNIDADE "
            "(total da linha = sale_fee * quantity)."
        ),
    )
    op.create_index("ix_ml_order_items_order_id", "ml_order_items", ["order_id"])
    op.create_index("ix_ml_order_items_mlb_id", "ml_order_items", ["mlb_id"])

    op.create_table(
        "ml_shipments",
        sa.Column("shipment_id", sa.BigInteger, primary_key=True, autoincrement=False),
        sa.Column("logistic_type", sa.String(30), nullable=True),
        sa.Column("seller_cost", sa.Numeric(12, 2), nullable=True),
        sa.Column("seller_save", sa.Numeric(12, 2), nullable=True),
        sa.Column("buyer_cost", sa.Numeric(12, 2), nullable=True),
        sa.Column("list_cost", sa.Numeric(12, 2), nullable=True),
        sa.Column(
            "fetched_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        comment=(
            "Envios do ML (1 envio pode atender vários pedidos de um pack). "
            "seller_cost = frete pago pelo vendedor, seller_save = desconto do ML "
            "ao vendedor, buyer_cost = frete do comprador, list_cost = gross_amount."
        ),
    )

    op.execute(
        """
        CREATE VIEW v_order_datetime AS
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
    )
    op.execute(
        "COMMENT ON VIEW v_order_datetime IS 'Hora exata do pedido do Tiny (timestamptz), "
        "a partir do pedido do ML mais antigo do pack. So pedidos do Mercado Livre.'"
    )
    op.execute("GRANT SELECT ON v_order_datetime TO tiny_readonly")


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS v_order_datetime")
    op.drop_table("ml_shipments")
    op.drop_table("ml_order_items")
    op.drop_table("ml_orders")
