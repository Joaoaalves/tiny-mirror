"""Create invoice_items table — line items of each Nota Fiscal.

Until now, invoices were synced as headers only. The `order_items` table
stores the line of the *order* — which always carries the parent kit
SKU — so when Tiny ships a kit and decrements component stock, the
component (e.g. CAMP-CNJ-FACPEG inside KIT-FACAPEG-ESCV-GARR) never
shows up in our DB. The NF, however, is generated with the *actual*
SKUs that got decremented; `GET /notas/{id}` returns one `itens[]` entry
per real product line.

This table mirrors that array so phantom detection (and any other
sales-history view) can compute true per-SKU sales counts from invoices
instead of guessing from order_items or stock_history deltas.

Revision ID: invoice_items
Revises: phantom_products_log
Create Date: 2026-05-27
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "invoice_items"
down_revision = "phantom_products_log"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "invoice_items",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "invoice_tiny_id",
            sa.BigInteger,
            sa.ForeignKey("invoices.tiny_id", ondelete="CASCADE"),
            nullable=False,
            comment="FK -> invoices.tiny_id",
        ),
        sa.Column(
            "tiny_item_id",
            sa.BigInteger,
            nullable=True,
            comment="idItem returned by Tiny — stable per invoice line.",
        ),
        sa.Column(
            "product_tiny_id",
            sa.BigInteger,
            nullable=True,
            comment=(
                "idProduto on the invoice line. Critical for phantom detection: "
                "this is the *actual* product whose stock was decremented, which "
                "may be a kit component never seen in order_items."
            ),
        ),
        sa.Column(
            "product_sku",
            sa.String(100),
            nullable=False,
            server_default="",
            comment="codigo on the invoice line (= SKU as Tiny knows it).",
        ),
        sa.Column("product_description", sa.Text, nullable=True),
        sa.Column("ncm", sa.String(20), nullable=True),
        sa.Column("unit", sa.String(20), nullable=True, comment="unidade (UN, CX, etc.)"),
        sa.Column(
            "quantity",
            sa.Numeric(10, 2),
            nullable=False,
            server_default="0",
        ),
        sa.Column("unit_value", sa.Numeric(12, 2), nullable=False, server_default="0"),
        sa.Column("total_value", sa.Numeric(12, 2), nullable=False, server_default="0"),
        sa.Column(
            "cfop",
            sa.String(10),
            nullable=True,
            comment="Fiscal operation code. 6XXX = outside state, 5XXX = inside.",
        ),
        sa.Column(
            "operation_nature",
            sa.String(200),
            nullable=True,
            comment="naturezaOperacao — 'Venda de mercadorias', 'Devolução', etc.",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        comment=(
            "Line items of each Nota Fiscal. Each row = one product line on "
            "the NF, captured from the GET /notas/{id} detail call. The source "
            "of truth for 'which SKU actually shipped on this NF', including "
            "kit components that order_items never sees."
        ),
    )
    op.create_index(
        "ix_invoice_items_invoice_tiny_id",
        "invoice_items",
        ["invoice_tiny_id"],
    )
    op.create_index(
        "ix_invoice_items_product_tiny_id",
        "invoice_items",
        ["product_tiny_id"],
    )
    op.create_index(
        "ix_invoice_items_product_sku",
        "invoice_items",
        ["product_sku"],
    )
    op.create_unique_constraint(
        "uq_invoice_items_invoice_line",
        "invoice_items",
        ["invoice_tiny_id", "tiny_item_id"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_invoice_items_invoice_line", "invoice_items", type_="unique")
    op.drop_index("ix_invoice_items_product_sku", table_name="invoice_items")
    op.drop_index("ix_invoice_items_product_tiny_id", table_name="invoice_items")
    op.drop_index("ix_invoice_items_invoice_tiny_id", table_name="invoice_items")
    op.drop_table("invoice_items")
