"""add invoices table and update sync_logs valid_sync_type constraint

Revision ID: 20260507_invoices
Revises: sku_prefix_supplier_v1
Create Date: 2026-05-07

"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260507_invoices"
down_revision: Union[str, None] = "sku_prefix_supplier_v1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "invoices",
        sa.Column(
            "tiny_id",
            sa.BigInteger(),
            autoincrement=False,
            nullable=False,
            comment="Unique NF identifier in Tiny ERP. Primary key. Never changes.",
        ),
        sa.Column("number", sa.String(20), nullable=False, comment="NF number (numero)."),
        sa.Column("series", sa.String(5), nullable=False, comment="NF series (serie)."),
        sa.Column(
            "access_key",
            sa.String(50),
            nullable=True,
            comment="44-digit SEFAZ access key (chaveAcesso). NULL if not yet authorized.",
        ),
        sa.Column(
            "status",
            sa.String(5),
            nullable=False,
            comment="Tiny status code (situacao). '6'=authorized, '4'=cancelled.",
        ),
        sa.Column(
            "type",
            sa.String(5),
            nullable=False,
            comment="NF type (tipo). 'S'=sale (saída).",
        ),
        sa.Column("issue_date", sa.Date(), nullable=False, comment="Emission date (dataEmissao)."),
        sa.Column(
            "forecast_date", sa.Date(), nullable=True, comment="Forecast date (dataPrevista)."
        ),
        sa.Column(
            "customer",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
            comment="Full customer object (cliente) — name, CPF/CNPJ, address.",
        ),
        sa.Column(
            "delivery_address",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            comment="Delivery address (enderecoEntrega). NULL when same as customer address.",
        ),
        sa.Column(
            "seller",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            comment="Seller object (vendedor). NULL when not assigned.",
        ),
        sa.Column(
            "total_value",
            sa.Numeric(12, 2),
            nullable=False,
            server_default=sa.text("0"),
            comment="Total NF value (valor).",
        ),
        sa.Column(
            "products_value",
            sa.Numeric(12, 2),
            nullable=False,
            server_default=sa.text("0"),
            comment="Products subtotal (valorProdutos).",
        ),
        sa.Column(
            "freight_value",
            sa.Numeric(12, 2),
            nullable=False,
            server_default=sa.text("0"),
            comment="Freight value (valorFrete).",
        ),
        sa.Column(
            "shipping_method_id",
            sa.BigInteger(),
            nullable=True,
            comment="Shipping method ID (idFormaEnvio).",
        ),
        sa.Column(
            "freight_type_id",
            sa.BigInteger(),
            nullable=True,
            comment="Freight type ID (idFormaFrete). 0 = not set.",
        ),
        sa.Column(
            "tracking_code",
            sa.String(100),
            nullable=True,
            comment="Carrier tracking code (codigoRastreamento).",
        ),
        sa.Column(
            "tracking_url",
            sa.Text(),
            nullable=True,
            comment="Carrier tracking URL (urlRastreamento).",
        ),
        sa.Column(
            "freight_responsibility",
            sa.String(5),
            nullable=True,
            comment="Freight responsibility (fretePorConta). 'T'=carrier, 'R'=recipient.",
        ),
        sa.Column(
            "volume_count",
            sa.Integer(),
            nullable=True,
            comment="Number of shipping volumes (qtdVolumes).",
        ),
        sa.Column(
            "gross_weight",
            sa.Numeric(10, 4),
            nullable=True,
            comment="Gross weight in kg (pesoBruto).",
        ),
        sa.Column(
            "net_weight",
            sa.Numeric(10, 4),
            nullable=True,
            comment="Net weight in kg (pesoLiquido).",
        ),
        sa.Column(
            "ecommerce",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            comment=(
                "Full ecommerce object: id, nome, numeroPedidoEcommerce, "
                "numeroPedidoCanalVenda, canalVenda."
            ),
        ),
        sa.Column(
            "ecommerce_order_number",
            sa.String(100),
            nullable=True,
            comment=(
                "Denormalised ecommerce.numeroPedidoEcommerce for fast lookup. "
                "For ML orders this is the pack_id or order_id stored by Tiny."
            ),
        ),
        sa.Column(
            "origin_id",
            sa.BigInteger(),
            nullable=True,
            comment="Tiny order ID that originated this NF (origem.id).",
        ),
        sa.Column(
            "origin_type",
            sa.String(20),
            nullable=True,
            comment="Origin document type (origem.tipo). Typically 'venda'.",
        ),
        sa.Column("synced_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("tiny_id", name="pk_invoices"),
    )

    op.create_index("ix_invoices_issue_date", "invoices", ["issue_date"])
    op.create_index("ix_invoices_status", "invoices", ["status"])
    op.create_index("ix_invoices_type", "invoices", ["type"])
    op.create_index("ix_invoices_ecommerce_order_number", "invoices", ["ecommerce_order_number"])
    op.create_index("ix_invoices_origin_id", "invoices", ["origin_id"])

    # Extend the sync_logs valid_sync_type constraint to include 'invoices'.
    op.drop_constraint("valid_sync_type", "sync_logs", type_="check")
    op.create_check_constraint(
        "valid_sync_type",
        "sync_logs",
        "sync_type IN ('products', 'orders', 'stock', 'sale_buckets', 'token_rotation', 'invoices')",
    )


def downgrade() -> None:
    op.drop_constraint("valid_sync_type", "sync_logs", type_="check")
    op.create_check_constraint(
        "valid_sync_type",
        "sync_logs",
        "sync_type IN ('products', 'orders', 'stock', 'sale_buckets', 'token_rotation')",
    )

    op.drop_index("ix_invoices_origin_id", table_name="invoices")
    op.drop_index("ix_invoices_ecommerce_order_number", table_name="invoices")
    op.drop_index("ix_invoices_type", table_name="invoices")
    op.drop_index("ix_invoices_status", table_name="invoices")
    op.drop_index("ix_invoices_issue_date", table_name="invoices")
    op.drop_table("invoices")
