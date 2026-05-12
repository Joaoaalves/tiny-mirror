"""Add stock_history, purchase_orders, and supplier_lead_times tables.

stock_history — daily deposit-level snapshots from Tiny v2
  lista.atualizacoes.estoque. Used as historical record for auditing
  stock movements and detecting A Caminho transitions.

purchase_orders — OCs from Tiny v3 /ordem-compra. Stores enough header
  data to compute planned lead times per supplier (dataPrevista - data).

supplier_lead_times — precomputed median lead time per supplier, derived
  from purchase_orders after each weekly OC sync. Read by mission-control
  reposicao route as primary lead-time source (fallback: 15 days).

Revision ID: stock_history_purchase_orders
Revises: mv_coverage_v9
Create Date: 2026-05-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "stock_history_purchase_orders"
down_revision = "mv_coverage_v9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE sync_logs DROP CONSTRAINT IF EXISTS ck_sync_logs_valid_sync_type;
        ALTER TABLE sync_logs ADD CONSTRAINT ck_sync_logs_valid_sync_type CHECK (
            sync_type = ANY (ARRAY[
                'products', 'orders', 'stock', 'sale_buckets',
                'token_rotation', 'mercadolivre_stock', 'invoices',
                'stock_history', 'purchase_orders'
            ])
        );
    """)

    op.create_table(
        "stock_history",
        sa.Column("product_tiny_id", sa.BigInteger, nullable=False),
        sa.Column("product_sku", sa.String(100), nullable=False),
        sa.Column("snapshot_date", sa.Date, nullable=False),
        sa.Column("deposit_name", sa.String(200), nullable=False),
        sa.Column("balance", sa.Integer, nullable=False, server_default="0"),
        sa.Column(
            "synced_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("product_tiny_id", "snapshot_date", "deposit_name"),
        comment=(
            "Daily deposit-level stock snapshots from Tiny v2 lista.atualizacoes.estoque. "
            "One row per (product, date, deposit). Used to track A Caminho transitions "
            "and audit stock movements over time."
        ),
    )
    op.create_index(
        "ix_stock_history_sku_date",
        "stock_history",
        ["product_sku", "snapshot_date"],
    )
    op.create_index(
        "ix_stock_history_deposit_date",
        "stock_history",
        ["deposit_name", "snapshot_date"],
    )

    op.create_table(
        "purchase_orders",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=False),
        sa.Column("numero", sa.String(50), nullable=True),
        sa.Column("data", sa.Date, nullable=True),
        sa.Column("situacao", sa.String(10), nullable=True),
        sa.Column("date_prevista", sa.Date, nullable=True),
        sa.Column("total_produtos", sa.Numeric(12, 2), nullable=True),
        sa.Column("total_pedido", sa.Numeric(12, 2), nullable=True),
        sa.Column("supplier_id", sa.BigInteger, nullable=True),
        sa.Column("supplier_name", sa.String(500), nullable=True),
        sa.Column("supplier_cnpj", sa.String(30), nullable=True),
        sa.Column("observacoes", sa.Text, nullable=True),
        sa.Column("observacoes_internas", sa.Text, nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "synced_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        comment=(
            "Purchase orders (Ordens de Compra) mirrored from Tiny v3 /ordem-compra. "
            "Synced weekly. completed_at is set the first time we observe situacao "
            "transition to a completed state (4). Used to compute supplier_lead_times."
        ),
    )
    op.create_index("ix_purchase_orders_supplier", "purchase_orders", ["supplier_name"])
    op.create_index("ix_purchase_orders_data", "purchase_orders", ["data"])
    op.create_index("ix_purchase_orders_situacao", "purchase_orders", ["situacao"])

    op.create_table(
        "supplier_lead_times",
        sa.Column("supplier_name", sa.String(500), primary_key=True),
        sa.Column("lead_time_days", sa.Numeric(5, 1), nullable=False),
        sa.Column("sample_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column(
            "last_computed",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        comment=(
            "Precomputed median lead time per supplier, derived from purchase_orders "
            "after each OC sync. Read by mission-control reposicao API as the primary "
            "lead-time source. Fallback when supplier not found: 15 days."
        ),
    )

    op.execute("GRANT SELECT ON stock_history TO tiny_readonly;")
    op.execute("GRANT SELECT ON purchase_orders TO tiny_readonly;")
    op.execute("GRANT SELECT ON supplier_lead_times TO tiny_readonly;")


def downgrade() -> None:
    op.drop_table("supplier_lead_times")
    op.drop_table("purchase_orders")
    op.drop_table("stock_history")
