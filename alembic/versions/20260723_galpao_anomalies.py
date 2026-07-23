"""galpao_anomalies: registro de mudanças suspeitas no depósito Galpão.

Material de estudo para os balanços de origem desconhecida (2026-07):
cada webhook de estoque cujo delta de Galpão não se explica por venda,
remessa ao Full ou fluxo nosso vira uma linha aqui; um dispatcher na VPS
notifica (Telegram/WhatsApp) as não-notificadas.

Revision ID: galpao_anomalies
Revises: panel_promo_id
Create Date: 2026-07-23
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "galpao_anomalies"
down_revision = "panel_promo_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "galpao_anomalies",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("product_tiny_id", sa.BigInteger(), nullable=False),
        sa.Column("sku", sa.String(length=120), nullable=False),
        sa.Column("prev_galpao", sa.Integer(), nullable=False),
        sa.Column("new_galpao", sa.Integer(), nullable=False),
        sa.Column("delta", sa.Integer(), nullable=False),
        sa.Column("reason", sa.String(length=200), nullable=False),
        sa.Column(
            "detected_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("notified_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_galpao_anomalies_detected_at", "galpao_anomalies", ["detected_at"])
    op.create_index(
        "ix_galpao_anomalies_unnotified",
        "galpao_anomalies",
        ["notified_at"],
        postgresql_where=sa.text("notified_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_galpao_anomalies_unnotified", table_name="galpao_anomalies")
    op.drop_index("ix_galpao_anomalies_detected_at", table_name="galpao_anomalies")
    op.drop_table("galpao_anomalies")
