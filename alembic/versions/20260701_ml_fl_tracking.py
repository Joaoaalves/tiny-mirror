"""Estoque Full — tabelas de acompanhamento de anúncios FULFILLMENT.

Três tabelas de estado do workflow (nenhum write no Tiny/ML, só nosso estado):

- ``ml_fl_tracking``     — anúncios em acompanhamento/finalizados + snapshots.
- ``ml_fl_tracking_events`` — timeline "última alteração" + anotações.
- ``ml_fl_dismissals``   — ignore temporário / remove permanente na aba Novos.

Revision ID: ml_fl_tracking
Revises: ml_sales_revenue
Create Date: 2026-07-01
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "ml_fl_tracking"
down_revision = "ml_sales_revenue"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ml_fl_tracking",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("mlb_id", sa.String(20), nullable=False),
        sa.Column("sku", sa.String(100), nullable=True),
        sa.Column("status", sa.String(20), nullable=False, server_default=sa.text("'tracking'")),
        sa.Column(
            "moved_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.Column("moved_by", sa.String(100), nullable=True),
        sa.Column("initial_stock_full", sa.Integer, nullable=True),
        sa.Column("initial_stock_galpao", sa.Integer, nullable=True),
        sa.Column("initial_daily_rate_30d", sa.Numeric(10, 2), nullable=True),
        sa.Column("initial_promo_pct", sa.Numeric(7, 2), nullable=True),
        sa.Column("initial_snapshot", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("finalized_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finalized_by", sa.String(100), nullable=True),
        sa.Column("final_stock_full", sa.Integer, nullable=True),
        sa.Column("final_daily_rate_30d", sa.Numeric(10, 2), nullable=True),
        sa.Column("final_promo_pct", sa.Numeric(7, 2), nullable=True),
        sa.Column("final_snapshot", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("result_summary", sa.Text, nullable=True),
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
        sa.CheckConstraint("status IN ('tracking', 'finalized')", name="ck_ml_fl_tracking_status"),
        comment=(
            "Estoque Full: anúncios fulfillment em acompanhamento/finalizados, "
            "com snapshot inicial e final do estado do anúncio."
        ),
    )
    op.create_index("ix_ml_fl_tracking_status", "ml_fl_tracking", ["status"])
    # No máximo um acompanhamento ATIVO por MLB; finalizados acumulam histórico.
    op.create_index(
        "uq_ml_fl_tracking_active_mlb",
        "ml_fl_tracking",
        ["mlb_id"],
        unique=True,
        postgresql_where=sa.text("status = 'tracking'"),
    )

    op.create_table(
        "ml_fl_tracking_events",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "tracking_id",
            sa.BigInteger,
            sa.ForeignKey("ml_fl_tracking.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("event_type", sa.String(20), nullable=False),
        sa.Column("author", sa.String(100), nullable=True),
        sa.Column("note", sa.Text, nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "event_type IN ('annotation', 'promo_change', 'status_change')",
            name="ck_ml_fl_tracking_events_event_type",
        ),
        comment="Estoque Full: linha do tempo de eventos/anotações por acompanhamento.",
    )
    op.create_index("ix_ml_fl_tracking_events_tracking", "ml_fl_tracking_events", ["tracking_id"])

    op.create_table(
        "ml_fl_dismissals",
        sa.Column("mlb_id", sa.String(20), primary_key=True),
        sa.Column("sku", sa.String(100), nullable=True),
        sa.Column("kind", sa.String(20), nullable=False),
        sa.Column("until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by", sa.String(100), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("kind IN ('ignore', 'remove')", name="ck_ml_fl_dismissals_kind"),
        comment="Estoque Full: dismissals da aba Novos (ignore temporário / remove permanente).",
    )
    op.create_index("ix_ml_fl_dismissals_kind", "ml_fl_dismissals", ["kind"])


def downgrade() -> None:
    op.drop_table("ml_fl_dismissals")
    op.drop_table("ml_fl_tracking_events")
    op.drop_table("ml_fl_tracking")
