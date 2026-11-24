"""Drop the Estoque Full feature: its workflow tables and the Full-only sales split.

The Estoque Full page was removed from the dashboard (24/11/2026). Its three
workflow tables go away, and so do ``ml_sales_daily.full_qty``/``full_revenue``:
they existed only for that page and cost one ML ``GET /shipments`` per order on
every sales sync.

Revision ID: drop_estoque_full
Revises: mp_marketplaces
Create Date: 2026-11-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "drop_estoque_full"
down_revision = "mp_marketplaces"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("ml_fl_tracking_events")
    op.drop_table("ml_fl_tracking")
    op.drop_table("ml_fl_dismissals")
    op.drop_column("ml_sales_daily", "full_revenue")
    op.drop_column("ml_sales_daily", "full_qty")


def downgrade() -> None:
    op.add_column(
        "ml_sales_daily",
        sa.Column("full_qty", sa.Integer, nullable=False, server_default=sa.text("0")),
    )
    op.add_column(
        "ml_sales_daily",
        sa.Column("full_revenue", sa.Numeric(12, 2), nullable=False, server_default=sa.text("0")),
    )
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
    )
    op.create_index("ix_ml_fl_dismissals_kind", "ml_fl_dismissals", ["kind"])
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
    )
    op.create_index("ix_ml_fl_tracking_status", "ml_fl_tracking", ["status"])
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
    )
    op.create_index("ix_ml_fl_tracking_events_tracking", "ml_fl_tracking_events", ["tracking_id"])
