"""ml_seller_reputation_daily + ml_infractions — ML account health.

DASH request (Anúncios, Saúde da conta): reputation, transactions and
infractions. Sources (read-only, verified live 2026-09-30):
``/users/{id}.seller_reputation`` (daily snapshot, BRT date) and
``/moderations/infractions/{user_id}`` (20 per page, newest first).

Revision ID: ml_account_health
Revises: ml_item_visits
Create Date: 2026-09-30
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "ml_account_health"
down_revision = "ml_item_visits"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ml_seller_reputation_daily",
        sa.Column("snapshot_date", sa.Date, primary_key=True),
        sa.Column("level_id", sa.String(30), nullable=True),
        sa.Column("power_seller_status", sa.String(30), nullable=True),
        sa.Column("transactions_total", sa.Integer, nullable=True),
        sa.Column("transactions_completed", sa.Integer, nullable=True),
        sa.Column("transactions_canceled", sa.Integer, nullable=True),
        sa.Column("rating_positive", sa.Numeric(6, 4), nullable=True),
        sa.Column("rating_neutral", sa.Numeric(6, 4), nullable=True),
        sa.Column("rating_negative", sa.Numeric(6, 4), nullable=True),
        sa.Column("metrics", JSONB, nullable=True),
        sa.Column("raw", JSONB, nullable=False),
        sa.Column(
            "fetched_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        comment=(
            "Snapshot diário (data BRT) da reputação da conta no ML, de "
            "/users/{id}.seller_reputation."
        ),
    )
    op.create_table(
        "ml_infractions",
        sa.Column("infraction_id", sa.String(40), primary_key=True),
        sa.Column("date_created", sa.DateTime(timezone=True), nullable=True),
        sa.Column("element_id", sa.Text, nullable=True),
        sa.Column("element_type", sa.String(40), nullable=True),
        sa.Column("filter_subgroup", sa.String(40), nullable=True),
        sa.Column("reason", sa.Text, nullable=True),
        sa.Column("remedy", sa.Text, nullable=True),
        sa.Column("related_item_id", sa.String(40), nullable=True),
        sa.Column("raw", JSONB, nullable=False),
        sa.Column(
            "synced_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        comment="Infrações/moderações da conta no ML, de /moderations/infractions/{user_id}.",
    )
    op.create_index("ix_ml_infractions_date_created", "ml_infractions", ["date_created"])
    op.create_index("ix_ml_infractions_related_item", "ml_infractions", ["related_item_id"])


def downgrade() -> None:
    op.drop_table("ml_infractions")
    op.drop_table("ml_seller_reputation_daily")
