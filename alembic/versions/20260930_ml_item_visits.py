"""ml_item_visits_daily — daily visits per ML listing.

DASH request (Anúncios Raio-X/Financeiro): visits and conversion per
listing. Source: ML ``/items/{id}/visits/time_window`` (read-only; max 150
days back, so the initial history is 150 days). Conversion comes from
joining ``ml_sales_daily`` on (mlb_id, day).

Revision ID: ml_item_visits
Revises: ml_orders
Create Date: 2026-09-30
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "ml_item_visits"
down_revision = "ml_orders"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ml_item_visits_daily",
        sa.Column("mlb_id", sa.String(20), primary_key=True),
        sa.Column("visit_date", sa.Date, primary_key=True),
        sa.Column("visits", sa.Integer, nullable=False),
        sa.Column(
            "fetched_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        comment=(
            "Visitas diárias por anúncio (MLB), de /items/{id}/visits/time_window do "
            "ML. O dia corrente é parcial e é regravado na rodada seguinte."
        ),
    )
    op.create_index("ix_ml_item_visits_daily_date", "ml_item_visits_daily", ["visit_date"])


def downgrade() -> None:
    op.drop_table("ml_item_visits_daily")
