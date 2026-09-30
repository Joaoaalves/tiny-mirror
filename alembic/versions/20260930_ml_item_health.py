"""ml_item_health — current health of each ML listing.

DASH request (Anúncios: Visão geral, Raio-X): quality/health scores, tags,
moderation state and purchase experience per listing. Three independent
parts, each with its own fetch timestamp so a failed call (e.g. the 403
PolicyAgent some items return for purchase experience) never blanks the
others. Sources, read-only, verified live 2026-09-30:

- ``/items?ids=...&attributes=id,status,sub_status,tags`` (multiget, 20/call)
- ``/item/{id}/performance`` (score, level, buckets/variables)
- ``/reputation/items/{id}/purchase_experience/integrators`` (302 → user product)

Revision ID: ml_item_health
Revises: ml_account_health
Create Date: 2026-09-30
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "ml_item_health"
down_revision = "ml_account_health"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ml_item_health",
        sa.Column("mlb_id", sa.String(20), primary_key=True),
        sa.Column("item_status", sa.String(30), nullable=True),
        sa.Column("item_sub_status", JSONB, nullable=True),
        sa.Column("item_tags", JSONB, nullable=True),
        sa.Column("item_fetched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("quality_score", sa.Numeric(6, 2), nullable=True),
        sa.Column("quality_level", sa.String(30), nullable=True),
        sa.Column("quality_level_wording", sa.String(80), nullable=True),
        sa.Column("quality_calculated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("quality_pending", JSONB, nullable=True),
        sa.Column("quality_buckets", JSONB, nullable=True),
        sa.Column("quality_fetched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("experience_color", sa.String(20), nullable=True),
        sa.Column("experience_text", sa.String(80), nullable=True),
        sa.Column("experience_value", sa.Numeric(6, 2), nullable=True),
        sa.Column("experience_status", sa.String(30), nullable=True),
        sa.Column("experience_raw", JSONB, nullable=True),
        sa.Column("experience_fetched_at", sa.DateTime(timezone=True), nullable=True),
        comment=(
            "Saúde atual de cada anúncio ML: moderação/tags (item_*), qualidade "
            "(quality_*) e experiência de compra (experience_*), cada parte com "
            "coleta própria."
        ),
    )


def downgrade() -> None:
    op.drop_table("ml_item_health")
