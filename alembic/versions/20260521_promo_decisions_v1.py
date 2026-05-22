"""ml_promo_decisions — operator approval queue for engine decisions.

Each row represents one candidate promo the engine wants to act on for
a given MLB. Status starts as ``pending`` and the operator flips it to
``approved`` or ``rejected`` from the dashboard. The unique constraint on
``(mlb_id, promo_key)`` makes the cron re-run idempotent: once a decision
exists in any status, the next enumeration skips it instead of creating
a duplicate. ``promo_key`` is the candidate promo's ML id when present,
otherwise a synthetic ``"CREATE-<kind>"`` token so create_price_discount
rows also get deduped per MLB.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision = "promo_decisions_v1"
down_revision: str | Sequence[str] | None = "caps_per_mlb_v1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "ml_promo_decisions",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("mlb_id", sa.String(20), nullable=False),
        sa.Column("sku", sa.String(100), nullable=False),
        sa.Column(
            "promo_key",
            sa.String(80),
            nullable=False,
            comment="ML promo id when known; synthetic CREATE-<kind> token otherwise",
        ),
        sa.Column("promo_id", sa.String(80), nullable=True),
        sa.Column("promo_type", sa.String(40), nullable=False),
        sa.Column("promo_name", sa.String(200), nullable=True),
        sa.Column(
            "decision_kind",
            sa.String(40),
            nullable=False,
            comment="would_activate | create_price_discount | activate_candidate",
        ),
        sa.Column("target_price", sa.Numeric(12, 2), nullable=True),
        sa.Column("target_total_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("target_seller_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("meli_percentage", sa.Numeric(6, 2), nullable=True),
        sa.Column("constraint_used", sa.String(40), nullable=True),
        sa.Column("list_price", sa.Numeric(12, 2), nullable=True),
        sa.Column("cap_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("floor_price", sa.Numeric(12, 2), nullable=True),
        sa.Column("reason", sa.Text, nullable=False),
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("decided_by", sa.String(100), nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'approved', 'rejected', 'ignored')",
            name="ck_ml_promo_decisions_status",
        ),
    )
    op.create_index(
        "ix_ml_promo_decisions_status",
        "ml_promo_decisions",
        ["status"],
    )
    op.create_index(
        "ix_ml_promo_decisions_sku",
        "ml_promo_decisions",
        ["sku"],
    )
    op.create_unique_constraint(
        "uq_ml_promo_decisions_mlb_promo",
        "ml_promo_decisions",
        ["mlb_id", "promo_key"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_ml_promo_decisions_mlb_promo", "ml_promo_decisions", type_="unique")
    op.drop_index("ix_ml_promo_decisions_sku", table_name="ml_promo_decisions")
    op.drop_index("ix_ml_promo_decisions_status", table_name="ml_promo_decisions")
    op.drop_table("ml_promo_decisions")
