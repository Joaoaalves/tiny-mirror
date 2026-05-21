"""Add ml_promo_caps.skip_when_winning (opt-in per SKU).

Operator policy change (2026-05-21): the keep_winning short-circuit is
no longer a default — ML's "winning" status is not a reliable signal of
real catalog dominance (other factors weigh in). To preserve the old
behaviour when the operator genuinely doesn't want to push more
discount on top of a winner, we expose an opt-in flag per cap.

Revises: ml_catalog_status_v1
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "cap_skip_winning_v1"
down_revision = "ml_catalog_status_v1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ml_promo_caps",
        sa.Column(
            "skip_when_winning",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
            comment=(
                "When true and catalog_status='winning' + visit_share='maximum', "
                "the engine returns keep_winning instead of activating new promos."
            ),
        ),
    )


def downgrade() -> None:
    op.drop_column("ml_promo_caps", "skip_when_winning")
