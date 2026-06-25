"""ml_promo_resubscribe_jobs: strict_promo_id flag (campaign migration).

The re-subscribe poller's "already active?" check used a same-type fallback (so
a re-issued offer under a new id still counted). That's wrong for CAMPAIGN
MIGRATION: source and target are DIFFERENT SELLER_CAMPAIGNs of the same type, so
the still-``started`` SOURCE matched the fallback and the job was marked done
WITHOUT ever enrolling into the target. ``strict_promo_id`` disables the fallback
for migration jobs — only the exact target promo_id counts.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "resub_strict_promo_id"
down_revision = "ml_promotions_enrolled_at"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ml_promo_resubscribe_jobs",
        sa.Column(
            "strict_promo_id",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )


def downgrade() -> None:
    op.drop_column("ml_promo_resubscribe_jobs", "strict_promo_id")
