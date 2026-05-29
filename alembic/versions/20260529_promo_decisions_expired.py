"""ml_promo_decisions — add 'expired' status + expired_at + expired_reason.

Phase 3 of the promo decisions overhaul: a daily job now sweeps the
pending queue and flips rows whose underlying inputs moved out from
under them (list_price drift, cap change, floor change, or just plain
age) to status='expired'. The operator can still see expired rows in
the dashboard with the reason, so nothing is silently dropped — they're
just out of the active approval queue.

Schema changes:
- relax the status CHECK to allow the new value;
- expired_at: when the auto-expire job marked it (nullable; only set
  when status='expired');
- expired_reason: short token describing why (list_price_drift,
  cap_changed, floor_changed, stale_age).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision = "promo_decisions_expired"
down_revision: str | Sequence[str] | None = "fl_in_transfer"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint(
        "ck_ml_promo_decisions_status",
        "ml_promo_decisions",
        type_="check",
    )
    op.create_check_constraint(
        "ck_ml_promo_decisions_status",
        "ml_promo_decisions",
        "status IN ('pending', 'approved', 'rejected', 'ignored', 'expired')",
    )
    op.add_column(
        "ml_promo_decisions",
        sa.Column(
            "expired_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="When the auto-expire job flipped this row to 'expired'.",
        ),
    )
    op.add_column(
        "ml_promo_decisions",
        sa.Column(
            "expired_reason",
            sa.String(40),
            nullable=True,
            comment=(
                "Why the row was expired: list_price_drift | cap_changed "
                "| floor_changed | stale_age."
            ),
        ),
    )


def downgrade() -> None:
    op.drop_column("ml_promo_decisions", "expired_reason")
    op.drop_column("ml_promo_decisions", "expired_at")
    op.drop_constraint(
        "ck_ml_promo_decisions_status",
        "ml_promo_decisions",
        type_="check",
    )
    op.create_check_constraint(
        "ck_ml_promo_decisions_status",
        "ml_promo_decisions",
        "status IN ('pending', 'approved', 'rejected', 'ignored')",
    )
