"""ml_promo_decisions — track ML execution status alongside operator status.

Phase 5 of the promo decisions overhaul: when the operator approves a
row AND ML_PROMO_APPLY_ENABLED=true, the service POSTs to
/seller-promotions/items/{MLB} and records the outcome here. The
operator's status column ('pending' → 'approved') stays decoupled
from the ML side so:

- The audit trail of WHO approved the decision is preserved even if
  ML never accepts it.
- A failed ML send can be retried without re-asking the operator.
- A row approved with the flag OFF carries ml_apply_status='skipped'
  forever, marking that the engine intentionally never touched ML.

Columns:
- ml_apply_status: 'pending' | 'ok' | 'failed' | 'skipped' | NULL.
  NULL = never tried (the default for rows from earlier phases).
  'pending' = approve handler kicked it off but hasn't heard back yet
  (only briefly visible — set + cleared in the same handler call).
  'skipped' = flag was OFF at approve time.
- ml_apply_status_code: HTTP code from ML on the last attempt (200,
  201, 400, 502, etc.). Nullable so 'skipped' rows don't lie about
  what code came back (none did).
- ml_apply_response: trimmed body from ML — useful for debugging the
  shape of 4xx rejections. Bounded length so a chatty ML error can't
  blow up the row.
- ml_applied_at: when the last attempt was made.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision = "promo_decisions_ml_apply"
down_revision: str | Sequence[str] | None = "promo_decisions_expired"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "ml_promo_decisions",
        sa.Column(
            "ml_apply_status",
            sa.String(20),
            nullable=True,
            comment=(
                "Outcome of the last attempt to push this row to ML: "
                "pending | ok | failed | skipped. NULL = never tried."
            ),
        ),
    )
    op.add_column(
        "ml_promo_decisions",
        sa.Column(
            "ml_apply_status_code",
            sa.Integer,
            nullable=True,
            comment="HTTP code from ML on the last attempt.",
        ),
    )
    op.add_column(
        "ml_promo_decisions",
        sa.Column(
            "ml_apply_response",
            sa.Text,
            nullable=True,
            comment="Trimmed ML response body — first 2KB for debugging.",
        ),
    )
    op.add_column(
        "ml_promo_decisions",
        sa.Column(
            "ml_applied_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="Timestamp of the last attempt to push this row to ML.",
        ),
    )
    op.create_check_constraint(
        "ck_ml_promo_decisions_ml_apply_status",
        "ml_promo_decisions",
        "ml_apply_status IS NULL OR ml_apply_status IN ('pending','ok','failed','skipped')",
    )
    op.create_index(
        "ix_ml_promo_decisions_ml_apply_status",
        "ml_promo_decisions",
        ["ml_apply_status"],
    )


def downgrade() -> None:
    op.drop_index("ix_ml_promo_decisions_ml_apply_status", table_name="ml_promo_decisions")
    op.drop_constraint(
        "ck_ml_promo_decisions_ml_apply_status",
        "ml_promo_decisions",
        type_="check",
    )
    op.drop_column("ml_promo_decisions", "ml_applied_at")
    op.drop_column("ml_promo_decisions", "ml_apply_response")
    op.drop_column("ml_promo_decisions", "ml_apply_status_code")
    op.drop_column("ml_promo_decisions", "ml_apply_status")
