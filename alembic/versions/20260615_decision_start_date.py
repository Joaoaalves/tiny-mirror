"""ml_promo_decisions — adiciona promo_start_date para a vigência da promoção.

Já temos promo_finish_date (término). Pra mostrar a vigência completa na UI
("de quando até quando") precisamos também do início. Populado no
generate_pending_decisions a partir do start_date do objeto Promo do ML.

Decisões existentes ficam com NULL — a UI mostra só o término quando faltar
o início.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision = "decision_start_date"
down_revision: str | Sequence[str] | None = "flex_calibration_payback"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ml_promo_decisions",
        sa.Column(
            "promo_start_date",
            sa.DateTime(timezone=True),
            nullable=True,
            comment=(
                "Data de início da campanha ML (start_date do objeto Promo). "
                "NULL quando o ML não informa. Usado junto com promo_finish_date "
                "para exibir a vigência da promoção na UI."
            ),
        ),
    )


def downgrade() -> None:
    op.drop_column("ml_promo_decisions", "promo_start_date")
