"""ml_promo_decisions — adiciona promo_finish_date para detectar campanhas expiradas.

Campanhas DEAL / LIGHTNING / DOD / SELLER_CAMPAIGN / SELLER_COUPON_CAMPAIGN têm
data de término. Sem isso o expire-stale não consegue descartar decisões de
campanhas que já encerraram. A coluna é populada no generate_pending_decisions
a partir do finish_date do objeto Promo retornado pela API do ML.

Decisões existentes ficam com NULL — o expire-stale já cobre via stale_age.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision = "decision_finish_date"
down_revision: str | Sequence[str] | None = "decision_context"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ml_promo_decisions",
        sa.Column(
            "promo_finish_date",
            sa.DateTime(timezone=True),
            nullable=True,
            comment=(
                "Data de término da campanha ML (finish_date do objeto Promo). "
                "NULL para PRICE_DISCOUNT (sem campanha). "
                "Usado pelo expire-stale para descartar decisões de campanhas encerradas."
            ),
        ),
    )


def downgrade() -> None:
    op.drop_column("ml_promo_decisions", "promo_finish_date")
