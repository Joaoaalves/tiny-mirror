"""Enriquece log de promoções com contexto para futura automação.

Adiciona:
  ml_promo_decisions.decision_context JSONB — snapshot do contexto no
    momento da decisão do operador: catalog_status, current_price,
    price_to_win, momentum, margin_pct, discount_pct, list_price.

  ml_promo_actions.decided_by VARCHAR — quem disparou a ação (operador
    ou 'engine' para ações automáticas).

  ml_promo_actions.context JSONB — dados adicionais por tipo de ação
    (catalog_status, margin_pct, momentum, etc.), estruturado para
    treinar regras de automação futura.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "decision_context"
down_revision: str | Sequence[str] | None = "promo_decisions_ml_apply"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ml_promo_decisions",
        sa.Column(
            "decision_context",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            comment=(
                "Snapshot do contexto no momento da decisão: "
                "{catalog_status, current_price, price_to_win, "
                "momentum, margin_pct, discount_pct, list_price}"
            ),
        ),
    )
    op.add_column(
        "ml_promo_actions",
        sa.Column(
            "decided_by",
            sa.String(),
            nullable=True,
            comment="Email do operador ou 'engine' para ações automáticas",
        ),
    )
    op.add_column(
        "ml_promo_actions",
        sa.Column(
            "context",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            comment=(
                "Dados de contexto para automação: "
                "{catalog_status, current_price, price_to_win, "
                "momentum, margin_pct, floor_price, list_price}"
            ),
        ),
    )


def downgrade() -> None:
    op.drop_column("ml_promo_actions", "context")
    op.drop_column("ml_promo_actions", "decided_by")
    op.drop_column("ml_promo_decisions", "decision_context")
