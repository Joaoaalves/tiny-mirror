"""ml_promo_resubscribe_jobs — fila de re-inscrição (raise = sair + reentrar).

Subir o preço de uma promoção no ML não tem edição in-place: o app SAI
(DELETE) e REENTRA (POST). Mas o ML pode demorar a re-sugerir a campanha como
candidata, então a reentrada imediata às vezes falha ("oferta não encontrada")
e o anúncio fica a preço CHEIO, sem promoção. Esta tabela é a fila que o poller
varre para reentrar assim que a oferta reaparece como candidata.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision = "ml_promo_resubscribe_jobs"
down_revision: str | Sequence[str] | None = "decision_start_date"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ml_promo_resubscribe_jobs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("mlb_id", sa.String(length=20), nullable=False),
        sa.Column("sku", sa.String(length=100), nullable=False),
        sa.Column("promo_type", sa.String(length=40), nullable=False),
        sa.Column(
            "promo_id",
            sa.String(length=80),
            nullable=True,
            comment="promotion_id da campanha a reentrar (resolvido ao vivo se mudar).",
        ),
        sa.Column(
            "target_price",
            sa.Numeric(precision=12, scale=2),
            nullable=False,
            comment="Preço (deal_price) para reentrar quando a oferta reaparecer.",
        ),
        sa.Column(
            "status",
            sa.String(length=20),
            server_default=sa.text("'pending'"),
            nullable=False,
            comment="pending | done | failed | cancelled",
        ),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("max_attempts", sa.Integer(), server_default=sa.text("288"), nullable=False),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "deadline",
            sa.DateTime(timezone=True),
            nullable=False,
            comment="Após esse instante, desiste e marca failed (alerta o operador).",
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_status_code", sa.Integer(), nullable=True),
        sa.Column(
            "op_id",
            sa.String(length=40),
            nullable=True,
            comment="op_id da operação modify que originou o job — correlaciona logs no Seq.",
        ),
        sa.Column("decided_by", sa.String(length=200), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'done', 'failed', 'cancelled')",
            name="ck_ml_promo_resub_status",
        ),
        sa.PrimaryKeyConstraint("id"),
        comment=(
            "Fila de re-inscrição automática. Subir o preço de uma promoção no ML "
            "exige SAIR (DELETE) e REENTRAR (POST), mas o ML pode levar um tempo até "
            "re-sugerir a campanha como candidata. Quando a reentrada imediata falha, "
            "enfileiramos aqui; o poller checa a elegibilidade do anúncio e reentra "
            "assim que a oferta reaparece."
        ),
    )
    op.create_index(
        "ix_ml_promo_resub_due",
        "ml_promo_resubscribe_jobs",
        ["status", "next_attempt_at"],
    )
    op.create_index(
        "ix_ml_promo_resub_mlb",
        "ml_promo_resubscribe_jobs",
        ["mlb_id"],
    )
    # No máximo UM job pendente por (mlb_id, promo_type).
    op.create_index(
        "uq_ml_promo_resub_pending",
        "ml_promo_resubscribe_jobs",
        ["mlb_id", "promo_type"],
        unique=True,
        postgresql_where=sa.text("status = 'pending'"),
    )


def downgrade() -> None:
    op.drop_index("uq_ml_promo_resub_pending", table_name="ml_promo_resubscribe_jobs")
    op.drop_index("ix_ml_promo_resub_mlb", table_name="ml_promo_resubscribe_jobs")
    op.drop_index("ix_ml_promo_resub_due", table_name="ml_promo_resubscribe_jobs")
    op.drop_table("ml_promo_resubscribe_jobs")
