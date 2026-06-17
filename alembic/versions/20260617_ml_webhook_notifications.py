"""ml_webhook_notifications — fila de notificações push do Mercado Livre.

O ML manda notificações (public_offers, public_candidates,
catalog_item_competition_status, ...) numa frequência alta. O endpoint só grava
aqui (ack rápido, 200) e um job processa depois, re-sincronizando o anúncio
afetado — assim a promoção aparece quase em tempo real sem depender do cron
diário.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "ml_webhook_notifications"
down_revision: str | Sequence[str] | None = "ml_promo_resubscribe_jobs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ml_webhook_notifications",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("topic", sa.String(length=60), nullable=False),
        sa.Column("resource", sa.Text(), nullable=False),
        sa.Column(
            "mlb_id",
            sa.String(length=20),
            nullable=True,
            comment="MLB extraído do resource (quando dá).",
        ),
        sa.Column("ml_user_id", sa.String(length=40), nullable=True),
        sa.Column("application_id", sa.String(length=40), nullable=True),
        sa.Column(
            "attempts",
            sa.Integer(),
            nullable=True,
            comment="Contador de tentativas do PRÓPRIO ML.",
        ),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("raw", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "status",
            sa.String(length=20),
            server_default=sa.text("'pending'"),
            nullable=False,
            comment="pending | processed | ignored | error",
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        comment=(
            "Fila de notificações push do ML. O endpoint só grava (ack rápido) e "
            "responde 200; um job processa depois — re-sincroniza o anúncio "
            "afetado. Idempotente por design."
        ),
    )
    op.create_index(
        "ix_ml_webhook_notif_pending",
        "ml_webhook_notifications",
        ["status", "received_at"],
    )
    op.create_index(
        "ix_ml_webhook_notif_mlb",
        "ml_webhook_notifications",
        ["mlb_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_ml_webhook_notif_mlb", table_name="ml_webhook_notifications")
    op.drop_index("ix_ml_webhook_notif_pending", table_name="ml_webhook_notifications")
    op.drop_table("ml_webhook_notifications")
