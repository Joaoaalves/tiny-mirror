"""ml_promotions — espelho AS-IS das promoções do Mercado Livre.

Separa FATO (quais promoções existem/estão ativas no ML, exatamente como o ML
reporta) de OPINIÃO (o motor de decisão cap/piso/margem, que continua em
``ml_promo_decisions``). Esta tabela é alimentada pelo webhook do ML (tempo
real, por MLB) + um reconcile diário + as nossas próprias ações de escrita. Uma
linha por (anuncio, promocao), com os campos crus do ``GET /seller-promotions/
items/{MLB}`` mais o ``raw`` JSONB completo pra nunca perder dado.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "ml_promotions_mirror"
down_revision: str | Sequence[str] | None = "ml_webhook_notifications"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ml_promotions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("mlb_id", sa.String(length=20), nullable=False),
        sa.Column("sku", sa.String(length=100), nullable=True),
        sa.Column(
            "promo_key",
            sa.String(length=80),
            nullable=False,
            comment="promotion_id quando existe; senão o type (seller PRICE_DISCOUNT não tem id).",
        ),
        sa.Column("promotion_id", sa.String(length=40), nullable=True),
        sa.Column("promotion_type", sa.String(length=40), nullable=False),
        sa.Column("sub_type", sa.String(length=40), nullable=True),
        sa.Column(
            "status",
            sa.String(length=20),
            nullable=False,
            comment="started | candidate | pending — exatamente como o ML retorna.",
        ),
        sa.Column("price", sa.Numeric(12, 2), nullable=True),
        sa.Column("original_price", sa.Numeric(12, 2), nullable=True),
        sa.Column("suggested_price", sa.Numeric(12, 2), nullable=True),
        sa.Column("min_price", sa.Numeric(12, 2), nullable=True),
        sa.Column("max_price", sa.Numeric(12, 2), nullable=True),
        sa.Column("seller_percentage", sa.Numeric(7, 2), nullable=True),
        sa.Column("meli_percentage", sa.Numeric(7, 2), nullable=True),
        sa.Column("offer_id", sa.String(length=80), nullable=True),
        sa.Column("name", sa.Text(), nullable=True),
        sa.Column("start_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finish_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("stock", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "raw",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            comment="Dict cru da promo, como veio do ML — fonte da verdade.",
        ),
        sa.Column(
            "first_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
            comment="Última vez que o ML retornou esta promo (mirror = estado atual do ML).",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("mlb_id", "promo_key", name="uq_ml_promotions_mlb_promo_key"),
        comment=(
            "Espelho AS-IS das promoções do ML por anúncio. FATO, não decisão — o "
            "motor cap/piso fica em ml_promo_decisions. Alimentado por webhook + "
            "reconcile diário + ações de escrita."
        ),
    )
    op.create_index("ix_ml_promotions_mlb", "ml_promotions", ["mlb_id"])
    op.create_index("ix_ml_promotions_sku", "ml_promotions", ["sku"])
    op.create_index("ix_ml_promotions_type_status", "ml_promotions", ["promotion_type", "status"])


def downgrade() -> None:
    op.drop_index("ix_ml_promotions_type_status", table_name="ml_promotions")
    op.drop_index("ix_ml_promotions_sku", table_name="ml_promotions")
    op.drop_index("ix_ml_promotions_mlb", table_name="ml_promotions")
    op.drop_table("ml_promotions")
