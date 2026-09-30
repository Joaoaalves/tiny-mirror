"""ml_claims + ml_claim_reasons + v_ml_claims — ML claims with reason and SKU.

DASH requests: "reclamações" per listing (Anúncios) and the REASON + SKU of
returns/cancellations (SKUs: Devoluções & cancelamentos). The Tiny return NF
carries no reason; ML claims do (reason_id + resolution), and point at the ML
order, which ml_order_items maps to MLB and SKU. Read-only sources, verified
live 2026-09-30: /post-purchase/v1/claims/search (status filter required,
100/page, sort=last_updated:desc) and /post-purchase/v1/claims/reasons/{id}.

v_ml_claims: one row per claim x order item (MLB, seller_sku) with the
reason name/detail. Claims older than ml_orders' window have no item row.

Revision ID: ml_claims
Revises: ml_item_health
Create Date: 2026-09-30
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "ml_claims"
down_revision = "ml_item_health"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ml_claims",
        sa.Column("claim_id", sa.BigInteger, primary_key=True, autoincrement=False),
        sa.Column("type", sa.String(30), nullable=True),
        sa.Column("stage", sa.String(30), nullable=True),
        sa.Column("status", sa.String(30), nullable=True),
        sa.Column("reason_id", sa.String(30), nullable=True),
        sa.Column("resource", sa.String(30), nullable=True),
        sa.Column("resource_id", sa.BigInteger, nullable=True),
        sa.Column("parent_id", sa.BigInteger, nullable=True),
        sa.Column("fulfilled", sa.Boolean, nullable=True),
        sa.Column("quantity_type", sa.String(20), nullable=True),
        sa.Column("date_created", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_updated", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolution", JSONB, nullable=True),
        sa.Column("raw", JSONB, nullable=False),
        sa.Column(
            "synced_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        comment=(
            "Reclamações/mediações/devoluções do ML. resource_id = order_id do ML "
            "quando resource='order'. Motivo em ml_claim_reasons."
        ),
    )
    op.create_index("ix_ml_claims_resource_id", "ml_claims", ["resource_id"])
    op.create_index("ix_ml_claims_date_created", "ml_claims", ["date_created"])
    op.create_index("ix_ml_claims_reason_id", "ml_claims", ["reason_id"])
    op.create_table(
        "ml_claim_reasons",
        sa.Column("reason_id", sa.String(30), primary_key=True),
        sa.Column("name", sa.String(120), nullable=True),
        sa.Column("detail", sa.Text, nullable=True),
        sa.Column("flow", sa.String(60), nullable=True),
        sa.Column(
            "fetched_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        comment="Catálogo dos motivos de reclamação do ML.",
    )
    op.execute(
        """
        CREATE VIEW v_ml_claims AS
        SELECT c.claim_id, c.type, c.stage, c.status, c.date_created, c.last_updated,
               c.reason_id, r.name AS reason_name, r.detail AS reason_detail,
               c.resolution ->> 'reason' AS resolution_reason,
               c.resolution ->> 'closed_by' AS resolution_closed_by,
               c.resource_id AS ml_order_id, o.tiny_ref,
               i.mlb_id, i.seller_sku, i.quantity
        FROM ml_claims c
        LEFT JOIN ml_claim_reasons r ON r.reason_id = c.reason_id
        LEFT JOIN ml_orders o ON c.resource = 'order' AND o.order_id = c.resource_id
        LEFT JOIN ml_order_items i ON i.order_id = o.order_id
        """
    )
    op.execute(
        "COMMENT ON VIEW v_ml_claims IS 'Reclamação do ML x item do pedido (MLB, SKU) com o "
        "motivo legível. tiny_ref liga em orders.ecommerce_order_number.'"
    )
    op.execute("GRANT SELECT ON v_ml_claims TO tiny_readonly")


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS v_ml_claims")
    op.drop_table("ml_claim_reasons")
    op.drop_table("ml_claims")
