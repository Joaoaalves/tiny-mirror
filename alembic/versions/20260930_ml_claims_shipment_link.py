"""v_ml_claims: also link claims whose resource is a shipment.

41% of the last 90 days' claims (361/876) come with resource='shipment'
and a shipment id in resource_id, so the order-only join left them without
MLB/SKU. They now join ml_orders through shipping_id (a pack's shipment
maps to every order of the pack, one row per order item).

Revision ID: ml_claims_shipment_link
Revises: ml_claims
Create Date: 2026-09-30
"""

from __future__ import annotations

from alembic import op

revision = "ml_claims_shipment_link"
down_revision = "ml_claims"
branch_labels = None
depends_on = None

_SELECT = """
        SELECT c.claim_id, c.type, c.stage, c.status, c.date_created, c.last_updated,
               c.reason_id, r.name AS reason_name, r.detail AS reason_detail,
               c.resolution ->> 'reason' AS resolution_reason,
               c.resolution ->> 'closed_by' AS resolution_closed_by,
               o.order_id AS ml_order_id, o.tiny_ref,
               i.mlb_id, i.seller_sku, i.quantity
        FROM ml_claims c
        LEFT JOIN ml_claim_reasons r ON r.reason_id = c.reason_id
        LEFT JOIN ml_orders o ON {join}
        LEFT JOIN ml_order_items i ON i.order_id = o.order_id
"""


def upgrade() -> None:
    op.execute(
        "CREATE OR REPLACE VIEW v_ml_claims AS"
        + _SELECT.format(
            join=(
                "(c.resource = 'order' AND o.order_id = c.resource_id) "
                "OR (c.resource = 'shipment' AND o.shipping_id = c.resource_id)"
            )
        )
    )


def downgrade() -> None:
    op.execute(
        "CREATE OR REPLACE VIEW v_ml_claims AS"
        + _SELECT.format(join="c.resource = 'order' AND o.order_id = c.resource_id")
    )
