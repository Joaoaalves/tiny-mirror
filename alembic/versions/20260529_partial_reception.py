"""Add partial-reception tracking to fulfillment_transfers.

When Tiny lance a transfer of N units (Galpão → FL), ML may confirm
those units in multiple events (TRANSFER_DELIVERY/INBOUND_RECEPTION)
spread across hours/days. The old FIFO matcher only marked a transfer
'received' once the full quantity was covered — meanwhile, partial
arrivals were invisible to the operator and to coverage math.

This migration adds:
- ``quantity_received`` (default 0): cumulative units confirmed by ML so far.
- ``last_event_at``: timestamp of the most recent ML event that touched
  this transfer. Lets us reason about staleness without re-querying ML.

Status semantics evolve:
- pending: quantity_received < quantity (still expecting arrivals)
- received: quantity_received == quantity (fully delivered)
- cancelled: row should not have existed (SKU not fulfillment in ML; webhook
  misfire). quantity_received untouched.

Backfill: every existing 'received' row gets ``quantity_received = quantity``
so coverage math stays consistent post-deploy.

Revision ID: partial_reception
Revises: invoice_items
Create Date: 2026-05-29
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "partial_reception"
down_revision = "invoice_items"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "fulfillment_transfers",
        sa.Column(
            "quantity_received",
            sa.Integer(),
            nullable=False,
            server_default="0",
            comment=(
                "Cumulative units confirmed by ML across one or more "
                "TRANSFER_DELIVERY/INBOUND_RECEPTION events. When equal to "
                "quantity, the row transitions to status='received'."
            ),
        ),
    )
    op.add_column(
        "fulfillment_transfers",
        sa.Column(
            "last_event_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment=(
                "Timestamp of the most recent ML inbound event that "
                "incremented quantity_received on this transfer."
            ),
        ),
    )
    # Backfill: already-received rows get quantity_received = quantity so
    # coverage math (effective FL = saldo - sum(pending.qty - pending.qty_received))
    # stays consistent after the migration.
    op.execute(
        "UPDATE fulfillment_transfers "
        "SET quantity_received = quantity, last_event_at = received_at "
        "WHERE status = 'received'"
    )


def downgrade() -> None:
    op.drop_column("fulfillment_transfers", "last_event_at")
    op.drop_column("fulfillment_transfers", "quantity_received")
