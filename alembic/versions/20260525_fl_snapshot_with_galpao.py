"""tiny_fl_stock_snapshots: track galpão qty alongside FL qty.

The webhook delta detector was treating *any* positive Tiny FL delta
as a galpão→Full transfer. That includes sale cancellations (customer
returns units to Full without ever touching galpão) and Tiny↔ML
reconciliation adjustments. The result: 134 false-positive
fulfillment_transfers rows polluting the dashboard's "Enviado ao Full"
column.

To corroborate that a delta is a real transfer we need to look at the
galpão side: a true transfer drops galpão by approximately the same
amount it lifts FL. Sale cancellations leave galpão untouched. Same
column the bulk daily sync already pulls (deposit_name ILIKE '%Galpão%')
— this migration just gives the webhook a place to remember the
previous value across snapshots so the diff is computable.

Revises: mv_coverage_v16
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "fl_snapshot_with_galpao"
down_revision = "mv_coverage_v16"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Default 0 so existing rows don't fail NOT NULL. The first webhook
    # for each product after deploy will refresh the value; until then
    # the detector treats prev galpão = 0 (i.e., can't corroborate, will
    # log and skip) — same conservative path as `previous is None`.
    op.add_column(
        "tiny_fl_stock_snapshots",
        sa.Column(
            "stock_galpao_qty",
            sa.Integer(),
            nullable=False,
            server_default="0",
            comment=(
                "Last seen 'Galpão' deposit qty for this product. Paired "
                "with tiny_fl_qty so the webhook delta detector can verify "
                "that a positive FL delta is matched by a galpão drop "
                "(real transfer) and not a sale cancellation or "
                "Tiny↔ML reconciliation."
            ),
        ),
    )
    # Drop the server_default so future inserts must set the value
    # explicitly — keeps repository code honest.
    op.alter_column("tiny_fl_stock_snapshots", "stock_galpao_qty", server_default=None)


def downgrade() -> None:
    op.drop_column("tiny_fl_stock_snapshots", "stock_galpao_qty")
