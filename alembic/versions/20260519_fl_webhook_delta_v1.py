"""FL webhook overhaul — phase 1: delta detection + transfer source.

Two changes, both inert until the new StockSyncService delta path lands:

- ``fulfillment_transfers.source`` (VARCHAR(20), default 'api') — marks the
  origin of each pending transfer. Existing rows backfill to 'api' (the
  operator-initiated `POST /fulfillment/transfer` flow). New entry-point
  for the webhook safety net = 'tiny_webhook'. Keeping a column instead of
  a separate table lets ``mv_coverage.pending_full_qty`` keep its existing
  shape — the SUM(quantity) GROUP BY product_sku is source-agnostic.

- ``tiny_fl_stock_snapshots`` table — per-product memory of Tiny's RAW FL
  deposit value. We need this because ``stock_deposits.Full Mercado Livre``
  is mutated in-place by the ML overlay on every sync — the original Tiny
  value is lost. Comparing the new raw Tiny value to a previously
  ML-overlaid value would yield false positives on every sync.

  Storing just the raw Tiny value as its own snapshot row keeps the delta
  comparison apples-to-apples regardless of whether the product has an
  ML listing.

Revises: mv_coverage_v13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "fl_webhook_delta_v1"
down_revision = "mv_coverage_v13"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # fulfillment_transfers.source — origin marker
    # ------------------------------------------------------------------
    op.add_column(
        "fulfillment_transfers",
        sa.Column(
            "source",
            sa.String(20),
            nullable=False,
            server_default=sa.text("'api'"),
            comment=(
                "Origin of the transfer row. 'api' = POST /fulfillment/transfer "
                "(operator-initiated via our API). 'tiny_webhook' = inferred from "
                "a positive Tiny FL stock delta detected on the stock webhook "
                "path — safety net for transfers the operator does directly in "
                "Tiny without going through our API. 'manual' = reserved for "
                "back-office insertions."
            ),
        ),
    )
    op.create_check_constraint(
        "valid_fulfillment_transfer_source",
        "fulfillment_transfers",
        "source IN ('api', 'tiny_webhook', 'manual')",
    )
    op.create_index(
        "ix_fulfillment_transfers_source",
        "fulfillment_transfers",
        ["source"],
    )

    # ------------------------------------------------------------------
    # tiny_fl_stock_snapshots — last seen Tiny RAW Full ML deposit value
    # ------------------------------------------------------------------
    op.create_table(
        "tiny_fl_stock_snapshots",
        sa.Column("product_tiny_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "tiny_fl_qty",
            sa.Integer(),
            nullable=False,
            comment=(
                "Last observed value of Tiny's 'Full Mercado Livre' deposit "
                "(deposit_tiny_id=912048995, 'available' column) for this "
                "product, BEFORE the ML overlay rewrites stock_deposits. "
                "Used by the stock webhook path to detect positive deltas "
                "(transfers done in Tiny UI we'd otherwise miss)."
            ),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["product_tiny_id"],
            ["products.tiny_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("product_tiny_id"),
        comment=(
            "Per-product memory of Tiny's raw Full ML deposit value (pre-overlay). "
            "Used purely for delta detection on the stock webhook path."
        ),
    )
    op.execute("GRANT SELECT ON tiny_fl_stock_snapshots TO tiny_readonly;")
    op.execute("GRANT INSERT, UPDATE, DELETE ON tiny_fl_stock_snapshots TO tiny_mirror;")


def downgrade() -> None:
    op.drop_table("tiny_fl_stock_snapshots")
    op.drop_index("ix_fulfillment_transfers_source", "fulfillment_transfers")
    op.drop_constraint(
        "valid_fulfillment_transfer_source",
        "fulfillment_transfers",
        type_="check",
    )
    op.drop_column("fulfillment_transfers", "source")
