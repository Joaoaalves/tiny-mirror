"""ML promotion automation — backend tables.

4 tables for the Promoção Automática feature:
- ml_promo_caps        — user-set cap per SKU (max seller share %, auto_apply flag,
                         freight optimization flag, excluded types, optional
                         margin_floor_price override).
- ml_costs_snapshot    — cached cost data fetched from the Google Apps Script
                         endpoint (planilha MERCADO LIVRE). Refreshed daily by a
                         cron job; used as floor of margin in the decision algo.
- ml_promo_actions     — audit log of every decision (dry-run and applied).
- ml_promo_alerts      — anomalies the operator should review (floor violations
                         on already-active promos, pending freight-band ops, etc.).

No live ML mutation is wired up by this migration. Routers and the service will
ship in follow-up commits; tables here are inert until those land.

Revision ID: ml_promo_v1
Revises: fulfillment_transfers_v1
Create Date: 2026-05-18
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "ml_promo_v1"
down_revision = "fulfillment_transfers_v1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # ml_promo_caps — user-set cap + automation flags per SKU
    # ------------------------------------------------------------------
    op.create_table(
        "ml_promo_caps",
        sa.Column("sku", sa.String(100), primary_key=True),
        sa.Column(
            "max_seller_share_pct",
            sa.Numeric(5, 2),
            nullable=False,
            comment="Cap on the % SELLER pays (excludes ML's meli_percentage share).",
        ),
        sa.Column(
            "margin_floor_price",
            sa.Numeric(12, 2),
            nullable=True,
            comment="Override floor price. NULL = use planilha.sheet_promo_price as floor.",
        ),
        sa.Column(
            "auto_apply",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "freight_band_opt",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
            comment="When true, algorithm drops price by 1 cent if it crosses a freight band and net gain is positive.",
        ),
        sa.Column(
            "excluded_promo_types",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("updated_by", sa.String(100), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    # ------------------------------------------------------------------
    # ml_costs_snapshot — cached cost data per MLB
    # ------------------------------------------------------------------
    op.create_table(
        "ml_costs_snapshot",
        sa.Column("mlb_id", sa.String(20), primary_key=True),
        sa.Column("sku", sa.String(100), nullable=False),
        sa.Column("active_on_sheet", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("base_cost", sa.Numeric(12, 2), nullable=True),
        sa.Column("commission_pct", sa.Numeric(5, 2), nullable=True),
        sa.Column("commission_label", sa.String(100), nullable=True),
        sa.Column("list_price", sa.Numeric(12, 2), nullable=True),
        sa.Column("sheet_promo_price", sa.Numeric(12, 2), nullable=True),
        sa.Column("sheet_discount_pct", sa.Numeric(5, 2), nullable=True),
        sa.Column("sheet_margin_pct", sa.Numeric(5, 2), nullable=True),
        sa.Column("sheet_margin_value", sa.Numeric(12, 2), nullable=True),
        sa.Column(
            "freight_bands",
            postgresql.JSONB(),
            nullable=True,
            comment="Array of {min, max, cost} from the spreadsheet (8 bands).",
        ),
        sa.Column("fetch_error", sa.Text(), nullable=True),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_ml_costs_snapshot_sku", "ml_costs_snapshot", ["sku"])

    # ------------------------------------------------------------------
    # ml_promo_actions — audit log
    # ------------------------------------------------------------------
    op.create_table(
        "ml_promo_actions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column("sku", sa.String(100), nullable=False),
        sa.Column("mlb_id", sa.String(20), nullable=False),
        sa.Column(
            "action",
            sa.String(30),
            nullable=False,
            comment="activated|created|removed|no_change|freight_opt|dry_run|error",
        ),
        sa.Column("promo_type", sa.String(40), nullable=True),
        sa.Column("promo_id", sa.String(60), nullable=True),
        sa.Column("price_before", sa.Numeric(12, 2), nullable=True),
        sa.Column("price_after", sa.Numeric(12, 2), nullable=True),
        sa.Column("total_pct", sa.Numeric(5, 2), nullable=True),
        sa.Column("seller_pct", sa.Numeric(5, 2), nullable=True),
        sa.Column("meli_pct", sa.Numeric(5, 2), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("ml_response", postgresql.JSONB(), nullable=True),
        sa.Column("dry_run", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column(
            "at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_ml_promo_actions_sku_at", "ml_promo_actions", ["sku", sa.text("at DESC")])
    op.create_index("ix_ml_promo_actions_at", "ml_promo_actions", [sa.text("at DESC")])

    # ------------------------------------------------------------------
    # ml_promo_alerts — anomalies / pending reviews
    # ------------------------------------------------------------------
    op.create_table(
        "ml_promo_alerts",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column("sku", sa.String(100), nullable=False),
        sa.Column("mlb_id", sa.String(20), nullable=False),
        sa.Column(
            "kind",
            sa.String(40),
            nullable=False,
            comment="floor_violation|freight_opt_pending|anomaly|no_cost_data|over_cap_existing",
        ),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("data", postgresql.JSONB(), nullable=True),
        sa.Column(
            "acknowledged",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("acknowledged_by", sa.String(100), nullable=True),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index(
        "ix_ml_promo_alerts_sku_kind_ack",
        "ml_promo_alerts",
        ["sku", "kind", "acknowledged"],
    )
    op.create_index(
        "ix_ml_promo_alerts_open",
        "ml_promo_alerts",
        ["acknowledged", sa.text("at DESC")],
    )

    # ------------------------------------------------------------------
    # Grants for the readonly role (analytics + MCP)
    # ------------------------------------------------------------------
    op.execute("GRANT SELECT ON ml_promo_caps TO tiny_readonly")
    op.execute("GRANT SELECT ON ml_costs_snapshot TO tiny_readonly")
    op.execute("GRANT SELECT ON ml_promo_actions TO tiny_readonly")
    op.execute("GRANT SELECT ON ml_promo_alerts TO tiny_readonly")


def downgrade() -> None:
    op.execute("REVOKE SELECT ON ml_promo_alerts FROM tiny_readonly")
    op.execute("REVOKE SELECT ON ml_promo_actions FROM tiny_readonly")
    op.execute("REVOKE SELECT ON ml_costs_snapshot FROM tiny_readonly")
    op.execute("REVOKE SELECT ON ml_promo_caps FROM tiny_readonly")

    op.drop_index("ix_ml_promo_alerts_open", table_name="ml_promo_alerts")
    op.drop_index("ix_ml_promo_alerts_sku_kind_ack", table_name="ml_promo_alerts")
    op.drop_table("ml_promo_alerts")

    op.drop_index("ix_ml_promo_actions_at", table_name="ml_promo_actions")
    op.drop_index("ix_ml_promo_actions_sku_at", table_name="ml_promo_actions")
    op.drop_table("ml_promo_actions")

    op.drop_index("ix_ml_costs_snapshot_sku", table_name="ml_costs_snapshot")
    op.drop_table("ml_costs_snapshot")

    op.drop_table("ml_promo_caps")
