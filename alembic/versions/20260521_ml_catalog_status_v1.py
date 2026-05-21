"""Add ml_catalog_status table (Mercado Livre buy-box / price_to_win cache).

Snapshot of the catalog-listing competitive context per MLB. Populated
daily by ``CatalogStatusSyncService`` from ``GET /items/{MLB}/price_to_win``.
The promo decision engine reads from here instead of calling ML live,
so the full-catalog dry run drops from ~3m35s to seconds.

Schema decisions:
- ``mlb_id`` PK so refresh is an idempotent upsert per item.
- ``status`` constrained to the value space we have observed in production
  plus ``unknown`` as escape hatch when ML returns something new.
- ``boosts`` kept as JSONB because the payload mixes booleans the engine
  may want to query later (free_shipping, fulfillment, etc.) but is not
  worth normalising.

Revises: mv_coverage_v15
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "ml_catalog_status_v1"
down_revision = "mv_coverage_v15"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ml_catalog_status",
        sa.Column("mlb_id", sa.String(20), primary_key=True),
        sa.Column("sku", sa.String(100), nullable=True),
        sa.Column("catalog_listing", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("catalog_product_id", sa.String(50), nullable=True),
        sa.Column("status", sa.String(40), nullable=True),
        sa.Column("visit_share", sa.String(20), nullable=True),
        sa.Column("current_price", sa.Numeric(10, 2), nullable=True),
        sa.Column("price_to_win", sa.Numeric(10, 2), nullable=True),
        sa.Column("winner_item_id", sa.String(20), nullable=True),
        sa.Column("winner_price", sa.Numeric(10, 2), nullable=True),
        sa.Column("competitors_sharing_first_place", sa.Integer(), nullable=True),
        sa.Column(
            "boosts",
            sa.JSON().with_variant(sa.dialects.postgresql.JSONB(), "postgresql"),
            nullable=True,
        ),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "status IS NULL OR status IN "
            "('winning', 'sharing_first_place', 'competing', 'losing', "
            "'not_listed', 'unknown')",
            name="valid_catalog_status",
        ),
    )
    op.create_index("ix_ml_catalog_status_sku", "ml_catalog_status", ["sku"])
    op.create_index("ix_ml_catalog_status_status", "ml_catalog_status", ["status"])
    op.create_index(
        "ix_ml_catalog_status_catalog_listing", "ml_catalog_status", ["catalog_listing"]
    )


def downgrade() -> None:
    op.drop_index("ix_ml_catalog_status_catalog_listing", table_name="ml_catalog_status")
    op.drop_index("ix_ml_catalog_status_status", table_name="ml_catalog_status")
    op.drop_index("ix_ml_catalog_status_sku", table_name="ml_catalog_status")
    op.drop_table("ml_catalog_status")
