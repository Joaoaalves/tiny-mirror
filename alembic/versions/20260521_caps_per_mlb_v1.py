"""ml_promo_caps becomes per-MLB instead of per-SKU.

Drops the old PK on sku and re-keys the table by mlb_id. The sku column
stays around (indexed) for grouping queries, but the canonical identity
is now the listing. Each existing per-SKU cap is expanded into one row
per active MLB of that SKU; orphan caps (no active MLB) are deleted —
they could not be acted on anyway.

Operator policy clarified 2026-05-21: the operator wanted the cap %
editable per anúncio. The recompute already computed per-MLB internally
and then consolidated; this migration removes the consolidation by
storing the per-MLB cap directly.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision = "caps_per_mlb_v1"
down_revision: str | Sequence[str] | None = "cap_skip_winning_v1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1) Add the mlb_id column nullable so existing rows survive the schema change.
    op.add_column("ml_promo_caps", sa.Column("mlb_id", sa.String(20), nullable=True))

    # 2) Expand per-SKU rows into per-MLB rows by joining with active listings.
    # We use a temp table to avoid mutating ml_promo_caps mid-query.
    op.execute(
        """
        CREATE TEMP TABLE _caps_expanded AS
        SELECT
            l.mlb_id,
            c.sku,
            c.max_seller_share_pct,
            c.margin_floor_price,
            c.auto_apply,
            c.freight_band_opt,
            c.skip_when_winning,
            c.excluded_promo_types,
            c.notes,
            c.updated_by,
            c.updated_at
        FROM ml_promo_caps c
        JOIN ml_listings l
          ON l.sku = c.sku
         AND l.status = 'active'
        """
    )

    # 3) Drop the old PK + clear the table. We rebuild it from the expanded set;
    #    any cap without an active listing is dropped on the floor.
    # The PK constraint name varies by SQLAlchemy naming convention
    # (`pk_ml_promo_caps` here, `ml_promo_caps_pkey` elsewhere), so look
    # it up at runtime instead of hard-coding.
    op.execute(
        """
        DO $$
        DECLARE pk_name text;
        BEGIN
          SELECT conname INTO pk_name
          FROM pg_constraint
          WHERE conrelid = 'ml_promo_caps'::regclass AND contype = 'p';
          IF pk_name IS NOT NULL THEN
            EXECUTE 'ALTER TABLE ml_promo_caps DROP CONSTRAINT ' || quote_ident(pk_name);
          END IF;
        END $$;
        """
    )
    op.execute("DELETE FROM ml_promo_caps")

    op.execute(
        """
        INSERT INTO ml_promo_caps (
            mlb_id, sku, max_seller_share_pct, margin_floor_price,
            auto_apply, freight_band_opt, skip_when_winning,
            excluded_promo_types, notes, updated_by, updated_at
        )
        SELECT
            mlb_id, sku, max_seller_share_pct, margin_floor_price,
            auto_apply, freight_band_opt, skip_when_winning,
            excluded_promo_types, notes, updated_by, updated_at
        FROM _caps_expanded
        """
    )
    op.execute("DROP TABLE _caps_expanded")

    # 4) Lock the new shape in: mlb_id is the PK, sku stays NOT NULL and indexed.
    op.alter_column("ml_promo_caps", "mlb_id", nullable=False)
    op.create_primary_key("pk_ml_promo_caps", "ml_promo_caps", ["mlb_id"])
    op.create_index("ix_ml_promo_caps_sku", "ml_promo_caps", ["sku"])


def downgrade() -> None:
    # Going back to per-SKU collapses every MLB cap of a given SKU into one row;
    # we keep the most aggressive cap (max seller share) so no live promo would
    # violate the consolidated value.
    op.execute(
        """
        CREATE TEMP TABLE _caps_collapsed AS
        SELECT DISTINCT ON (sku)
            sku,
            max_seller_share_pct,
            margin_floor_price,
            auto_apply,
            freight_band_opt,
            skip_when_winning,
            excluded_promo_types,
            notes,
            updated_by,
            updated_at
        FROM ml_promo_caps
        ORDER BY sku, max_seller_share_pct DESC, mlb_id
        """
    )

    op.drop_index("ix_ml_promo_caps_sku", table_name="ml_promo_caps")
    op.execute(
        """
        DO $$
        DECLARE pk_name text;
        BEGIN
          SELECT conname INTO pk_name
          FROM pg_constraint
          WHERE conrelid = 'ml_promo_caps'::regclass AND contype = 'p';
          IF pk_name IS NOT NULL THEN
            EXECUTE 'ALTER TABLE ml_promo_caps DROP CONSTRAINT ' || quote_ident(pk_name);
          END IF;
        END $$;
        """
    )
    op.execute("DELETE FROM ml_promo_caps")
    op.drop_column("ml_promo_caps", "mlb_id")

    op.execute(
        """
        INSERT INTO ml_promo_caps (
            sku, max_seller_share_pct, margin_floor_price,
            auto_apply, freight_band_opt, skip_when_winning,
            excluded_promo_types, notes, updated_by, updated_at
        )
        SELECT
            sku, max_seller_share_pct, margin_floor_price,
            auto_apply, freight_band_opt, skip_when_winning,
            excluded_promo_types, notes, updated_by, updated_at
        FROM _caps_collapsed
        """
    )
    op.execute("DROP TABLE _caps_collapsed")
    op.create_primary_key("pk_ml_promo_caps", "ml_promo_caps", ["sku"])
