"""Make products.sku UNIQUE partial: only for situation IN ('A','I').

Tiny lets the same SKU exist on multiple products at once: typically one
active product + N excluded "phantom" duplicates (created automatically
when ML orders arrive with unmapped seller_skus). Our products_sync now
also pulls situation='E' for visibility, so the previous full UNIQUE
constraint on sku started conflicting (1102 failures in sync 1510).

The fix: drop the unconditional unique, create a partial unique that
applies only to situation IN ('A','I'). Excluded duplicates can coexist
freely; active/inactive catalog stays unambiguous.

Revision ID: products_sku_partial_unique
Revises: fl_stock_corrections_log
Create Date: 2026-05-27
"""

from __future__ import annotations

from alembic import op

revision = "products_sku_partial_unique"
down_revision = "fl_stock_corrections_log"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE products DROP CONSTRAINT IF EXISTS uq_products_sku")
    op.execute("DROP INDEX IF EXISTS uq_products_sku")
    op.execute(
        "CREATE UNIQUE INDEX uq_products_sku " "ON products (sku) WHERE situation IN ('A','I')"
    )


def downgrade() -> None:
    # Downgrade is destructive if excluded duplicates exist — drop them first.
    # Caller responsibility; we just recreate the strict unique.
    op.execute("DROP INDEX IF EXISTS uq_products_sku")
    op.execute("CREATE UNIQUE INDEX uq_products_sku ON products (sku)")
