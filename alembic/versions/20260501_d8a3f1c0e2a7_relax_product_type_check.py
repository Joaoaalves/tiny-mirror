"""relax product type check to accept V and F

Tiny v3 returns ``tipo`` values beyond the original P/S/K trio: ``V`` for
variation-aware products and ``F`` for manufactured items. The previous
constraint dropped these to the DLQ during sync. Expand the allow-list
so the catalog mirror is complete.

Revision ID: d8a3f1c0e2a7
Revises: c441566144e8
Create Date: 2026-05-01 13:10:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "d8a3f1c0e2a7"
down_revision: str | Sequence[str] | None = "c441566144e8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint("ck_products_valid_type", "products", type_="check")
    op.create_check_constraint(
        "ck_products_valid_type",
        "products",
        "type IN ('P', 'S', 'K', 'V', 'F')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_products_valid_type", "products", type_="check")
    op.create_check_constraint(
        "ck_products_valid_type",
        "products",
        "type IN ('P', 'S', 'K')",
    )
