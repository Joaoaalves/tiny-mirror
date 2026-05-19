"""Add products.manual_status + manual_status_synced_at.

The operator classifies SKUs manually in the Controle 4.0 spreadsheet
by coloring cells on the GERAL tab (columns B = COD.FAB, C = SKU):
red `#f4cccc` = queima, yellow `#fff2cc` = analise, otherwise normal.

A daily scheduler job pulls a GAS Web App that exposes this mapping
(see ``gas/manual_status/``) and upserts the result here, so the
queima / reposição / FL crons can skip SKUs the operator already
marked.

NULL means "never synced yet" (e.g. a fresh deploy before the first
job run, or a Tiny SKU not present on the sheet). Treat NULL as
"normal" at the consumer side.

Revises: fl_webhook_delta_v1
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "manual_status_v1"
down_revision = "fl_webhook_delta_v1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "products",
        sa.Column(
            "manual_status",
            sa.String(20),
            nullable=True,
            comment=(
                "Operator's manual classification from the GERAL spreadsheet. "
                "'queima' = already in/destined for queima (never re-suggest), "
                "'analise' = under review (never re-suggest), "
                "'normal' = eligible for queima/reposição/FL. "
                "NULL = never synced or SKU not on the sheet (treat as normal)."
            ),
        ),
    )
    op.add_column(
        "products",
        sa.Column(
            "manual_status_synced_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment=(
                "Last time the manual_status was refreshed from the GAS endpoint. "
                "NULL means never synced."
            ),
        ),
    )
    op.create_check_constraint(
        "valid_manual_status",
        "products",
        "manual_status IS NULL OR manual_status IN ('queima', 'analise', 'normal')",
    )
    op.create_index(
        "ix_products_manual_status",
        "products",
        ["manual_status"],
    )


def downgrade() -> None:
    op.drop_index("ix_products_manual_status", table_name="products")
    op.drop_constraint("ck_products_valid_manual_status", "products", type_="check")
    op.drop_column("products", "manual_status_synced_at")
    op.drop_column("products", "manual_status")
