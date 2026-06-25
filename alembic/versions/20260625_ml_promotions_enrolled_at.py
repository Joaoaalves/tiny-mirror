"""ml_promotions: enrolled_at marker (authoritative enrollment).

When WE enroll a listing in a promotion the ML write returns 201, but ML's
``eligible`` endpoint keeps listing the promo as ``candidate`` (the invitation)
for a while — with lag/flapping — instead of ``started``. The AS-IS mirror sync
re-reads that and DOWNGRADED the just-enrolled promo back to ``candidate``, so it
reappeared as "available / not subscribed" and could make the whole SKU panel
vanish. ``enrolled_at`` records that we enrolled it: while set, the sync preserves
``status='started'`` and never deletes the row as a phantom. Cleared on exit.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "ml_promotions_enrolled_at"
down_revision = "ml_listing_variations_sku"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ml_promotions",
        sa.Column("enrolled_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("ml_promotions", "enrolled_at")
