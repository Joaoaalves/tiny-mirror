"""Live check for ``ix_products_sku`` (alembic ``products_sku_index``).

After ``alembic upgrade head`` the full index must exist, be VALID (a failed
CONCURRENTLY build leaves an invalid one), and serve a plain ``WHERE sku = $1``
lookup — the shape the caps enrichment and the dashboard use without the
``situation IN ('A','I')`` predicate of the partial ``uq_products_sku``.
Read-only.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from tiny_mirror.database import AsyncSessionLocal

pytestmark = pytest.mark.e2e


@pytest.mark.asyncio
async def test_index_exists_and_is_valid(live_db: None) -> None:
    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(
                text(
                    "SELECT i.indisvalid, pg_get_indexdef(i.indexrelid) "
                    "FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
                    "WHERE c.relname = 'ix_products_sku'"
                )
            )
        ).first()
    assert row is not None, "ix_products_sku missing — run alembic upgrade head"
    assert row[0] is True
    assert "WHERE" not in row[1].upper()


@pytest.mark.asyncio
async def test_plain_sku_lookup_uses_the_index(live_db: None) -> None:
    async with AsyncSessionLocal() as session:
        await session.execute(text("SET LOCAL enable_seqscan = off"))
        plan = "\n".join(
            r[0]
            for r in (
                await session.execute(
                    text("EXPLAIN SELECT manual_status FROM products WHERE sku = :sku LIMIT 1"),
                    {"sku": "E2E-NO-SUCH-SKU"},
                )
            ).all()
        )
        await session.rollback()
    assert "ix_products_sku" in plan
