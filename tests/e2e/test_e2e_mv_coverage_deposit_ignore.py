"""E2E: mv_coverage / mv_coverage_fl_kits (v18) count the named deposits even
when Tiny flags them ``desconsiderar`` (mirrored as ``stock_deposits.ignore``).

Reproduces the 2026-09-11 incident with synthetic rows: an "A Caminho"
deposit flagged ``ignore = true`` must still surface as ``stock_chegando``,
Galpão must still count, and an unnamed flagged deposit (Avaria) must stay
excluded from ``stock_total``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal

import pytest
import pytest_asyncio
from sqlalchemy import delete, text

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import ProductORM, StockDepositORM, StockORM

pytestmark = pytest.mark.e2e

_TINY_ID = 999_000_181
_SKU = "E2EIGN-DEPOSIT-FLAG"
_DEPOSITS = (
    # (deposit_tiny_id, name, ignore, available, in_transfer)
    (1, "Galpão", True, 3, 0),
    (2, "Full Mercado Livre", True, 4, 2),
    (3, "A Caminho", True, 7, 0),
    (4, "Avaria", True, 5, 0),
)


@pytest_asyncio.fixture
async def flagged_product(live_db: None) -> AsyncIterator[str]:
    now = datetime.now(UTC)
    async with AsyncSessionLocal() as session:
        session.add(
            ProductORM(
                tiny_id=_TINY_ID,
                sku=_SKU,
                description="e2e deposit-ignore fixture",
                type="S",
                situation="A",
                prices={},
            )
        )
        session.add(
            StockORM(
                product_tiny_id=_TINY_ID,
                sku=_SKU,
                balance=Decimal(19),
                reserved=Decimal(0),
                available=Decimal(19),
                synced_at=now,
            )
        )
        for dep_id, name, ignore, available, in_transfer in _DEPOSITS:
            session.add(
                StockDepositORM(
                    product_tiny_id=_TINY_ID,
                    deposit_tiny_id=dep_id,
                    deposit_name=name,
                    ignore=ignore,
                    balance=Decimal(available + in_transfer),
                    reserved=Decimal(0),
                    available=Decimal(available),
                    in_transfer=Decimal(in_transfer),
                )
            )
        await session.commit()
        await session.execute(text("REFRESH MATERIALIZED VIEW mv_coverage"))
        await session.commit()
    try:
        yield _SKU
    finally:
        async with AsyncSessionLocal() as session:
            await session.execute(
                delete(StockDepositORM).where(StockDepositORM.product_tiny_id == _TINY_ID)
            )
            await session.execute(delete(StockORM).where(StockORM.product_tiny_id == _TINY_ID))
            await session.execute(delete(ProductORM).where(ProductORM.tiny_id == _TINY_ID))
            await session.commit()
            await session.execute(text("REFRESH MATERIALIZED VIEW mv_coverage"))
            await session.commit()


async def test_named_deposits_survive_tiny_desconsiderar(flagged_product: str) -> None:
    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(
                text(
                    "SELECT stock_total, stock_galpao, stock_full_ml, "
                    "stock_fl_in_transfer, stock_chegando "
                    "FROM mv_coverage WHERE sku = :sku"
                ),
                {"sku": flagged_product},
            )
        ).one()

    assert row.stock_chegando == 7, "A Caminho flagged ignore must still be 'recebendo'"
    assert row.stock_galpao == 3
    assert row.stock_full_ml == 6  # available + in_transfer
    assert row.stock_fl_in_transfer == 2
    # Galpão(3) + Full available(4) + A Caminho(7); Avaria stays excluded.
    assert row.stock_total == 14
