"""End-to-end coverage for stage 04 — stock endpoint of TinyAPIClient.

Stock sync (StockSyncService, deposit-level upserts) is implemented in
stage 08 and its tests live alongside that work.
"""

from __future__ import annotations

import pytest

from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient

pytestmark = pytest.mark.e2e


async def test_get_stock_returns_deposits_and_balance(
    live_tiny_client: TinyAPIClient,
    e2e_product_id: int,
) -> None:
    stock = await live_tiny_client.get_stock(e2e_product_id)

    assert "depositos" in stock and isinstance(stock["depositos"], list)
    assert "saldo" in stock
    assert "disponivel" in stock
