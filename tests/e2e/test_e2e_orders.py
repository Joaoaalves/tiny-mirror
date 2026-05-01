"""End-to-end coverage for stage 04 — orders endpoints of TinyAPIClient.

Order *sync* (mapping, persistence, fan-out) is implemented in stage 07
and its tests live alongside that work. This file only covers the
read-only API client behavior that exists today.
"""

from __future__ import annotations

import pytest

from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient

pytestmark = pytest.mark.e2e


async def test_list_orders_returns_valid_structure(
    live_tiny_client: TinyAPIClient,
) -> None:
    response = await live_tiny_client.list_orders(limit=1)

    assert "itens" in response and isinstance(response["itens"], list)
    assert "paginacao" in response
    assert response["paginacao"].get("total", 0) >= 1, (
        "expected at least one order in the live Tiny account"
    )

    item = response["itens"][0]
    for required in ("id", "numeroPedido", "situacao"):
        assert required in item, f"missing field {required!r} in list item"


async def test_get_order_returns_items_array(
    live_tiny_client: TinyAPIClient,
    e2e_order_id: int,
) -> None:
    detail = await live_tiny_client.get_order(e2e_order_id)

    assert int(detail["id"]) == e2e_order_id
    assert "itens" in detail and isinstance(detail["itens"], list)
