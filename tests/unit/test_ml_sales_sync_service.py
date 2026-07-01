"""Unit tests for :class:`tiny_mirror.services.ml_sales_sync_service.MLSalesSyncService`.

Focus: the ``_fetch_day`` HTTP loop — 401 recovery must force a token refresh
(not re-read the same cached token), and pagination must not truncate when the
``paging`` metadata is missing from the response.
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from tiny_mirror.services.ml_sales_sync_service import MLSalesSyncService

pytestmark = pytest.mark.unit


def _response(status_code: int, body: dict[str, Any] | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json = MagicMock(return_value=body or {})
    resp.raise_for_status = MagicMock()
    return resp


def _order(
    mlb: str,
    qty: int,
    sku: str = "SKU-1",
    unit_price: float | None = None,
    tags: list[str] | None = None,
) -> dict[str, Any]:
    item: dict[str, Any] = {"item": {"id": mlb, "seller_sku": sku}, "quantity": qty}
    if unit_price is not None:
        item["unit_price"] = unit_price
    order: dict[str, Any] = {
        "status": "paid",
        "date_created": "2026-06-10T10:00:00.000-00:00",
        "order_items": [item],
    }
    if tags is not None:
        order["tags"] = tags
    return order


@pytest.fixture
def token_service() -> MagicMock:
    tok = MagicMock()
    tok.get_valid_access_token = AsyncMock(return_value="cached.token")
    tok.handle_unauthorized = AsyncMock(return_value="refreshed.token")
    return tok


async def test_fetch_day_401_forces_refresh_and_retries_with_new_token(
    token_service: MagicMock,
) -> None:
    http = AsyncMock()
    http.get = AsyncMock(
        side_effect=[
            _response(401),
            _response(
                200,
                {
                    "results": [_order("MLB1", 2)],
                    "paging": {"total": 1},
                },
            ),
        ]
    )
    service = MLSalesSyncService(token_service, http, "12345")

    agg = await service._fetch_day(date(2026, 6, 10))

    token_service.handle_unauthorized.assert_awaited_once()
    retry_headers = http.get.await_args_list[1].kwargs["headers"]
    assert retry_headers == {"Authorization": "Bearer refreshed.token"}
    assert agg[("MLB1", date(2026, 6, 10))]["qty"] == 2


async def test_fetch_day_missing_paging_keeps_fetching_until_empty_page(
    token_service: MagicMock,
) -> None:
    """A non-empty page without paging metadata must not end the loop —
    only an empty page (or the 10k cap) does."""
    http = AsyncMock()
    http.get = AsyncMock(
        side_effect=[
            _response(200, {"results": [_order("MLB1", 1)]}),
            _response(200, {"results": [_order("MLB2", 3)]}),
            _response(200, {"results": []}),
        ]
    )
    service = MLSalesSyncService(token_service, http, "12345")

    agg = await service._fetch_day(date(2026, 6, 10))

    assert http.get.await_count == 3
    assert agg[("MLB1", date(2026, 6, 10))]["qty"] == 1
    assert agg[("MLB2", date(2026, 6, 10))]["qty"] == 3


async def test_fetch_day_aggregates_revenue_from_unit_price(
    token_service: MagicMock,
) -> None:
    """Revenue = sum of unit_price * quantity across the day's order_items."""
    http = AsyncMock()
    http.get = AsyncMock(
        side_effect=[
            _response(
                200,
                {
                    "results": [
                        _order("MLB1", 2, unit_price=49.90),
                        _order("MLB1", 1, unit_price=49.90),
                        _order("MLB2", 3, unit_price=10.00),
                    ],
                    "paging": {"total": 3},
                },
            ),
        ]
    )
    service = MLSalesSyncService(token_service, http, "12345")

    agg = await service._fetch_day(date(2026, 6, 10))

    assert agg[("MLB1", date(2026, 6, 10))]["qty"] == 3
    assert agg[("MLB1", date(2026, 6, 10))]["revenue"] == pytest.approx(149.70)
    assert agg[("MLB2", date(2026, 6, 10))]["revenue"] == pytest.approx(30.00)


async def test_fetch_day_missing_unit_price_defaults_revenue_zero(
    token_service: MagicMock,
) -> None:
    """An order_item without unit_price must not crash — revenue contributes 0."""
    http = AsyncMock()
    http.get = AsyncMock(
        side_effect=[
            _response(200, {"results": [_order("MLB1", 2)], "paging": {"total": 1}}),
        ]
    )
    service = MLSalesSyncService(token_service, http, "12345")

    agg = await service._fetch_day(date(2026, 6, 10))

    assert agg[("MLB1", date(2026, 6, 10))]["qty"] == 2
    assert agg[("MLB1", date(2026, 6, 10))]["revenue"] == 0.0


async def test_fetch_day_excludes_d2c_direct_orders(token_service: MagicMock) -> None:
    """direct-to-consumer (tag d2c) orders are excluded — the ML listing doesn't
    count them in its 'vendas', so the mirror shouldn't either."""
    http = AsyncMock()
    http.get = AsyncMock(
        side_effect=[
            _response(
                200,
                {
                    "results": [
                        _order("MLB1", 1, unit_price=50.0),  # marketplace → counts
                        _order(
                            "MLB1", 1, unit_price=50.0, tags=["d2c", "one_shot", "paid"]
                        ),  # d2c → skip
                    ],
                    "paging": {"total": 2},
                },
            ),
        ]
    )
    service = MLSalesSyncService(token_service, http, "12345")

    agg = await service._fetch_day(date(2026, 6, 10))

    assert agg[("MLB1", date(2026, 6, 10))]["qty"] == 1  # only the marketplace order
    assert agg[("MLB1", date(2026, 6, 10))]["revenue"] == pytest.approx(50.0)


async def test_fetch_day_stops_at_paging_total(token_service: MagicMock) -> None:
    http = AsyncMock()
    http.get = AsyncMock(
        side_effect=[
            _response(200, {"results": [_order("MLB1", 1)], "paging": {"total": 1}}),
        ]
    )
    service = MLSalesSyncService(token_service, http, "12345")

    agg = await service._fetch_day(date(2026, 6, 10))

    assert http.get.await_count == 1
    assert agg[("MLB1", date(2026, 6, 10))]["qty"] == 1
