"""Unit tests for :class:`MercadoLivreAPIClient`.

All external collaborators are mocked. ``asyncio.sleep`` is patched so
retry loops complete instantly.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from tiny_mirror.exceptions import (
    RateLimitException,
    TinyAPIException,
    TokenExpiredException,
)
from tiny_mirror.infrastructure.external.mercadolivre_client import MercadoLivreAPIClient

pytestmark = pytest.mark.unit

ML_USER_ID = "227584372"


@pytest.fixture
def fake_ml_token_service() -> AsyncMock:
    svc = AsyncMock()
    svc.get_valid_access_token = AsyncMock(return_value="ml.access.token")
    svc.handle_unauthorized = AsyncMock(return_value="refreshed.ml.token")
    return svc


@pytest.fixture
def client(fake_ml_token_service: AsyncMock, mock_http_client: AsyncMock) -> MercadoLivreAPIClient:
    return MercadoLivreAPIClient(
        token_service=fake_ml_token_service,
        http_client=mock_http_client,
        ml_user_id=ML_USER_ID,
    )


@pytest.fixture(autouse=True)
def _no_real_sleep(mocker):
    mocker.patch(
        "tiny_mirror.infrastructure.external.mercadolivre_client.asyncio.sleep",
        new=AsyncMock(),
    )


# ---------------------------------------------------------------------------
# list_items_by_sku
# ---------------------------------------------------------------------------
async def test_list_items_by_sku_returns_results(
    client: MercadoLivreAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(
        return_value=make_response(
            200,
            json_body={
                "results": ["MLB123456789", "MLB987654321"],
                "paging": {"total": 2},
            },
        )
    )

    result = await client.list_items_by_sku("SKU-001")

    assert result == ["MLB123456789", "MLB987654321"]
    mock_http_client.request.assert_awaited_once()
    call_kwargs = mock_http_client.request.call_args
    assert f"/users/{ML_USER_ID}/items/search" in call_kwargs.args[1]
    assert call_kwargs.kwargs["params"]["seller_sku"] == "SKU-001"


async def test_list_items_by_sku_empty_returns_empty_list(
    client: MercadoLivreAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(
        return_value=make_response(200, json_body={"results": [], "paging": {"total": 0}})
    )

    result = await client.list_items_by_sku("SKU-NOTLISTED")

    assert result == []


async def test_list_items_by_sku_missing_results_key_returns_empty_list(
    client: MercadoLivreAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(
        return_value=make_response(200, json_body={"paging": {"total": 0}})
    )

    result = await client.list_items_by_sku("SKU-ODD-RESPONSE")

    assert result == []


# ---------------------------------------------------------------------------
# get_item
# ---------------------------------------------------------------------------
async def test_get_item_returns_item_detail(
    client: MercadoLivreAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    item_payload = {
        "id": "MLB123456789",
        "available_quantity": 42,
        "status": "active",
        "shipping": {"logistic_type": "fulfillment"},
    }
    mock_http_client.request = AsyncMock(return_value=make_response(200, json_body=item_payload))

    result = await client.get_item("MLB123456789")

    assert result["id"] == "MLB123456789"
    assert result["available_quantity"] == 42
    assert result["shipping"]["logistic_type"] == "fulfillment"
    mock_http_client.request.assert_awaited_once()
    assert "/items/MLB123456789" in mock_http_client.request.call_args.args[1]


async def test_get_item_non_fulfillment_logistic_type(
    client: MercadoLivreAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    item_payload = {
        "id": "MLB111",
        "available_quantity": 10,
        "status": "active",
        "shipping": {"logistic_type": "me2"},
    }
    mock_http_client.request = AsyncMock(return_value=make_response(200, json_body=item_payload))

    result = await client.get_item("MLB111")

    assert result["shipping"]["logistic_type"] == "me2"


# ---------------------------------------------------------------------------
# Auth: Bearer header injection
# ---------------------------------------------------------------------------
async def test_request_injects_bearer_token(
    client: MercadoLivreAPIClient,
    mock_http_client: AsyncMock,
    fake_ml_token_service: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(return_value=make_response(200, json_body={"results": []}))

    await client.list_items_by_sku("SKU-X")

    call_headers = mock_http_client.request.call_args.kwargs["headers"]
    assert call_headers["Authorization"] == "Bearer ml.access.token"
    fake_ml_token_service.get_valid_access_token.assert_awaited_once()


# ---------------------------------------------------------------------------
# 401 → refresh once → retry
# ---------------------------------------------------------------------------
async def test_401_triggers_token_refresh_and_retry(
    client: MercadoLivreAPIClient,
    mock_http_client: AsyncMock,
    fake_ml_token_service: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(
        side_effect=[
            make_response(401, text="Unauthorized"),
            make_response(200, json_body={"results": ["MLB1"]}),
        ]
    )

    result = await client.list_items_by_sku("SKU-Y")

    assert result == ["MLB1"]
    fake_ml_token_service.handle_unauthorized.assert_awaited_once()
    assert mock_http_client.request.await_count == 2


async def test_double_401_raises_token_expired(
    client: MercadoLivreAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(
        side_effect=[
            make_response(401, text="Unauthorized"),
            make_response(401, text="Unauthorized"),
        ]
    )

    with pytest.raises(TokenExpiredException):
        await client.list_items_by_sku("SKU-Z")


# ---------------------------------------------------------------------------
# 404 → raises TinyAPIException
# ---------------------------------------------------------------------------
async def test_404_raises_api_exception(
    client: MercadoLivreAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(return_value=make_response(404, text="Not Found"))

    with pytest.raises(TinyAPIException) as exc_info:
        await client.get_item("MLB_GONE")

    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# 429 → retry with backoff
# ---------------------------------------------------------------------------
async def test_429_retries_up_to_max_then_raises_rate_limit(
    client: MercadoLivreAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(return_value=make_response(429, text="Too Many Requests"))

    with pytest.raises(RateLimitException):
        await client.list_items_by_sku("SKU-RATELIMIT")

    assert mock_http_client.request.await_count == MercadoLivreAPIClient.MAX_RETRIES + 1


async def test_429_succeeds_on_retry(
    client: MercadoLivreAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(
        side_effect=[
            make_response(429, text="Too Many Requests"),
            make_response(200, json_body={"results": ["MLB42"]}),
        ]
    )

    result = await client.list_items_by_sku("SKU-RETRY")

    assert result == ["MLB42"]
    assert mock_http_client.request.await_count == 2


# ---------------------------------------------------------------------------
# 5xx transient → retry
# ---------------------------------------------------------------------------
async def test_503_retries_and_eventually_raises(
    client: MercadoLivreAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(
        return_value=make_response(503, text="Service Unavailable")
    )

    with pytest.raises(TinyAPIException) as exc_info:
        await client.get_item("MLB_DOWN")

    assert exc_info.value.status_code == 503
    assert mock_http_client.request.await_count == MercadoLivreAPIClient.MAX_RETRIES + 1


async def test_500_succeeds_on_second_attempt(
    client: MercadoLivreAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    item = {"id": "MLB1", "available_quantity": 5, "shipping": {"logistic_type": "fulfillment"}}
    mock_http_client.request = AsyncMock(
        side_effect=[
            make_response(500, text="Internal Server Error"),
            make_response(200, json_body=item),
        ]
    )

    result = await client.get_item("MLB1")

    assert result["id"] == "MLB1"
    assert mock_http_client.request.await_count == 2


# ---------------------------------------------------------------------------
# Timeout → retry
# ---------------------------------------------------------------------------
async def test_timeout_retries_and_raises_after_max(
    client: MercadoLivreAPIClient,
    mock_http_client: AsyncMock,
) -> None:
    mock_http_client.request = AsyncMock(side_effect=httpx.TimeoutException("timed out"))

    with pytest.raises(TinyAPIException, match="timed out"):
        await client.list_items_by_sku("SKU-TIMEOUT")

    assert mock_http_client.request.await_count == MercadoLivreAPIClient.MAX_RETRIES + 1


# ---------------------------------------------------------------------------
# Network error → retry
# ---------------------------------------------------------------------------
async def test_connect_error_retries_and_raises(
    client: MercadoLivreAPIClient,
    mock_http_client: AsyncMock,
) -> None:
    mock_http_client.request = AsyncMock(side_effect=httpx.ConnectError("connection refused"))

    with pytest.raises(TinyAPIException, match="network error"):
        await client.list_items_by_sku("SKU-NONET")

    assert mock_http_client.request.await_count == MercadoLivreAPIClient.MAX_RETRIES + 1


# ---------------------------------------------------------------------------
# Invalid JSON
# ---------------------------------------------------------------------------
async def test_invalid_json_response_raises(
    client: MercadoLivreAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    bad_resp = make_response(200, text="not-json")
    bad_resp.json = MagicMock(side_effect=ValueError("No JSON"))
    mock_http_client.request = AsyncMock(return_value=bad_resp)

    with pytest.raises(TinyAPIException, match="Invalid JSON"):
        await client.list_items_by_sku("SKU-BADJSON")
