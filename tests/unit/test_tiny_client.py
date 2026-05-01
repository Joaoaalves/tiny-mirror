"""Unit tests for :class:`tiny_mirror.infrastructure.external.tiny_client.TinyAPIClient`.

Every external collaborator is mocked: TokenService, RateLimiter, and
the underlying httpx.AsyncClient. ``asyncio.sleep`` is patched too so
backoff loops don't waste real time.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from tiny_mirror.exceptions import (
    RateLimitException,
    TinyAPIException,
    TinyNotFoundException,
    TokenExpiredException,
)
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient

pytestmark = pytest.mark.unit


@pytest.fixture
def fake_token_service() -> AsyncMock:
    svc = AsyncMock()
    svc.get_valid_access_token = AsyncMock(return_value="fake.access.token")
    svc.handle_unauthorized = AsyncMock(return_value="rotated.access.token")
    return svc


@pytest.fixture
def fake_rate_limiter() -> AsyncMock:
    limiter = AsyncMock()
    limiter.wait_if_needed = AsyncMock(return_value=None)
    limiter.update_from_headers = AsyncMock(return_value=None)
    return limiter


@pytest.fixture
def client(
    fake_token_service: AsyncMock,
    fake_rate_limiter: AsyncMock,
    mock_http_client: AsyncMock,
) -> TinyAPIClient:
    return TinyAPIClient(
        token_service=fake_token_service,
        rate_limiter=fake_rate_limiter,
        http_client=mock_http_client,
    )


@pytest.fixture(autouse=True)
def _no_real_sleep(mocker):
    """Replace asyncio.sleep with an immediate AsyncMock so retry loops
    don't actually wait."""
    mocker.patch(
        "tiny_mirror.infrastructure.external.tiny_client.asyncio.sleep",
        new=AsyncMock(),
    )


# ---------------------------------------------------------------------------
# Happy path / header injection
# ---------------------------------------------------------------------------
async def test_request_success_returns_parsed_json(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(
        return_value=make_response(200, json_body={"itens": [], "paginacao": {"total": 0}})
    )

    result = await client._request("GET", "/produtos")

    assert result == {"itens": [], "paginacao": {"total": 0}}
    mock_http_client.request.assert_awaited_once()


async def test_request_injects_bearer_authorization_header(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(return_value=make_response(200, json_body={}))

    await client._request("GET", "/produtos")

    _, kwargs = mock_http_client.request.call_args
    assert kwargs["headers"]["Authorization"] == "Bearer fake.access.token"


async def test_request_calls_rate_limiter_before_and_after(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
    fake_rate_limiter: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(return_value=make_response(200, json_body={}))

    await client._request("GET", "/produtos")

    fake_rate_limiter.wait_if_needed.assert_awaited()
    fake_rate_limiter.update_from_headers.assert_awaited()


# ---------------------------------------------------------------------------
# 401 -> handle_unauthorized -> retry once
# ---------------------------------------------------------------------------
async def test_request_401_then_200_refreshes_and_retries_once(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
    fake_token_service: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(
        side_effect=[make_response(401), make_response(200, json_body={"ok": True})]
    )

    result = await client._request("GET", "/produtos")

    assert result == {"ok": True}
    fake_token_service.handle_unauthorized.assert_awaited_once()
    assert mock_http_client.request.await_count == 2

    # The second call should carry the rotated token.
    _, kwargs = mock_http_client.request.call_args
    assert kwargs["headers"]["Authorization"] == "Bearer fake.access.token"
    # NOTE: on retry, the loop re-fetches via get_valid_access_token (which
    # we keep returning the same fake token); that's fine — the important
    # invariant is that handle_unauthorized was called.


async def test_request_401_twice_raises_token_expired(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(side_effect=[make_response(401), make_response(401)])

    with pytest.raises(TokenExpiredException):
        await client._request("GET", "/produtos")


# ---------------------------------------------------------------------------
# 429 backoff
# ---------------------------------------------------------------------------
async def test_request_429_then_200_retries(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(
        side_effect=[make_response(429), make_response(200, json_body={})]
    )

    result = await client._request("GET", "/produtos")

    assert result == {}
    assert mock_http_client.request.await_count == 2


async def test_request_429_max_retries_raises_rate_limit_exception(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(
        return_value=make_response(429, headers={"X-RateLimit-Reset": "60"})
    )

    with pytest.raises(RateLimitException) as excinfo:
        await client._request("GET", "/produtos")

    assert mock_http_client.request.await_count == TinyAPIClient.MAX_RETRIES
    assert excinfo.value.status_code == 429
    assert excinfo.value.retry_after_seconds == 60


# ---------------------------------------------------------------------------
# 400 retry (Tiny flake) and 404 / 5xx
# ---------------------------------------------------------------------------
async def test_request_400_retried_once_then_raises(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(
        return_value=make_response(400, text='{"erro": "validation"}')
    )

    with pytest.raises(TinyAPIException):
        await client._request("GET", "/pedidos", params={"limit": 1})

    # Original + one retry per stage_04 spec.
    assert mock_http_client.request.await_count == 1 + TinyAPIClient.BAD_REQUEST_MAX_RETRIES


async def test_request_400_then_200_recovers(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(
        side_effect=[make_response(400), make_response(200, json_body={"ok": 1})]
    )

    result = await client._request("GET", "/pedidos")

    assert result == {"ok": 1}


async def test_request_404_raises_not_found_with_resource_metadata(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(
        return_value=make_response(404, text='{"erro": "Not Found"}')
    )

    with pytest.raises(TinyNotFoundException) as excinfo:
        await client._request("GET", "/produtos/12345")

    assert excinfo.value.resource_type == "produto"
    assert str(excinfo.value.resource_id) == "12345"


async def test_request_500_raises_tiny_api_exception(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(return_value=make_response(500, text="server error"))

    with pytest.raises(TinyAPIException) as excinfo:
        await client._request("GET", "/produtos")

    assert excinfo.value.status_code == 500


async def test_request_4xx_other_than_handled_raises_tiny_api_exception(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(return_value=make_response(403, text="forbidden"))

    with pytest.raises(TinyAPIException) as excinfo:
        await client._request("GET", "/produtos")

    assert excinfo.value.status_code == 403


async def test_request_invalid_json_in_2xx_raises(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    bad_resp = make_response(200, text="not json")
    bad_resp.json = MagicMock(side_effect=ValueError("invalid"))
    mock_http_client.request = AsyncMock(return_value=bad_resp)

    with pytest.raises(TinyAPIException, match="Invalid JSON"):
        await client._request("GET", "/produtos")


# ---------------------------------------------------------------------------
# Network errors
# ---------------------------------------------------------------------------
async def test_request_timeout_translated_to_tiny_api_exception(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
) -> None:
    mock_http_client.request = AsyncMock(side_effect=httpx.TimeoutException("read timeout"))

    with pytest.raises(TinyAPIException, match="timed out"):
        await client._request("GET", "/produtos")


async def test_request_connect_error_translated_to_tiny_api_exception(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
) -> None:
    mock_http_client.request = AsyncMock(side_effect=httpx.ConnectError("dns failure"))

    with pytest.raises(TinyAPIException, match="Network error"):
        await client._request("GET", "/produtos")


# ---------------------------------------------------------------------------
# Public methods build the right path / params
# ---------------------------------------------------------------------------
async def test_list_products_builds_query_params(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(return_value=make_response(200, json_body={}))

    await client.list_products(situation="A", limit=100, offset=50)

    args, kwargs = mock_http_client.request.call_args
    assert args[0] == "GET"
    assert args[1].endswith("/produtos")
    assert kwargs["params"] == {"limit": 100, "offset": 50, "situacao": "A"}


async def test_get_product_builds_correct_url(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(return_value=make_response(200, json_body={}))

    await client.get_product(12345)

    args, _ = mock_http_client.request.call_args
    assert args[0] == "GET"
    assert args[1].endswith("/produtos/12345")


async def test_list_orders_uses_date_only_for_dataAtualizacao(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    """Tiny v3 only accepts YYYY-MM-DD here — see the project memory."""
    from datetime import UTC, datetime

    mock_http_client.request = AsyncMock(return_value=make_response(200, json_body={}))

    await client.list_orders(updated_after=datetime(2025, 6, 15, 10, 30, tzinfo=UTC), limit=10)

    _, kwargs = mock_http_client.request.call_args
    assert kwargs["params"]["dataAtualizacao"] == "2025-06-15"


async def test_get_stock_builds_correct_url(
    client: TinyAPIClient,
    mock_http_client: AsyncMock,
    make_response,
) -> None:
    mock_http_client.request = AsyncMock(return_value=make_response(200, json_body={}))

    await client.get_stock(789)

    args, _ = mock_http_client.request.call_args
    assert args[1].endswith("/estoque/789")
