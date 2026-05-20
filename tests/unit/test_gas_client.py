"""Unit tests for the unified GAS client."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from tiny_mirror.services.gas_client import GASClient, GASClientError

pytestmark = pytest.mark.unit


def _http(json_body: Any = None, status: int = 200) -> Any:
    mock = AsyncMock(spec=httpx.AsyncClient)
    resp = MagicMock()
    resp.status_code = status
    resp.text = json.dumps(json_body) if json_body is not None else ""
    resp.json = MagicMock(return_value=json_body)
    mock.get = AsyncMock(return_value=resp)
    return mock


@pytest.mark.asyncio
async def test_manual_status_sends_action_and_token_params() -> None:
    http = _http({"skus": {"X": {"status": "queima"}}, "counts": {"queima": 1}})
    gas = GASClient(http=http, base_url="https://script/exec", token="tok")
    await gas.manual_status()
    args, kwargs = http.get.call_args
    assert args[0] == "https://script/exec"
    assert kwargs["params"]["action"] == "manual_status"
    assert kwargs["params"]["token"] == "tok"


@pytest.mark.asyncio
async def test_costs_all_returns_difal_and_items() -> None:
    payload = {
        "generatedAt": "2026-05-19T20:30:00Z",
        "difalPct": 0.115,
        "count": 2,
        "items": {
            "MLB1": {"sku": "A", "baseCost": 10, "listPrice": 50},
            "MLB2": {"sku": "B", "baseCost": 20, "listPrice": 60},
        },
    }
    gas = GASClient(http=_http(payload), base_url="https://script/exec", token="tok")
    body = await gas.costs_all()
    assert body["difalPct"] == 0.115
    assert set(body["items"].keys()) == {"MLB1", "MLB2"}


@pytest.mark.asyncio
async def test_cost_single_passes_mlbid() -> None:
    http = _http({"mlbId": "MLB1", "sku": "A"})
    gas = GASClient(http=http, base_url="https://script/exec", token="tok")
    await gas.cost_single("MLB1")
    assert http.get.call_args.kwargs["params"]["mlbid"] == "MLB1"


@pytest.mark.asyncio
async def test_rejects_unconfigured_client() -> None:
    gas = GASClient(http=_http({}), base_url="", token="")
    with pytest.raises(GASClientError, match="not configured"):
        await gas.manual_status()


@pytest.mark.asyncio
async def test_wraps_http_error_status() -> None:
    gas = GASClient(http=_http({"error": "x"}, status=500), base_url="u", token="t")
    with pytest.raises(GASClientError, match="HTTP 500"):
        await gas.manual_status()


@pytest.mark.asyncio
async def test_wraps_payload_error_field() -> None:
    gas = GASClient(http=_http({"error": "unauthorized"}), base_url="u", token="t")
    with pytest.raises(GASClientError, match="unauthorized"):
        await gas.manual_status()


@pytest.mark.asyncio
async def test_wraps_request_error() -> None:
    http = AsyncMock(spec=httpx.AsyncClient)
    http.get = AsyncMock(side_effect=httpx.ConnectError("down"))
    gas = GASClient(http=http, base_url="u", token="t")
    with pytest.raises(GASClientError, match="request failed"):
        await gas.manual_status()


@pytest.mark.asyncio
async def test_rejects_non_object_body() -> None:
    gas = GASClient(http=_http(["not", "a", "dict"]), base_url="u", token="t")
    with pytest.raises(GASClientError, match="non-object"):
        await gas.manual_status()
