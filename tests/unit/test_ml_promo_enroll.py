"""Unit tests for co-participation enrol/exit (SMART etc.).

SMART / PRICE_MATCHING / MARKETPLACE_CAMPAIGN are ML-priced: the seller does
NOT send a price, only accepts the offer. Enrol = POST
{promotion_id, promotion_type, offer_id}; exit = DELETE with the same ids in
the query. The ``offer_id`` (``ref_id`` of the candidate in the per-item GET)
is resolved live — the decision row never stores it. These tests pin the exact
shapes we send to ML, which IS the contract.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from tiny_mirror.services.ml_promotion_service import MLPromotionService

pytestmark = pytest.mark.unit


def _resp(json_body: Any, status: int = 200) -> Any:
    resp = MagicMock()
    resp.status_code = status
    resp.content = (json.dumps(json_body) if json_body is not None else "").encode()
    resp.json = MagicMock(return_value=json_body if json_body is not None else {})
    resp.text = json.dumps(json_body) if json_body is not None else ""
    return resp


def _http(*, get_body: Any, post_body: Any = None, post_status: int = 200) -> Any:
    mock = AsyncMock(spec=httpx.AsyncClient)
    mock.get = AsyncMock(return_value=_resp(get_body))
    mock.post = AsyncMock(return_value=_resp(post_body, post_status))
    mock.delete = AsyncMock(return_value=_resp(post_body, post_status))
    return mock


def _service(http: Any) -> MLPromotionService:
    token = MagicMock()
    token.get_valid_access_token = AsyncMock(return_value="tok")
    token.handle_unauthorized = AsyncMock(return_value="tok2")
    return MLPromotionService(token_service=token, http_client=http)


SMART_CANDIDATE = [
    {
        "id": "P-MLB123",
        "type": "SMART",
        "ref_id": "CANDIDATE-MLB999-7000",
        "status": "candidate",
        "price": 82.67,
        "meli_percentage": 20,
        "seller_percentage": 0,
    }
]

SMART_STARTED = [
    {
        "id": "P-MLB123",
        "type": "SMART",
        "ref_id": "OFFER-MLB999-7000",
        "status": "started",
        "price": 82.67,
        "meli_percentage": 20,
        "seller_percentage": 0,
    }
]


@pytest.mark.asyncio
async def test_enroll_smart_sends_offer_id_and_no_price() -> None:
    http = _http(get_body=SMART_CANDIDATE, post_body={"status": "started"})
    svc = _service(http)
    out = await svc.enroll_offer(mlb_id="MLB999", promotion_type="SMART")
    assert out["status_code"] == 200
    _, kwargs = http.post.call_args
    assert kwargs["json"] == {
        "promotion_type": "SMART",
        "promotion_id": "P-MLB123",
        "offer_id": "CANDIDATE-MLB999-7000",
    }
    # ML define o preço — nunca mandamos deal_price.
    assert "deal_price" not in kwargs["json"]
    assert kwargs["params"] == {"app_version": "v2"}


@pytest.mark.asyncio
async def test_enroll_no_offer_found_does_not_post() -> None:
    # Item sem oferta SMART (não foi convidado): não chamamos o POST.
    http = _http(get_body=[{"id": "P-X", "type": "DEAL", "status": "candidate"}])
    svc = _service(http)
    out = await svc.enroll_offer(mlb_id="MLB999", promotion_type="SMART")
    assert out["status_code"] is None
    assert out["sent_body"] is None
    http.post.assert_not_called()


@pytest.mark.asyncio
async def test_enroll_surfaces_ml_error() -> None:
    http = _http(get_body=SMART_CANDIDATE, post_body={"message": "policy"}, post_status=403)
    svc = _service(http)
    out = await svc.enroll_offer(mlb_id="MLB999", promotion_type="SMART")
    assert out["status_code"] == 403
    assert out["response"] == {"message": "policy"}


@pytest.mark.asyncio
async def test_exit_offer_resolves_ids_into_delete_params() -> None:
    http = _http(get_body=SMART_STARTED, post_body={})
    svc = _service(http)
    await svc.exit_offer(mlb_id="MLB999", promotion_type="SMART")
    _, kwargs = http.delete.call_args
    assert kwargs["params"] == {
        "app_version": "v2",
        "promotion_type": "SMART",
        "promotion_id": "P-MLB123",
        "offer_id": "OFFER-MLB999-7000",
    }


@pytest.mark.asyncio
async def test_exit_promotion_still_minimal_for_seller_types() -> None:
    # Caminho não-co-participação segue só com promotion_type (sem offer_id).
    http = _http(get_body=[], post_body={})
    svc = _service(http)
    await svc.exit_promotion(mlb_id="MLB1", promotion_type="PRICE_DISCOUNT")
    _, kwargs = http.delete.call_args
    assert kwargs["params"] == {"app_version": "v2", "promotion_type": "PRICE_DISCOUNT"}
    assert "offer_id" not in kwargs["params"]
