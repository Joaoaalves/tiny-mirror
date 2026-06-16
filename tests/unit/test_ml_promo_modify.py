"""Unit tests for MLPromotionService.modify_promotion.

"Alterar" mirrors ML's native in-place edit of a subscribed promotion: it
re-POSTs to /seller-promotions/items/{mlb} with the SAME promotion_id +
promotion_type, changing only the deal_price. The lower-only rule is enforced
at the endpoint/front layer, not here — this method just sends the body. These
tests pin the exact shape we send, which IS the contract with ML.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from tiny_mirror.services.ml_promotion_service import MLPromotionService

pytestmark = pytest.mark.unit


def _http(json_body: Any = None, status: int = 200) -> Any:
    mock = AsyncMock(spec=httpx.AsyncClient)
    resp = MagicMock()
    resp.status_code = status
    resp.content = (json.dumps(json_body) if json_body is not None else "").encode()
    resp.json = MagicMock(return_value=json_body or {})
    resp.text = json.dumps(json_body) if json_body is not None else ""
    mock.post = AsyncMock(return_value=resp)
    mock.put = AsyncMock(return_value=resp)
    mock.delete = AsyncMock(return_value=resp)
    return mock


def _service(http: Any) -> MLPromotionService:
    token = MagicMock()
    token.get_valid_access_token = AsyncMock(return_value="tok")
    token.handle_unauthorized = AsyncMock(return_value="tok2")
    return MLPromotionService(token_service=token, http_client=http)


@pytest.mark.asyncio
async def test_edit_in_place_uses_put_not_post() -> None:
    # Doc ML "Modify items": editar o preço de um item JÁ inscrito é PUT
    # (≠ do POST que é inscrição). Só BAIXAR é permitido in-place.
    http = _http({"price": 29.9, "top_price": 0, "original_price": 50})
    svc = _service(http)
    out = await svc.edit_promotion_price(
        mlb_id="MLB123",
        deal_price=29.90,
        promotion_id="P-MLB1",
        promotion_type="DEAL",
    )
    assert out["status_code"] == 200
    http.post.assert_not_called()
    _, kwargs = http.put.call_args
    assert kwargs["json"] == {
        "promotion_type": "DEAL",
        "deal_price": 29.90,
        "promotion_id": "P-MLB1",
    }
    assert kwargs["params"] == {"app_version": "v2"}


def test_editable_type_sets_match_ml_business_rules() -> None:
    # Regra de negócio do ML (conforme docs): só DEAL/SELLER_CAMPAIGN têm edição
    # in-place (PUT "Modificar item"); PRICE_DISCOUNT é editável só via
    # remover+recriar; co-participação (ML-priced), cupom (fixo) e DOD/LIGHTNING
    # NÃO permitem alterar o preço.
    from tiny_mirror.services.ml_promotion_service import (
        CO_PARTICIPATION_TYPES,
        EDITABLE_INPLACE_TYPES,
        PRICE_EDITABLE_TYPES,
    )

    assert EDITABLE_INPLACE_TYPES == {"DEAL", "SELLER_CAMPAIGN"}
    assert "PRICE_DISCOUNT" in PRICE_EDITABLE_TYPES
    assert "PRICE_DISCOUNT" not in EDITABLE_INPLACE_TYPES
    for t in CO_PARTICIPATION_TYPES | {"SELLER_COUPON_CAMPAIGN", "DOD", "LIGHTNING"}:
        assert t not in PRICE_EDITABLE_TYPES


@pytest.mark.asyncio
async def test_create_price_discount_sends_required_local_dates() -> None:
    # Doc ML "Desconto individual": o POST EXIGE start_date + finish_date em
    # formato LOCAL (sem timezone). Default = hoje → +30d quando não informado.
    http = _http({"id": "P-1"})
    svc = _service(http)
    out = await svc.create_price_discount(mlb_id="MLB1", deal_price=10.0)
    assert out["status_code"] == 200
    _, kwargs = http.post.call_args
    b = kwargs["json"]
    assert b["promotion_type"] == "PRICE_DISCOUNT"
    assert b["deal_price"] == 10.0
    assert "start_date" in b and "finish_date" in b
    assert not b["start_date"].endswith("Z")  # formato local, sem timezone

    # Datas explícitas passam direto.
    await svc.create_price_discount(
        mlb_id="MLB1",
        deal_price=10.0,
        start_date="2026-06-15T00:00:00",
        finish_date="2026-07-15T00:00:00",
    )
    _, kwargs2 = http.post.call_args
    assert kwargs2["json"]["start_date"] == "2026-06-15T00:00:00"
    assert kwargs2["json"]["finish_date"] == "2026-07-15T00:00:00"


@pytest.mark.asyncio
async def test_modify_preserves_promotion_id_and_type() -> None:
    http = _http({"status": "started"})
    svc = _service(http)
    out = await svc.modify_promotion(
        mlb_id="MLB123",
        deal_price=29.90,
        promotion_id="PROMO_XYZ",
        promotion_type="DEAL",
    )
    assert out["status_code"] == 200
    _, kwargs = http.post.call_args
    assert kwargs["json"] == {
        "promotion_type": "DEAL",
        "deal_price": 29.90,
        "promotion_id": "PROMO_XYZ",
    }
    assert kwargs["params"] == {"app_version": "v2"}


@pytest.mark.asyncio
async def test_modify_without_promo_id_omits_it() -> None:
    # Seller PRICE_DISCOUNT (no campaign id): re-POST without promotion_id
    # updates the seller-driven discount in place.
    http = _http({})
    svc = _service(http)
    await svc.modify_promotion(mlb_id="MLB1", deal_price=10.0)
    _, kwargs = http.post.call_args
    assert kwargs["json"] == {"promotion_type": "PRICE_DISCOUNT", "deal_price": 10.0}
    assert "promotion_id" not in kwargs["json"]


@pytest.mark.asyncio
async def test_modify_surfaces_error_status() -> None:
    http = _http({"message": "invalid"}, status=400)
    svc = _service(http)
    out = await svc.modify_promotion(mlb_id="MLB1", deal_price=10.0)
    assert out["status_code"] == 400
    assert out["response"] == {"message": "invalid"}


@pytest.mark.asyncio
async def test_exit_passes_promotion_type_in_delete() -> None:
    # Doc do ML exige promotion_type no DELETE — sem ele o ML pode não saber
    # qual oferta remover. Reentrada automática (raise) depende disso.
    http = _http({})
    svc = _service(http)
    await svc.exit_promotion(mlb_id="MLB1", promotion_type="DEAL")
    _, kwargs = http.delete.call_args
    assert kwargs["params"] == {"app_version": "v2", "promotion_type": "DEAL"}


@pytest.mark.asyncio
async def test_exit_without_type_omits_param() -> None:
    http = _http({})
    svc = _service(http)
    await svc.exit_promotion(mlb_id="MLB1")
    _, kwargs = http.delete.call_args
    assert kwargs["params"] == {"app_version": "v2"}
    assert "promotion_type" not in kwargs["params"]
