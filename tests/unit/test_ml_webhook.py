"""Unit tests for the ML webhook receiver's pure helpers.

The endpoint itself is exercised in e2e (needs DB); here we pin the two pure
parsers that decide WHICH listing a notification touches and when it was sent.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from tiny_mirror.api.routers.webhooks import _parse_iso, _parse_mlb_from_resource
from tiny_mirror.services.ml_webhook_processor import WebhookProcessor

pytestmark = pytest.mark.unit


def test_parse_mlb_from_item_resource() -> None:
    assert _parse_mlb_from_resource("/items/MLB3699928397/price_to_win") == "MLB3699928397"


def test_parse_mlb_from_offer_resource() -> None:
    # Offers do ML embutem o MLB no id: OFFER-MLB...-...
    assert (
        _parse_mlb_from_resource("/seller-promotions/offers/OFFER-MLB1970246686-42701792")
        == "MLB1970246686"
    )


def test_parse_mlb_none_when_absent() -> None:
    # Candidatos podem não trazer o MLB no id — o processador resolve via GET.
    assert _parse_mlb_from_resource("/seller-promotions/candidates/CANDIDATE-abc") is None
    assert _parse_mlb_from_resource("") is None


def test_parse_iso_handles_z_and_offset() -> None:
    assert _parse_iso("2026-06-17T13:44:33.006Z") == datetime(
        2026, 6, 17, 13, 44, 33, 6000, tzinfo=UTC
    )
    assert _parse_iso("2026-06-17T00:00:00-03:00") is not None
    assert _parse_iso(None) is None
    assert _parse_iso("not-a-date") is None


def _processor(http: object) -> WebhookProcessor:
    token = MagicMock()
    token.get_valid_access_token = AsyncMock(return_value="tok")
    token.handle_unauthorized = AsyncMock(return_value="tok2")
    return WebhookProcessor(promotion_service=MagicMock(), token_service=token, http_client=http)


@pytest.mark.asyncio
async def test_resolve_mlb_from_resource_text_no_http() -> None:
    # MLB já está no texto do resource → não precisa bater no ML.
    http = MagicMock()
    http.get = AsyncMock()
    proc = _processor(http)
    out = await proc._resolve_mlb("/seller-promotions/offers/OFFER-MLB4745867172-9")
    assert out == "MLB4745867172"
    http.get.assert_not_called()


@pytest.mark.asyncio
async def test_resolve_mlb_via_get_when_absent() -> None:
    # Candidato sem MLB no id → GET no resource e extrai o MLB da resposta.
    resp = MagicMock()
    resp.status_code = 200
    resp.text = '{"item_id":"MLB3699928397","status":"candidate"}'
    http = MagicMock()
    http.get = AsyncMock(return_value=resp)
    proc = _processor(http)
    out = await proc._resolve_mlb("/seller-promotions/candidates/CANDIDATE-xyz")
    assert out == "MLB3699928397"
    http.get.assert_awaited_once()


@pytest.mark.asyncio
async def test_resolve_mlb_none_on_non_path() -> None:
    proc = _processor(MagicMock())
    assert await proc._resolve_mlb("garbage") is None
