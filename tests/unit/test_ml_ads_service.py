"""Unit coverage for the Mercado Ads read bridge (MLAdsService).

Upstream HTTP is mocked; what we pin down is URL/param construction
against the post-May-2026 API contract, the 401 → refresh → retry
path, advertiser discovery caching, the provenance wrapper
(collected_at) and the shared window validation.
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from tiny_mirror.services.ml_ads_service import (
    DEFAULT_METRICS,
    MLAdsService,
    MLAdsUpstreamError,
    validate_window,
)

pytestmark = pytest.mark.unit


def _response(status_code: int = 200, json_body: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json = MagicMock(return_value=json_body or {})
    resp.text = str(json_body or {})
    return resp


@pytest.fixture
def token_service() -> AsyncMock:
    svc = AsyncMock()
    svc.get_valid_access_token = AsyncMock(return_value="tok-1")
    svc.handle_unauthorized = AsyncMock(return_value="tok-2")
    return svc


@pytest.fixture
def http_client() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def service(token_service: AsyncMock, http_client: AsyncMock) -> MLAdsService:
    return MLAdsService(token_service=token_service, http_client=http_client)


ADVERTISERS_BODY = {
    "advertisers": [{"advertiser_id": 45944, "site_id": "MLB", "advertiser_name": "OFFSHOP_"}]
}


@pytest.mark.asyncio
async def test_advertiser_discovery_uses_v1_and_caches(
    service: MLAdsService, http_client: AsyncMock
) -> None:
    http_client.get = AsyncMock(return_value=_response(200, ADVERTISERS_BODY))

    assert await service.get_advertiser_id() == 45944
    assert await service.get_advertiser_id() == 45944  # cached — no second call

    assert http_client.get.await_count == 1
    call = http_client.get.await_args
    assert call.args[0] == MLAdsService.ADVERTISERS_URL
    assert call.kwargs["params"] == {"product_id": "PADS"}
    assert call.kwargs["headers"]["Api-Version"] == "1"


@pytest.mark.asyncio
async def test_advertiser_discovery_empty_raises(
    service: MLAdsService, http_client: AsyncMock
) -> None:
    http_client.get = AsyncMock(return_value=_response(200, {"advertisers": []}))
    with pytest.raises(MLAdsUpstreamError):
        await service.get_advertiser_id()


@pytest.mark.asyncio
async def test_list_campaigns_builds_search_url_and_wraps(
    service: MLAdsService, http_client: AsyncMock
) -> None:
    campaigns_body = {
        "paging": {"total": 17, "offset": 0, "limit": 2},
        "results": [{"id": 357599140, "name": "CRESC 3", "metrics": {"cost": 1857.85}}],
    }
    http_client.get = AsyncMock(
        side_effect=[_response(200, ADVERTISERS_BODY), _response(200, campaigns_body)]
    )

    out = await service.list_campaigns(date(2026, 7, 1), date(2026, 7, 28), limit=2)

    url = http_client.get.await_args.args[0]
    assert url == (
        "https://api.mercadolibre.com/marketplace/advertising/MLB"
        "/advertisers/45944/product_ads/campaigns/search"
    )
    params = http_client.get.await_args.kwargs["params"]
    assert params["date_from"] == "2026-07-01"
    assert params["date_to"] == "2026-07-28"
    assert params["metrics"] == DEFAULT_METRICS
    assert http_client.get.await_args.kwargs["headers"]["Api-Version"] == "2"

    assert out["source"] == "mercado-ads"
    assert out["date_from"] == "2026-07-01"
    assert "collected_at" in out
    assert out["campaigns"] == campaigns_body["results"]
    assert out["paging"]["total"] == 17


@pytest.mark.asyncio
async def test_campaign_daily_metrics_hits_campaign_detail_daily(
    service: MLAdsService, http_client: AsyncMock
) -> None:
    body = {"results": [{"date": "2026-07-01", "cost": 74.81}]}
    http_client.get = AsyncMock(return_value=_response(200, body))

    out = await service.campaign_daily_metrics(357599140, date(2026, 7, 1), date(2026, 7, 28))

    url = http_client.get.await_args.args[0]
    assert url.endswith("/marketplace/advertising/MLB/product_ads/campaigns/357599140")
    params = http_client.get.await_args.kwargs["params"]
    assert params["aggregation_type"] == "DAILY"
    assert "impression_share" in params["metrics"]
    assert out["campaign_id"] == 357599140
    assert out["days"] == body["results"]


@pytest.mark.asyncio
async def test_items_metrics_passes_filters(service: MLAdsService, http_client: AsyncMock) -> None:
    body = {"paging": {"total": 1}, "results": [{"id": 779372681}]}
    http_client.get = AsyncMock(
        side_effect=[_response(200, ADVERTISERS_BODY), _response(200, body)]
    )

    out = await service.items_metrics(
        date(2026, 7, 1),
        date(2026, 7, 28),
        campaign_id=357583820,
        item_ids=["MLB3709682777", "MLB123"],
    )

    url = http_client.get.await_args.args[0]
    assert url.endswith("/advertisers/45944/product_ads/ad_groups/search")
    params = http_client.get.await_args.kwargs["params"]
    assert params["filters[campaign_id]"] == 357583820
    assert params["filters[item_ids]"] == "MLB3709682777,MLB123"
    assert out["ads"] == body["results"]


@pytest.mark.asyncio
async def test_daily_summary_aggregates_account_wide(
    service: MLAdsService, http_client: AsyncMock
) -> None:
    body = {"results": [{"date": "2026-07-25", "cost": 123.6, "acos": 8.4}]}
    http_client.get = AsyncMock(
        side_effect=[_response(200, ADVERTISERS_BODY), _response(200, body)]
    )

    out = await service.daily_summary(date(2026, 7, 25), date(2026, 7, 28))

    url = http_client.get.await_args.args[0]
    assert url.endswith("/advertisers/45944/product_ads/campaigns/search")
    params = http_client.get.await_args.kwargs["params"]
    assert params["aggregation_type"] == "DAILY"
    assert "limit" not in params  # account series, not a paged campaign list
    assert out["days"] == body["results"]


@pytest.mark.asyncio
async def test_401_triggers_refresh_and_single_retry(
    service: MLAdsService, token_service: AsyncMock, http_client: AsyncMock
) -> None:
    ok = _response(200, {"results": []})
    http_client.get = AsyncMock(side_effect=[_response(401), ok])

    out = await service.campaign_daily_metrics(1, date(2026, 7, 1), date(2026, 7, 2))

    token_service.handle_unauthorized.assert_awaited_once()
    assert http_client.get.await_count == 2
    # retry must carry the refreshed token
    retry_headers = http_client.get.await_args.kwargs["headers"]
    assert retry_headers["Authorization"] == "Bearer tok-2"
    assert out["days"] == []


@pytest.mark.asyncio
async def test_upstream_4xx_raises_with_status(
    service: MLAdsService, http_client: AsyncMock
) -> None:
    http_client.get = AsyncMock(return_value=_response(500, {"error": "internal_error"}))
    with pytest.raises(MLAdsUpstreamError) as exc_info:
        await service.campaign_daily_metrics(1, date(2026, 7, 1), date(2026, 7, 2))
    assert exc_info.value.status_code == 500


def test_validate_window_rules() -> None:
    today = date.today()
    validate_window(today - timedelta(days=30), today)  # fine

    with pytest.raises(ValueError, match=">= 'from'"):
        validate_window(today, today - timedelta(days=1))

    with pytest.raises(ValueError, match="window larger"):
        validate_window(today - timedelta(days=120), today)

    with pytest.raises(ValueError, match="older than"):
        validate_window(today - timedelta(days=91), today - timedelta(days=89))
