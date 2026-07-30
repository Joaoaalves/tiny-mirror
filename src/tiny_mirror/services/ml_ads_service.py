"""Read-only bridge to the Mercado Ads (Product Ads) API.

Serves the ``/ads`` routes: campaigns, per-campaign daily series,
per-ad (ad group) metrics and the account-wide daily summary. Strictly
GET — this service never mutates anything on Mercado Livre.

Upstream is the post-May-2026 "marketplace" generation of the API
(``/marketplace/advertising/{site}/...``, header ``Api-Version: 2``);
the legacy ``/advertising/{site}/product_ads`` paths are dead. The one
exception is advertiser discovery, which still lives on
``/advertising/advertisers`` with ``Api-Version: 1``.

Ads metrics mature retroactively (attribution window), so every payload
is stamped with ``collected_at`` — consumers must compare snapshots of
the same maturity, not trust a day-old number as final. ML serves at
most 90 days back and refreshes figures at 10:00 GMT-3.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import structlog

from tiny_mirror.exceptions import TinyMirrorException
from tiny_mirror.services.mercadolivre_token_service import MercadoLivreTokenService

logger = structlog.get_logger(__name__)


class MLAdsUpstreamError(TinyMirrorException):
    """Raised when the Mercado Ads API errors or is unreachable."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


# ML caps metrics lookback at 90 days.
MAX_WINDOW_DAYS = 90

# Everything the panel shows, minus the campaign-detail-only extras
# (impression_share & friends), which upstream rejects on list routes.
DEFAULT_METRICS = (
    "clicks,prints,ctr,cost,cpc,acos,"
    "direct_units_quantity,indirect_units_quantity,units_quantity,"
    "direct_amount,indirect_amount,total_amount"
)
CAMPAIGN_DETAIL_METRICS = DEFAULT_METRICS + (
    ",impression_share,top_impression_share,"
    "lost_impression_share_by_budget,lost_impression_share_by_ad_rank,acos_benchmark"
)


class MLAdsService:
    """Thin authenticated proxy over the Product Ads read endpoints."""

    ADVERTISERS_URL = "https://api.mercadolibre.com/advertising/advertisers"
    BASE_URL = "https://api.mercadolibre.com/marketplace/advertising"
    SITE_ID = "MLB"
    PRODUCT_ID = "PADS"

    def __init__(
        self,
        token_service: MercadoLivreTokenService,
        http_client: httpx.AsyncClient,
    ) -> None:
        self._token_service = token_service
        self._http = http_client
        self._advertiser_id: int | None = None

    # ------------------------------------------------------------------
    # Upstream plumbing
    # ------------------------------------------------------------------
    async def _get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        *,
        api_version: str = "2",
        op: str,
    ) -> Any:
        token = await self._token_service.get_valid_access_token()

        async def _send(tok: str) -> httpx.Response:
            return await self._http.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {tok}", "Api-Version": api_version},
                timeout=30.0,
            )

        try:
            resp = await _send(token)
            if resp.status_code == 401:
                token = await self._token_service.handle_unauthorized()
                resp = await _send(token)
        except httpx.RequestError as exc:
            raise MLAdsUpstreamError(f"ML Ads request failed ({op}): {exc}") from exc

        if resp.status_code >= 400:
            logger.warning(
                "ml_ads.upstream_error",
                op=op,
                status=resp.status_code,
                body=resp.text[:500],
            )
            raise MLAdsUpstreamError(
                f"ML Ads {op} returned {resp.status_code}: {resp.text[:200]}",
                status_code=resp.status_code,
            )
        return resp.json()

    async def get_advertiser_id(self) -> int:
        """Discover (once per process) the PADS advertiser of this account."""
        if self._advertiser_id is not None:
            return self._advertiser_id
        data = await self._get(
            self.ADVERTISERS_URL,
            {"product_id": self.PRODUCT_ID},
            api_version="1",
            op="advertisers",
        )
        advertisers = data.get("advertisers") or []
        if not advertisers:
            raise MLAdsUpstreamError("No Product Ads advertiser on this ML account")
        self._advertiser_id = int(advertisers[0]["advertiser_id"])
        logger.info("ml_ads.advertiser_discovered", advertiser_id=self._advertiser_id)
        return self._advertiser_id

    @staticmethod
    def _wrap(payload: dict[str, Any], date_from: date, date_to: date) -> dict[str, Any]:
        """Stamp provenance — ads numbers mutate retroactively upstream."""
        return {
            "source": "mercado-ads",
            "collected_at": datetime.now(UTC).isoformat(),
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            **payload,
        }

    # ------------------------------------------------------------------
    # Public reads (one per /ads route)
    # ------------------------------------------------------------------
    async def list_campaigns(
        self,
        date_from: date,
        date_to: date,
        *,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        advertiser_id = await self.get_advertiser_id()
        data = await self._get(
            f"{self.BASE_URL}/{self.SITE_ID}/advertisers/{advertiser_id}"
            "/product_ads/campaigns/search",
            {
                "limit": limit,
                "offset": offset,
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "metrics": DEFAULT_METRICS,
            },
            op="campaigns_search",
        )
        return self._wrap(
            {"paging": data.get("paging"), "campaigns": data.get("results", [])},
            date_from,
            date_to,
        )

    async def campaign_daily_metrics(
        self,
        campaign_id: int,
        date_from: date,
        date_to: date,
    ) -> dict[str, Any]:
        data = await self._get(
            f"{self.BASE_URL}/{self.SITE_ID}/product_ads/campaigns/{campaign_id}",
            {
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "metrics": CAMPAIGN_DETAIL_METRICS,
                "aggregation_type": "DAILY",
            },
            op="campaign_daily",
        )
        return self._wrap(
            {"campaign_id": campaign_id, "days": data.get("results", [])},
            date_from,
            date_to,
        )

    async def items_metrics(
        self,
        date_from: date,
        date_to: date,
        *,
        campaign_id: int | None = None,
        item_ids: list[str] | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Per-ad metrics via ad_groups (the per-item unit of the new API).

        One ad group ≈ one listing family: traditional listings carry the
        MLB in ``ad_group_external_id``; catalog listings carry the catalog
        product id there (title/thumbnail identify the product either way).
        """
        advertiser_id = await self.get_advertiser_id()
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "metrics": DEFAULT_METRICS,
        }
        if campaign_id is not None:
            params["filters[campaign_id]"] = campaign_id
        if item_ids:
            params["filters[item_ids]"] = ",".join(item_ids)
        data = await self._get(
            f"{self.BASE_URL}/{self.SITE_ID}/advertisers/{advertiser_id}"
            "/product_ads/ad_groups/search",
            params,
            op="ad_groups_search",
        )
        return self._wrap(
            {"paging": data.get("paging"), "ads": data.get("results", [])},
            date_from,
            date_to,
        )

    async def daily_summary(self, date_from: date, date_to: date) -> dict[str, Any]:
        """Account-wide series, one row per day (all campaigns combined)."""
        advertiser_id = await self.get_advertiser_id()
        data = await self._get(
            f"{self.BASE_URL}/{self.SITE_ID}/advertisers/{advertiser_id}"
            "/product_ads/campaigns/search",
            {
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "metrics": DEFAULT_METRICS,
                "aggregation_type": "DAILY",
            },
            op="daily_summary",
        )
        return self._wrap({"days": data.get("results", [])}, date_from, date_to)


def validate_window(date_from: date, date_to: date) -> None:
    """Shared range guard for the /ads routes (raises ``ValueError``)."""
    if date_to < date_from:
        raise ValueError("'to' must be >= 'from'")
    if (date_to - date_from).days > MAX_WINDOW_DAYS:
        raise ValueError(f"window larger than {MAX_WINDOW_DAYS} days (ML limit)")
    if date_from < date.today() - timedelta(days=MAX_WINDOW_DAYS):
        raise ValueError(f"'from' older than {MAX_WINDOW_DAYS} days back (ML limit)")
