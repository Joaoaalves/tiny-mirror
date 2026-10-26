"""Minimal read-only TikTok Shop Open API (202309) client, one shop.

Credentials come from :class:`MarketplaceCredentials` (``tiktok_shop``,
exported from OpenClaw). OpenClaw OWNS the token and renews it hourly (:14);
this client NEVER refreshes and never sees the refresh token — an expired
token raises :class:`TikTokTokenExpired` and the run is skipped until the
next export.

The OAuth grant carries write scopes (products, promotions, fulfilment), so
only reads can be called: the search endpoints in ``READ_PATHS`` (POSTs that
read) and the product detail GET (``get_product``). Signature: HMAC-SHA256(app_secret, app_secret + path + sorted
query key+value pairs (without ``sign``/``access_token``) + body + app_secret).
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import random
import re
import time
from typing import Any

import httpx
import structlog

from tiny_mirror.infrastructure.external.marketplace_credentials import MarketplaceCredentials

logger = structlog.get_logger(__name__)

BASE = "https://open-api.tiktokglobalshop.com"
ORDERS_SEARCH = "/order/202309/orders/search"
PRODUCTS_SEARCH = "/product/202309/products/search"
READ_PATHS = frozenset({ORDERS_SEARCH, PRODUCTS_SEARCH})
PRODUCT_DETAIL = "/product/202309/products/{product_id}"
_PRODUCT_ID = re.compile(r"\d{1,30}")
_MAX_ATTEMPTS = 4
_MAX_BACKOFF_SECONDS = 30.0
_TOKEN_MARGIN_SECONDS = 60


class TikTokNotConfigured(RuntimeError):
    """No ``tiktok_shop`` credentials exported for tiny-mirror."""


class TikTokTokenExpired(RuntimeError):
    """The exported token expired; OpenClaw renews it, we never do."""


class TikTokAPIError(RuntimeError):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


def signature(secret: str, path: str, query: dict[str, Any], body: str) -> str:
    pairs = "".join(f"{k}{query[k]}" for k in sorted(query) if k not in ("sign", "access_token"))
    base = f"{secret}{path}{pairs}{body}{secret}"
    return hmac.new(secret.encode(), base.encode(), hashlib.sha256).hexdigest()


class TikTokShopClient:
    def __init__(self, http_client: httpx.AsyncClient, credentials: MarketplaceCredentials) -> None:
        self._http = http_client
        self._creds = credentials

    def credentials(self) -> dict[str, Any]:
        section = self._creds.section("tiktok_shop")
        if not section:
            raise TikTokNotConfigured("tiktok_shop credentials not exported")
        if int(section.get("expires_at") or 0) <= time.time() + _TOKEN_MARGIN_SECONDS:
            raise TikTokTokenExpired("exported TikTok token expired; waiting for OpenClaw renewal")
        return section

    async def search(
        self, path: str, params: dict[str, Any], body: dict[str, Any]
    ) -> dict[str, Any]:
        if path not in READ_PATHS:
            raise ValueError(f"{path} is not a read endpoint")
        return await self._call("POST", path, params, json.dumps(body, separators=(",", ":")))

    async def get_product(self, product_id: str) -> dict[str, Any]:
        """Product detail (images, variation names, attributes, package...). GET only."""
        if not _PRODUCT_ID.fullmatch(product_id):
            raise ValueError(f"invalid product id {product_id!r}")
        return await self._call("GET", PRODUCT_DETAIL.format(product_id=product_id), {}, "")

    async def _call(
        self, method: str, path: str, params: dict[str, Any], payload: str
    ) -> dict[str, Any]:
        for attempt in range(_MAX_ATTEMPTS):
            c = self.credentials()
            query = {
                **{k: str(v) for k, v in params.items()},
                "app_key": c["app_key"],
                "timestamp": str(int(time.time())),
                "shop_cipher": c["shop_cipher"],
            }
            query["sign"] = signature(c["app_secret"], path, query, payload)
            r = await self._http.request(
                method,
                f"{BASE}{path}",
                params=query,
                content=payload or None,
                headers={
                    "x-tts-access-token": c["access_token"],
                    "content-type": "application/json",
                },
            )
            if r.status_code == 429 and attempt + 1 < _MAX_ATTEMPTS:
                await asyncio.sleep(
                    min(2.0**attempt + random.uniform(0, 0.5), _MAX_BACKOFF_SECONDS)
                )
                continue
            body_json: dict[str, Any] = {}
            try:
                body_json = r.json()
            except ValueError:
                pass
            code = body_json.get("code")
            if code is not None and code != 0:
                raise TikTokAPIError(int(code), str(body_json.get("message") or ""))
            r.raise_for_status()
            return dict(body_json.get("data") or {})
        raise TikTokAPIError(429, "gave up after retries")
