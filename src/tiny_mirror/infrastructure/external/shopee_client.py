"""Minimal read-only Shopee Open Platform v2 client (seller app, one shop).

Credentials come from :class:`MarketplaceCredentials` (``shopee_seller``,
exported from OpenClaw). OpenClaw OWNS the shop token and refreshes it
hourly; Shopee refresh tokens rotate on use, so this client NEVER refreshes —
an expired token just raises :class:`ShopeeTokenExpired` and the run is
skipped until the next export (every 30 min, right after OpenClaw's refresh).
Only GET is exposed. Signature: HMAC-SHA256(partner_key,
partner_id + path + timestamp + access_token + shop_id).
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import random
import time
from typing import Any

import httpx
import structlog

from tiny_mirror.infrastructure.external.marketplace_credentials import MarketplaceCredentials

logger = structlog.get_logger(__name__)

BASE = "https://partner.shopeemobile.com"
_MAX_ATTEMPTS = 4
_MAX_BACKOFF_SECONDS = 30.0
_TOKEN_MARGIN_SECONDS = 60


class ShopeeNotConfigured(RuntimeError):
    """No ``shopee_seller`` credentials exported for tiny-mirror."""


class ShopeeTokenExpired(RuntimeError):
    """The exported shop token expired; OpenClaw refreshes it, we never do."""


class ShopeeAPIError(RuntimeError):
    def __init__(self, error: str, message: str) -> None:
        super().__init__(f"{error}: {message}")
        self.error = error


def signature(
    partner_key: str, partner_id: int, path: str, ts: int, token: str, shop_id: int
) -> str:
    base = f"{partner_id}{path}{ts}{token}{shop_id}"
    return hmac.new(partner_key.encode(), base.encode(), hashlib.sha256).hexdigest()


class ShopeeClient:
    def __init__(self, http_client: httpx.AsyncClient, credentials: MarketplaceCredentials) -> None:
        self._http = http_client
        self._creds = credentials

    def credentials(self) -> dict[str, Any]:
        section = self._creds.section("shopee_seller")
        if not section:
            raise ShopeeNotConfigured("shopee_seller credentials not exported")
        if int(section.get("expires_at") or 0) <= time.time() + _TOKEN_MARGIN_SECONDS:
            raise ShopeeTokenExpired("exported Shopee token expired; waiting for OpenClaw refresh")
        return section

    @property
    def shop_id(self) -> int:
        return int(self.credentials()["shop_id"])

    async def get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        for attempt in range(_MAX_ATTEMPTS):
            c = self.credentials()
            ts = int(time.time())
            query = {
                **params,
                "partner_id": c["partner_id"],
                "timestamp": ts,
                "access_token": c["access_token"],
                "shop_id": c["shop_id"],
                "sign": signature(
                    c["partner_key"], c["partner_id"], path, ts, c["access_token"], c["shop_id"]
                ),
            }
            r = await self._http.get(f"{BASE}{path}", params=query)
            body: dict[str, Any] = {}
            try:
                body = r.json()
            except ValueError:
                pass
            error = str(body.get("error") or "")
            rate_limited = r.status_code == 429 or error == "error_rate_limit"
            if rate_limited and attempt + 1 < _MAX_ATTEMPTS:
                await asyncio.sleep(
                    min(2.0**attempt + random.uniform(0, 0.5), _MAX_BACKOFF_SECONDS)
                )
                continue
            if error:
                raise ShopeeAPIError(error, str(body.get("message") or ""))
            r.raise_for_status()
            return dict(body.get("response") or {})
        raise ShopeeAPIError("error_rate_limit", "gave up after retries")
