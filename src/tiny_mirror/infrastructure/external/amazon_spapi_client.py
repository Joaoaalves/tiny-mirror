"""Minimal read-only Amazon SP-API client (Brazil marketplace, NA endpoint).

Credentials come from :class:`MarketplaceCredentials` (``amazon_spapi``
section, exported from OpenClaw). LWA refresh tokens do not rotate, so this
client mints its own short-lived access tokens without disturbing OpenClaw.
Only GET requests are exposed. 429s are retried with backoff (SP-API rate
limits are per operation and small).
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any

import httpx
import structlog

from tiny_mirror.infrastructure.external.marketplace_credentials import MarketplaceCredentials

logger = structlog.get_logger(__name__)

SPAPI = "https://sellingpartnerapi-na.amazon.com"
LWA = "https://api.amazon.com/auth/o2/token"
_MAX_ATTEMPTS = 5
_MAX_BACKOFF_SECONDS = 30.0


class AmazonNotConfigured(RuntimeError):
    """No ``amazon_spapi`` credentials exported for tiny-mirror."""


class AmazonSPAPIClient:
    def __init__(self, http_client: httpx.AsyncClient, credentials: MarketplaceCredentials) -> None:
        self._http = http_client
        self._creds = credentials
        self._token: str | None = None
        self._token_expires = 0.0
        self._lock = asyncio.Lock()

    def credentials(self) -> dict[str, Any]:
        section = self._creds.section("amazon_spapi")
        if not section:
            raise AmazonNotConfigured("amazon_spapi credentials not exported")
        return section

    async def _access_token(self, force: bool = False) -> str:
        async with self._lock:
            if not force and self._token and time.time() < self._token_expires:
                return self._token
            c = self.credentials()
            r = await self._http.post(
                LWA,
                data={
                    "grant_type": "refresh_token",
                    "client_id": c["client_id"],
                    "client_secret": c["client_secret"],
                    "refresh_token": c["refresh_token"],
                },
            )
            r.raise_for_status()
            body = r.json()
            self._token = str(body["access_token"])
            self._token_expires = time.time() + int(body.get("expires_in", 3600)) - 120
            return self._token

    async def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """GET ``path`` with retry on 429 and one token refresh on 401/403."""
        refreshed = False
        for attempt in range(_MAX_ATTEMPTS):
            token = await self._access_token()
            r = await self._http.get(
                f"{SPAPI}{path}",
                params=params,
                headers={"x-amz-access-token": token, "Accept": "application/json"},
            )
            if r.status_code in (401, 403) and not refreshed:
                refreshed = True
                await self._access_token(force=True)
                continue
            if r.status_code == 429 and attempt + 1 < _MAX_ATTEMPTS:
                await asyncio.sleep(_backoff(r, attempt))
                continue
            r.raise_for_status()
            return dict(r.json())
        r.raise_for_status()
        return dict(r.json())


def _backoff(response: httpx.Response, attempt: int) -> float:
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return min(float(retry_after), _MAX_BACKOFF_SECONDS)
        except ValueError:
            pass
    return min(2.0**attempt + random.uniform(0, 0.5), _MAX_BACKOFF_SECONDS)
