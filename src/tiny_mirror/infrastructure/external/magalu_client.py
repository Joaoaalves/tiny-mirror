"""Minimal read-only Magalu Open API (seller) client, one store.

Credentials come from :class:`MarketplaceCredentials` (``magalu``, exported
from OpenClaw). OpenClaw OWNS the OAuth pair: the ID Magalu refresh token
rotates on every renewal, so only its cron (:15/:45) may refresh. This client
NEVER refreshes and never sees the refresh token — an expired (or rejected)
token raises :class:`MagaluTokenExpired` and the run is skipped until the next
export.

The app was granted read scopes only, and the client enforces it anyway: GET
on the paths in ``_READ_PATHS`` and nothing else. Money comes in cents with a
``normalizer`` (100); pages are ``_offset``/``_limit``. The rate limit is per
seller and per module (orders 850/min, SKUs 550/min, prices/stocks 850/min);
calls are spaced by ``_MIN_INTERVAL`` and a 429/5xx is retried with backoff.
"""

from __future__ import annotations

import asyncio
import random
import re
import time
from typing import Any

import httpx
import structlog

from tiny_mirror.infrastructure.external.marketplace_credentials import MarketplaceCredentials

logger = structlog.get_logger(__name__)

BASE = "https://api.magalu.com"
ORDERS = "/seller/v1/orders"
SKUS = "/seller/v1/portfolios/skus"
PRICES = "/seller/v1/portfolios/prices/{sku}"
STOCKS = "/seller/v1/portfolios/stocks/{sku}"
_READ_PATHS = re.compile(
    r"/seller/v1/(orders|portfolios/skus|portfolios/(prices|stocks)/[A-Za-z0-9_\-.%]{1,96})"
)
_SKU = re.compile(r"[A-Za-z0-9_\-./]{1,32}")
_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
_MAX_ATTEMPTS = 5
_MAX_BACKOFF_SECONDS = 30.0
_MIN_INTERVAL = 0.15  # ~400 calls/min, below every module's per-minute limit
_TOKEN_MARGIN_SECONDS = 60
# Integrators report the API refuses offsets past 5000; callers split windows.
MAX_OFFSET = 5000


class MagaluNotConfigured(RuntimeError):
    """No ``magalu`` credentials exported for tiny-mirror."""


class MagaluTokenExpired(RuntimeError):
    """The exported token expired or was refused; OpenClaw renews it, we never do."""


class MagaluAPIError(RuntimeError):
    def __init__(self, status: int, slug: str, message: str) -> None:
        super().__init__(f"{status} {slug}: {message}")
        self.status = status
        self.slug = slug


class MagaluClient:
    def __init__(self, http_client: httpx.AsyncClient, credentials: MarketplaceCredentials) -> None:
        self._http = http_client
        self._creds = credentials
        self._pace = asyncio.Lock()
        self._last_call = 0.0

    def credentials(self) -> dict[str, Any]:
        section = self._creds.section("magalu")
        if not section or not section.get("access_token"):
            raise MagaluNotConfigured("magalu credentials not exported")
        if int(section.get("expires_at") or 0) <= time.time() + _TOKEN_MARGIN_SECONDS:
            raise MagaluTokenExpired("exported Magalu token expired; waiting for OpenClaw renewal")
        return section

    async def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if not _READ_PATHS.fullmatch(path):
            raise ValueError(f"{path} is not a read endpoint")
        for attempt in range(_MAX_ATTEMPTS):
            token = self.credentials()["access_token"]
            await self._wait_turn()
            r = await self._http.get(
                f"{BASE}{path}",
                params={k: str(v) for k, v in (params or {}).items() if v is not None},
                headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            )
            if r.status_code in _RETRY_STATUS and attempt + 1 < _MAX_ATTEMPTS:
                await asyncio.sleep(_backoff(attempt, r.headers.get("Retry-After")))
                continue
            body: Any = None
            try:
                body = r.json()
            except ValueError:
                pass
            if r.status_code == 401:
                raise MagaluTokenExpired("Magalu refused the exported token (401)")
            if r.status_code >= 400:
                err = body if isinstance(body, dict) else {}
                raise MagaluAPIError(
                    r.status_code, str(err.get("slug") or ""), str(err.get("message") or "")[:200]
                )
            if not isinstance(body, dict):
                raise MagaluAPIError(r.status_code, "NON_JSON", "response is not a JSON object")
            return body
        raise MagaluAPIError(429, "TOO_MANY_REQUESTS", "gave up after retries")

    async def pages(self, path: str, params: dict[str, Any], limit: int) -> list[dict[str, Any]]:
        """Every page of a list endpoint (``_offset``/``_limit``). Raises instead
        of silently truncating when the window would pass ``MAX_OFFSET``."""
        out: list[dict[str, Any]] = []
        offset = 0
        while True:
            if offset > MAX_OFFSET:
                raise MagaluAPIError(422, "OFFSET_CEILING", f"{path} window exceeds {MAX_OFFSET}")
            page = await self.get(path, {**params, "_limit": limit, "_offset": offset})
            got = [x for x in page.get("results") or [] if isinstance(x, dict)]
            out.extend(got)
            offset += len(got)
            links = (page.get("meta") or {}).get("links")
            if len(got) < limit or (isinstance(links, dict) and not links.get("next")):
                return out

    async def get_price(self, sku: str) -> dict[str, Any]:
        return await self.get(PRICES.format(sku=_sku_path(sku)))

    async def get_stock(self, sku: str) -> dict[str, Any]:
        return await self.get(STOCKS.format(sku=_sku_path(sku)))

    async def _wait_turn(self) -> None:
        async with self._pace:
            delay = self._last_call + _MIN_INTERVAL - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            self._last_call = time.monotonic()


def _sku_path(sku: str) -> str:
    if not _SKU.fullmatch(sku):
        raise ValueError(f"invalid sku {sku!r}")
    return sku.replace("/", "%2F")


def _backoff(attempt: int, retry_after: str | None) -> float:
    try:
        wait = float(retry_after) if retry_after else 0.0
    except ValueError:
        wait = 0.0
    return min(max(wait, 2.0**attempt + random.uniform(0, 0.5)), _MAX_BACKOFF_SECONDS)
