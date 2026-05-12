"""Async HTTP client for the Mercado Livre REST API.

Mirrors the structure of :mod:`tiny_client` — same retry pipeline, same
unified transient-error budget, same 401 → refresh-once guard. The only
structural difference is that there is no shared :class:`RateLimiter` (the
ML API is much more permissive than Tiny's 60 req/min budget).
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import TYPE_CHECKING, Any

import httpx
import structlog

from tiny_mirror.exceptions import (
    RateLimitException,
    TinyAPIException,
    TokenExpiredException,
)

if TYPE_CHECKING:
    from tiny_mirror.services.mercadolivre_token_service import MercadoLivreTokenService

logger = structlog.get_logger(__name__)


class MercadoLivreAPIClient:
    BASE_URL = "https://api.mercadolibre.com"

    MAX_RETRIES = 5
    BASE_DELAY_SECONDS = 1.0
    MAX_DELAY_SECONDS = 60.0
    MAX_JITTER_SECONDS = 0.5
    REQUEST_TIMEOUT_SECONDS = 30
    TRANSIENT_STATUS_CODES = frozenset({408, 425, 500, 502, 503, 504})

    def __init__(
        self,
        token_service: MercadoLivreTokenService,
        http_client: httpx.AsyncClient,
        ml_user_id: str,
    ) -> None:
        self._tokens = token_service
        self._http = http_client
        self._user_id = ml_user_id

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------
    async def list_items_by_sku(self, sku: str) -> list[str]:
        """Return the MLB IDs for all listings with the given seller SKU.

        The ML API returns ``{ results: [MLB_ID, …], paging: { total } }``.
        Returns an empty list if the SKU has no ML listings.
        """
        data = await self._request(
            "GET",
            f"/users/{self._user_id}/items/search",
            params={"seller_sku": sku},
        )
        results: list[str] = data.get("results") or []
        return results

    async def get_item(self, mlb_id: str) -> dict[str, Any]:
        """Return the full item detail for a given MLB ID.

        Relevant fields: ``available_quantity``, ``shipping.logistic_type``,
        ``inventory_id``, ``status``.
        """
        return await self._request("GET", f"/items/{mlb_id}")

    async def list_active_item_ids(
        self, *, offset: int = 0, limit: int = 100
    ) -> tuple[list[str], int]:
        """Return one page of active item IDs and the total count.

        Calls ``GET /users/{user_id}/items/search?status=active``.
        Returns ``(mlb_ids, total)``.
        """
        data = await self._request(
            "GET",
            f"/users/{self._user_id}/items/search",
            params={"status": "active", "limit": limit, "offset": offset},
        )
        results: list[str] = data.get("results") or []
        total: int = int((data.get("paging") or {}).get("total", 0))
        return results, total

    async def batch_get_items(self, mlb_ids: list[str]) -> list[dict[str, Any]]:
        """Fetch full item details for up to 20 MLB IDs in one request.

        ``GET /items?ids=MLB1,MLB2,...`` returns a JSON array where each
        element is ``{"code": 200, "body": {...}}``. Only items with a
        successful code and a non-null ``id`` in the body are returned.
        """
        if not mlb_ids:
            return []

        # The batch endpoint returns a list, not a dict; use the raw HTTP
        # pipeline via _request which returns response.json() as-is.
        raw: Any = await self._request(
            "GET",
            "/items",
            params={"ids": ",".join(mlb_ids[:20])},
        )
        if not isinstance(raw, list):
            return []
        items: list[dict[str, Any]] = []
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            body: dict[str, Any] = entry.get("body") or entry
            if body.get("id"):
                items.append(body)
        return items

    async def get_inventory_stock(self, inventory_id: str) -> dict[str, Any]:
        """Return fulfillment stock for the given inventory_id.

        Calls ``GET /inventories/{inventory_id}/stock/fulfillment``.
        Relevant field: ``available_quantity`` (units available to sell in FL).
        Returns an empty dict on 404 (inventory exists but has no FL stock).
        """
        try:
            return await self._request(
                "GET",
                f"/inventories/{inventory_id}/stock/fulfillment",
            )
        except TinyAPIException as exc:
            if exc.status_code == 404:
                return {}
            raise

    # ------------------------------------------------------------------
    # Internal: request pipeline
    # ------------------------------------------------------------------
    async def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        url = f"{self.BASE_URL}{path}"
        attempt = 0
        token_already_refreshed = False
        caller_headers = dict(kwargs.pop("headers", {}) or {})

        while True:
            access_token = await self._tokens.get_valid_access_token()
            headers = dict(caller_headers)
            headers["Authorization"] = f"Bearer {access_token}"

            logger.debug(
                "ML API request starting",
                method=method,
                path=path,
                attempt=attempt + 1,
                max_attempts=self.MAX_RETRIES + 1,
            )
            start = time.perf_counter()

            try:
                response = await self._http.request(
                    method,
                    url,
                    headers=headers,
                    timeout=self.REQUEST_TIMEOUT_SECONDS,
                    **kwargs,
                )
            except httpx.TimeoutException as exc:
                if attempt < self.MAX_RETRIES:
                    attempt += 1
                    wait_time = self._backoff_seconds(attempt)
                    logger.warning(
                        "ML API request timed out, retrying",
                        method=method,
                        path=path,
                        attempt=attempt,
                        wait_seconds=round(wait_time, 2),
                    )
                    await asyncio.sleep(wait_time)
                    continue
                logger.error(
                    "ML API request timed out after retries",
                    method=method,
                    path=path,
                    attempts=attempt + 1,
                )
                raise TinyAPIException("ML API request timed out") from exc
            except (httpx.ConnectError, httpx.NetworkError) as exc:
                if attempt < self.MAX_RETRIES:
                    attempt += 1
                    wait_time = self._backoff_seconds(attempt)
                    logger.warning(
                        "ML API network error, retrying",
                        method=method,
                        path=path,
                        attempt=attempt,
                        wait_seconds=round(wait_time, 2),
                        error=str(exc),
                    )
                    await asyncio.sleep(wait_time)
                    continue
                logger.error(
                    "ML API network error after retries",
                    method=method,
                    path=path,
                    attempts=attempt + 1,
                    error=str(exc),
                )
                raise TinyAPIException(f"ML API network error: {exc}") from exc

            duration_ms = round((time.perf_counter() - start) * 1000, 2)
            logger.debug(
                "ML API request completed",
                method=method,
                path=path,
                status_code=response.status_code,
                duration_ms=duration_ms,
                attempt=attempt + 1,
            )

            status = response.status_code

            if 200 <= status < 300:
                try:
                    return response.json()  # type: ignore[no-any-return]
                except ValueError as exc:
                    raise TinyAPIException(
                        "Invalid JSON from ML API",
                        status_code=status,
                        response_body=response.text[:500],
                    ) from exc

            if status == 401:
                if token_already_refreshed:
                    raise TokenExpiredException(
                        "ML authentication failed after token refresh",
                        status_code=status,
                        response_body=response.text[:500],
                    )
                logger.warning(
                    "ML API 401, refreshing token and retrying",
                    method=method,
                    path=path,
                )
                await self._tokens.handle_unauthorized()
                token_already_refreshed = True
                continue

            if status == 404:
                raise TinyAPIException(
                    f"ML resource not found: {path}",
                    status_code=status,
                    response_body=response.text[:500],
                )

            body_preview = response.text[:500]

            if status == 429:
                if attempt < self.MAX_RETRIES:
                    attempt += 1
                    wait_time = self._backoff_seconds(attempt)
                    logger.warning(
                        "ML API rate limit hit, retrying",
                        method=method,
                        path=path,
                        attempt=attempt,
                        wait_seconds=round(wait_time, 2),
                    )
                    await asyncio.sleep(wait_time)
                    continue
                raise RateLimitException(
                    f"ML API rate limit exceeded after {self.MAX_RETRIES} retries",
                    status_code=status,
                    response_body=body_preview,
                )

            if status in self.TRANSIENT_STATUS_CODES:
                if attempt < self.MAX_RETRIES:
                    attempt += 1
                    wait_time = self._backoff_seconds(attempt)
                    logger.warning(
                        "ML API transient error, retrying",
                        method=method,
                        path=path,
                        status_code=status,
                        attempt=attempt,
                        wait_seconds=round(wait_time, 2),
                    )
                    await asyncio.sleep(wait_time)
                    continue
                raise TinyAPIException(
                    f"ML API error after {self.MAX_RETRIES} retries: {status}",
                    status_code=status,
                    response_body=body_preview,
                )

            logger.error(
                "ML API client/server error",
                method=method,
                path=path,
                status_code=status,
                response_body_preview=body_preview,
            )
            raise TinyAPIException(
                f"ML API error: {status}",
                status_code=status,
                response_body=body_preview,
            )

    def _backoff_seconds(self, attempt: int) -> float:
        base: float = min(
            self.BASE_DELAY_SECONDS * (2 ** (attempt - 1)),
            self.MAX_DELAY_SECONDS,
        )
        return base + random.uniform(0, self.MAX_JITTER_SECONDS)
