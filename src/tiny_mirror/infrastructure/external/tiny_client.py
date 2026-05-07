"""Async HTTP client for the Tiny ERP public REST API (v3).

Every other module that needs to talk to Tiny goes through this client. It
owns the cross-cutting concerns: bearer-token injection, 401 → refresh →
retry-once, 429 → exponential backoff with jitter, rate-limit accounting,
and uniform error mapping into the project exception hierarchy.
"""

from __future__ import annotations

import asyncio
import random
import time
from datetime import UTC, date, datetime
from typing import Any

import httpx
import structlog

from tiny_mirror.exceptions import (
    RateLimitException,
    TinyAPIException,
    TinyNotFoundException,
    TokenExpiredException,
)
from tiny_mirror.infrastructure.external.rate_limiter import RateLimiter
from tiny_mirror.services.token_service import TokenService

logger = structlog.get_logger(__name__)


class TinyAPIClient:
    BASE_URL = "https://api.tiny.com.br/public-api/v3"

    MAX_RETRIES = 5
    BASE_DELAY_SECONDS = 1.0
    MAX_DELAY_SECONDS = 60.0
    MAX_JITTER_SECONDS = 0.5
    REQUEST_TIMEOUT_SECONDS = 30
    # Tiny v3 is unreliable: it returns spurious 400s, 5xx and times out
    # under load. Treat these as transient and retry with exponential
    # backoff up to the same budget as 429. Genuine validation errors
    # will still fail on the last attempt and surface to the caller.
    TRANSIENT_STATUS_CODES = frozenset({400, 408, 425, 500, 502, 503, 504})

    def __init__(
        self,
        token_service: TokenService,
        rate_limiter: RateLimiter,
        http_client: httpx.AsyncClient,
    ) -> None:
        self._tokens = token_service
        self._rate_limiter = rate_limiter
        self._http = http_client

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------
    async def list_products(
        self,
        situation: str | None = None,
        updated_after: datetime | date | str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if situation is not None:
            params["situacao"] = situation
        if updated_after is not None:
            params["dataAlteracao"] = _format_date_only(updated_after)
        return await self._request("GET", "/produtos", params=params)

    async def get_product(self, product_id: int) -> dict[str, Any]:
        return await self._request("GET", f"/produtos/{product_id}")

    async def list_orders(
        self,
        date_initial: datetime | date | str | None = None,
        date_final: datetime | date | str | None = None,
        updated_after: datetime | date | str | None = None,
        situation: int | None = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "asc",
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": limit, "offset": offset, "orderBy": order_by}
        if date_initial is not None:
            params["dataInicial"] = _format_date_only(date_initial)
        if date_final is not None:
            params["dataFinal"] = _format_date_only(date_final)
        if updated_after is not None:
            # Tiny v3 accepts ONLY YYYY-MM-DD here (datetime variants -> 400);
            # see memory/project_tiny_dataAtualizacao.md.
            params["dataAtualizacao"] = _format_date_only(updated_after)
        if situation is not None:
            params["situacao"] = situation
        return await self._request("GET", "/pedidos", params=params)

    async def get_order(self, order_id: int) -> dict[str, Any]:
        return await self._request("GET", f"/pedidos/{order_id}")

    async def get_stock(self, product_id: int) -> dict[str, Any]:
        return await self._request("GET", f"/estoque/{product_id}")

    async def list_invoices(
        self,
        date_initial: datetime | date | str | None = None,
        date_final: datetime | date | str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if date_initial is not None:
            params["dataInicial"] = _format_date_only(date_initial)
        if date_final is not None:
            params["dataFinal"] = _format_date_only(date_final)
        return await self._request("GET", "/notas", params=params)

    # ------------------------------------------------------------------
    # Internal: request pipeline
    # ------------------------------------------------------------------
    async def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        """Run the full pipeline (rate limit → token → send → retry/backoff).

        Retries are unified: any transient outcome — connection error,
        timeout, 429, or any code in :attr:`TRANSIENT_STATUS_CODES` —
        consumes one attempt from the same ``MAX_RETRIES`` budget and
        backs off with exponential delay + jitter.
        """
        url = f"{self.BASE_URL}{path}"
        attempt = 0
        token_already_refreshed = False
        caller_headers = dict(kwargs.pop("headers", {}) or {})

        while True:
            await self._rate_limiter.wait_if_needed()
            access_token = await self._tokens.get_valid_access_token()
            headers = dict(caller_headers)
            headers["Authorization"] = f"Bearer {access_token}"

            logger.debug(
                "Tiny API request starting",
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
                        "Tiny API request timed out, retrying after backoff",
                        method=method,
                        path=path,
                        attempt=attempt,
                        max_retries=self.MAX_RETRIES,
                        wait_seconds=round(wait_time, 2),
                        timeout_seconds=self.REQUEST_TIMEOUT_SECONDS,
                    )
                    await asyncio.sleep(wait_time)
                    continue
                logger.error(
                    "Request to Tiny API timed out after retries",
                    method=method,
                    path=path,
                    attempts=attempt + 1,
                    timeout_seconds=self.REQUEST_TIMEOUT_SECONDS,
                )
                raise TinyAPIException("Request timed out") from exc
            except (httpx.ConnectError, httpx.NetworkError) as exc:
                if attempt < self.MAX_RETRIES:
                    attempt += 1
                    wait_time = self._backoff_seconds(attempt)
                    logger.warning(
                        "Network error to Tiny API, retrying after backoff",
                        method=method,
                        path=path,
                        attempt=attempt,
                        max_retries=self.MAX_RETRIES,
                        wait_seconds=round(wait_time, 2),
                        error_message=str(exc),
                    )
                    await asyncio.sleep(wait_time)
                    continue
                logger.error(
                    "Network error connecting to Tiny API after retries",
                    method=method,
                    path=path,
                    attempts=attempt + 1,
                    error_message=str(exc),
                )
                raise TinyAPIException(f"Network error: {exc}") from exc

            duration_ms = round((time.perf_counter() - start) * 1000, 2)
            await self._rate_limiter.update_from_headers(dict(response.headers))
            logger.debug(
                "Tiny API request completed",
                method=method,
                path=path,
                status_code=response.status_code,
                duration_ms=duration_ms,
                attempt=attempt + 1,
            )

            status = response.status_code

            # ------------------------------------------------------------------
            # 2xx — happy path
            # ------------------------------------------------------------------
            if 200 <= status < 300:
                try:
                    return response.json()  # type: ignore[no-any-return]
                except ValueError as exc:
                    raise TinyAPIException(
                        "Invalid JSON response from Tiny API",
                        status_code=status,
                        response_body=response.text[:500],
                    ) from exc

            # ------------------------------------------------------------------
            # 401 — refresh once, retry once (does not count against budget)
            # ------------------------------------------------------------------
            if status == 401:
                if token_already_refreshed:
                    raise TokenExpiredException(
                        "Authentication failed after token refresh",
                        status_code=status,
                        response_body=response.text[:500],
                    )
                logger.warning(
                    "Received 401, refreshing token and retrying",
                    method=method,
                    path=path,
                )
                await self._tokens.handle_unauthorized()
                token_already_refreshed = True
                continue

            # ------------------------------------------------------------------
            # 404 — not found
            # ------------------------------------------------------------------
            if status == 404:
                resource_type, resource_id = _parse_resource_from_path(path)
                logger.warning(
                    "Received 404 from Tiny API",
                    method=method,
                    path=path,
                    resource_id=resource_id,
                )
                raise TinyNotFoundException(
                    f"Resource not found: {path}",
                    resource_type=resource_type,
                    resource_id=resource_id,
                    status_code=status,
                    response_body=response.text[:500],
                )

            body_preview = response.text[:500]

            # ------------------------------------------------------------------
            # 429 — rate-limit hit
            # ------------------------------------------------------------------
            if status == 429:
                if attempt < self.MAX_RETRIES:
                    attempt += 1
                    wait_time = self._backoff_seconds(attempt)
                    logger.warning(
                        "Rate limit hit, retrying after backoff",
                        method=method,
                        path=path,
                        attempt=attempt,
                        max_retries=self.MAX_RETRIES,
                        wait_seconds=round(wait_time, 2),
                    )
                    await asyncio.sleep(wait_time)
                    continue
                reset_seconds = response.headers.get("X-RateLimit-Reset") or (
                    response.headers.get("x-ratelimit-reset")
                )
                retry_after: int | None
                try:
                    retry_after = int(reset_seconds) if reset_seconds else None
                except (TypeError, ValueError):
                    retry_after = None
                logger.error(
                    "Rate limit exceeded after retries",
                    method=method,
                    path=path,
                    attempts=attempt + 1,
                    max_retries=self.MAX_RETRIES,
                )
                raise RateLimitException(
                    f"Rate limit exceeded after {self.MAX_RETRIES} retries",
                    status_code=status,
                    response_body=body_preview,
                    retry_after_seconds=retry_after,
                )

            # ------------------------------------------------------------------
            # Transient 4xx (400/408/425) and 5xx — same retry budget as 429
            # ------------------------------------------------------------------
            if status in self.TRANSIENT_STATUS_CODES:
                if attempt < self.MAX_RETRIES:
                    attempt += 1
                    wait_time = self._backoff_seconds(attempt)
                    logger.warning(
                        "Transient error from Tiny API, retrying after backoff",
                        method=method,
                        path=path,
                        status_code=status,
                        attempt=attempt,
                        max_retries=self.MAX_RETRIES,
                        wait_seconds=round(wait_time, 2),
                        response_body_preview=body_preview,
                    )
                    await asyncio.sleep(wait_time)
                    continue
                logger.error(
                    "Transient error from Tiny API after retries",
                    method=method,
                    path=path,
                    status_code=status,
                    attempts=attempt + 1,
                    response_body_preview=body_preview,
                )
                raise TinyAPIException(
                    f"Tiny API error after {self.MAX_RETRIES} retries: {status}",
                    status_code=status,
                    response_body=body_preview,
                )

            # ------------------------------------------------------------------
            # Other 4xx — surface to caller
            # ------------------------------------------------------------------
            if 400 <= status < 500:
                logger.error(
                    "Client error from Tiny API",
                    method=method,
                    path=path,
                    status_code=status,
                    response_body_preview=body_preview,
                )
                raise TinyAPIException(
                    f"Client error from Tiny API: {status}",
                    status_code=status,
                    response_body=body_preview,
                )

            logger.error(
                "Server error from Tiny API",
                method=method,
                path=path,
                status_code=status,
                response_body_preview=body_preview,
            )
            raise TinyAPIException(
                f"Server error from Tiny API: {status}",
                status_code=status,
                response_body=body_preview,
            )

    def _backoff_seconds(self, attempt: int) -> float:
        base: float = min(
            self.BASE_DELAY_SECONDS * (2 ** (attempt - 1)),
            self.MAX_DELAY_SECONDS,
        )
        return base + random.uniform(0, self.MAX_JITTER_SECONDS)


# ---------------------------------------------------------------------------
# Param formatting helpers
# ---------------------------------------------------------------------------
def _format_date_only(value: datetime | date | str) -> str:
    """Return a YYYY-MM-DD string accepted by the Tiny v3 API."""
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        return value.date().isoformat()
    return value.isoformat()


def _format_iso_datetime(value: datetime | str) -> str:
    """Return ``YYYY-MM-DDTHH:MM:SS`` for ``dataAtualizacao``-style params.

    Tiny rejects ISO strings that include a timezone offset (``+00:00``),
    even though it accepts the rest of ISO 8601. Strip the offset and emit
    naive seconds-precision. Aware inputs are converted to UTC before the
    drop so we never silently shift local time.
    """
    if isinstance(value, str):
        return value
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return value.replace(microsecond=0).isoformat(timespec="seconds")


def _parse_resource_from_path(path: str) -> tuple[str, str]:
    """Best-effort split of '/produtos/123' into ('produto', '123')."""
    parts = [p for p in path.split("/") if p]
    if not parts:
        return ("unknown", path)
    resource_type = parts[0].rstrip("s") or parts[0]
    resource_id = parts[-1] if len(parts) > 1 else ""
    return (resource_type, resource_id)
