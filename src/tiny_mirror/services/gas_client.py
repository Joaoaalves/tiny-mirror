"""Unified client for the Controle 4.0 Apps Script Web App.

A single deployment URL (``settings.gas_base_url``) routes every request
through ``?action=...``. This client centralises:

- the URL/token plumbing,
- HTTP error handling (timeouts, non-JSON bodies, payload-level errors),
- typed helpers for the three actions tiny-mirror needs today.

See ``gas/code.gs`` in the repo for the matching Apps Script source.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import structlog

logger = structlog.get_logger(__name__)


class GASClientError(Exception):
    """Raised when the GAS endpoint is unreachable or returns an error."""


class GASClient:
    """Thin async wrapper around the unified GAS Web App.

    The client is stateless aside from the injected ``httpx.AsyncClient``;
    callers can hold one instance per request or per job and reuse it.
    """

    def __init__(
        self,
        http: httpx.AsyncClient,
        base_url: str,
        token: str,
        timeout_seconds: float = 60.0,
    ) -> None:
        self._http = http
        self._url = base_url
        self._token = token
        self._timeout = timeout_seconds

    def is_configured(self) -> bool:
        return bool(self._url) and bool(self._token)

    async def _call(self, action: str, **extra: str) -> dict[str, Any]:
        if not self.is_configured():
            raise GASClientError("GAS URL/token not configured")
        params = {"action": action, "token": self._token, **extra}
        try:
            resp = await self._http.get(
                self._url,
                params=params,
                timeout=self._timeout,
                follow_redirects=True,
            )
        except httpx.RequestError as exc:
            raise GASClientError(f"GAS request failed ({action}): {exc}") from exc
        if resp.status_code >= 400:
            raise GASClientError(f"GAS HTTP {resp.status_code} ({action}): {resp.text[:200]}")
        try:
            body = resp.json()
        except json.JSONDecodeError as exc:
            raise GASClientError(f"GAS returned non-JSON ({action}): {exc}") from exc
        if not isinstance(body, dict):
            raise GASClientError(f"GAS returned non-object ({action}): {body!r}")
        if "error" in body:
            raise GASClientError(f"GAS error ({action}): {body['error']}")
        return body

    async def manual_status(self) -> dict[str, Any]:
        """Return ``{ generatedAt, counts, skus: { sku: {status, ...} } }``."""
        return await self._call("manual_status")

    async def costs_all(self) -> dict[str, Any]:
        """Return ``{ generatedAt, difalPct, count, items: { mlb_id: {...} } }``.

        Single HTTP call replaces N per-MLB fetches.
        """
        body = await self._call("costs_all")
        logger.info(
            "gas_costs_all_ok",
            count=body.get("count"),
            difal_pct=body.get("difalPct"),
            generated_at=body.get("generatedAt"),
        )
        return body

    async def cost_single(self, mlb_id: str) -> dict[str, Any]:
        """Legacy single-MLB endpoint. Kept for ad-hoc lookups."""
        return await self._call("cost", mlbid=mlb_id)
