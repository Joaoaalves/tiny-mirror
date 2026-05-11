"""Thin async client for Tiny ERP API v2 (legacy token auth).

The v2 API is distinct from v3 — it uses a static API token embedded in the
query string rather than OAuth2 Bearer headers. We use it exclusively for
write operations that the v3 API does not yet expose, specifically updating
stock quantities for individual deposits (``produto.atualizar.estoque.php``).

The quantity supplied is **absolute** (not a delta): it becomes the new
``available`` value in the target deposit, overwriting whatever Tiny had.
"""

from __future__ import annotations

import json
import urllib.parse
from typing import Any

import httpx
import structlog

from tiny_mirror.exceptions import TinyAPIException

logger = structlog.get_logger(__name__)

BASE_URL = "https://api.tiny.com.br/api2"


class TinyV2Client:
    def __init__(self, token: str, http_client: httpx.AsyncClient) -> None:
        self._token = token
        self._http = http_client

    async def update_stock(
        self, product_id: int, qty: int, deposit: str = "Full Mercado Livre"
    ) -> dict[str, Any]:
        """Set the stock quantity for *deposit* on *product_id* to *qty*.

        Tiny v2 treats the supplied quantity as the new absolute balance —
        it is NOT a delta. Returns the parsed response body.

        Raises :exc:`TinyAPIException` on any non-OK status or when the
        response body contains ``status != "OK"``.
        """
        payload = json.dumps(
            {"estoque": {"idProduto": product_id, "quantidade": qty, "deposito": deposit}}
        )
        params = {
            "token": self._token,
            "formato": "json",
            "estoque": payload,
        }
        url = f"{BASE_URL}/produto.atualizar.estoque.php?{urllib.parse.urlencode(params)}"

        try:
            response = await self._http.post(url, timeout=30)
        except httpx.TimeoutException as exc:
            raise TinyAPIException("Tiny v2 request timed out") from exc
        except (httpx.ConnectError, httpx.NetworkError) as exc:
            raise TinyAPIException(f"Tiny v2 network error: {exc}") from exc

        if response.status_code != 200:
            raise TinyAPIException(
                f"Tiny v2 returned HTTP {response.status_code}",
                status_code=response.status_code,
                response_body=response.text[:500],
            )

        try:
            body: dict[str, Any] = response.json()
        except ValueError as exc:
            raise TinyAPIException(
                "Tiny v2 returned invalid JSON",
                response_body=response.text[:200],
            ) from exc

        retorno = body.get("retorno", {})
        if retorno.get("status") != "OK":
            raise TinyAPIException(
                f"Tiny v2 stock update failed: {retorno.get('erros') or retorno}",
                response_body=response.text[:500],
            )

        logger.debug(
            "Tiny v2 stock updated",
            product_id=product_id,
            qty=qty,
            deposit=deposit,
            saldo=retorno.get("registros", {}).get("registro", {}).get("saldoEstoque"),
        )
        return body
