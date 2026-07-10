"""Manual SKU status — read straight from the Controle 4.0 spreadsheet.

Replaces the Apps Script dependency (``?action=manual_status``). That Web App
lived *inside* the spreadsheet, was never versioned, and read cell background
colours by fixed column — so when a column was inserted on 2026-07-08 it
silently started returning COD. FAB values as if they were SKUs, and the
daily sync would have reset every manual classification to ``normal``.

We now read the ``GERAL`` tab through the Sheets API with a service account,
keyed on the **text** of the status column (verified 244/244 rows populated),
not on background colours. Layout as of 2026-07-08:

    GERAL!B = status text ("OK" / "Atenção" / "Descontinuado")
    GERAL!C = COD. FAB
    GERAL!D = SKU

Supplier-block separator rows carry a CNPJ in the SKU column; they are skipped.
"""

from __future__ import annotations

import asyncio
import unicodedata
from typing import Any, Final

import httpx
import structlog
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import service_account

logger = structlog.get_logger(__name__)

_SCOPES: Final[list[str]] = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
_SHEETS_API: Final[str] = "https://sheets.googleapis.com/v4/spreadsheets"

# Column offsets inside the configured range (default ``GERAL!B3:D1200``).
_STATUS_IDX: Final[int] = 0  # coluna B
_SKU_IDX: Final[int] = 2  # coluna D

# Texto da planilha -> manual_status. Cores (verde/amarelo/vermelho) apenas
# acompanham o texto; o texto é a fonte, por ser 100% preenchido.
STATUS_TEXT_MAP: Final[dict[str, str]] = {
    "descontinuado": "queima",  # vermelho — DESCONTINUAR/QUEIMAR
    "atencao": "analise",  # amarelo — ANALISANDO
    "ok": "normal",  # verde — OK
}


class SheetsManualStatusError(Exception):
    """Raised when the spreadsheet cannot be read."""


def _normalize(text: str) -> str:
    """lowercase + sem acentos, pra 'Atenção' casar com 'atencao'."""
    stripped = unicodedata.normalize("NFKD", text.strip().lower())
    return "".join(c for c in stripped if not unicodedata.combining(c))


def parse_manual_status_rows(rows: list[list[str]]) -> dict[str, str]:
    """``[[status, cod_fab, sku], ...]`` -> ``{sku: manual_status}``.

    Pure function (sem I/O) — é aqui que mora a regra, e é o que os testes cobrem.
    Linhas sem SKU, com status desconhecido, ou de bloco de fornecedor (CNPJ na
    coluna do SKU) são ignoradas.
    """
    out: dict[str, str] = {}
    for row in rows:
        sku = (row[_SKU_IDX] if len(row) > _SKU_IDX else "").strip()
        if not sku or "/" in sku:  # "/" => CNPJ da linha de fornecedor
            continue
        raw_status = row[_STATUS_IDX] if len(row) > _STATUS_IDX else ""
        status = STATUS_TEXT_MAP.get(_normalize(raw_status))
        if status is None:
            continue
        out[sku] = status
    return out


class SheetsManualStatusFetcher:
    """Lê o range configurado da planilha e devolve ``{sku: manual_status}``."""

    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        key_path: str,
        spreadsheet_id: str,
        range_a1: str,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._http = http
        self._key_path = key_path
        self._spreadsheet_id = spreadsheet_id
        self._range = range_a1
        self._timeout = timeout_seconds
        self._creds: service_account.Credentials | None = None

    def is_configured(self) -> bool:
        return bool(self._key_path and self._spreadsheet_id and self._range)

    async def _access_token(self) -> str:
        """google-auth é síncrono; refresca fora do event loop."""

        def _refresh() -> str:
            if self._creds is None:
                # google-auth não é tipado; o ignore é local e intencional.
                self._creds = service_account.Credentials.from_service_account_file(  # type: ignore[no-untyped-call]
                    self._key_path, scopes=_SCOPES
                )
            if not self._creds.valid:
                self._creds.refresh(GoogleAuthRequest())
            return str(self._creds.token)

        try:
            return await asyncio.to_thread(_refresh)
        except Exception as exc:  # pragma: no cover — credencial/IO
            raise SheetsManualStatusError(f"service-account auth failed: {exc}") from exc

    async def fetch_statuses(self) -> dict[str, str]:
        if not self.is_configured():
            raise SheetsManualStatusError("sheets manual_status not configured")
        token = await self._access_token()
        url = f"{_SHEETS_API}/{self._spreadsheet_id}/values/{self._range}"
        try:
            resp = await self._http.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                params={"majorDimension": "ROWS"},
                timeout=self._timeout,
            )
        except httpx.RequestError as exc:
            raise SheetsManualStatusError(f"sheets request failed: {exc}") from exc
        if resp.status_code >= 400:
            raise SheetsManualStatusError(f"sheets HTTP {resp.status_code}: {resp.text[:200]}")

        body: Any = resp.json()
        rows = body.get("values") or []
        statuses = parse_manual_status_rows(rows)
        if not statuses:
            # Nunca aplicar um payload vazio: o apply() resetaria tudo pra 'normal'.
            raise SheetsManualStatusError(f"no valid rows parsed from {self._range}")
        counts = {v: list(statuses.values()).count(v) for v in set(statuses.values())}
        logger.info(
            "sheets_manual_status_ok",
            range=self._range,
            rows_read=len(rows),
            skus=len(statuses),
            counts=counts,
        )
        return statuses
