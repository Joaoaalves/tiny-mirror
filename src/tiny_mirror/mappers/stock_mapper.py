"""Translation between the Tiny ERP stock schema (PT) and ours (EN).

The Tiny `GET /estoque/{idProduto}` payload bundles the consolidated
balance with a per-deposit breakdown. The mapper splits these into the
``stock`` and ``stock_deposits`` shapes used by the persistence layer.

Negative stock values returned by Tiny (a frequent artefact of Full ML
oversells and bookkeeping errors) are clamped to ``0`` on the way in:
no matter what Tiny says, "less than nothing in stock" is not a
business reality the coverage report should react to.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class StockMapper:
    @staticmethod
    def from_tiny_api(raw: dict[str, Any]) -> dict[str, Any]:
        return {
            "product_tiny_id": int(raw["id"]),
            "product_name": raw.get("nome"),
            # Tiny calls the SKU `codigo` on the stock endpoint.
            "sku": raw.get("codigo") or "",
            "unit": raw.get("unidade"),
            "balance": _to_non_negative_float(raw.get("saldo")),
            "reserved": _to_non_negative_float(raw.get("reservado")),
            "available": _to_non_negative_float(raw.get("disponivel")),
            "location": raw.get("localizacao"),
            "synced_at": datetime.now(UTC),
        }

    @staticmethod
    def extract_deposits(raw: dict[str, Any]) -> list[dict[str, Any]]:
        deposits_raw = raw.get("depositos") or []
        deposits: list[dict[str, Any]] = []
        for deposit in deposits_raw:
            if not isinstance(deposit, dict):
                continue
            deposit_id = deposit.get("id")
            if deposit_id is None:
                logger.warning(
                    "Skipping deposit without id",
                    product_tiny_id=raw.get("id"),
                    deposit=deposit,
                )
                continue
            deposits.append(
                {
                    "deposit_tiny_id": int(deposit_id),
                    "deposit_name": deposit.get("nome") or "",
                    "ignore": bool(deposit.get("desconsiderar", False)),
                    "balance": _to_non_negative_float(deposit.get("saldo")),
                    "reserved": _to_non_negative_float(deposit.get("reservado")),
                    "available": _to_non_negative_float(deposit.get("disponivel")),
                    "company": deposit.get("empresa"),
                }
            )
        return deposits


def _to_float(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_non_negative_float(value: Any) -> float:
    """Coerce to float and clamp to ``0`` so negative Tiny values never
    leak into our stock tables. See module docstring for context."""
    return max(0.0, _to_float(value))
