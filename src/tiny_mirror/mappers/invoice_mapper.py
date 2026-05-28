"""Translation between the Tiny ERP NF schema (PT) and ours (EN).

The list endpoint ``GET /notas`` returns enough for the header table. To
get ``itens`` (one row per product line on the NF — the source of truth
for which SKU actually shipped, including kit components) the caller must
also hit ``GET /notas/{id}``; ``items_from_tiny_detail`` parses that
response into rows for ``invoice_items``.

Tiny sends empty strings instead of null for missing optional fields; this
mapper normalises those to Python ``None`` before persisting.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any


class InvoiceMapper:
    @staticmethod
    def items_from_tiny_detail(
        invoice_tiny_id: int, raw_detail: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Extract one row per ``itens[]`` entry from ``GET /notas/{id}``.

        Tiny returns each line under top-level ``itens`` (not nested under a
        ``produto`` object). Empty strings are normalised to None; quantity
        and value fields default to 0.
        """
        out: list[dict[str, Any]] = []
        for line in raw_detail.get("itens") or []:
            if not isinstance(line, dict):
                continue
            out.append(
                {
                    "invoice_tiny_id": invoice_tiny_id,
                    "tiny_item_id": _to_int_or_none(line.get("idItem")),
                    "product_tiny_id": _to_int_or_none(line.get("idProduto")),
                    "product_sku": (line.get("codigo") or "").strip(),
                    "product_description": _str_or_none(line.get("descricao")),
                    "ncm": _str_or_none(line.get("ncm")),
                    "unit": _str_or_none(line.get("unidade")),
                    "quantity": _to_decimal(line.get("quantidade")),
                    "unit_value": _to_decimal(line.get("valorUnitario")),
                    "total_value": _to_decimal(line.get("valorTotal")),
                    "cfop": _str_or_none(line.get("cfop")),
                    "operation_nature": _str_or_none(line.get("naturezaOperacao")),
                }
            )
        return out

    @staticmethod
    def from_tiny_api(raw: dict[str, Any]) -> dict[str, Any]:
        ecommerce = raw.get("ecommerce") or {}
        origem = raw.get("origem") or {}

        return {
            "tiny_id": int(raw["id"]),
            "number": str(raw.get("numero") or ""),
            "series": str(raw.get("serie") or ""),
            "access_key": _str_or_none(raw.get("chaveAcesso")),
            "status": str(raw.get("situacao") or ""),
            "type": str(raw.get("tipo") or ""),
            "issue_date": _parse_date(raw.get("dataEmissao")),
            "forecast_date": _parse_date(raw.get("dataPrevista")),
            "customer": raw.get("cliente") or {},
            "delivery_address": raw.get("enderecoEntrega") or None,
            "seller": raw.get("vendedor") or None,
            "total_value": _to_decimal(raw.get("valor")),
            "products_value": _to_decimal(raw.get("valorProdutos")),
            "freight_value": _to_decimal(raw.get("valorFrete")),
            "shipping_method_id": _to_int_or_none(raw.get("idFormaEnvio")),
            "freight_type_id": _to_int_or_none(raw.get("idFormaFrete")),
            "tracking_code": _str_or_none(raw.get("codigoRastreamento")),
            "tracking_url": _str_or_none(raw.get("urlRastreamento")),
            "freight_responsibility": _str_or_none(raw.get("fretePorConta")),
            "volume_count": _to_int_or_none(raw.get("qtdVolumes")),
            "gross_weight": _to_decimal_or_none(raw.get("pesoBruto")),
            "net_weight": _to_decimal_or_none(raw.get("pesoLiquido")),
            "ecommerce": ecommerce or None,
            "ecommerce_order_number": _str_or_none(ecommerce.get("numeroPedidoEcommerce")),
            "origin_id": _to_int_or_none(origem.get("id")),
            "origin_type": _str_or_none(origem.get("tipo")),
            "synced_at": datetime.now(UTC),
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _to_int_or_none(value: Any) -> int | None:
    if value is None or value == "" or value == 0:
        return None
    try:
        v = int(value)
        return v if v != 0 else None
    except (TypeError, ValueError):
        return None


def _to_decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (TypeError, ValueError):
        return Decimal("0")


def _to_decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (TypeError, ValueError):
        return None
