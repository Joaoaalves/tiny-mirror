"""Translation between the Tiny ERP order schema (PT) and ours (EN).

The shape we care about lives at ``GET /pedidos/{id}`` — the listing
endpoint omits the ``itens`` array, so :meth:`OrderSyncService.process_order_item`
always fetches the detail before mapping.

Like :class:`ProductMapper`, this module never substitutes empty strings
for missing data — a NULL slot in the DB means Tiny did not send it.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, ClassVar

import structlog

logger = structlog.get_logger(__name__)


class OrderMapper:
    # Webhook payloads (stage 11) carry the situation as a localized string;
    # the order detail uses an int. This dict gives the consumer a single
    # source of truth for the mapping.
    SITUATION_NAME_TO_CODE: ClassVar[dict[str, int]] = {
        "Aberta": 0,
        "Faturada": 1,
        "Cancelada": 2,
        "Aprovada": 3,
        "Preparando Envio": 4,
        "Enviada": 5,
        "Entregue": 6,
        "Pronto Envio": 7,
        "Dados Incompletos": 8,
        "Nao Entregue": 9,
    }

    @staticmethod
    def from_tiny_api(raw: dict[str, Any]) -> dict[str, Any]:
        ecommerce = raw.get("ecommerce") or {}
        deposito = raw.get("deposito") or {}

        return {
            "tiny_id": int(raw["id"]),
            "order_number": int(raw.get("numeroPedido") or raw.get("numero") or 0),
            "invoice_id": _to_int_or_none(raw.get("idNotaFiscal")),
            "invoice_date": _parse_date(raw.get("dataFaturamento")),
            "total_products_value": _to_decimal_or_none(raw.get("valorTotalProdutos")),
            "total_order_value": _to_decimal_or_none(raw.get("valorTotalPedido")),
            "price_list": raw.get("listaPreco"),
            "customer": raw.get("cliente") or {},
            "delivery_address": raw.get("enderecoEntrega"),
            "ecommerce_id": _to_int_or_none(ecommerce.get("id")),
            "ecommerce_name": ecommerce.get("nome"),
            "ecommerce_order_number": ecommerce.get("numeroPedidoEcommerce"),
            "channel_order_number": ecommerce.get("numeroPedidoCanalVenda"),
            "sales_channel": ecommerce.get("canalVenda"),
            "carrier": raw.get("transportador"),
            "warehouse_id": _to_int_or_none(deposito.get("id")),
            "warehouse_name": deposito.get("nome"),
            "seller": raw.get("vendedor"),
            "operation_nature": raw.get("naturezaOperacao"),
            "intermediary": raw.get("intermediador"),
            "payment": raw.get("pagamento"),
            "integrated_payments": raw.get("pagamentosIntegrados") or [],
            "situation": int(raw["situacao"]),
            "order_date": _parse_date(raw.get("data") or raw.get("dataCriacao")),
            "delivery_date": _parse_date(raw.get("dataEntrega")),
            "expected_date": _parse_date(raw.get("dataPrevista")),
            "shipping_date": _parse_iso_utc(raw.get("dataEnvio")),
            "purchase_order_number": raw.get("numeroOrdemCompra"),
            "discount_value": _to_decimal_or_zero(raw.get("valorDesconto")),
            "shipping_value": _to_decimal_or_zero(raw.get("valorFrete")),
            "other_expenses_value": _to_decimal_or_zero(raw.get("valorOutrasDespesas")),
            "observations": raw.get("observacoes"),
            "internal_observations": raw.get("observacoesInternas"),
            "order_origin": int(raw.get("origemPedido", 0) or 0),
            "synced_at": datetime.now(UTC),
        }

    @staticmethod
    def extract_items(raw: dict[str, Any]) -> list[dict[str, Any]]:
        items_raw = raw.get("itens") or []
        items: list[dict[str, Any]] = []
        for item in items_raw:
            produto = item.get("produto") if isinstance(item, dict) else None
            if not produto:
                logger.warning(
                    "Skipping order item without product",
                    order_tiny_id=raw.get("id"),
                    item=item,
                )
                continue
            items.append(
                {
                    "product_tiny_id": _to_int_or_none(produto.get("id")),
                    "product_sku": produto.get("sku") or "",
                    "product_description": produto.get("descricao"),
                    "product_type": produto.get("tipo"),
                    "quantity": _to_decimal_or_zero(item.get("quantidade")),
                    "unit_value": _to_decimal_or_zero(item.get("valorUnitario")),
                    "additional_info": item.get("infoAdicional"),
                }
            )
        return items


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _to_int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (TypeError, ValueError):
        return None


def _to_decimal_or_zero(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (TypeError, ValueError):
        return Decimal("0")


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
        # Accept full ISO datetime strings too — Tiny is inconsistent here.
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _parse_iso_utc(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(s)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
