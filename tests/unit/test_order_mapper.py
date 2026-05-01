"""Unit tests for :class:`tiny_mirror.mappers.order_mapper.OrderMapper`."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from tiny_mirror.mappers.order_mapper import OrderMapper

pytestmark = pytest.mark.unit


def _full_order() -> dict[str, Any]:
    return {
        "id": 999,
        "numeroPedido": 1810,
        "idNotaFiscal": 5005,
        "dataFaturamento": "2025-01-20",
        "valorTotalProdutos": "150.00",
        "valorTotalPedido": "175.00",
        "listaPreco": {"id": 1},
        "cliente": {
            "id": 11,
            "nome": "John Doe",
            "cpfCnpj": "123.456.789-00",
        },
        "enderecoEntrega": {"endereco": "Rua A, 123"},
        "ecommerce": {
            "id": 7,
            "nome": "Shopify",
            "numeroPedidoEcommerce": "S-001",
            "numeroPedidoCanalVenda": "C-002",
            "canalVenda": "Web",
        },
        "transportador": {"nome": "Carrier"},
        "deposito": {"id": 33, "nome": "Main"},
        "vendedor": {"nome": "Seller"},
        "naturezaOperacao": {"id": 1},
        "intermediador": None,
        "pagamento": {"forma": "Pix"},
        "pagamentosIntegrados": [{"id": 1}],
        "situacao": 3,
        "data": "2025-01-15",
        "dataEntrega": "2025-01-25",
        "dataPrevista": "2025-01-22",
        "dataEnvio": "2025-01-21T10:00:00Z",
        "numeroOrdemCompra": "PO-1",
        "valorDesconto": "5.00",
        "valorFrete": "20.00",
        "valorOutrasDespesas": "0",
        "observacoes": "obs",
        "observacoesInternas": "internal",
        "origemPedido": 1,
    }


def test_from_tiny_api_full_order_maps_all_fields() -> None:
    raw = _full_order()
    mapped = OrderMapper.from_tiny_api(raw)

    assert mapped["tiny_id"] == 999
    assert mapped["order_number"] == 1810
    assert mapped["invoice_id"] == 5005
    assert mapped["invoice_date"] == date(2025, 1, 20)
    assert mapped["total_order_value"] == Decimal("175.00")
    assert mapped["situation"] == 3
    assert mapped["order_date"] == date(2025, 1, 15)
    assert mapped["customer"]["nome"] == "John Doe"
    assert mapped["delivery_address"] == {"endereco": "Rua A, 123"}
    assert mapped["ecommerce_id"] == 7
    assert mapped["ecommerce_name"] == "Shopify"
    assert mapped["ecommerce_order_number"] == "S-001"
    assert mapped["channel_order_number"] == "C-002"
    assert mapped["sales_channel"] == "Web"
    assert mapped["carrier"] == {"nome": "Carrier"}
    assert mapped["warehouse_id"] == 33
    assert mapped["warehouse_name"] == "Main"
    assert mapped["discount_value"] == Decimal("5.00")
    assert mapped["shipping_value"] == Decimal("20.00")
    assert mapped["other_expenses_value"] == Decimal("0")
    assert mapped["order_origin"] == 1
    # itens never leaks into the order dict.
    assert "itens" not in mapped


def test_from_tiny_api_without_delivery_address_returns_none() -> None:
    raw = _full_order()
    raw["enderecoEntrega"] = None
    mapped = OrderMapper.from_tiny_api(raw)
    assert mapped["delivery_address"] is None


def test_from_tiny_api_without_ecommerce_returns_none_fields() -> None:
    raw = _full_order()
    raw["ecommerce"] = None
    mapped = OrderMapper.from_tiny_api(raw)
    assert mapped["ecommerce_id"] is None
    assert mapped["ecommerce_name"] is None
    assert mapped["ecommerce_order_number"] is None


def test_from_tiny_api_missing_discount_defaults_to_zero() -> None:
    raw = _full_order()
    raw.pop("valorDesconto", None)
    mapped = OrderMapper.from_tiny_api(raw)
    assert mapped["discount_value"] == Decimal("0")


def test_from_tiny_api_uses_numero_when_numeroPedido_missing() -> None:
    raw = _full_order()
    raw.pop("numeroPedido")
    raw["numero"] = 42
    mapped = OrderMapper.from_tiny_api(raw)
    assert mapped["order_number"] == 42


def test_from_tiny_api_handles_dataEnvio_with_tz() -> None:
    raw = _full_order()
    raw["dataEnvio"] = "2025-01-21T10:00:00+00:00"
    mapped = OrderMapper.from_tiny_api(raw)
    assert mapped["shipping_date"] is not None
    assert mapped["shipping_date"].tzinfo is not None


def test_from_tiny_api_invalid_dataEnvio_becomes_none() -> None:
    raw = _full_order()
    raw["dataEnvio"] = "not-a-datetime"
    mapped = OrderMapper.from_tiny_api(raw)
    assert mapped["shipping_date"] is None


# ---------------------------------------------------------------------------
# extract_items
# ---------------------------------------------------------------------------
def test_extract_items_maps_all_fields() -> None:
    raw = {
        "id": 1,
        "itens": [
            {
                "produto": {
                    "id": 100,
                    "sku": "SKU-A",
                    "descricao": "Prod A",
                    "tipo": "P",
                },
                "quantidade": 2,
                "valorUnitario": 25.5,
                "infoAdicional": "info",
            },
            {
                "produto": {
                    "id": 200,
                    "sku": "SKU-B",
                    "descricao": "Prod B",
                    "tipo": "K",
                },
                "quantidade": 1,
                "valorUnitario": 100,
            },
        ],
    }
    items = OrderMapper.extract_items(raw)

    assert len(items) == 2
    assert items[0]["product_sku"] == "SKU-A"
    assert items[0]["quantity"] == Decimal("2")
    assert items[0]["unit_value"] == Decimal("25.5")
    assert items[0]["product_type"] == "P"
    assert items[1]["product_type"] == "K"


def test_extract_items_empty_list_returns_empty() -> None:
    assert OrderMapper.extract_items({"itens": []}) == []


def test_extract_items_missing_field_returns_empty() -> None:
    assert OrderMapper.extract_items({}) == []


def test_extract_items_skips_items_without_produto() -> None:
    raw = {
        "id": 1,
        "itens": [
            {"quantidade": 2, "valorUnitario": 10},  # malformed
            {
                "produto": {
                    "id": 100,
                    "sku": "OK",
                    "descricao": "ok",
                    "tipo": "P",
                },
                "quantidade": 1,
                "valorUnitario": 5,
            },
        ],
    }
    items = OrderMapper.extract_items(raw)
    assert len(items) == 1
    assert items[0]["product_sku"] == "OK"


# ---------------------------------------------------------------------------
# Status mapping
# ---------------------------------------------------------------------------
def test_situation_name_to_code_contains_all_ten_statuses() -> None:
    expected = {
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
    assert OrderMapper.SITUATION_NAME_TO_CODE == expected
