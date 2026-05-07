"""Unit tests for :class:`tiny_mirror.mappers.invoice_mapper.InvoiceMapper`."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from tiny_mirror.mappers.invoice_mapper import InvoiceMapper

pytestmark = pytest.mark.unit


def _full_nf() -> dict[str, Any]:
    return {
        "id": 971921915,
        "situacao": "6",
        "tipo": "S",
        "numero": "138780",
        "serie": "2",
        "chaveAcesso": "35260547756569000145550020001387801989624435",
        "dataEmissao": "2026-05-07",
        "dataPrevista": "",
        "cliente": {
            "nome": "JAMILSON MACHADO DOS SANTOS JUNIOR",
            "tipoPessoa": "F",
            "cpfCnpj": "041.974.277-86",
            "id": 929722497,
            "endereco": {
                "municipio": "Aracaju",
                "uf": "SE",
            },
        },
        "enderecoEntrega": None,
        "valor": 358.95,
        "valorProdutos": 359.7,
        "valorFrete": 0,
        "vendedor": None,
        "idFormaEnvio": 851264407,
        "idFormaFrete": 0,
        "codigoRastreamento": "",
        "urlRastreamento": "",
        "fretePorConta": "T",
        "qtdVolumes": 1,
        "pesoBruto": 1.16,
        "pesoLiquido": 1.16,
        "ecommerce": {
            "id": 12930,
            "nome": "Mercado Livre FULL",
            "numeroPedidoEcommerce": "2000012866227583",
            "numeroPedidoCanalVenda": "",
            "canalVenda": "",
        },
        "origem": {
            "id": "971921914",
            "tipo": "venda",
        },
    }


def test_from_tiny_api_maps_all_core_fields() -> None:
    mapped = InvoiceMapper.from_tiny_api(_full_nf())

    assert mapped["tiny_id"] == 971921915
    assert mapped["number"] == "138780"
    assert mapped["series"] == "2"
    assert mapped["access_key"] == "35260547756569000145550020001387801989624435"
    assert mapped["status"] == "6"
    assert mapped["type"] == "S"
    assert mapped["issue_date"] == date(2026, 5, 7)
    assert mapped["forecast_date"] is None  # empty string → None


def test_from_tiny_api_maps_values() -> None:
    mapped = InvoiceMapper.from_tiny_api(_full_nf())

    assert mapped["total_value"] == Decimal("358.95")
    assert mapped["products_value"] == Decimal("359.7")
    assert mapped["freight_value"] == Decimal("0")


def test_from_tiny_api_maps_ecommerce_fields() -> None:
    mapped = InvoiceMapper.from_tiny_api(_full_nf())

    assert mapped["ecommerce_order_number"] == "2000012866227583"
    assert mapped["ecommerce"] is not None
    assert mapped["ecommerce"]["nome"] == "Mercado Livre FULL"


def test_from_tiny_api_maps_origin() -> None:
    mapped = InvoiceMapper.from_tiny_api(_full_nf())

    assert mapped["origin_id"] == 971921914
    assert mapped["origin_type"] == "venda"


def test_from_tiny_api_normalises_empty_strings_to_none() -> None:
    mapped = InvoiceMapper.from_tiny_api(_full_nf())

    # Empty strings from Tiny become None.
    assert mapped["tracking_code"] is None
    assert mapped["tracking_url"] is None


def test_from_tiny_api_freight_type_id_zero_becomes_none() -> None:
    mapped = InvoiceMapper.from_tiny_api(_full_nf())

    # idFormaFrete=0 is "not set" in Tiny; normalised to None.
    assert mapped["freight_type_id"] is None


def test_from_tiny_api_freight_responsibility() -> None:
    mapped = InvoiceMapper.from_tiny_api(_full_nf())
    assert mapped["freight_responsibility"] == "T"


def test_from_tiny_api_shipping_method_id() -> None:
    mapped = InvoiceMapper.from_tiny_api(_full_nf())
    assert mapped["shipping_method_id"] == 851264407


def test_from_tiny_api_volume_and_weights() -> None:
    mapped = InvoiceMapper.from_tiny_api(_full_nf())

    assert mapped["volume_count"] == 1
    assert mapped["gross_weight"] == Decimal("1.16")
    assert mapped["net_weight"] == Decimal("1.16")


def test_from_tiny_api_customer_preserved_as_dict() -> None:
    mapped = InvoiceMapper.from_tiny_api(_full_nf())
    assert isinstance(mapped["customer"], dict)
    assert mapped["customer"]["nome"] == "JAMILSON MACHADO DOS SANTOS JUNIOR"


def test_from_tiny_api_delivery_address_none_stays_none() -> None:
    mapped = InvoiceMapper.from_tiny_api(_full_nf())
    assert mapped["delivery_address"] is None


def test_from_tiny_api_no_ecommerce_returns_none_fields() -> None:
    raw = _full_nf()
    raw["ecommerce"] = None
    mapped = InvoiceMapper.from_tiny_api(raw)

    assert mapped["ecommerce"] is None
    assert mapped["ecommerce_order_number"] is None


def test_from_tiny_api_ecommerce_order_number_empty_string_becomes_none() -> None:
    raw = _full_nf()
    raw["ecommerce"]["numeroPedidoEcommerce"] = ""
    mapped = InvoiceMapper.from_tiny_api(raw)

    assert mapped["ecommerce_order_number"] is None


def test_from_tiny_api_no_origem_returns_none_fields() -> None:
    raw = _full_nf()
    raw["origem"] = None
    mapped = InvoiceMapper.from_tiny_api(raw)

    assert mapped["origin_id"] is None
    assert mapped["origin_type"] is None


def test_from_tiny_api_synced_at_is_set() -> None:
    from datetime import UTC, datetime

    mapped = InvoiceMapper.from_tiny_api(_full_nf())
    assert isinstance(mapped["synced_at"], datetime)
    assert mapped["synced_at"].tzinfo is not None
    # Freshly set — within a few seconds of now.
    delta = (datetime.now(UTC) - mapped["synced_at"]).total_seconds()
    assert abs(delta) < 5


def test_from_tiny_api_forecast_date_valid_string() -> None:
    raw = _full_nf()
    raw["dataPrevista"] = "2026-05-10"
    mapped = InvoiceMapper.from_tiny_api(raw)
    assert mapped["forecast_date"] == date(2026, 5, 10)


def test_from_tiny_api_missing_optional_fields_default_to_zero() -> None:
    raw = {
        "id": 1,
        "situacao": "6",
        "tipo": "S",
        "numero": "1",
        "serie": "1",
        "dataEmissao": "2026-01-01",
        "cliente": {},
        "ecommerce": None,
        "origem": None,
    }
    mapped = InvoiceMapper.from_tiny_api(raw)

    assert mapped["total_value"] == Decimal("0")
    assert mapped["products_value"] == Decimal("0")
    assert mapped["freight_value"] == Decimal("0")
    assert mapped["access_key"] is None
    assert mapped["forecast_date"] is None
