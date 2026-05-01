"""Unit tests for :class:`tiny_mirror.mappers.product_mapper.ProductMapper`."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest

from tiny_mirror.mappers.product_mapper import ProductMapper

pytestmark = pytest.mark.unit


def _full_product_p() -> dict[str, Any]:
    return {
        "id": 100,
        "sku": "SKU-100",
        "descricao": "Product 100",
        "descricaoComplementar": "Detailed",
        "tipo": "P",
        "situacao": "A",
        "unidade": "UN",
        "unidadePorCaixa": "10",
        "ncm": "9999.99.99",
        "gtin": "7891234567890",
        "origem": "0",
        "garantia": "12 months",
        "observacoes": "obs",
        "categoria": {
            "id": 7,
            "nome": "Electronics",
            "caminhoCompleto": "Electronics > Phones",
        },
        "marca": {"id": 3, "nome": "Acme"},
        "produtoPai": {"id": 99},
        "dimensoes": {
            "embalagem": "Caixa",
            "largura": 10.5,
            "altura": 5.0,
            "comprimento": 2.0,
            "diametro": 0.0,
            "pesoLiquido": 0.5,
            "pesoBruto": 0.6,
            "quantidadeVolumes": 1,
        },
        "precos": {
            "preco": 100.0,
            "precoPromocional": 80.0,
            "precoCusto": 60.0,
            "precoCustoMedio": 62.0,
        },
        "estoque": {
            "controlar": True,
            "sobEncomenda": False,
            "diasPreparacao": 2,
            "localizacao": "A1",
            "minimo": 5.0,
            "maximo": 100.0,
            "quantidade": 50.0,
        },
        "fornecedores": [{"id": 1, "nome": "Sup A"}],
        "seo": {"title": "SEO"},
        "tributacao": {"icms": 18.0},
        "anexos": [{"url": "x"}],
        "tipoVariacao": None,
        "dataCriacao": "2025-01-15T10:30:00",
        "dataAlteracao": "2025-01-20T12:00:00Z",
    }


# ---------------------------------------------------------------------------
# from_tiny_api
# ---------------------------------------------------------------------------
def test_from_tiny_api_p_product_maps_all_fields() -> None:
    raw = _full_product_p()

    mapped = ProductMapper.from_tiny_api(raw)

    # No PT keys leaked through.
    pt_keys = {
        "descricao",
        "descricaoComplementar",
        "tipo",
        "situacao",
        "categoria",
        "marca",
        "produtoPai",
        "dimensoes",
        "precos",
        "estoque",
    }
    assert pt_keys.isdisjoint(mapped.keys())

    assert mapped["tiny_id"] == 100
    assert mapped["sku"] == "SKU-100"
    assert mapped["description"] == "Product 100"
    assert mapped["complementary_description"] == "Detailed"
    assert mapped["type"] == "P"
    assert mapped["situation"] == "A"
    assert mapped["unit"] == "UN"
    assert mapped["category_id"] == 7
    assert mapped["category_name"] == "Electronics"
    assert mapped["category_full_path"] == "Electronics > Phones"
    assert mapped["brand_id"] == 3
    assert mapped["brand_name"] == "Acme"
    assert mapped["parent_product_tiny_id"] == 99
    assert mapped["dimensions"] == {
        "packaging_type": "Caixa",
        "width": 10.5,
        "height": 5.0,
        "length": 2.0,
        "diameter": 0.0,
        "net_weight": 0.5,
        "gross_weight": 0.6,
        "volume_count": 1,
    }
    assert mapped["prices"] == {
        "price": 100.0,
        "promotional_price": 80.0,
        "cost_price": 60.0,
        "average_cost_price": 62.0,
    }
    assert mapped["stock_control"] is True
    assert mapped["stock_quantity"] == 50.0
    assert mapped["created_at_tiny"] is not None
    assert mapped["updated_at_tiny"].tzinfo is not None
    assert isinstance(mapped["synced_at"], datetime)


def test_from_tiny_api_kit_product_keeps_type_k() -> None:
    raw = _full_product_p()
    raw["tipo"] = "K"
    mapped = ProductMapper.from_tiny_api(raw)
    assert mapped["type"] == "K"


def test_from_tiny_api_missing_optional_fields_become_none() -> None:
    raw = {
        "id": 1,
        "sku": "X",
        "descricao": "X",
        "tipo": "P",
        "situacao": "A",
    }
    mapped = ProductMapper.from_tiny_api(raw)

    assert mapped["unit"] is None
    assert mapped["complementary_description"] is None
    assert mapped["category_id"] is None
    assert mapped["brand_name"] is None
    assert mapped["dimensions"] is None
    # ``prices`` defaults to {} (NOT None) — the column is NOT NULL.
    assert mapped["prices"] == {}
    assert mapped["created_at_tiny"] is None
    assert mapped["updated_at_tiny"] is None
    assert mapped["suppliers"] == []
    assert mapped["attachments"] == []


def test_from_tiny_api_blank_strings_in_int_fields_become_none() -> None:
    raw = {
        "id": 5,
        "sku": "Y",
        "descricao": "Y",
        "tipo": "P",
        "situacao": "A",
        "categoria": {"id": "", "nome": "C"},
    }
    mapped = ProductMapper.from_tiny_api(raw)
    assert mapped["category_id"] is None
    assert mapped["category_name"] == "C"


def test_from_tiny_api_normalizes_z_suffix_in_dates() -> None:
    raw = {
        "id": 9,
        "sku": "Z",
        "descricao": "Z",
        "tipo": "P",
        "situacao": "A",
        "dataCriacao": "2025-01-15T10:30:00Z",
    }
    mapped = ProductMapper.from_tiny_api(raw)
    assert mapped["created_at_tiny"] is not None
    assert mapped["created_at_tiny"].tzinfo is not None


def test_from_tiny_api_invalid_date_returns_none() -> None:
    raw = {
        "id": 9,
        "sku": "Z",
        "descricao": "Z",
        "tipo": "P",
        "situacao": "A",
        "dataCriacao": "not-a-date",
    }
    mapped = ProductMapper.from_tiny_api(raw)
    assert mapped["created_at_tiny"] is None


# ---------------------------------------------------------------------------
# extract_kit_components
# ---------------------------------------------------------------------------
def test_extract_kit_components_returns_list_for_kit() -> None:
    raw = {
        "id": 1,
        "tipo": "K",
        "kit": [
            {
                "produto": {
                    "id": 10,
                    "sku": "COMP-A",
                    "descricao": "Component A",
                    "tipo": "P",
                },
                "quantidade": 5,
            }
        ],
    }
    components = ProductMapper.extract_kit_components(raw)
    assert components == [
        {
            "component_product_tiny_id": 10,
            "component_sku": "COMP-A",
            "component_description": "Component A",
            "component_type": "P",
            "quantity": 5.0,
        }
    ]


def test_extract_kit_components_non_kit_returns_empty_list() -> None:
    raw = {"id": 1, "tipo": "P", "kit": [{"produto": {"id": 10}, "quantidade": 1}]}
    assert ProductMapper.extract_kit_components(raw) == []


def test_extract_kit_components_empty_kit_returns_empty_list() -> None:
    raw = {"id": 1, "tipo": "K", "kit": []}
    assert ProductMapper.extract_kit_components(raw) == []


def test_extract_kit_components_missing_kit_field_returns_empty_list() -> None:
    raw = {"id": 1, "tipo": "K"}
    assert ProductMapper.extract_kit_components(raw) == []


def test_extract_kit_components_skips_malformed_items() -> None:
    raw = {
        "id": 1,
        "tipo": "K",
        "kit": [
            {"produto": None, "quantidade": 1},  # malformed
            {"quantidade": 2},  # missing produto
            {
                "produto": {"id": 10, "sku": "GOOD", "descricao": "G", "tipo": "P"},
                "quantidade": 3,
            },
        ],
    }
    components = ProductMapper.extract_kit_components(raw)
    assert len(components) == 1
    assert components[0]["component_sku"] == "GOOD"
