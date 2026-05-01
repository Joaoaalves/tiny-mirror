"""Unit tests for :class:`tiny_mirror.mappers.stock_mapper.StockMapper`."""

from __future__ import annotations

import pytest

from tiny_mirror.mappers.stock_mapper import StockMapper

pytestmark = pytest.mark.unit


def test_from_tiny_api_maps_all_fields() -> None:
    raw = {
        "id": 100,
        "nome": "Stock Product",
        "codigo": "SKU-100",
        "unidade": "UN",
        "saldo": 50,
        "reservado": 5,
        "disponivel": 45,
        "localizacao": "A1-B2",
    }
    mapped = StockMapper.from_tiny_api(raw)

    assert mapped["product_tiny_id"] == 100
    assert mapped["product_name"] == "Stock Product"
    assert mapped["sku"] == "SKU-100"
    assert mapped["unit"] == "UN"
    assert mapped["balance"] == 50.0
    assert mapped["reserved"] == 5.0
    assert mapped["available"] == 45.0
    assert mapped["location"] == "A1-B2"


def test_from_tiny_api_codigo_renamed_to_sku() -> None:
    raw = {"id": 1, "codigo": "ABC", "saldo": 0, "reservado": 0, "disponivel": 0}
    mapped = StockMapper.from_tiny_api(raw)
    assert mapped["sku"] == "ABC"


@pytest.mark.parametrize("value", [None, "", "abc"])
def test_from_tiny_api_unparseable_numeric_falls_back_to_zero(value) -> None:
    raw = {
        "id": 1,
        "saldo": value,
        "reservado": value,
        "disponivel": value,
    }
    mapped = StockMapper.from_tiny_api(raw)
    assert mapped["balance"] == 0.0
    assert mapped["reserved"] == 0.0
    assert mapped["available"] == 0.0


def test_from_tiny_api_string_numbers_get_converted() -> None:
    raw = {
        "id": 1,
        "saldo": "12.5",
        "reservado": "1.0",
        "disponivel": "11.5",
    }
    mapped = StockMapper.from_tiny_api(raw)
    assert mapped["balance"] == 12.5
    assert mapped["reserved"] == 1.0
    assert mapped["available"] == 11.5


# ---------------------------------------------------------------------------
# extract_deposits
# ---------------------------------------------------------------------------
def test_extract_deposits_maps_multiple_deposits() -> None:
    raw = {
        "id": 1,
        "depositos": [
            {
                "id": 10,
                "nome": "Main",
                "desconsiderar": False,
                "saldo": 30,
                "reservado": 5,
                "disponivel": 25,
                "empresa": "Co A",
            },
            {
                "id": 11,
                "nome": "Backup",
                "desconsiderar": True,
                "saldo": 5,
                "reservado": 0,
                "disponivel": 5,
            },
        ],
    }
    deposits = StockMapper.extract_deposits(raw)

    assert len(deposits) == 2
    assert deposits[0]["deposit_tiny_id"] == 10
    assert deposits[0]["ignore"] is False
    assert deposits[0]["balance"] == 30.0
    assert deposits[0]["company"] == "Co A"
    assert deposits[1]["ignore"] is True
    assert deposits[1]["company"] is None


def test_extract_deposits_empty_returns_empty_list() -> None:
    assert StockMapper.extract_deposits({"id": 1, "depositos": []}) == []


def test_extract_deposits_missing_field_returns_empty_list() -> None:
    assert StockMapper.extract_deposits({"id": 1}) == []


def test_extract_deposits_skips_deposits_without_id() -> None:
    raw = {
        "id": 1,
        "depositos": [
            {"nome": "no id"},  # malformed
            {"id": 10, "nome": "ok", "saldo": 5, "reservado": 0, "disponivel": 5},
        ],
    }
    deposits = StockMapper.extract_deposits(raw)
    assert len(deposits) == 1
    assert deposits[0]["deposit_tiny_id"] == 10
