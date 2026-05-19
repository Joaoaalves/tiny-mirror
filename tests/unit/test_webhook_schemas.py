"""Unit tests for webhook payload schemas.

Locks the lenient v3 stock-webhook contract that Tiny actually sends —
``idEcommerce`` and ``dados.tipoEstoque`` are NOT in the v3 payload, so the
schema must accept them missing. Legacy payloads that include them must
keep validating too.
"""

from __future__ import annotations

import pytest

from tiny_mirror.api.schemas import StockWebhookPayload

pytestmark = pytest.mark.unit


def test_stock_webhook_accepts_v3_minimal_payload() -> None:
    """Real Tiny v3 stock webhook: no idEcommerce, no tipoEstoque, no skuMapeamento."""
    raw = {
        "versao": "1.0.1",
        "cnpj": "47756569000145",
        "tipo": "estoque",
        "dados": {
            "idProduto": 971992238,
            "sku": "SKU-TEST-FULL",
            "nome": "TESTE FULFILLMENT",
            "saldo": 70,
        },
    }

    payload = StockWebhookPayload.model_validate(raw)

    assert payload.cnpj == "47756569000145"
    assert payload.tipo == "estoque"
    assert payload.id_ecommerce is None
    assert payload.dados.id_produto == 971992238
    assert payload.dados.sku == "SKU-TEST-FULL"
    assert payload.dados.saldo == 70
    assert payload.dados.tipo_estoque is None
    assert payload.dados.sku_mapeamento is None


def test_stock_webhook_still_accepts_legacy_payload_with_all_fields() -> None:
    raw = {
        "versao": "2",
        "cnpj": "47756569000145",
        "idEcommerce": "12345",
        "tipo": "estoque",
        "dados": {
            "idProduto": 1,
            "sku": "X",
            "saldo": 10.0,
            "tipoEstoque": "F",
            "skuMapeamento": "MAP",
            "skuMapeamentoPai": "PARENT",
        },
    }

    payload = StockWebhookPayload.model_validate(raw)

    assert payload.id_ecommerce == "12345"
    assert payload.dados.tipo_estoque == "F"
    assert payload.dados.sku_mapeamento == "MAP"
    assert payload.dados.sku_mapeamento_pai == "PARENT"


def test_stock_webhook_rejects_invalid_tipo_estoque_when_present() -> None:
    raw = {
        "versao": "2",
        "cnpj": "47756569000145",
        "tipo": "estoque",
        "dados": {
            "idProduto": 1,
            "sku": "X",
            "saldo": 10.0,
            "tipoEstoque": "Z",
        },
    }

    with pytest.raises(Exception):  # noqa: B017 — pydantic ValidationError wraps
        StockWebhookPayload.model_validate(raw)
