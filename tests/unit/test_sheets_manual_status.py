"""Unit tests for the Controle 4.0 GERAL parsing (manual_status).

This is exactly where the 2026-07-08 breakage lived: the old Apps Script read
the SKU by fixed column position and, after a column was inserted, returned
COD. FAB values as SKUs. The parsing rule now lives here, versioned and tested.

Range is ``GERAL!B3:D1200`` → each row is ``[status, cod_fab, sku]``.
"""

from __future__ import annotations

import pytest

from tiny_mirror.services.sheets_manual_status import (
    STATUS_TEXT_MAP,
    parse_manual_status_rows,
)

pytestmark = pytest.mark.unit


def test_maps_the_three_operator_labels() -> None:
    rows = [
        ["Descontinuado", "IXAU3041PR34", "BRG-CAD-AUTO-RECLI"],  # vermelho
        ["Atenção", "IXAP0342BR", "BRG-ASSEN-BN-RGD"],  # amarelo
        ["OK", "IXBA3015BR", "BRG-BAN-RGD"],  # verde
    ]
    assert parse_manual_status_rows(rows) == {
        "BRG-CAD-AUTO-RECLI": "queima",
        "BRG-ASSEN-BN-RGD": "analise",
        "BRG-BAN-RGD": "normal",
    }


def test_status_text_is_accent_and_case_insensitive() -> None:
    rows = [
        ["atencao", "X", "SKU-A"],
        ["ATENÇÃO", "X", "SKU-B"],
        ["  ok  ", "X", "SKU-C"],
        ["DESCONTINUADO", "X", "SKU-D"],
    ]
    assert parse_manual_status_rows(rows) == {
        "SKU-A": "analise",
        "SKU-B": "analise",
        "SKU-C": "normal",
        "SKU-D": "queima",
    }


def test_skips_supplier_block_rows_carrying_a_cnpj_in_the_sku_column() -> None:
    rows = [
        ["OK", "IXBA3015BR", "BRG-BAN-RGD"],
        ["", "BURIGOTTO S/A", "51.460.277/0001-70"],  # linha de bloco de fornecedor
    ]
    assert parse_manual_status_rows(rows) == {"BRG-BAN-RGD": "normal"}


def test_skips_blank_and_unknown_status_rows() -> None:
    rows = [
        ["OK", "X", "SKU-KEEP"],
        [],  # linha vazia
        ["", "X", "SKU-NO-STATUS"],  # sem status
        ["Talvez", "X", "SKU-WEIRD"],  # status desconhecido
        ["OK", "X", ""],  # sem SKU
    ]
    assert parse_manual_status_rows(rows) == {"SKU-KEEP": "normal"}


def test_short_rows_do_not_crash() -> None:
    """A Sheets API omite células vazias à direita — linhas vêm truncadas."""
    assert parse_manual_status_rows([["OK"], ["OK", "COD"]]) == {}


def test_cod_fab_is_never_used_as_the_sku() -> None:
    """Regressão do bug: COD. FAB (coluna C) não pode virar chave."""
    rows = [["Descontinuado", "7551", "BUB-ASPR-NAS-ESTJ"]]
    parsed = parse_manual_status_rows(rows)
    assert "7551" not in parsed
    assert parsed == {"BUB-ASPR-NAS-ESTJ": "queima"}


def test_status_map_only_yields_valid_manual_statuses() -> None:
    assert set(STATUS_TEXT_MAP.values()) == {"queima", "analise", "normal"}
