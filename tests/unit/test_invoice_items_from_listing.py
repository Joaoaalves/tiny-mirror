"""Unit tests: invoice lines come from the ``/notas`` list payload.

Re-reading every NF of the day through ``GET /notas/{id}`` on each order
sync was most of the mirror's Tiny traffic; the list rows already carry the
same ``itens``. The detail call stays only as a fallback. Tiny and DB are
mocked.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tiny_mirror.services import invoice_sync_service as mod
from tiny_mirror.services.invoice_sync_service import InvoiceSyncService

pytestmark = pytest.mark.unit


def _line(sku: str, qty: int) -> dict[str, Any]:
    return {
        "idItem": 10,
        "idProduto": 20,
        "codigo": sku,
        "descricao": "Item",
        "ncm": "1234",
        "unidade": "UN",
        "quantidade": qty,
        "valorUnitario": 5,
        "valorTotal": 5 * qty,
        "cfop": "5102",
        "naturezaOperacao": "Venda",
    }


@asynccontextmanager
async def _session() -> Any:
    yield MagicMock(rollback=AsyncMock())


async def _run(
    rows: list[dict[str, Any]],
    *,
    supplier: bool = False,
    missing: set[int] | None = None,
    replace_error_for: int | None = None,
) -> tuple[list[int], dict[int, list[dict[str, Any]]], MagicMock]:
    tiny = MagicMock()
    tiny.list_invoices = AsyncMock(return_value={"itens": rows, "paginacao": {"total": len(rows)}})
    service = InvoiceSyncService(tiny_client=tiny, queue_publisher=MagicMock())
    detail_fetched: list[int] = []

    async def fake_detail(tiny_id: int) -> int:
        detail_fetched.append(tiny_id)
        return 1

    service.sync_items_for_invoice = fake_detail  # type: ignore[method-assign]
    stored: dict[int, list[dict[str, Any]]] = {}

    async def replace(tiny_id: int, items: list[dict[str, Any]]) -> int:
        if tiny_id == replace_error_for:
            raise RuntimeError("db down")
        stored[tiny_id] = items
        return len(items)

    item_repo = MagicMock(replace_for_invoice=AsyncMock(side_effect=replace))
    header_repo = MagicMock(upsert_batch=AsyncMock(return_value=len(rows)))

    async def without_items(invoices: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [inv for inv in invoices if missing is None or inv["tiny_id"] in missing]

    logger = MagicMock()
    with (
        patch.object(mod, "AsyncSessionLocal", _session),
        patch.object(mod, "PostgreSQLInvoiceRepository", return_value=header_repo),
        patch.object(mod, "PostgreSQLInvoiceItemRepository", return_value=item_repo),
        patch.object(mod, "SyncLogRepository", return_value=MagicMock()),
        patch.object(mod.InvoiceMapper, "from_tiny_api", lambda item: {"tiny_id": item["id"]}),
        patch.object(mod, "_without_items", without_items),
        patch.object(mod, "logger", logger),
    ):
        if supplier:
            await service.sync_supplier_entries(date(2026, 8, 1), date(2026, 11, 24), None)
        else:
            await service.run_date_range_sync(date(2026, 11, 23), date(2026, 11, 24), None)
    return detail_fetched, stored, logger


async def test_lines_in_the_listing_are_stored_without_a_detail_call() -> None:
    rows = [
        {"id": 1, "itens": [_line("A", 2)]},
        {"id": 2, "itens": [_line("B", 1), _line("C", 3)]},
    ]

    fetched, stored, _ = await _run(rows)

    assert fetched == []
    assert sorted(stored) == [1, 2]
    assert [r["product_sku"] for r in stored[2]] == ["B", "C"]
    assert stored[1][0]["quantity"] == Decimal("2")
    assert stored[1][0]["invoice_tiny_id"] == 1


async def test_row_without_lines_falls_back_to_the_detail_call() -> None:
    rows = [
        {"id": 1, "itens": [_line("A", 1)]},
        {"id": 2},
        {"id": 3, "itens": []},
        {"id": 4, "itens": None},
    ]

    fetched, stored, _ = await _run(rows)

    assert fetched == [2, 3, 4]
    assert sorted(stored) == [1]


async def test_supplier_pass_stores_listing_lines_only_for_nfs_without_items() -> None:
    rows = [
        {"id": 1, "itens": [_line("A", 1)]},
        {"id": 2, "itens": [_line("B", 1)]},
        {"id": 3},
    ]

    fetched, stored, _ = await _run(rows, supplier=True, missing={2, 3})

    assert sorted(stored) == [2]
    assert fetched == [3]


async def test_one_failed_replace_does_not_stop_the_rest() -> None:
    rows = [
        {"id": 1, "itens": [_line("A", 1)]},
        {"id": 2, "itens": [_line("B", 1)]},
    ]

    fetched, stored, logger = await _run(rows, replace_error_for=1)

    assert sorted(stored) == [2]
    assert fetched == []
    warning = logger.warning.call_args
    assert warning.args[0] == "Invoice items from listing failed, continuing"
    assert warning.kwargs["invoice_tiny_id"] == 1


async def test_cold_start_window_still_skips_items() -> None:
    tiny = MagicMock()
    tiny.list_invoices = AsyncMock(
        return_value={"itens": [{"id": 1, "itens": [_line("A", 1)]}], "paginacao": {"total": 1}}
    )
    service = InvoiceSyncService(tiny_client=tiny, queue_publisher=MagicMock())
    item_repo_factory = MagicMock(side_effect=AssertionError("items must not be written"))

    with (
        patch.object(mod, "AsyncSessionLocal", _session),
        patch.object(
            mod, "PostgreSQLInvoiceRepository", return_value=MagicMock(upsert_batch=AsyncMock())
        ),
        patch.object(mod, "PostgreSQLInvoiceItemRepository", item_repo_factory),
        patch.object(mod, "SyncLogRepository", return_value=MagicMock()),
        patch.object(mod.InvoiceMapper, "from_tiny_api", lambda item: {"tiny_id": item["id"]}),
    ):
        await service.run_date_range_sync(
            date(2026, 11, 23), date(2026, 11, 24), None, fetch_items=False
        )

    item_repo_factory.assert_not_called()
