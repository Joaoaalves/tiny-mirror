"""Unit tests for :class:`FLStockCorrectionService`.

Covers:
  - _extract_full_saldo: pure helper
  - _handle_one: 3 scenarios (aligned, corrected ok, correction fails)
  - run_correction: smoke test (empty candidate list)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tiny_mirror.services.fl_stock_correction_service import (
    FLStockCorrectionService,
    _extract_full_saldo,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# _extract_full_saldo
# ---------------------------------------------------------------------------
def test_extract_full_saldo_present() -> None:
    body = {
        "depositos": [
            {"nome": "Galpão", "saldo": 100},
            {"nome": "Full Mercado Livre", "saldo": 42, "disponivel": 40, "reservado": 2},
        ]
    }
    assert _extract_full_saldo(body) == 42


def test_extract_full_saldo_missing_deposit_returns_zero() -> None:
    body = {"depositos": [{"nome": "Galpão", "saldo": 5}]}
    assert _extract_full_saldo(body) == 0


def test_extract_full_saldo_uses_saldo_not_disponivel() -> None:
    """Critical: bug from 2026-05-27. Saldo=10, reservado=3, disponivel=7.
    For correction, we want saldo (10), not disponivel (7).
    """
    body = {"depositos": [{"nome": "Full Mercado Livre", "saldo": 10, "disponivel": 7}]}
    assert _extract_full_saldo(body) == 10


def test_extract_full_saldo_empty_returns_zero() -> None:
    assert _extract_full_saldo({}) == 0
    assert _extract_full_saldo({"depositos": []}) == 0
    assert _extract_full_saldo({"depositos": None}) == 0


# ---------------------------------------------------------------------------
# _handle_one
# ---------------------------------------------------------------------------
@pytest.fixture
def service() -> FLStockCorrectionService:
    tiny = MagicMock()
    tiny.get_stock = AsyncMock()
    tiny.record_stock_movement = AsyncMock()
    return FLStockCorrectionService(tiny_client=tiny)


@patch("tiny_mirror.services.fl_stock_correction_service.AsyncSessionLocal")
async def test_handle_one_aligned_no_correction(
    mock_session_local: MagicMock, service: FLStockCorrectionService
) -> None:
    """Saldo Tiny já = ml_qty → retorna 'aligned' sem POST."""
    service._tiny.get_stock = AsyncMock(  # type: ignore[method-assign]
        return_value={"depositos": [{"nome": "Full Mercado Livre", "saldo": 5}]}
    )

    result = await service._handle_one(tiny_id=100, sku="SKU-A", ml_qty=5)

    assert result == "aligned"
    service._tiny.record_stock_movement.assert_not_awaited()  # type: ignore[attr-defined]


@patch("tiny_mirror.services.fl_stock_correction_service.FLStockCorrectionLogRepository")
@patch("tiny_mirror.services.fl_stock_correction_service.AsyncSessionLocal")
async def test_handle_one_applies_balance_when_mismatch(
    mock_session_local: MagicMock,
    mock_repo_cls: MagicMock,
    service: FLStockCorrectionService,
) -> None:
    """Tiny 3 vs ML 5 → applies tipo=B with quantidade=5."""
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)
    mock_session.execute = AsyncMock()
    fake_result = MagicMock()
    fake_result.all = MagicMock(return_value=[])
    mock_session.execute.return_value = fake_result

    repo = MagicMock()
    repo.record = AsyncMock(return_value=1)
    mock_repo_cls.return_value = repo

    service._tiny.get_stock = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            {"depositos": [{"nome": "Full Mercado Livre", "saldo": 3}]},
            {"depositos": [{"nome": "Full Mercado Livre", "saldo": 5}]},  # post-correction
        ]
    )
    service._tiny.record_stock_movement = AsyncMock(  # type: ignore[method-assign]
        return_value={"idLancamento": 999}
    )

    result = await service._handle_one(tiny_id=100, sku="SKU-A", ml_qty=5)

    assert result == "corrected"
    service._tiny.record_stock_movement.assert_awaited_once()  # type: ignore[attr-defined]
    call_kwargs = service._tiny.record_stock_movement.await_args.kwargs  # type: ignore[attr-defined]
    assert call_kwargs["tipo"] == "B"
    assert call_kwargs["quantity"] == 5
    assert call_kwargs["price_unit"] == 0
    repo.record.assert_awaited_once()
    record_kwargs = repo.record.await_args.kwargs
    assert record_kwargs["correction_applied"] is True
    assert record_kwargs["tiny_saldo_before"] == 3
    assert record_kwargs["ml_qty"] == 5
    assert record_kwargs["delta"] == 2


@patch("tiny_mirror.services.fl_stock_correction_service.FLStockCorrectionLogRepository")
@patch("tiny_mirror.services.fl_stock_correction_service.AsyncSessionLocal")
async def test_handle_one_records_failure_when_tiny_rejects(
    mock_session_local: MagicMock,
    mock_repo_cls: MagicMock,
    service: FLStockCorrectionService,
) -> None:
    """If record_stock_movement raises (e.g. Tiny HTTP 400 for kit), audit row
    still gets written with correction_applied=False."""
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)
    fake_result = MagicMock()
    fake_result.all = MagicMock(return_value=[])
    mock_session.execute = AsyncMock(return_value=fake_result)

    repo = MagicMock()
    repo.record = AsyncMock(return_value=1)
    mock_repo_cls.return_value = repo

    service._tiny.get_stock = AsyncMock(  # type: ignore[method-assign]
        return_value={"depositos": [{"nome": "Full Mercado Livre", "saldo": 10}]}
    )
    service._tiny.record_stock_movement = AsyncMock(  # type: ignore[method-assign]
        side_effect=RuntimeError("Tiny HTTP 400 — kit rejected")
    )

    result = await service._handle_one(tiny_id=100, sku="2U-X", ml_qty=5)

    assert result == "corrected"  # we still claim the slot — it's logged
    repo.record.assert_awaited_once()
    record_kwargs = repo.record.await_args.kwargs
    assert record_kwargs["correction_applied"] is False
    assert "kit rejected" in (record_kwargs["error_message"] or "")


# ---------------------------------------------------------------------------
# run_correction smoke
# ---------------------------------------------------------------------------
@patch("tiny_mirror.services.fl_stock_correction_service.SyncLogRepository")
@patch("tiny_mirror.services.fl_stock_correction_service.AsyncSessionLocal")
async def test_run_correction_with_no_candidates(
    mock_session_local: MagicMock,
    mock_sync_log_cls: MagicMock,
    service: FLStockCorrectionService,
) -> None:
    """Empty candidate set → finalize sync_log and exit cleanly."""
    mock_session = AsyncMock()
    mock_session_local.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)
    fake_result = MagicMock()
    fake_result.all = MagicMock(return_value=[])
    mock_session.execute = AsyncMock(return_value=fake_result)

    sync_logs = MagicMock()
    sync_logs.update_sync_log_complete = AsyncMock()
    mock_sync_log_cls.return_value = sync_logs

    await service.run_correction(sync_log_id=7)

    sync_logs.update_sync_log_complete.assert_awaited_once_with(
        7, items_processed=0, items_failed=0
    )
