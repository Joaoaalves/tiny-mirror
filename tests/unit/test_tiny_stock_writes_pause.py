"""TINY_STOCK_WRITES_ENABLED=false must gate every Tiny stock writer."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tiny_mirror.services.fl_stock_correction_service import FLStockCorrectionService

pytestmark = pytest.mark.unit


async def test_fl_correction_skips_entirely_when_writes_paused() -> None:
    tiny = AsyncMock()
    service = FLStockCorrectionService(tiny_client=tiny)
    service._load_candidates = AsyncMock()  # type: ignore[method-assign]

    fake_session = AsyncMock()
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)
    sync_logs = MagicMock()
    sync_logs.update_sync_log_complete = AsyncMock()

    with (
        patch("tiny_mirror.services.fl_stock_correction_service.settings") as mock_settings,
        patch(
            "tiny_mirror.services.fl_stock_correction_service.AsyncSessionLocal",
            return_value=fake_session,
        ),
        patch(
            "tiny_mirror.services.fl_stock_correction_service.SyncLogRepository",
            return_value=sync_logs,
        ),
    ):
        mock_settings.tiny_stock_writes_enabled = False
        await service.run_correction(sync_log_id=77)

    # nothing touched Tiny, no candidates were even loaded, log finalized clean
    service._load_candidates.assert_not_awaited()
    tiny.record_stock_movement.assert_not_awaited()
    sync_logs.update_sync_log_complete.assert_awaited_once_with(
        77, items_processed=0, items_failed=0
    )


def test_flag_defaults_to_enabled() -> None:
    from tiny_mirror.config import Settings

    assert Settings.model_fields["tiny_stock_writes_enabled"].default is True
