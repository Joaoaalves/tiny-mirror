"""Unit tests for `_parse_iso_dt` — the ML ISO-timestamp parser used to
capture promo start/finish dates from the eligible-promos payload.

ML sends e.g. "2026-06-08T00:00:00-03:00" (offset) or a naive
"2026-06-08T00:00:00"; we normalize both to an aware datetime (UTC when
no offset). Garbage / missing must degrade to None, never crash the cron.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tiny_mirror.services.ml_promotion_service import _parse_iso_dt

pytestmark = pytest.mark.unit


def test_offset_aware_is_preserved() -> None:
    dt = _parse_iso_dt("2026-06-08T00:00:00-03:00")
    assert dt is not None
    assert dt.tzinfo is not None
    # Same instant as 03:00Z.
    assert dt.astimezone(UTC) == datetime(2026, 6, 8, 3, 0, tzinfo=UTC)


def test_naive_is_assumed_utc() -> None:
    dt = _parse_iso_dt("2026-06-08T00:00:00")
    assert dt == datetime(2026, 6, 8, 0, 0, tzinfo=UTC)


def test_utc_zulu() -> None:
    dt = _parse_iso_dt("2026-06-08T12:30:00+00:00")
    assert dt == datetime(2026, 6, 8, 12, 30, tzinfo=UTC)


@pytest.mark.parametrize("raw", [None, "", "not-a-date", "2026-13-99", 12345.0])
def test_missing_or_invalid_returns_none(raw: object) -> None:
    assert _parse_iso_dt(raw) is None
