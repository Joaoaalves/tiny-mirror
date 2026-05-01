"""Unit tests for the Seq logging handler.

The HTTP shipping path is intentionally not exercised here — it is best
verified end-to-end against a live Seq instance. These tests cover the
CLEF translation, since that is where bugs would silently corrupt every
shipped event.
"""

from __future__ import annotations

import json

import pytest

from tiny_mirror.observability.seq_handler import (
    SeqHandler,
    _to_clef_level,
)

pytestmark = pytest.mark.unit


def test_to_clef_level_maps_known_levels() -> None:
    assert _to_clef_level("debug") == "Debug"
    assert _to_clef_level("info") == "Information"
    assert _to_clef_level("warning") == "Warning"
    assert _to_clef_level("error") == "Error"
    assert _to_clef_level("critical") == "Fatal"


def test_to_clef_level_handles_unknown_and_non_string() -> None:
    assert _to_clef_level("trace") == "Information"
    assert _to_clef_level(None) == "Information"
    assert _to_clef_level(42) == "Information"


def test_to_clef_translates_structlog_event() -> None:
    event = {
        "timestamp": "2026-05-01T12:00:00Z",
        "event": "Product synced",
        "level": "info",
        "logger": "tiny_mirror.services.product_sync_service",
        "tiny_id": 12345,
        "sku": "ABC-123",
    }
    clef = SeqHandler._to_clef(event)
    assert clef["@t"] == "2026-05-01T12:00:00Z"
    assert clef["@m"] == "Product synced"
    assert clef["@l"] == "Information"
    assert clef["tiny_id"] == 12345
    assert clef["sku"] == "ABC-123"
    assert "timestamp" not in clef
    assert "event" not in clef
    assert "level" not in clef


def test_to_clef_emits_exception_alias() -> None:
    event = {
        "level": "error",
        "exception": "Traceback (most recent call last):\n  ...",
        "event": "boom",
    }
    clef = SeqHandler._to_clef(event)
    assert clef["@x"] == "Traceback (most recent call last):\n  ..."
    assert clef["@m"] == "boom"
    assert clef["@l"] == "Error"
    assert "exception" not in clef


def test_to_clef_serialization_is_json_safe() -> None:
    event = {
        "timestamp": "2026-05-01T12:00:00Z",
        "event": "ok",
        "level": "info",
        "nested": {"a": 1, "b": [1, 2]},
    }
    clef = SeqHandler._to_clef(event)
    serialized = json.dumps(clef)
    assert "Information" in serialized
    assert '"@m": "ok"' in serialized
