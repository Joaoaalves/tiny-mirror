"""Unit tests for the feature-flag wrapper.

Goals:
- The wrapper reads from the live Settings instance (so env overrides
  apply naturally — no caching surprises).
- Unknown flag names return False instead of raising.
- public_state() round-trips every known flag.
"""

from __future__ import annotations

import pytest

from tiny_mirror.services import feature_flags

pytestmark = pytest.mark.unit


def test_unknown_flag_returns_false() -> None:
    assert feature_flags.is_enabled("does-not-exist") is False


def test_ml_promo_apply_reads_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    # Default in production is OFF.
    monkeypatch.setattr(feature_flags.settings, "ml_promo_apply_enabled", False, raising=True)
    assert feature_flags.is_enabled("ml_promo_apply") is False

    monkeypatch.setattr(feature_flags.settings, "ml_promo_apply_enabled", True, raising=True)
    assert feature_flags.is_enabled("ml_promo_apply") is True


def test_public_state_lists_every_known_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(feature_flags.settings, "ml_promo_apply_enabled", True, raising=True)
    state = feature_flags.public_state()
    # The wrapper currently exposes exactly one flag; if a new one is
    # added the registry must be updated and this assertion bumps too.
    assert "ml_promo_apply" in state
    assert state["ml_promo_apply"] is True
