"""Unit tests for MLPanelScrapeService._cookie_header.

Backport of a prod hotfix (2026-09-15): the host's cookie jar can carry the
optional UI cookie LAST_SEARCH as raw Unicode (e.g. an accented search term).
HTTP headers must be ASCII, so httpx failed every sweep. LAST_SEARCH is now
dropped; any other non-ASCII cookie means a broken session export.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tiny_mirror.services.ml_panel_scrape_service import (
    MLPanelScrapeService,
    PanelSessionExpired,
)

pytestmark = pytest.mark.unit


def _jar(tmp_path: Path, cookies: list[tuple[str, str]]) -> str:
    lines = ["# Netscape HTTP Cookie File"]
    for name, value in cookies:
        lines.append(f".mercadolivre.com.br\tTRUE\t/\tTRUE\t1893456000\t{name}\t{value}")
    path = tmp_path / "jar.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


def _service(jar_path: str) -> MLPanelScrapeService:
    return MLPanelScrapeService(MagicMock(), cookie_jar_path=jar_path, ml_user_id="123")


def test_ascii_cookies_are_joined(tmp_path: Path) -> None:
    header = _service(_jar(tmp_path, [("ssid", "abc"), ("orguserid", "42")]))._cookie_header()

    assert header == "ssid=abc; orguserid=42"


def test_non_ascii_last_search_is_dropped(tmp_path: Path) -> None:
    jar = _jar(tmp_path, [("ssid", "abc"), ("LAST_SEARCH", "lâmpada"), ("nsa_rotok", "x")])

    header = _service(jar)._cookie_header()

    assert header == "ssid=abc; nsa_rotok=x"
    assert header.isascii()


def test_ascii_last_search_is_kept(tmp_path: Path) -> None:
    header = _service(_jar(tmp_path, [("LAST_SEARCH", "lampada")]))._cookie_header()

    assert header == "LAST_SEARCH=lampada"


def test_other_non_ascii_cookie_means_broken_session(tmp_path: Path) -> None:
    jar = _jar(tmp_path, [("ssid", "ábc")])

    with pytest.raises(PanelSessionExpired):
        _service(jar)._cookie_header()
