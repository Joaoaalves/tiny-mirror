"""The OpenAPI schema maps every route, so production must not serve it."""

from __future__ import annotations

import pytest

from tiny_mirror import main
from tiny_mirror.config import settings

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("app_env", "openapi_url", "docs_url"),
    [
        ("production", None, None),
        ("development", "/openapi.json", "/docs"),
    ],
)
def test_openapi_and_docs_only_in_development(
    monkeypatch: pytest.MonkeyPatch, app_env: str, openapi_url: str | None, docs_url: str | None
) -> None:
    monkeypatch.setattr(settings, "app_env", app_env)

    app = main.create_app()

    assert app.openapi_url == openapi_url
    assert app.docs_url == docs_url
