"""Features retired from the dashboard must not keep serving routes."""

from __future__ import annotations

import pytest

from tiny_mirror import main

pytestmark = pytest.mark.unit

RETIRED_PREFIXES = ["/ml-fl-tracking"]


@pytest.mark.parametrize("prefix", RETIRED_PREFIXES)
def test_retired_feature_has_no_routes(prefix: str) -> None:
    paths = [getattr(route, "path", "") for route in main.create_app().routes]

    assert not [path for path in paths if path.startswith(prefix)]
