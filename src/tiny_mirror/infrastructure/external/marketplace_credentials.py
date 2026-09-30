"""Read-only access to marketplace credentials exported from OpenClaw.

OpenClaw owns the integrations (and the Shopee token refresh); a root cron
(``deploy/export_marketplace_credentials.py``) copies what tiny-mirror needs
into ``settings.marketplace_credentials_file``. This module re-reads the file
when it changes, so a Shopee token refresh or a renewed partner key reaches
the running service without a restart.
"""

from __future__ import annotations

import json
import os
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class MarketplaceCredentials:
    def __init__(self, path: str) -> None:
        self._path = path
        self._mtime: float | None = None
        self._data: dict[str, Any] = {}

    def section(self, name: str) -> dict[str, Any] | None:
        """The credentials of one integration, or None when absent/unreadable."""
        if not self._path:
            return None
        try:
            mtime = os.stat(self._path).st_mtime
            if mtime != self._mtime:
                with open(self._path) as fh:
                    self._data = json.load(fh)
                self._mtime = mtime
        except (OSError, ValueError) as exc:
            logger.warning("marketplace_credentials.unreadable", path=self._path, error=str(exc))
            return None
        section = self._data.get(name)
        return section if isinstance(section, dict) else None
