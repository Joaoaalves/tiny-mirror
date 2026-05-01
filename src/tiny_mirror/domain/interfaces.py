"""Repository interfaces — the domain layer's contracts.

Concrete implementations live under ``infrastructure/repositories``. Services
depend on these abstractions so they can be unit-tested with simple fakes
(no database required).
"""

from __future__ import annotations

import abc

from tiny_mirror.domain.models import OAuthToken


class TokenRepository(abc.ABC):
    """Persistence contract for the singleton OAuth2 token row."""

    @abc.abstractmethod
    async def get_current_token(self) -> OAuthToken | None:
        """Return the active token row, or ``None`` if the table is empty."""

    @abc.abstractmethod
    async def save_token(self, token: OAuthToken) -> None:
        """Insert or update the singleton row.

        Implementations must guarantee that at most one row exists in
        ``oauth_tokens`` and must update ``updated_at`` to the current time.
        """
