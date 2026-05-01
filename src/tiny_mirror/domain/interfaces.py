"""Repository interfaces — the domain layer's contracts.

Concrete implementations live under ``infrastructure/repositories``. Services
depend on these abstractions so they can be unit-tested with simple fakes
(no database required).
"""

from __future__ import annotations

import abc
from typing import Any

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


class ProductRepository(abc.ABC):
    """Persistence contract for products and their kit components."""

    @abc.abstractmethod
    async def upsert(self, product_data: dict[str, Any]) -> str:
        """Insert or update a product row. Returns ``"created"`` or ``"updated"``."""

    @abc.abstractmethod
    async def get_by_tiny_id(self, tiny_id: int) -> dict[str, Any] | None:
        """Return the product as a dict, or ``None`` if not found."""

    @abc.abstractmethod
    async def get_by_sku(self, sku: str) -> dict[str, Any] | None:
        """Return the product as a dict, or ``None`` if not found."""

    @abc.abstractmethod
    async def list_active(self) -> list[int]:
        """Return all ``tiny_id``s with ``situation = 'A'``."""

    @abc.abstractmethod
    async def count(self) -> int:
        """Return the total number of products in the table."""

    @abc.abstractmethod
    async def upsert_kit_components(
        self, kit_tiny_id: int, components: list[dict[str, Any]]
    ) -> None:
        """Replace the components of a kit atomically (DELETE + INSERT)."""

    @abc.abstractmethod
    async def get_kit_components(
        self, kit_tiny_id: int
    ) -> list[dict[str, Any]]:
        """Return the components of a kit, in insertion order."""
