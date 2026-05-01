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


class OrderRepository(abc.ABC):
    """Persistence contract for orders and their line items."""

    @abc.abstractmethod
    async def upsert(self, order_data: dict[str, Any]) -> str:
        """Insert or update an order row. Returns ``"created"`` or ``"updated"``."""

    @abc.abstractmethod
    async def upsert_items(
        self, order_tiny_id: int, items: list[dict[str, Any]]
    ) -> None:
        """Replace every line item of an order atomically (DELETE + bulk INSERT)."""

    @abc.abstractmethod
    async def get_by_tiny_id(self, tiny_id: int) -> dict[str, Any] | None:
        """Return the order with its items as a nested ``items`` field."""

    @abc.abstractmethod
    async def get_recent_product_tiny_ids(self, hours: int) -> list[int]:
        """Return DISTINCT product tiny ids from items of orders updated in
        the last ``hours`` hours. Used to fan out incremental stock sync.
        """

    @abc.abstractmethod
    async def exists(self, tiny_id: int) -> bool:
        """Return whether an order row with the given tiny id is in the DB."""

    @abc.abstractmethod
    async def count(self) -> int:
        """Return the number of orders currently stored."""

    @abc.abstractmethod
    async def get_orders_in_period(
        self, date_from: Any, date_to: Any
    ) -> list[dict[str, Any]]:
        """Return orders (with items) whose ``order_date`` falls in the range."""


class StockRepository(abc.ABC):
    """Persistence contract for product stock and deposit-level breakdown."""

    @abc.abstractmethod
    async def upsert(self, stock_data: dict[str, Any]) -> None:
        """Insert or update the stock row for ``product_tiny_id``."""

    @abc.abstractmethod
    async def upsert_deposits(
        self, product_tiny_id: int, deposits: list[dict[str, Any]]
    ) -> None:
        """Atomically replace every deposit row for the given product."""

    @abc.abstractmethod
    async def get_product_tiny_ids_to_sync(self) -> list[int]:
        """Return the tiny ids of every active product (situation='A')."""

    @abc.abstractmethod
    async def get_by_product_tiny_id(
        self, product_tiny_id: int
    ) -> dict[str, Any] | None:
        """Return the stock row with its ``deposits`` array."""
