"""Service for transferring units from Galpão to Full ML via Tiny ERP API."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import structlog

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.infrastructure.orm.models import FulfillmentTransferORM
from tiny_mirror.infrastructure.repositories.fulfillment_transfer_repository import (
    FulfillmentTransferRepository,
)

logger = structlog.get_logger(__name__)

# Deposit IDs (Offshop specific — do not change without a migration)
DEPOSIT_GALPAO = 851264346
DEPOSIT_FULL_ML = 912048995

TRANSFER_OBS = "[AUTO] TRANSFERÊNCIA FULFILLMENT"


@dataclass
class TransferResult:
    id: int
    product_tiny_id: int
    product_sku: str
    quantity: int
    cost_per_unit: Decimal
    transferred_at: datetime
    status: str


class InsufficientStockError(Exception):
    """Raised when Galpão available stock is less than the requested quantity."""

    def __init__(self, sku: str, requested: int, available: float) -> None:
        self.sku = sku
        self.requested = requested
        self.available = available
        super().__init__(
            f"Insufficient Galpão stock for {sku}: requested {requested}, available {available}"
        )


class ProductNotFoundError(Exception):
    """Raised when the SKU is not in the products table."""

    def __init__(self, sku: str) -> None:
        self.sku = sku
        super().__init__(f"Product not found: {sku}")


class FulfillmentTransferService:
    def __init__(self, tiny_client: TinyAPIClient) -> None:
        self._tiny = tiny_client

    async def transfer_to_full(self, sku: str, quantity: int) -> TransferResult:
        """Transfer `quantity` units of `sku` from Galpão to Full ML.

        Steps:
        1. Look up product tiny_id from DB by SKU.
        2. Fetch current cost from Tiny GET /produtos/{id}.
        3. Fetch Galpão available stock from Tiny GET /estoque/{id}.
        4. POST Saída (exit) from Galpão.
        5. POST Entrada (entry) to Full ML.
        6. Record the transfer in fulfillment_transfers table.
        """
        from sqlalchemy import select

        from tiny_mirror.infrastructure.orm.models import ProductORM

        async with AsyncSessionLocal() as session:
            result = await session.execute(select(ProductORM.tiny_id).where(ProductORM.sku == sku))
            tiny_id = result.scalar_one_or_none()

        if tiny_id is None:
            raise ProductNotFoundError(sku)

        product_data = await self._tiny.get_product(tiny_id)
        cost_per_unit = _extract_cost(product_data)

        stock_data = await self._tiny.get_stock(tiny_id)
        galpao_available = _extract_deposit_available(stock_data, DEPOSIT_GALPAO)

        if galpao_available < quantity:
            raise InsufficientStockError(sku, quantity, galpao_available)

        now = datetime.now(UTC)
        data_str = now.strftime("%Y-%m-%d %H:%M:%S")

        logger.info(
            "Recording Galpão exit for fulfillment transfer",
            sku=sku,
            tiny_id=tiny_id,
            quantity=quantity,
            deposit_id=DEPOSIT_GALPAO,
        )
        await self._tiny.record_stock_movement(
            product_id=tiny_id,
            deposit_id=DEPOSIT_GALPAO,
            tipo="S",
            quantity=quantity,
            price_unit=float(cost_per_unit),
            data=data_str,
            observacoes=TRANSFER_OBS,
        )

        logger.info(
            "Recording Full ML entry for fulfillment transfer",
            sku=sku,
            tiny_id=tiny_id,
            quantity=quantity,
            deposit_id=DEPOSIT_FULL_ML,
        )
        await self._tiny.record_stock_movement(
            product_id=tiny_id,
            deposit_id=DEPOSIT_FULL_ML,
            tipo="E",
            quantity=quantity,
            price_unit=float(cost_per_unit),
            data=data_str,
            observacoes=TRANSFER_OBS,
        )

        async with AsyncSessionLocal() as session:
            repo = FulfillmentTransferRepository(session)
            row: FulfillmentTransferORM = await repo.create(
                product_tiny_id=tiny_id,
                product_sku=sku,
                quantity=quantity,
                cost_per_unit=cost_per_unit,
                transferred_at=now,
                notes=TRANSFER_OBS,
            )
            await session.commit()
            transfer_id = row.id

        logger.info(
            "Fulfillment transfer completed",
            transfer_id=transfer_id,
            sku=sku,
            quantity=quantity,
        )

        return TransferResult(
            id=transfer_id,
            product_tiny_id=tiny_id,
            product_sku=sku,
            quantity=quantity,
            cost_per_unit=cost_per_unit,
            transferred_at=now,
            status="pending",
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _extract_cost(product_data: dict[str, Any]) -> Decimal:
    """Pull precoCusto from the Tiny GET /produtos/{id} response."""
    item = product_data.get("produto") or product_data
    precos = item.get("precos") or {}
    raw = precos.get("precoCusto") or precos.get("preco") or "0"
    try:
        return Decimal(str(raw))
    except Exception:
        return Decimal("0")


def _extract_deposit_available(stock_data: dict[str, Any], deposit_id: int) -> float:
    """Return the disponivel value for a specific deposit from GET /estoque/{id}."""
    depositos = stock_data.get("depositos") or []
    for dep in depositos:
        if dep.get("id") == deposit_id or dep.get("deposito", {}).get("id") == deposit_id:
            return float(dep.get("disponivel") or dep.get("saldo") or 0)
    return 0.0
