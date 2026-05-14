"""Unit tests for FulfillmentTransferService."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tiny_mirror.services.fulfillment_transfer_service import (
    DEPOSIT_FULL_ML,
    DEPOSIT_GALPAO,
    FulfillmentTransferService,
    InsufficientStockError,
    ProductNotFoundError,
    _extract_cost,
    _extract_deposit_available,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helper extraction tests
# ---------------------------------------------------------------------------
class TestExtractCost:
    def test_extracts_preco_custo(self) -> None:
        data = {"produto": {"precos": {"precoCusto": "12.50"}}}
        assert _extract_cost(data) == Decimal("12.50")

    def test_falls_back_to_preco(self) -> None:
        data = {"produto": {"precos": {"preco": "9.99"}}}
        assert _extract_cost(data) == Decimal("9.99")

    def test_returns_zero_on_missing(self) -> None:
        assert _extract_cost({}) == Decimal("0")

    def test_flat_response_shape(self) -> None:
        data = {"precos": {"precoCusto": "7.00"}}
        assert _extract_cost(data) == Decimal("7.00")


class TestExtractDepositAvailable:
    def test_finds_deposit_by_id(self) -> None:
        stock = {
            "depositos": [
                {"id": 851264346, "disponivel": 50},
                {"id": 912048995, "disponivel": 10},
            ]
        }
        assert _extract_deposit_available(stock, 851264346) == 50.0

    def test_returns_zero_when_not_found(self) -> None:
        stock = {"depositos": [{"id": 999, "disponivel": 5}]}
        assert _extract_deposit_available(stock, 851264346) == 0.0

    def test_handles_nested_deposito_key(self) -> None:
        stock = {
            "depositos": [
                {"deposito": {"id": 851264346}, "disponivel": 25},
            ]
        }
        assert _extract_deposit_available(stock, 851264346) == 25.0

    def test_empty_depositos(self) -> None:
        assert _extract_deposit_available({"depositos": []}, DEPOSIT_GALPAO) == 0.0


# ---------------------------------------------------------------------------
# Service tests
# ---------------------------------------------------------------------------
@pytest.fixture
def tiny_client() -> AsyncMock:
    client = AsyncMock()
    client.get_product = AsyncMock(return_value={"produto": {"precos": {"precoCusto": "15.00"}}})
    client.get_stock = AsyncMock(
        return_value={
            "depositos": [
                {"id": DEPOSIT_GALPAO, "disponivel": 100},
                {"id": DEPOSIT_FULL_ML, "disponivel": 5},
            ]
        }
    )
    client.record_stock_movement = AsyncMock(return_value={"ok": True})
    return client


_FAKE_TRANSFER = MagicMock(
    id=42,
    product_tiny_id=971992238,
    product_sku="SKU-TEST-FULL",
    quantity=10,
    cost_per_unit=Decimal("15.00"),
    transferred_at=datetime(2026, 5, 14, 12, 0, 0, tzinfo=UTC),
    status="pending",
)


class TestFulfillmentTransferService:
    @pytest.mark.asyncio
    async def test_successful_transfer(self, tiny_client: AsyncMock) -> None:
        service = FulfillmentTransferService(tiny_client=tiny_client)

        with (
            patch(
                "tiny_mirror.services.fulfillment_transfer_service.AsyncSessionLocal"
            ) as mock_session_factory,
        ):
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session.commit = AsyncMock()
            mock_session_factory.return_value = mock_session

            mock_repo = AsyncMock()
            mock_repo.create = AsyncMock(return_value=_FAKE_TRANSFER)

            with patch(
                "tiny_mirror.services.fulfillment_transfer_service.FulfillmentTransferRepository",
                return_value=mock_repo,
            ):
                # First session: product lookup
                mock_execute_result = MagicMock()
                mock_execute_result.scalar_one_or_none.return_value = 971992238
                mock_session.execute = AsyncMock(return_value=mock_execute_result)

                result = await service.transfer_to_full(sku="SKU-TEST-FULL", quantity=10)

        assert result.product_sku == "SKU-TEST-FULL"
        assert result.quantity == 10
        assert result.status == "pending"

        # Verify Tiny API calls
        assert tiny_client.get_product.called
        assert tiny_client.get_stock.called
        calls = tiny_client.record_stock_movement.call_args_list
        assert len(calls) == 2
        # First call: Saída from Galpão
        assert calls[0].kwargs["tipo"] == "S"
        assert calls[0].kwargs["deposit_id"] == DEPOSIT_GALPAO
        assert calls[0].kwargs["quantity"] == 10
        # Second call: Entrada to Full ML
        assert calls[1].kwargs["tipo"] == "E"
        assert calls[1].kwargs["deposit_id"] == DEPOSIT_FULL_ML

    @pytest.mark.asyncio
    async def test_product_not_found_raises(self, tiny_client: AsyncMock) -> None:
        service = FulfillmentTransferService(tiny_client=tiny_client)

        with patch(
            "tiny_mirror.services.fulfillment_transfer_service.AsyncSessionLocal"
        ) as mock_session_factory:
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session

            mock_execute_result = MagicMock()
            mock_execute_result.scalar_one_or_none.return_value = None
            mock_session.execute = AsyncMock(return_value=mock_execute_result)

            with pytest.raises(ProductNotFoundError) as exc_info:
                await service.transfer_to_full(sku="NONEXISTENT", quantity=5)

        assert exc_info.value.sku == "NONEXISTENT"

    @pytest.mark.asyncio
    async def test_insufficient_stock_raises(self, tiny_client: AsyncMock) -> None:
        tiny_client.get_stock = AsyncMock(
            return_value={"depositos": [{"id": DEPOSIT_GALPAO, "disponivel": 3}]}
        )
        service = FulfillmentTransferService(tiny_client=tiny_client)

        with patch(
            "tiny_mirror.services.fulfillment_transfer_service.AsyncSessionLocal"
        ) as mock_session_factory:
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_factory.return_value = mock_session

            mock_execute_result = MagicMock()
            mock_execute_result.scalar_one_or_none.return_value = 971992238
            mock_session.execute = AsyncMock(return_value=mock_execute_result)

            with pytest.raises(InsufficientStockError) as exc_info:
                await service.transfer_to_full(sku="SKU-TEST-FULL", quantity=10)

        assert exc_info.value.requested == 10
        assert exc_info.value.available == 3.0
        # Tiny API movement calls must NOT have been made
        tiny_client.record_stock_movement.assert_not_called()
