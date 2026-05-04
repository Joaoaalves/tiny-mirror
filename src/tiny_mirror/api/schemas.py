"""Pydantic V2 response models exposed by the REST API.

Every model uses ``model_config = ConfigDict(from_attributes=True)`` so a
handler can pass either a dict or an ORM instance and Pydantic will pick
out the right attributes.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class _Base(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------
class PaginationResponse(_Base):
    page: int
    page_size: int
    total: int
    total_pages: int


# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------
class ProductPrices(_Base):
    price: float | None = None
    promotional_price: float | None = None
    cost_price: float | None = None
    average_cost_price: float | None = None


class StockSummary(_Base):
    balance: float
    reserved: float
    available: float
    location: str | None = None


class StockDepositResponse(_Base):
    deposit_tiny_id: int
    deposit_name: str
    ignore: bool
    balance: float
    reserved: float
    available: float
    company: str | None = None


class KitComponentResponse(_Base):
    component_sku: str
    component_description: str | None = None
    component_type: str | None = None
    quantity: float
    component_tiny_id: int | None = None


class SaleBucketResponse(_Base):
    bucket_date: date
    ecommerce_name: str
    quantity_sold: float
    total_revenue: float
    order_count: int
    is_kit_expansion: bool
    source_kit_sku: str | None = None


class ProductListItem(_Base):
    tiny_id: int
    sku: str
    description: str
    type: str
    situation: str
    unit: str | None = None
    gtin: str | None = None
    prices: ProductPrices = Field(default_factory=ProductPrices)
    stock: StockSummary | None = None
    variation_type: str | None = None
    created_at_tiny: datetime | None = None
    updated_at_tiny: datetime | None = None
    synced_at: datetime


class ProductDetailResponse(ProductListItem):
    complementary_description: str | None = None
    ncm: str | None = None
    origin: str | None = None
    warranty: str | None = None
    observations: str | None = None
    category_id: int | None = None
    category_name: str | None = None
    category_full_path: str | None = None
    brand_id: int | None = None
    brand_name: str | None = None
    dimensions: dict[str, Any] | None = None
    suppliers: list[Any] = Field(default_factory=list)
    taxation: dict[str, Any] | None = None
    attachments: list[Any] = Field(default_factory=list)
    stock_deposits: list[StockDepositResponse] = Field(default_factory=list)
    kit_components: list[KitComponentResponse] = Field(default_factory=list)
    sale_buckets_90d: list[SaleBucketResponse] = Field(default_factory=list)


class ProductListResponse(_Base):
    items: list[ProductListItem]
    pagination: PaginationResponse


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------
class OrderListItem(_Base):
    tiny_id: int
    order_number: int
    situation: int
    order_date: date
    total_order_value: float | None = None
    ecommerce_name: str | None = None
    ecommerce_order_number: str | None = None
    warehouse_name: str | None = None
    shipping_date: datetime | None = None
    synced_at: datetime


class OrderItemResponse(_Base):
    product_sku: str
    product_description: str | None = None
    product_type: str | None = None
    quantity: float
    unit_value: float
    additional_info: str | None = None


class OrderDetailResponse(OrderListItem):
    invoice_id: int | None = None
    invoice_date: date | None = None
    total_products_value: float | None = None
    price_list: dict[str, Any] | None = None
    customer: dict[str, Any]
    delivery_address: dict[str, Any] | None = None
    ecommerce_id: int | None = None
    sales_channel: str | None = None
    channel_order_number: str | None = None
    carrier: dict[str, Any] | None = None
    warehouse_id: int | None = None
    seller: dict[str, Any] | None = None
    operation_nature: dict[str, Any] | None = None
    intermediary: dict[str, Any] | None = None
    payment: dict[str, Any] | None = None
    integrated_payments: list[Any] = Field(default_factory=list)
    delivery_date: date | None = None
    purchase_order_number: str | None = None
    discount_value: float = 0
    shipping_value: float = 0
    other_expenses_value: float = 0
    expected_date: date | None = None
    observations: str | None = None
    internal_observations: str | None = None
    order_origin: int = 0
    items: list[OrderItemResponse] = Field(default_factory=list)


class OrderListResponse(_Base):
    items: list[OrderListItem]
    pagination: PaginationResponse


# ---------------------------------------------------------------------------
# Sync logs
# ---------------------------------------------------------------------------
class SyncLogResponse(_Base):
    id: int
    sync_type: str
    status: str
    started_at: datetime
    completed_at: datetime | None = None
    items_processed: int
    items_failed: int
    error_message: str | None = None
    metadata: dict[str, Any] | None = None


class SyncLogListResponse(_Base):
    items: list[SyncLogResponse]
    pagination: PaginationResponse


class SyncTriggerResponse(_Base):
    message: str
    sync_log_id: int


class SyncReconciliationResponse(_Base):
    message: str
    sync_log_ids: list[int]
    days_count: int
    date_from: date
    date_to: date


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------
class HealthResponse(_Base):
    status: str
    timestamp: datetime
    version: str
    environment: str
    components: dict[str, str] | None = None


# ---------------------------------------------------------------------------
# Webhooks (Tiny -> tiny-mirror)
# ---------------------------------------------------------------------------
class _WebhookBase(BaseModel):
    """Base for webhook payloads — accepts both alias (Tiny camelCase) and
    snake_case so tests / consumers can build payloads either way.
    """

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class OrderWebhookData(_WebhookBase):
    id_pedido_ecommerce: str = Field(alias="idPedidoEcommerce")
    id_venda_tiny: int = Field(alias="idVendaTiny")
    situacao: str
    descricao_situacao: str = Field(alias="descricaoSituacao")


class OrderWebhookPayload(_WebhookBase):
    cnpj: str
    id_ecommerce: str | int = Field(alias="idEcommerce")
    tipo: str
    versao: str
    dados: OrderWebhookData


class StockWebhookData(_WebhookBase):
    tipo_estoque: str = Field(alias="tipoEstoque")
    saldo: float
    id_produto: int = Field(alias="idProduto")
    sku: str
    sku_mapeamento: str | None = Field(default=None, alias="skuMapeamento")
    sku_mapeamento_pai: str | None = Field(default=None, alias="skuMapeamentoPai")

    @field_validator("tipo_estoque")
    @classmethod
    def _validate_tipo_estoque(cls, value: str) -> str:
        if value not in ("F", "D"):
            raise ValueError(f"tipoEstoque must be 'F' (Físico) or 'D' (Disponível); got {value!r}")
        return value


class StockWebhookPayload(_WebhookBase):
    cnpj: str
    id_ecommerce: str | int = Field(alias="idEcommerce")
    tipo: str
    versao: str
    dados: StockWebhookData


class WebhookAck(_Base):
    """Stable response shape for every webhook endpoint."""

    status: str
