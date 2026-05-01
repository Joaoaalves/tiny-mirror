"""SQLAlchemy 2.0 ORM models for every persisted entity.

Every model maps a table described in stage 02. Comments on tables and
columns are propagated to PostgreSQL via SQLAlchemy's ``comment=`` argument
so the read-only LLM user can introspect the schema.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from tiny_mirror.database import Base


# ---------------------------------------------------------------------------
# oauth_tokens
# ---------------------------------------------------------------------------
class OAuthTokenORM(Base):
    __tablename__ = "oauth_tokens"
    __table_args__ = {
        "comment": (
            "Stores the single active OAuth2 token for Tiny ERP API authentication. "
            "Always contains exactly one row. Updated on every token refresh cycle "
            "(every 2 hours proactively, or on 401 response)."
        ),
    }

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    access_token: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="JWT access token used in Authorization: Bearer header. Valid for 4 hours.",
    )
    refresh_token: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment=(
            "Token used to obtain a new access_token without re-authentication. "
            "Valid for 1 day."
        ),
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment=(
            "UTC timestamp when the access_token expires. Service rotates "
            "proactively when less than 30 minutes remain."
        ),
    )
    refresh_expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment=(
            "UTC timestamp when the refresh_token expires. If this is past, manual "
            "re-authentication is required."
        ),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


# ---------------------------------------------------------------------------
# products
# ---------------------------------------------------------------------------
class ProductORM(Base):
    __tablename__ = "products"
    __table_args__ = (
        CheckConstraint("type IN ('P', 'S', 'K')", name="valid_type"),
        CheckConstraint("situation IN ('A', 'I', 'E')", name="valid_situation"),
        Index("ix_products_type", "type"),
        Index("ix_products_situation", "situation"),
        Index("ix_products_updated_at_tiny", "updated_at_tiny"),
        Index("ix_products_brand_name", "brand_name"),
        {
            "comment": (
                "Mirror of all products from Tiny ERP. Products of type K (kit) have "
                "their components stored in product_kit_components. The prices and "
                "dimensions columns use JSONB for flexibility as the Tiny API schema "
                "may evolve. The stock_quantity field is a snapshot from the listing "
                "API; authoritative stock data is in the stock table."
            ),
        },
    )

    tiny_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment=(
            "Unique product identifier in Tiny ERP. Used as primary key. "
            "Never changes for a given product."
        ),
    )
    sku: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        unique=True,
        comment=(
            "Stock Keeping Unit code. Unique identifier used in orders and stock "
            "management."
        ),
    )
    description: Mapped[str] = mapped_column(Text, nullable=False)
    complementary_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    type: Mapped[str] = mapped_column(
        String(1),
        nullable=False,
        comment=(
            "Product type: P=Physical Product, S=Service, K=Kit/Bundle. "
            "Kit products have components in product_kit_components table."
        ),
    )
    situation: Mapped[str] = mapped_column(
        String(1),
        nullable=False,
        comment="Product status in Tiny: A=Active (synced), I=Inactive, E=Deleted/Archived.",
    )
    parent_product_tiny_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("products.tiny_id", ondelete="SET NULL"),
        nullable=True,
    )
    unit: Mapped[str | None] = mapped_column(String(20), nullable=True)
    unit_per_box: Mapped[str | None] = mapped_column(String(20), nullable=True)
    ncm: Mapped[str | None] = mapped_column(String(20), nullable=True)
    gtin: Mapped[str | None] = mapped_column(String(20), nullable=True)
    origin: Mapped[str | None] = mapped_column(String(5), nullable=True)
    warranty: Mapped[str | None] = mapped_column(String(100), nullable=True)
    observations: Mapped[str | None] = mapped_column(Text, nullable=True)
    category_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    category_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    category_full_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    brand_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    brand_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    dimensions: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    prices: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        comment=(
            "JSONB with price, promotional_price, cost_price, average_cost_price as "
            "decimal numbers."
        ),
    )
    stock_control: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    stock_on_order: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    stock_preparation_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    stock_location: Mapped[str | None] = mapped_column(String(200), nullable=True)
    stock_min: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    stock_max: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    stock_quantity: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    suppliers: Mapped[list[Any] | None] = mapped_column(
        JSONB,
        nullable=True,
        server_default=text("'[]'::jsonb"),
    )
    seo: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    taxation: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    attachments: Mapped[list[Any] | None] = mapped_column(
        JSONB,
        nullable=True,
        server_default=text("'[]'::jsonb"),
    )
    variation_type: Mapped[str | None] = mapped_column(String(5), nullable=True)
    created_at_tiny: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    updated_at_tiny: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment=(
            "Last time this record was fetched from the Tiny API. "
            "Used to detect stale data."
        ),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


# ---------------------------------------------------------------------------
# product_kit_components
# ---------------------------------------------------------------------------
class ProductKitComponentORM(Base):
    __tablename__ = "product_kit_components"
    __table_args__ = (
        UniqueConstraint(
            "kit_product_tiny_id",
            "component_sku",
            name="uq_product_kit_components_kit_product_tiny_id_component_sku",
        ),
        Index(
            "ix_product_kit_components_kit_product_tiny_id",
            "kit_product_tiny_id",
        ),
        Index(
            "ix_product_kit_components_component_sku",
            "component_sku",
        ),
        Index(
            "ix_product_kit_components_component_product_tiny_id",
            "component_product_tiny_id",
        ),
        {
            "comment": (
                "Components of kit/bundle products (type K). When a kit is sold, each "
                "component's quantity sold must be computed as kit_quantity * "
                "component_quantity. The component_product_tiny_id may be NULL if the "
                "component product has not been synced yet; component_sku is always "
                "populated and should be used for lookups."
            ),
        },
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    kit_product_tiny_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("products.tiny_id", ondelete="CASCADE"),
        nullable=False,
    )
    component_product_tiny_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("products.tiny_id", ondelete="SET NULL"),
        nullable=True,
    )
    component_sku: Mapped[str] = mapped_column(String(100), nullable=False)
    component_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    component_type: Mapped[str | None] = mapped_column(String(1), nullable=True)
    quantity: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


# ---------------------------------------------------------------------------
# orders
# ---------------------------------------------------------------------------
class OrderORM(Base):
    __tablename__ = "orders"
    __table_args__ = (
        CheckConstraint("situation BETWEEN 0 AND 9", name="valid_situation"),
        Index("ix_orders_order_number", "order_number"),
        Index("ix_orders_situation", "situation"),
        Index("ix_orders_order_date", "order_date"),
        Index("ix_orders_ecommerce_id", "ecommerce_id"),
        Index("ix_orders_ecommerce_order_number", "ecommerce_order_number"),
        Index("ix_orders_shipping_date", "shipping_date"),
        Index("ix_orders_updated_at", "updated_at"),
        {
            "comment": (
                "Mirror of all orders from Tiny ERP. Nested objects (customer, "
                "carrier, payment) are stored as JSONB to preserve the complete Tiny "
                "API structure. Order items are stored separately in order_items. The "
                "situation column uses Tiny's numeric codes: 0=Open, 1=Invoiced, "
                "2=Cancelled, 3=Approved, 4=PreparingShipment, 5=Shipped, 6=Delivered, "
                "7=ReadyToShip, 8=IncompleteData, 9=NotDelivered."
            ),
        },
    )

    tiny_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment=(
            "Unique order identifier in Tiny ERP. Primary key. "
            "Used for deduplication in upsert operations."
        ),
    )
    order_number: Mapped[int] = mapped_column(Integer, nullable=False, unique=True)
    invoice_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    invoice_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    total_products_value: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 2), nullable=True
    )
    total_order_value: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 2), nullable=True
    )
    price_list: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    customer: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        comment=(
            "JSONB with full customer data: name, tax_id (CPF/CNPJ), email, phone, "
            "address fields. Query with customer->>'name' syntax."
        ),
    )
    delivery_address: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    ecommerce_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    ecommerce_name: Mapped[str | None] = mapped_column(
        String(200),
        nullable=True,
        comment=(
            "Name of the e-commerce platform (e.g., 'Shopify', 'Mercado Livre'). "
            "NULL for direct sales. Used in sale_buckets for revenue attribution."
        ),
    )
    ecommerce_order_number: Mapped[str | None] = mapped_column(String(100), nullable=True)
    channel_order_number: Mapped[str | None] = mapped_column(String(100), nullable=True)
    sales_channel: Mapped[str | None] = mapped_column(String(100), nullable=True)
    carrier: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
        comment=(
            "JSONB with carrier info: name, shipping_method, tracking_code, "
            "tracking_url, freight_type."
        ),
    )
    warehouse_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    warehouse_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    seller: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    operation_nature: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    intermediary: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    payment: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    integrated_payments: Mapped[list[Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
    )
    situation: Mapped[int] = mapped_column(
        SmallInteger,
        nullable=False,
        comment=(
            "Order status as integer. See table comment for value mapping. "
            "Use to filter active/completed/cancelled orders."
        ),
    )
    order_date: Mapped[date] = mapped_column(Date, nullable=False)
    delivery_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    purchase_order_number: Mapped[str | None] = mapped_column(String(100), nullable=True)
    discount_value: Mapped[Decimal] = mapped_column(
        Numeric(12, 2), nullable=False, server_default=text("0")
    )
    shipping_value: Mapped[Decimal] = mapped_column(
        Numeric(12, 2), nullable=False, server_default=text("0")
    )
    other_expenses_value: Mapped[Decimal] = mapped_column(
        Numeric(12, 2), nullable=False, server_default=text("0")
    )
    expected_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    shipping_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    observations: Mapped[str | None] = mapped_column(Text, nullable=True)
    internal_observations: Mapped[str | None] = mapped_column(Text, nullable=True)
    order_origin: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, server_default=text("0")
    )
    synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


# ---------------------------------------------------------------------------
# order_items
# ---------------------------------------------------------------------------
class OrderItemORM(Base):
    __tablename__ = "order_items"
    __table_args__ = (
        Index("ix_order_items_order_tiny_id", "order_tiny_id"),
        Index("ix_order_items_product_sku", "product_sku"),
        Index("ix_order_items_product_tiny_id", "product_tiny_id"),
        {
            "comment": (
                "Line items of each order. Each row is one product in one order. "
                "product_type K (kit) items should be expanded using "
                "product_kit_components for individual component counting. The "
                "product_tiny_id may be NULL if the product was not yet synced when "
                "the order was processed; always use product_sku as the reliable "
                "identifier."
            ),
        },
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_tiny_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("orders.tiny_id", ondelete="CASCADE"),
        nullable=False,
    )
    product_tiny_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("products.tiny_id", ondelete="SET NULL"),
        nullable=True,
    )
    product_sku: Mapped[str] = mapped_column(String(100), nullable=False)
    product_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    product_type: Mapped[str | None] = mapped_column(String(1), nullable=True)
    quantity: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    unit_value: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    additional_info: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ---------------------------------------------------------------------------
# stock
# ---------------------------------------------------------------------------
class StockORM(Base):
    __tablename__ = "stock"
    __table_args__ = (
        Index("ix_stock_sku", "sku"),
        Index(
            "ix_stock_available",
            "available",
            postgresql_where=text("available > 0"),
        ),
        {
            "comment": (
                "Current stock levels for each product. The balance is the physical "
                "stock count, reserved is the quantity committed to open orders, and "
                "available = balance - reserved. Updated by webhooks (immediate) and "
                "by scheduled sync (hourly for products appearing in recent orders, "
                "daily full sync). Deposit-level breakdown is in stock_deposits."
            ),
        },
    )

    product_tiny_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("products.tiny_id", ondelete="CASCADE"),
        primary_key=True,
        autoincrement=False,
    )
    product_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    sku: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    unit: Mapped[str | None] = mapped_column(String(20), nullable=True)
    balance: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, server_default=text("0")
    )
    reserved: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, server_default=text("0")
    )
    available: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, server_default=text("0")
    )
    location: Mapped[str | None] = mapped_column(String(200), nullable=True)
    synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


# ---------------------------------------------------------------------------
# stock_deposits
# ---------------------------------------------------------------------------
class StockDepositORM(Base):
    __tablename__ = "stock_deposits"
    __table_args__ = (
        UniqueConstraint(
            "product_tiny_id",
            "deposit_tiny_id",
            name="uq_stock_deposits_product_tiny_id_deposit_tiny_id",
        ),
        Index("ix_stock_deposits_product_tiny_id", "product_tiny_id"),
        Index("ix_stock_deposits_deposit_tiny_id", "deposit_tiny_id"),
        {
            "comment": (
                "Stock levels broken down by physical deposit/warehouse. When "
                "ignore=true, this deposit should be excluded from available stock "
                "calculations. Updated atomically with the stock table — all deposits "
                "are replaced on each sync."
            ),
        },
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    product_tiny_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("stock.product_tiny_id", ondelete="CASCADE"),
        nullable=False,
    )
    deposit_tiny_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    deposit_name: Mapped[str] = mapped_column(String(200), nullable=False)
    ignore: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    balance: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, server_default=text("0")
    )
    reserved: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, server_default=text("0")
    )
    available: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, server_default=text("0")
    )
    company: Mapped[str | None] = mapped_column(String(200), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


# ---------------------------------------------------------------------------
# sale_buckets
# ---------------------------------------------------------------------------
class SaleBucketORM(Base):
    __tablename__ = "sale_buckets"
    __table_args__ = (
        # Unique index using COALESCE so that NULL source_kit_sku is treated as ''
        # for deduplication (NULL != NULL would otherwise allow duplicates).
        Index(
            "uq_sale_buckets_natural_key",
            "bucket_date",
            "sku",
            "ecommerce_name",
            "is_kit_expansion",
            text("COALESCE(source_kit_sku, '')"),
            unique=True,
        ),
        Index("ix_sale_buckets_sku", "sku"),
        Index("ix_sale_buckets_bucket_date", "bucket_date"),
        Index("ix_sale_buckets_ecommerce_name", "ecommerce_name"),
        Index("ix_sale_buckets_sku_date", "sku", "bucket_date"),
        {
            "comment": (
                "Pre-computed daily sales aggregations per SKU and sales channel. "
                "Two types of rows: direct sales (is_kit_expansion=false, revenue "
                "populated) and kit expansion rows (is_kit_expansion=true, "
                "revenue=0). Kit expansion rows represent how many units of a "
                "component SKU were implicitly sold through kit sales. For example, "
                "if kit '10U-MAST' containing 10x 'MAST-FIT' was sold 2 times: "
                "direct bucket sku='10U-MAST' quantity=2 revenue=X, expansion bucket "
                "sku='MAST-FIT' quantity=20 revenue=0 source_kit_sku='10U-MAST'. "
                "Recomputed daily and after each order sync batch."
            ),
        },
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    bucket_date: Mapped[date] = mapped_column(Date, nullable=False)
    sku: Mapped[str] = mapped_column(String(100), nullable=False)
    ecommerce_name: Mapped[str] = mapped_column(
        String(200),
        nullable=False,
        server_default=text("'Direct'"),
        comment=(
            "Sales channel name. 'Direct' means the order had no ecommerce "
            "association. Used to attribute revenue to channels."
        ),
    )
    quantity_sold: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, server_default=text("0")
    )
    total_revenue: Mapped[Decimal] = mapped_column(
        Numeric(12, 2), nullable=False, server_default=text("0")
    )
    order_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    is_kit_expansion: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
        comment=(
            "When true, this bucket was generated by expanding a kit sale into its "
            "components. total_revenue is always 0 for expanded rows — revenue is "
            "only on the kit's direct bucket."
        ),
    )
    source_kit_sku: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
        comment=(
            "Only populated when is_kit_expansion=true. Identifies which kit "
            "product generated this component quantity."
        ),
    )
    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


# ---------------------------------------------------------------------------
# sync_logs
# ---------------------------------------------------------------------------
class SyncLogORM(Base):
    __tablename__ = "sync_logs"
    __table_args__ = (
        CheckConstraint(
            "sync_type IN ('products', 'orders', 'stock', 'sale_buckets', 'token_rotation')",
            name="valid_sync_type",
        ),
        CheckConstraint(
            "status IN ('running', 'completed', 'failed')",
            name="valid_status",
        ),
        Index("ix_sync_logs_sync_type", "sync_type"),
        Index("ix_sync_logs_status", "status"),
        Index("ix_sync_logs_started_at", "started_at"),
        {
            "comment": (
                "Audit log of all synchronization operations. Each sync job creates "
                "one row with status='running', then updates to 'completed' or "
                "'failed'. The metadata column stores context-specific data like "
                "date ranges and page counts. Use this table to monitor sync health "
                "and diagnose failures."
            ),
        },
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sync_type: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    items_processed: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    items_failed: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    sync_metadata: Mapped[dict[str, Any] | None] = mapped_column(
        "metadata", JSONB, nullable=True
    )
