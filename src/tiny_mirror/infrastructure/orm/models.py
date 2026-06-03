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
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
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
            "Token used to obtain a new access_token without re-authentication. " "Valid for 1 day."
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
        CheckConstraint("type IN ('P', 'S', 'K', 'V', 'F')", name="valid_type"),
        CheckConstraint("situation IN ('A', 'I', 'E')", name="valid_situation"),
        CheckConstraint(
            "manual_status IS NULL OR manual_status IN ('queima', 'analise', 'normal')",
            name="valid_manual_status",
        ),
        Index("ix_products_type", "type"),
        Index("ix_products_situation", "situation"),
        Index("ix_products_updated_at_tiny", "updated_at_tiny"),
        Index("ix_products_brand_name", "brand_name"),
        Index("ix_products_manual_status", "manual_status"),
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
            "Stock Keeping Unit code. Unique identifier used in orders and stock " "management."
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
    created_at_tiny: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at_tiny: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment=(
            "Last time this record was fetched from the Tiny API. " "Used to detect stale data."
        ),
    )
    manual_status: Mapped[str | None] = mapped_column(
        String(20),
        nullable=True,
        comment=(
            "Operator's manual classification from the GERAL spreadsheet. "
            "'queima' / 'analise' / 'normal'. NULL = never synced or not on the sheet."
        ),
    )
    manual_status_synced_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
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
    total_products_value: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    total_order_value: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
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
    shipping_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
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
    ignore: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    balance: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, server_default=text("0")
    )
    reserved: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, server_default=text("0")
    )
    available: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, server_default=text("0")
    )
    in_transfer: Mapped[Decimal] = mapped_column(
        Numeric(10, 2),
        nullable=False,
        server_default=text("0"),
        comment=(
            "ML internal-transfer qty for the 'Full Mercado Livre' row "
            "(not_available_detail[status=transfer]). Always 0 elsewhere."
        ),
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
    order_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
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
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


# ---------------------------------------------------------------------------
# sync_logs
# ---------------------------------------------------------------------------
class SyncLogORM(Base):
    __tablename__ = "sync_logs"
    __table_args__ = (
        CheckConstraint(
            "sync_type IN ('products', 'orders', 'stock', 'sale_buckets', 'token_rotation', 'mercadolivre_stock', 'invoices')",
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
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    items_processed: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    items_failed: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    sync_metadata: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB, nullable=True)


# ---------------------------------------------------------------------------
# ml_oauth_tokens
# ---------------------------------------------------------------------------
class MLOAuthTokenORM(Base):
    __tablename__ = "ml_oauth_tokens"
    __table_args__ = {
        "comment": (
            "Stores the single active OAuth2 token for Mercado Livre API authentication. "
            "Always contains exactly one row. Updated on every token refresh (6h access_token "
            "TTL). refresh_expires_at is set to access_token expiry + 365 days since ML does "
            "not return an explicit refresh_token expiry."
        ),
    }

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    access_token: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="ML access token (Bearer). Valid for 6 hours.",
    )
    refresh_token: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="ML refresh token. Used to obtain a new access_token without re-authentication.",
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="UTC timestamp when the access_token expires.",
    )
    refresh_expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="Always set to expires_at + 365 days (ML has no explicit refresh expiry).",
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
# invoices
# ---------------------------------------------------------------------------
class InvoiceORM(Base):
    __tablename__ = "invoices"
    __table_args__ = (
        Index("ix_invoices_issue_date", "issue_date"),
        Index("ix_invoices_status", "status"),
        Index("ix_invoices_type", "type"),
        Index("ix_invoices_ecommerce_order_number", "ecommerce_order_number"),
        Index("ix_invoices_origin_id", "origin_id"),
        {
            "comment": (
                "Mirror of all Notas Fiscais from Tiny ERP. "
                "The ecommerce_order_number column is denormalised from the ecommerce JSONB "
                "for fast lookup when reconciling NFs against orders. "
                "origin_id links back to orders.tiny_id for the originating sale."
            ),
        },
    )

    tiny_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=False,
        comment="Unique NF identifier in Tiny ERP. Never changes.",
    )
    number: Mapped[str] = mapped_column(String(20), nullable=False, comment="NF number (numero).")
    series: Mapped[str] = mapped_column(String(5), nullable=False, comment="NF series (serie).")
    access_key: Mapped[str | None] = mapped_column(
        String(50),
        nullable=True,
        comment="44-digit SEFAZ access key (chaveAcesso). NULL if not yet authorized.",
    )
    status: Mapped[str] = mapped_column(
        String(5),
        nullable=False,
        comment="Tiny status code (situacao). '6'=authorized, '4'=cancelled.",
    )
    type: Mapped[str] = mapped_column(
        String(5),
        nullable=False,
        comment="NF type (tipo). 'S'=sale (saída).",
    )
    issue_date: Mapped[date] = mapped_column(
        Date, nullable=False, comment="Emission date (dataEmissao)."
    )
    forecast_date: Mapped[date | None] = mapped_column(
        Date, nullable=True, comment="Forecast date (dataPrevista)."
    )
    customer: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        comment="Full customer object (cliente) — name, CPF/CNPJ, address.",
    )
    delivery_address: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
        comment="Delivery address (enderecoEntrega). NULL when same as customer address.",
    )
    seller: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True, comment="Seller object (vendedor)."
    )
    total_value: Mapped[Decimal] = mapped_column(
        Numeric(12, 2),
        nullable=False,
        server_default=text("0"),
        comment="Total NF value (valor).",
    )
    products_value: Mapped[Decimal] = mapped_column(
        Numeric(12, 2),
        nullable=False,
        server_default=text("0"),
        comment="Products subtotal (valorProdutos).",
    )
    freight_value: Mapped[Decimal] = mapped_column(
        Numeric(12, 2),
        nullable=False,
        server_default=text("0"),
        comment="Freight value (valorFrete).",
    )
    shipping_method_id: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True, comment="Shipping method ID (idFormaEnvio)."
    )
    freight_type_id: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True, comment="Freight type ID (idFormaFrete). 0 treated as NULL."
    )
    tracking_code: Mapped[str | None] = mapped_column(
        String(100), nullable=True, comment="Carrier tracking code (codigoRastreamento)."
    )
    tracking_url: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Carrier tracking URL (urlRastreamento)."
    )
    freight_responsibility: Mapped[str | None] = mapped_column(
        String(5),
        nullable=True,
        comment="Freight responsibility (fretePorConta). 'T'=carrier, 'R'=recipient.",
    )
    volume_count: Mapped[int | None] = mapped_column(
        Integer, nullable=True, comment="Number of shipping volumes (qtdVolumes)."
    )
    gross_weight: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 4), nullable=True, comment="Gross weight in kg (pesoBruto)."
    )
    net_weight: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 4), nullable=True, comment="Net weight in kg (pesoLiquido)."
    )
    ecommerce: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
        comment=(
            "Full ecommerce object: id, nome, numeroPedidoEcommerce, "
            "numeroPedidoCanalVenda, canalVenda."
        ),
    )
    ecommerce_order_number: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
        comment=(
            "Denormalised ecommerce.numeroPedidoEcommerce. "
            "For ML: may be the pack_id or the order_id stored by Tiny."
        ),
    )
    origin_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        comment="Tiny order ID that originated this NF (origem.id cast to int).",
    )
    origin_type: Mapped[str | None] = mapped_column(
        String(20), nullable=True, comment="Origin document type (origem.tipo). Typically 'venda'."
    )
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="Last time this record was fetched from the Tiny API.",
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
# invoice_items
# ---------------------------------------------------------------------------
class InvoiceItemORM(Base):
    __tablename__ = "invoice_items"
    __table_args__ = (
        Index("ix_invoice_items_invoice_tiny_id", "invoice_tiny_id"),
        Index("ix_invoice_items_product_tiny_id", "product_tiny_id"),
        Index("ix_invoice_items_product_sku", "product_sku"),
        UniqueConstraint(
            "invoice_tiny_id",
            "tiny_item_id",
            name="uq_invoice_items_invoice_line",
        ),
        {
            "comment": (
                "Line items of each Nota Fiscal. Each row = one product line "
                "on the NF, captured from GET /notas/{id}. Source of truth for "
                "which SKU actually shipped on this NF, including kit "
                "components that order_items never sees."
            ),
        },
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    invoice_tiny_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("invoices.tiny_id", ondelete="CASCADE"),
        nullable=False,
    )
    tiny_item_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    product_tiny_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    product_sku: Mapped[str] = mapped_column(String(100), nullable=False, server_default=text("''"))
    product_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    ncm: Mapped[str | None] = mapped_column(String(20), nullable=True)
    unit: Mapped[str | None] = mapped_column(String(20), nullable=True)
    quantity: Mapped[Decimal] = mapped_column(
        Numeric(10, 2), nullable=False, server_default=text("0")
    )
    unit_value: Mapped[Decimal] = mapped_column(
        Numeric(12, 2), nullable=False, server_default=text("0")
    )
    total_value: Mapped[Decimal] = mapped_column(
        Numeric(12, 2), nullable=False, server_default=text("0")
    )
    cfop: Mapped[str | None] = mapped_column(String(10), nullable=True)
    operation_nature: Mapped[str | None] = mapped_column(String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ---------------------------------------------------------------------------
# ml_listings
# ---------------------------------------------------------------------------
class MLListingORM(Base):
    __tablename__ = "ml_listings"
    __table_args__ = (
        Index("ix_ml_listings_sku", "sku"),
        Index("ix_ml_listings_logistic_type", "logistic_type"),
        {
            "comment": (
                "One row per active ML listing, refreshed daily by the ml_listings sync. "
                "Allows stock sync to look up MLB IDs from the DB instead of calling the "
                "ML search API per product."
            ),
        },
    )

    mlb_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    sku: Mapped[str | None] = mapped_column(String(100), nullable=True)
    logistic_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    inventory_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    has_variations: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    thumbnail: Mapped[str | None] = mapped_column(Text, nullable=True)
    permalink: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Full listing price on ML (item.price) — the displayed "preço cheio".
    # Source of truth for the product price; planilha stays for cost/margin.
    price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ---------------------------------------------------------------------------
# fulfillment_transfers
# ---------------------------------------------------------------------------
class FulfillmentTransferORM(Base):
    __tablename__ = "fulfillment_transfers"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'received', 'cancelled')",
            name="valid_fulfillment_transfer_status",
        ),
        CheckConstraint(
            "source IN ('api', 'tiny_webhook', 'manual')",
            name="valid_fulfillment_transfer_source",
        ),
        Index("ix_fulfillment_transfers_product_sku", "product_sku"),
        Index("ix_fulfillment_transfers_status", "status"),
        Index("ix_fulfillment_transfers_transferred_at", "transferred_at"),
        Index("ix_fulfillment_transfers_source", "source"),
        {
            "comment": (
                "Tracks units transferred from Galpão to Full ML via Tiny API. "
                "status=pending until ML INBOUND_RECEPTION confirms arrival. "
                "Used to compute effective Full ML stock in mv_coverage so we "
                "don't double-send while transfers are in transit."
            ),
        },
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    product_tiny_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("products.tiny_id", ondelete="CASCADE"),
        nullable=False,
    )
    product_sku: Mapped[str] = mapped_column(String(100), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    cost_per_unit: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    transferred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    quantity_received: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    last_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=text("'pending'")
    )
    source: Mapped[str] = mapped_column(String(20), nullable=False, server_default=text("'api'"))
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ---------------------------------------------------------------------------
# tiny_fl_stock_snapshots
# ---------------------------------------------------------------------------
class TinyFLStockSnapshotORM(Base):
    __tablename__ = "tiny_fl_stock_snapshots"
    __table_args__ = (
        {
            "comment": (
                "Per-product memory of Tiny's raw Full ML deposit value "
                "(pre-overlay). Used purely for delta detection on the "
                "stock webhook path."
            ),
        },
    )

    product_tiny_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("products.tiny_id", ondelete="CASCADE"),
        primary_key=True,
    )
    tiny_fl_qty: Mapped[int] = mapped_column(Integer, nullable=False)
    # Paired with tiny_fl_qty for the webhook corroboration rule: a real
    # galpão→Full transfer drops galpão by approximately the FL gain, while
    # sale cancellations leave galpão untouched. Defaults to 0 only for
    # rows that pre-date the column — webhook code treats 0 prev as
    # "no corroboration possible, skip".
    stock_galpao_qty: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ---------------------------------------------------------------------------
# ml_listing_variations
# ---------------------------------------------------------------------------
class MLListingVariationORM(Base):
    __tablename__ = "ml_listing_variations"
    __table_args__ = (
        Index("ix_ml_listing_variations_inventory_id", "inventory_id"),
        {
            "comment": (
                "Per-variation inventory tracking for ML listings that have variations. "
                "When a listing has variations, item-level inventory_id is null and each "
                "variation carries its own inventory_id."
            ),
        },
    )

    mlb_id: Mapped[str] = mapped_column(
        String(50),
        ForeignKey("ml_listings.mlb_id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    variation_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, nullable=False)
    inventory_id: Mapped[str | None] = mapped_column(String(50), nullable=True)


# ---------------------------------------------------------------------------
# ml_promo_caps
# ---------------------------------------------------------------------------
class MLPromoCapORM(Base):
    __tablename__ = "ml_promo_caps"
    __table_args__ = (
        Index("ix_ml_promo_caps_sku", "sku"),
        {
            "comment": (
                "User-set cap on Mercado Livre promotion automation, per MLB. "
                "PK changed from sku to mlb_id on 2026-05-21 so each anúncio "
                "carries its own cap; sku stays as a non-unique column for "
                "grouping queries (e.g. drawer fetch by SKU)."
            ),
        },
    )

    mlb_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    sku: Mapped[str] = mapped_column(String(100), nullable=False)
    max_seller_share_pct: Mapped[Decimal] = mapped_column(
        Numeric(5, 2),
        nullable=False,
        comment="Cap on the % SELLER pays (excludes ML's meli_percentage co-funding share).",
    )
    margin_floor_price: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 2),
        nullable=True,
        comment="Override floor price. NULL means use ml_costs_snapshot.sheet_promo_price.",
    )
    auto_apply: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    has_active_promo: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
        comment=(
            "True when ML's seller-promotions API returned a STARTED promo for "
            "this MLB at the last cap recompute. Authoritative current 'has an "
            "active promo on ML' signal for the dashboard filter."
        ),
    )
    active_promo_price: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 2),
        nullable=True,
        comment=(
            "Lowest STARTED promo price on the MLB at the last recompute — the "
            "real current selling price the customer sees. Only meaningful when "
            "has_active_promo is true; ignore otherwise."
        ),
    )
    freight_band_opt: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("true")
    )
    skip_when_winning: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
        comment=(
            "When true and catalog_status='winning' + visit_share='maximum', "
            "the engine returns keep_winning instead of activating new promos."
        ),
    )
    excluded_promo_types: Mapped[list[str]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_by: Mapped[str | None] = mapped_column(String(100), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ---------------------------------------------------------------------------
# ml_costs_snapshot
# ---------------------------------------------------------------------------
class MLCostsSnapshotORM(Base):
    __tablename__ = "ml_costs_snapshot"
    __table_args__ = (
        Index("ix_ml_costs_snapshot_sku", "sku"),
        {
            "comment": (
                "Cached cost data fetched from the Google Apps Script endpoint backed "
                "by the planilha MERCADO LIVRE. Refreshed daily by the ml-costs-refresh "
                "cron. Used as the source of truth for freight bands, base cost, "
                "commission %, and the floor price (sheet_promo_price) in the promotion "
                "decision algorithm."
            ),
        },
    )

    mlb_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    sku: Mapped[str] = mapped_column(String(100), nullable=False)
    active_on_sheet: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("true")
    )
    base_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    commission_pct: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), nullable=True)
    commission_label: Mapped[str | None] = mapped_column(String(100), nullable=True)
    list_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    sheet_promo_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    sheet_discount_pct: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), nullable=True)
    sheet_margin_pct: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), nullable=True)
    sheet_margin_value: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    freight_bands: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
    fetch_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ---------------------------------------------------------------------------
# ml_catalog_status — buy-box / price_to_win snapshot per MLB
# ---------------------------------------------------------------------------
class MLCatalogStatusORM(Base):
    __tablename__ = "ml_catalog_status"
    __table_args__ = (
        CheckConstraint(
            "status IS NULL OR status IN "
            "('winning', 'sharing_first_place', 'competing', 'losing', "
            "'not_listed', 'unknown')",
            name="valid_catalog_status",
        ),
        Index("ix_ml_catalog_status_sku", "sku"),
        Index("ix_ml_catalog_status_status", "status"),
        Index("ix_ml_catalog_status_catalog_listing", "catalog_listing"),
        {
            "comment": (
                "Cached buy-box / price_to_win competitive data per MLB. "
                "Refreshed daily by CatalogStatusSyncService from "
                "GET /items/{MLB}/price_to_win. The promo decision engine "
                "reads this table instead of calling ML live, so daily "
                "analysis runs over the whole catalog in seconds."
            ),
        },
    )

    mlb_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    sku: Mapped[str | None] = mapped_column(String(100), nullable=True)
    catalog_listing: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    catalog_product_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    status: Mapped[str | None] = mapped_column(String(40), nullable=True)
    visit_share: Mapped[str | None] = mapped_column(String(20), nullable=True)
    current_price: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    price_to_win: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    winner_item_id: Mapped[str | None] = mapped_column(String(20), nullable=True)
    winner_price: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    competitors_sharing_first_place: Mapped[int | None] = mapped_column(Integer, nullable=True)
    boosts: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ---------------------------------------------------------------------------
# ml_promo_actions
# ---------------------------------------------------------------------------
class MLPromoActionORM(Base):
    __tablename__ = "ml_promo_actions"
    __table_args__ = (
        Index("ix_ml_promo_actions_sku_at", "sku", text("at DESC")),
        Index("ix_ml_promo_actions_at", text("at DESC")),
        {
            "comment": (
                "Audit log of every promotion decision. Includes dry-runs (dry_run=true) "
                "so the operator can see what would have been done before the auto-apply "
                "flag flips."
            ),
        },
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    sku: Mapped[str] = mapped_column(String(100), nullable=False)
    mlb_id: Mapped[str] = mapped_column(String(20), nullable=False)
    action: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        comment="activated|created|removed|no_change|freight_opt|dry_run|error",
    )
    promo_type: Mapped[str | None] = mapped_column(String(40), nullable=True)
    promo_id: Mapped[str | None] = mapped_column(String(60), nullable=True)
    price_before: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    price_after: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    total_pct: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), nullable=True)
    seller_pct: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), nullable=True)
    meli_pct: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    ml_response: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
    dry_run: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    decided_by: Mapped[str | None] = mapped_column(
        String(200),
        nullable=True,
        comment="Email do operador ou 'engine' para ações automáticas",
    )
    context: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
        comment=(
            "Snapshot de contexto para automação futura: "
            "{catalog_status, current_price, price_to_win, "
            "momentum, margin_pct, floor_price, list_price}"
        ),
    )


# ---------------------------------------------------------------------------
# ml_promo_alerts
# ---------------------------------------------------------------------------
class MLPromoAlertORM(Base):
    __tablename__ = "ml_promo_alerts"
    __table_args__ = (
        Index("ix_ml_promo_alerts_sku_kind_ack", "sku", "kind", "acknowledged"),
        Index("ix_ml_promo_alerts_open", "acknowledged", text("at DESC")),
        {
            "comment": (
                "Operator-actionable anomalies detected during scans: promotion already "
                "active below the planilha floor, pending freight-band opt opportunities, "
                "missing cost data, etc. Acknowledge to hide from dashboard list."
            ),
        },
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    sku: Mapped[str] = mapped_column(String(100), nullable=False)
    mlb_id: Mapped[str] = mapped_column(String(20), nullable=False)
    kind: Mapped[str] = mapped_column(
        String(40),
        nullable=False,
        comment="floor_violation|freight_opt_pending|anomaly|no_cost_data|over_cap_existing",
    )
    message: Mapped[str] = mapped_column(Text, nullable=False)
    data: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
    acknowledged: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    acknowledged_by: Mapped[str | None] = mapped_column(String(100), nullable=True)
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ---------------------------------------------------------------------------
# ml_promo_decisions — operator approval queue (per (mlb_id, promo_key))
# ---------------------------------------------------------------------------
class MLPromoDecisionORM(Base):
    __tablename__ = "ml_promo_decisions"
    __table_args__ = (
        Index("ix_ml_promo_decisions_status", "status"),
        Index("ix_ml_promo_decisions_sku", "sku"),
        Index("ix_ml_promo_decisions_ml_apply_status", "ml_apply_status"),
        UniqueConstraint("mlb_id", "promo_key", name="uq_ml_promo_decisions_mlb_promo"),
        CheckConstraint(
            "status IN ('pending', 'approved', 'rejected', 'ignored', 'expired')",
            name="ck_ml_promo_decisions_status",
        ),
        CheckConstraint(
            "ml_apply_status IS NULL OR ml_apply_status IN " "('pending','ok','failed','skipped')",
            name="ck_ml_promo_decisions_ml_apply_status",
        ),
        {
            "comment": (
                "Approval queue for engine candidate activations. The cron "
                "writes pending rows here; the operator approves/rejects via "
                "the dashboard. Unique on (mlb_id, promo_key) so re-runs are "
                "idempotent — a decision is recorded once per anúncio + promo."
            ),
        },
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    mlb_id: Mapped[str] = mapped_column(String(20), nullable=False)
    sku: Mapped[str] = mapped_column(String(100), nullable=False)
    promo_key: Mapped[str] = mapped_column(
        String(80),
        nullable=False,
        comment="ML promo id when known; synthetic CREATE-<kind> token otherwise",
    )
    promo_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    promo_type: Mapped[str] = mapped_column(String(40), nullable=False)
    promo_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    decision_kind: Mapped[str] = mapped_column(
        String(40),
        nullable=False,
        comment="would_activate | create_price_discount | activate_candidate",
    )
    target_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    target_total_pct: Mapped[Decimal | None] = mapped_column(Numeric(6, 2), nullable=True)
    target_seller_pct: Mapped[Decimal | None] = mapped_column(Numeric(6, 2), nullable=True)
    meli_percentage: Mapped[Decimal | None] = mapped_column(Numeric(6, 2), nullable=True)
    constraint_used: Mapped[str | None] = mapped_column(String(40), nullable=True)
    list_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    cap_pct: Mapped[Decimal | None] = mapped_column(Numeric(6, 2), nullable=True)
    floor_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        server_default=text("'pending'"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    decided_by: Mapped[str | None] = mapped_column(String(100), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    expired_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="When the auto-expire job flipped this row to 'expired'.",
    )
    expired_reason: Mapped[str | None] = mapped_column(
        String(40),
        nullable=True,
        comment=(
            "Why the row was expired: list_price_drift | cap_changed "
            "| floor_changed | stale_age."
        ),
    )
    ml_apply_status: Mapped[str | None] = mapped_column(
        String(20),
        nullable=True,
        comment=(
            "Outcome of the last attempt to push this row to ML: "
            "pending | ok | failed | skipped. NULL = never tried."
        ),
    )
    ml_apply_status_code: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="HTTP code from ML on the last attempt.",
    )
    ml_apply_response: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Trimmed ML response body — first 2KB for debugging.",
    )
    ml_applied_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of the last attempt to push this row to ML.",
    )
    decision_context: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
        comment=(
            "Snapshot do contexto no momento da decisão do operador. "
            "Estruturado para treinar regras de automação: "
            "{catalog_status, current_price, price_to_win, momentum, "
            "margin_pct, discount_pct, list_price, floor_price}"
        ),
    )
    promo_finish_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment=(
            "Data de término da campanha ML (finish_date do objeto Promo). "
            "NULL para PRICE_DISCOUNT (sem campanha). "
            "Usado pelo expire-stale para descartar decisões de campanhas encerradas."
        ),
    )


# ---------------------------------------------------------------------------
# fl_stock_corrections_log
# ---------------------------------------------------------------------------
class FLStockCorrectionLogORM(Base):
    __tablename__ = "fl_stock_corrections_log"
    __table_args__ = (
        Index("ix_fl_corrections_log_sku", "sku"),
        Index("ix_fl_corrections_log_created_at", "created_at"),
        {
            "comment": (
                "Audit trail of FL stock corrections: every mismatch detected by the "
                "hourly cron, whether or not the correction succeeded. Append-only — "
                "never delete rows. Investigation payload preserves enough context to "
                "diagnose recurring drift causes (Hypothesis 1 = NFs not cancelled, "
                "Hypothesis 2 = phantom products — see docs/03)."
            ),
        },
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    product_tiny_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sku: Mapped[str] = mapped_column(String(100), nullable=False)
    tiny_saldo_before: Mapped[int] = mapped_column(Integer, nullable=False)
    ml_qty: Mapped[int] = mapped_column(Integer, nullable=False)
    delta: Mapped[int] = mapped_column(Integer, nullable=False)
    correction_applied: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    tiny_id_lancamento: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    tiny_saldo_after: Mapped[int | None] = mapped_column(Integer, nullable=True)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    investigation_payload: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
        comment=(
            "Snapshot for forensic analysis: tiny estoque response (all deposits "
            "+ saldo/reservado/disponivel), recent orders affecting the SKU, "
            "recent fulfillment_transfers, recent stock_history. Captured BEFORE "
            "the correction POST so we can later prove what state the SKU was in."
        ),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# ---------------------------------------------------------------------------
# phantom_products_log
# ---------------------------------------------------------------------------
class PhantomProductsLogORM(Base):
    __tablename__ = "phantom_products_log"
    __table_args__ = (
        Index("ix_phantom_log_sku", "sku"),
        Index("ix_phantom_log_run", "detection_run_id"),
        Index("ix_phantom_log_detected_at", "detected_at"),
        {
            "comment": (
                "Audit trail of phantom products (Tiny SKUs with excluded "
                "duplicates absorbing ML orders). One row per (run, sku). "
                "Append-only — never delete; the trend across runs is the value."
            ),
        },
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    detection_run_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sku: Mapped[str] = mapped_column(String(100), nullable=False)
    product_active_tiny_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        comment=(
            "tiny_id of the 'real' active/inactive product with this SKU. "
            "NULL when the catalog has zero non-excluded copies (critical)."
        ),
    )
    num_excluded: Mapped[int] = mapped_column(Integer, nullable=False)
    excluded_tiny_ids: Mapped[list[int]] = mapped_column(
        ARRAY(BigInteger),
        nullable=False,
        server_default=text("'{}'::bigint[]"),
    )
    orders_ml_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    units_ml: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    first_sale_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    last_sale_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    investigation_payload: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
        comment=(
            "Forensic snapshot per phantom: descriptions of active+excluded "
            "products, recent ML orders that hit this SKU, suggested action."
        ),
    )
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
