"""Product endpoints — list with filters and detail with aggregations."""

from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.api.dependencies import (
    db_session,
    get_product_repository,
    get_sale_bucket_repository,
    get_stock_repository,
)
from tiny_mirror.api.schemas import (
    KitComponentResponse,
    PaginationResponse,
    ProductDetailResponse,
    ProductListItem,
    ProductListResponse,
    ProductPrices,
    SaleBucketResponse,
    StockDepositResponse,
    StockSummary,
)
from tiny_mirror.infrastructure.orm.models import ProductORM, StockORM
from tiny_mirror.infrastructure.repositories.product_repository import (
    PostgreSQLProductRepository,
)
from tiny_mirror.infrastructure.repositories.sale_bucket_repository import (
    PostgreSQLSaleBucketRepository,
)
from tiny_mirror.infrastructure.repositories.stock_repository import (
    PostgreSQLStockRepository,
)

router = APIRouter()


@router.get("", response_model=ProductListResponse)
async def list_products(
    sku: str | None = Query(default=None, description="Filter by SKU (case-insensitive)"),
    situation: Annotated[
        Literal["A", "I", "E"] | None,
        Query(description="Filter by situation (A=Active, I=Inactive, E=Deleted)"),
    ] = "A",
    type: Annotated[
        Literal["P", "S", "K"] | None,
        Query(description="Filter by product type"),
    ] = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=100),
    session: AsyncSession = Depends(db_session),
) -> ProductListResponse:
    filters = []
    if situation is not None:
        filters.append(ProductORM.situation == situation)
    if type is not None:
        filters.append(ProductORM.type == type)
    if sku is not None:
        filters.append(ProductORM.sku.ilike(f"%{sku}%"))

    base_query = select(ProductORM).outerjoin(
        StockORM, StockORM.product_tiny_id == ProductORM.tiny_id
    )
    for clause in filters:
        base_query = base_query.where(clause)

    list_query = (
        base_query.order_by(ProductORM.sku.asc()).limit(page_size).offset((page - 1) * page_size)
    )
    count_query = select(func.count(ProductORM.tiny_id))
    for clause in filters:
        count_query = count_query.where(clause)

    # SQLAlchemy disallows concurrent operations on a single session, so the
    # two queries run sequentially. The cost is a few hundred microseconds
    # vs the round-trip to the API client.
    items_result = await session.execute(list_query)
    count_result = await session.execute(count_query)

    products = items_result.scalars().all()
    total = int(count_result.scalar_one())
    total_pages = max(1, math.ceil(total / page_size)) if total > 0 else 0

    if total > 0 and page > total_pages:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Page out of range")

    # Pull stock data in one extra query so the LEFT JOIN above doesn't
    # explode the row count for products whose stock has multiple rows
    # (it shouldn't — stock PK is product_tiny_id — but defensively keep
    # them separate). We limit the lookup to the products on this page.
    page_ids = [int(p.tiny_id) for p in products]
    stock_by_id: dict[int, StockORM] = {}
    if page_ids:
        stock_result = await session.execute(
            select(StockORM).where(StockORM.product_tiny_id.in_(page_ids))
        )
        stock_by_id = {int(s.product_tiny_id): s for s in stock_result.scalars().all()}

    items = [_to_list_item(p, stock_by_id.get(int(p.tiny_id))) for p in products]

    return ProductListResponse(
        items=items,
        pagination=PaginationResponse(
            page=page,
            page_size=page_size,
            total=total,
            total_pages=total_pages,
        ),
    )


@router.get("/{tiny_id}", response_model=ProductDetailResponse)
async def get_product(
    tiny_id: int,
    products: PostgreSQLProductRepository = Depends(get_product_repository),
    stock_repo: PostgreSQLStockRepository = Depends(get_stock_repository),
    buckets_repo: PostgreSQLSaleBucketRepository = Depends(get_sale_bucket_repository),
) -> ProductDetailResponse:
    product = await products.get_by_tiny_id(tiny_id)
    if product is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")

    # All three repos share the same DB session via FastAPI's dependency
    # cache, so the queries must run sequentially.
    stock = await stock_repo.get_by_product_tiny_id(tiny_id)
    components = await products.get_kit_components(tiny_id)
    buckets = await buckets_repo.get_buckets_for_sku(product["sku"], days=90)

    return _to_detail_response(product, stock, components, buckets)


class CostPriceUpdateRequest(BaseModel):
    """Body for PATCH /products/{tiny_id}/cost-price."""

    cost_price: float = Field(..., ge=0, description="New cost_price in BRL.")


class CostPriceUpdateResponse(BaseModel):
    tiny_id: int
    sku: str
    cost_price: float


@router.patch("/{tiny_id}/cost-price", response_model=CostPriceUpdateResponse)
async def update_cost_price(
    tiny_id: int,
    payload: Annotated[CostPriceUpdateRequest, Body()],
    session: AsyncSession = Depends(db_session),
) -> CostPriceUpdateResponse:
    """Update the ``cost_price`` field inside ``products.prices`` JSONB.

    Why: external cost-updater scripts (Telegram cron, etc.) push price
    changes to Tiny ERP via their REST API. Without writing back to the
    mirror DB, the next sync cycle will not catch up for 24h and analyses
    that read from ``products.prices->>'cost_price'`` (queima, reposição,
    cost-updater divergence detection) will still see the old value —
    causing duplicate Tiny updates and message flood.

    This endpoint surgically updates ONLY the cost_price key inside the
    JSONB blob, leaving the rest of ``prices`` (price, promotional_price,
    average_cost_price) untouched. ``updated_at`` and ``synced_at`` are
    NOT touched on purpose — the next Tiny sync will refresh them
    organically.
    """
    stmt = text(
        """
        UPDATE products
        SET prices = jsonb_set(
            COALESCE(prices, '{}'::jsonb),
            '{cost_price}',
            to_jsonb(:cost_price::numeric)
        )
        WHERE tiny_id = :tiny_id
        RETURNING tiny_id, sku, (prices->>'cost_price')::numeric AS cost_price
        """
    )
    result = await session.execute(stmt, {"tiny_id": tiny_id, "cost_price": payload.cost_price})
    row = result.first()
    if row is None:
        await session.rollback()
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
    await session.commit()
    return CostPriceUpdateResponse(
        tiny_id=int(row.tiny_id),
        sku=str(row.sku),
        cost_price=float(row.cost_price or 0.0),
    )


# ---------------------------------------------------------------------------
# Mapping helpers (ORM/dict -> Pydantic schemas)
# ---------------------------------------------------------------------------
def _prices_from(raw: dict[str, Any] | None) -> ProductPrices:
    if not raw:
        return ProductPrices()
    return ProductPrices(
        price=_to_float(raw.get("price")),
        promotional_price=_to_float(raw.get("promotional_price")),
        cost_price=_to_float(raw.get("cost_price")),
        average_cost_price=_to_float(raw.get("average_cost_price")),
    )


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_list_item(p: ProductORM, stock: StockORM | None) -> ProductListItem:
    return ProductListItem(
        tiny_id=int(p.tiny_id),
        sku=p.sku,
        description=p.description,
        type=p.type,
        situation=p.situation,
        unit=p.unit,
        gtin=p.gtin,
        prices=_prices_from(p.prices),
        stock=_stock_summary(stock),
        variation_type=p.variation_type,
        created_at_tiny=p.created_at_tiny,
        updated_at_tiny=p.updated_at_tiny,
        synced_at=p.synced_at,
    )


def _stock_summary(stock: StockORM | None) -> StockSummary | None:
    if stock is None:
        return None
    return StockSummary(
        balance=float(stock.balance),
        reserved=float(stock.reserved),
        available=float(stock.available),
        location=stock.location,
    )


def _to_detail_response(
    product: dict[str, Any],
    stock: dict[str, Any] | None,
    components: list[dict[str, Any]],
    buckets: list[dict[str, Any]],
) -> ProductDetailResponse:
    stock_summary: StockSummary | None
    deposits: list[StockDepositResponse]
    if stock is None:
        stock_summary = None
        deposits = []
    else:
        stock_summary = StockSummary(
            balance=float(stock["balance"]),
            reserved=float(stock["reserved"]),
            available=float(stock["available"]),
            location=stock.get("location"),
        )
        deposits = [
            StockDepositResponse(
                deposit_tiny_id=int(d["deposit_tiny_id"]),
                deposit_name=d["deposit_name"],
                ignore=bool(d["ignore"]),
                balance=float(d["balance"]),
                reserved=float(d["reserved"]),
                available=float(d["available"]),
                company=d.get("company"),
            )
            for d in stock.get("deposits", [])
        ]

    return ProductDetailResponse(
        tiny_id=int(product["tiny_id"]),
        sku=product["sku"],
        description=product["description"],
        type=product["type"],
        situation=product["situation"],
        unit=product.get("unit"),
        gtin=product.get("gtin"),
        prices=_prices_from(product.get("prices")),
        stock=stock_summary,
        variation_type=product.get("variation_type"),
        created_at_tiny=product.get("created_at_tiny"),
        updated_at_tiny=product.get("updated_at_tiny"),
        synced_at=product["synced_at"],
        complementary_description=product.get("complementary_description"),
        ncm=product.get("ncm"),
        origin=product.get("origin"),
        warranty=product.get("warranty"),
        observations=product.get("observations"),
        category_id=product.get("category_id"),
        category_name=product.get("category_name"),
        category_full_path=product.get("category_full_path"),
        brand_id=product.get("brand_id"),
        brand_name=product.get("brand_name"),
        dimensions=product.get("dimensions"),
        suppliers=product.get("suppliers") or [],
        taxation=product.get("taxation"),
        attachments=product.get("attachments") or [],
        stock_deposits=deposits,
        kit_components=[
            KitComponentResponse(
                component_sku=c["component_sku"],
                component_description=c.get("component_description"),
                component_type=c.get("component_type"),
                quantity=float(c["quantity"]),
                component_tiny_id=(
                    int(c["component_product_tiny_id"])
                    if c.get("component_product_tiny_id") is not None
                    else None
                ),
            )
            for c in components
        ],
        sale_buckets_90d=[
            SaleBucketResponse(
                bucket_date=b["bucket_date"],
                ecommerce_name=b["ecommerce_name"],
                quantity_sold=float(b["quantity_sold"]),
                total_revenue=float(b["total_revenue"]),
                order_count=int(b["order_count"]),
                is_kit_expansion=bool(b["is_kit_expansion"]),
                source_kit_sku=b.get("source_kit_sku"),
            )
            for b in buckets
        ],
    )


# Touch ``date``/``datetime``/``timedelta``/``UTC`` to keep the imports tidy
# even when type-checkers ignore Pydantic's ForwardRefs (these are exercised
# indirectly by the response_model machinery).
_ = (UTC, date, datetime, timedelta)
