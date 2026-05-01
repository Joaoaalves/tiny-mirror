"""Order endpoints — list with filters and detail with line items."""

from __future__ import annotations

import math
from datetime import date
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.api.dependencies import db_session, get_order_repository
from tiny_mirror.api.schemas import (
    OrderDetailResponse,
    OrderItemResponse,
    OrderListItem,
    OrderListResponse,
    PaginationResponse,
)
from tiny_mirror.infrastructure.orm.models import OrderORM
from tiny_mirror.infrastructure.repositories.order_repository import (
    PostgreSQLOrderRepository,
)

router = APIRouter()


@router.get("", response_model=OrderListResponse)
async def list_orders(
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    situation: int | None = Query(default=None, ge=0, le=9),
    ecommerce_name: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=100),
    order_by: Annotated[Literal["asc", "desc"], Query()] = "desc",
    session: AsyncSession = Depends(db_session),
) -> OrderListResponse:
    filters = []
    if date_from is not None:
        filters.append(OrderORM.order_date >= date_from)
    if date_to is not None:
        filters.append(OrderORM.order_date <= date_to)
    if situation is not None:
        filters.append(OrderORM.situation == situation)
    if ecommerce_name is not None:
        filters.append(OrderORM.ecommerce_name.ilike(f"%{ecommerce_name}%"))

    base_query = select(OrderORM)
    for clause in filters:
        base_query = base_query.where(clause)

    order_clause = OrderORM.order_date.desc() if order_by == "desc" else OrderORM.order_date.asc()
    list_query = base_query.order_by(order_clause).limit(page_size).offset((page - 1) * page_size)

    count_query = select(func.count(OrderORM.tiny_id))
    for clause in filters:
        count_query = count_query.where(clause)

    # See products.list_products: same session can't run queries concurrently.
    items_result = await session.execute(list_query)
    count_result = await session.execute(count_query)

    orders = items_result.scalars().all()
    total = int(count_result.scalar_one())
    total_pages = max(1, math.ceil(total / page_size)) if total > 0 else 0

    if total > 0 and page > total_pages:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Page out of range")

    return OrderListResponse(
        items=[_to_list_item(o) for o in orders],
        pagination=PaginationResponse(
            page=page,
            page_size=page_size,
            total=total,
            total_pages=total_pages,
        ),
    )


@router.get("/{tiny_id}", response_model=OrderDetailResponse)
async def get_order(
    tiny_id: int,
    orders: PostgreSQLOrderRepository = Depends(get_order_repository),
) -> OrderDetailResponse:
    order = await orders.get_by_tiny_id(tiny_id)
    if order is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")
    return _to_detail_response(order)


def _to_list_item(o: OrderORM) -> OrderListItem:
    return OrderListItem(
        tiny_id=int(o.tiny_id),
        order_number=int(o.order_number),
        situation=int(o.situation),
        order_date=o.order_date,
        total_order_value=_to_float(o.total_order_value),
        ecommerce_name=o.ecommerce_name,
        ecommerce_order_number=o.ecommerce_order_number,
        warehouse_name=o.warehouse_name,
        shipping_date=o.shipping_date,
        synced_at=o.synced_at,
    )


def _to_detail_response(order: dict[str, Any]) -> OrderDetailResponse:
    return OrderDetailResponse(
        tiny_id=int(order["tiny_id"]),
        order_number=int(order["order_number"]),
        situation=int(order["situation"]),
        order_date=order["order_date"],
        total_order_value=_to_float(order.get("total_order_value")),
        ecommerce_name=order.get("ecommerce_name"),
        ecommerce_order_number=order.get("ecommerce_order_number"),
        warehouse_name=order.get("warehouse_name"),
        shipping_date=order.get("shipping_date"),
        synced_at=order["synced_at"],
        invoice_id=order.get("invoice_id"),
        invoice_date=order.get("invoice_date"),
        total_products_value=_to_float(order.get("total_products_value")),
        price_list=order.get("price_list"),
        customer=order.get("customer") or {},
        delivery_address=order.get("delivery_address"),
        ecommerce_id=order.get("ecommerce_id"),
        sales_channel=order.get("sales_channel"),
        channel_order_number=order.get("channel_order_number"),
        carrier=order.get("carrier"),
        warehouse_id=order.get("warehouse_id"),
        seller=order.get("seller"),
        operation_nature=order.get("operation_nature"),
        intermediary=order.get("intermediary"),
        payment=order.get("payment"),
        integrated_payments=order.get("integrated_payments") or [],
        delivery_date=order.get("delivery_date"),
        purchase_order_number=order.get("purchase_order_number"),
        discount_value=_to_float(order.get("discount_value")) or 0.0,
        shipping_value=_to_float(order.get("shipping_value")) or 0.0,
        other_expenses_value=_to_float(order.get("other_expenses_value")) or 0.0,
        expected_date=order.get("expected_date"),
        observations=order.get("observations"),
        internal_observations=order.get("internal_observations"),
        order_origin=int(order.get("order_origin") or 0),
        items=[
            OrderItemResponse(
                product_sku=item["product_sku"],
                product_description=item.get("product_description"),
                product_type=item.get("product_type"),
                quantity=float(item["quantity"]),
                unit_value=float(item["unit_value"]),
                additional_info=item.get("additional_info"),
            )
            for item in order.get("items", [])
        ],
    )


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
