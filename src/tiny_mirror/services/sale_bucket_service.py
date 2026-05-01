"""Sale-bucket recompute service.

Walks every order in a date range, expands kit (type=K) line items into
per-component buckets, and persists the per-day x SKU x channel
aggregations in ``sale_buckets``. The whole period is cleared before
inserting, so calling ``refresh_buckets`` repeatedly is idempotent.

Decimal arithmetic is used throughout — `unit_value` and `quantity` come
back from the DB as :class:`Decimal`, and float arithmetic on those
would silently lose precision on currency values.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import structlog

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.repositories.order_repository import (
    PostgreSQLOrderRepository,
)
from tiny_mirror.infrastructure.repositories.product_repository import (
    PostgreSQLProductRepository,
)
from tiny_mirror.infrastructure.repositories.sale_bucket_repository import (
    PostgreSQLSaleBucketRepository,
)

logger = structlog.get_logger(__name__)

# Composite key into the in-memory accumulator. source_kit_sku may be None
# for direct buckets — a tuple key is fine because tuple hashing handles
# None correctly.
BucketKey = tuple[date, str, str, bool, str | None]


@dataclass
class _BucketAccumulator:
    quantity_sold: Decimal = field(default_factory=lambda: Decimal("0"))
    total_revenue: Decimal = field(default_factory=lambda: Decimal("0"))
    order_count: int = 0


class SaleBucketService:
    async def refresh_buckets(self, date_from: date, date_to: date) -> None:
        start = time.perf_counter()

        async with AsyncSessionLocal() as session:
            buckets_repo = PostgreSQLSaleBucketRepository(session)
            orders_repo = PostgreSQLOrderRepository(session)
            products_repo = PostgreSQLProductRepository(session)

            deleted = await buckets_repo.delete_buckets_for_period(date_from, date_to)
            logger.info(
                "Cleared existing buckets",
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                deleted_count=deleted,
            )

            orders = await orders_repo.get_orders_in_period(date_from, date_to)
            logger.info("Orders to process for buckets", count=len(orders))

            kit_ids = {
                int(item["product_tiny_id"])
                for order in orders
                for item in order.get("items", [])
                if item.get("product_type") == "K" and item.get("product_tiny_id") is not None
            }
            kit_components_map = await products_repo.get_kit_components_for_ids(list(kit_ids))

            accumulator: dict[BucketKey, _BucketAccumulator] = defaultdict(_BucketAccumulator)

            for order in orders:
                ecommerce_name = order.get("ecommerce_name") or ""
                if not ecommerce_name.strip():
                    ecommerce_name = "Direct"

                bucket_date: date = order["order_date"]
                items = order.get("items", []) or []

                for item in items:
                    sku = item.get("product_sku") or ""
                    if not sku:
                        continue
                    quantity = _to_decimal(item.get("quantity"))
                    unit_value = _to_decimal(item.get("unit_value"))

                    # Direct bucket
                    direct_key: BucketKey = (
                        bucket_date,
                        sku,
                        ecommerce_name,
                        False,
                        None,
                    )
                    direct = accumulator[direct_key]
                    direct.quantity_sold += quantity
                    direct.total_revenue += quantity * unit_value
                    direct.order_count += 1

                    # Kit expansion buckets (only for kits that have
                    # components mirrored locally — otherwise we just emit
                    # the direct bucket and warn).
                    if item.get("product_type") == "K" and item.get("product_tiny_id") is not None:
                        kit_id = int(item["product_tiny_id"])
                        components = kit_components_map.get(kit_id, [])
                        if not components:
                            logger.warning(
                                "Kit has no components in database, skipping expansion",
                                sku=sku,
                                product_tiny_id=kit_id,
                            )
                            continue
                        for component in components:
                            comp_sku = component.get("component_sku")
                            if not comp_sku:
                                continue
                            comp_qty = _to_decimal(component.get("quantity"))
                            expanded_quantity = quantity * comp_qty
                            exp_key: BucketKey = (
                                bucket_date,
                                comp_sku,
                                ecommerce_name,
                                True,
                                sku,
                            )
                            expansion = accumulator[exp_key]
                            expansion.quantity_sold += expanded_quantity
                            # total_revenue is ALWAYS zero for expansion buckets.
                            expansion.order_count += 1

            now = datetime.now(UTC)
            bucket_list: list[dict[str, Any]] = [
                {
                    "bucket_date": key[0],
                    "sku": key[1],
                    "ecommerce_name": key[2],
                    "is_kit_expansion": key[3],
                    "source_kit_sku": key[4],
                    "quantity_sold": value.quantity_sold,
                    "total_revenue": value.total_revenue,
                    "order_count": value.order_count,
                    "computed_at": now,
                }
                for key, value in accumulator.items()
            ]

            await buckets_repo.upsert_buckets_batch(bucket_list)

        direct_count = sum(1 for b in bucket_list if not b["is_kit_expansion"])
        expansion_count = len(bucket_list) - direct_count
        duration_ms = round((time.perf_counter() - start) * 1000, 2)

        logger.info(
            "Sale buckets computed and saved",
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
            orders_processed=len(orders),
            total_buckets=len(bucket_list),
            direct_buckets=direct_count,
            expansion_buckets=expansion_count,
            duration_ms=duration_ms,
        )


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (TypeError, ValueError):
        return Decimal("0")
