"""ML listing sync service.

Fetches all active ML listings for the configured seller, extracts the
seller SKU (from the SELLER_SKU attribute), logistic type, and
inventory_id, then writes a clean snapshot to ml_listings /
ml_listing_variations. The tables are fully replaced on each run.

This lets the rest of the codebase look up MLB IDs by SKU from the DB
instead of calling the ML search API on every stock sync.
"""

from __future__ import annotations

from typing import Any

import structlog

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.external.mercadolivre_client import MercadoLivreAPIClient
from tiny_mirror.infrastructure.repositories.ml_listing_repository import MLListingRepository
from tiny_mirror.infrastructure.repositories.sync_log_repository import SyncLogRepository

logger = structlog.get_logger(__name__)

_BATCH_SIZE = 20
_PAGE_SIZE = 100


class MLListingSyncService:
    def __init__(self, ml_client: MercadoLivreAPIClient) -> None:
        self._ml = ml_client

    async def run_sync(self, sync_log_id: int) -> None:
        logger.info("ML listings sync started", sync_log_id=sync_log_id)

        # 1. Collect all MLB IDs across all statuses (paginated).
        all_mlb_ids = await self._fetch_all_item_ids()
        logger.info("ML listings fetched", count=len(all_mlb_ids))

        # 2. Batch-fetch item details (20 per call) and build DB rows.
        listings: list[dict[str, Any]] = []
        variations: list[dict[str, Any]] = []
        items_processed = 0
        items_failed = 0

        for i in range(0, len(all_mlb_ids), _BATCH_SIZE):
            batch = all_mlb_ids[i : i + _BATCH_SIZE]
            try:
                items = await self._ml.batch_get_items(batch)
            except Exception as exc:
                logger.warning(
                    "ML batch item fetch failed, skipping batch",
                    offset=i,
                    batch_size=len(batch),
                    error=str(exc),
                )
                items_failed += len(batch)
                continue

            for item in items:
                mlb_id = item.get("id") or ""
                if not mlb_id:
                    continue

                sku = _extract_seller_sku(item)
                shipping = item.get("shipping") or {}
                logistic_type = shipping.get("logistic_type")
                inventory_id = item.get("inventory_id")
                item_variations = item.get("variations") or []
                has_variations = bool(item_variations)

                # secure_thumbnail é a URL https; thumbnail é http. Preferimos
                # a https pra não dar mixed-content no dashboard. Quando o ML
                # só devolve a http (acontece no multiget /items), forçamos o
                # scheme — o host mlstatic.com serve o mesmo path em https.
                thumbnail = item.get("secure_thumbnail") or item.get("thumbnail") or None
                if thumbnail and thumbnail.startswith("http://"):
                    thumbnail = "https://" + thumbnail[len("http://") :]

                # item_relations = vínculo catálogo↔tradicional. Guardamos só
                # os MLB ids relacionados; vazio = anúncios independentes.
                linked_mlb_ids = [
                    rel["id"]
                    for rel in (item.get("item_relations") or [])
                    if isinstance(rel, dict) and rel.get("id")
                ]

                listings.append(
                    {
                        "mlb_id": mlb_id,
                        "sku": sku,
                        "logistic_type": logistic_type,
                        "status": item.get("status"),
                        "inventory_id": inventory_id if not has_variations else None,
                        "has_variations": has_variations,
                        "title": (item.get("title") or "")[:500] or None,
                        "thumbnail": thumbnail,
                        "permalink": item.get("permalink") or None,
                        "price": item.get("price"),
                        "linked_mlb_ids": linked_mlb_ids,
                    }
                )

                for var in item_variations:
                    var_id = var.get("id")
                    if var_id:
                        variations.append(
                            {
                                "mlb_id": mlb_id,
                                "variation_id": int(var_id),
                                "inventory_id": var.get("inventory_id"),
                            }
                        )

                items_processed += 1

        # 3. Replace all rows in one transaction.
        async with AsyncSessionLocal() as session:
            await MLListingRepository(session).replace_all(listings, variations)

        logger.info(
            "ML listings sync completed",
            sync_log_id=sync_log_id,
            listings=len(listings),
            variations=len(variations),
            items_processed=items_processed,
            items_failed=items_failed,
        )

        # 4. Mark sync_log as completed.
        async with AsyncSessionLocal() as log_session:
            await SyncLogRepository(log_session).update_sync_log_complete(
                sync_log_id,
                items_processed=items_processed,
                items_failed=items_failed,
            )

    async def _fetch_all_item_ids(self) -> list[str]:
        all_ids: list[str] = []
        scroll_id: str | None = None
        while True:
            ids, _total, scroll_id = await self._ml.list_all_item_ids(
                scroll_id=scroll_id, limit=_PAGE_SIZE
            )
            all_ids.extend(ids)
            if not ids or not scroll_id:
                break
        return all_ids


def _extract_seller_sku(item: dict[str, Any]) -> str | None:
    """Extract SELLER_SKU from the item's attributes array."""
    for attr in item.get("attributes") or []:
        if attr.get("id") == "SELLER_SKU":
            value = attr.get("value_name") or ""
            return value if value else None
    return None
