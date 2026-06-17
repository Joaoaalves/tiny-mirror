"""Daily refresh of ``ml_catalog_status`` from ML.

For every MLB in ``ml_listings`` we call ``GET /items/{MLB}/price_to_win``
and upsert the catalog buy-box context. Items that return 404 are
recorded with ``catalog_listing=false`` so the downstream engine knows
they have no competitor signal at all (vs. simply never having been
fetched).

The promo decision engine reads from this table instead of calling ML
on every analysis pass. That cuts the full-catalog dry-run from minutes
to seconds.

Cron: usually wired alongside the cap recompute (~05:00 UTC). The job
no-ops when ML credentials are missing.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx
import structlog
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.orm.models import MLCatalogStatusORM, MLListingORM
from tiny_mirror.services.mercadolivre_token_service import MercadoLivreTokenService

logger = structlog.get_logger(__name__)

ML_API_BASE = "https://api.mercadolibre.com"

# Status values we accept verbatim. Anything else from ML gets normalised
# to "unknown" so the CHECK constraint never blocks a write.
_KNOWN_STATUSES = {
    "winning",
    "sharing_first_place",
    "competing",
    "losing",
    "not_listed",
    "unknown",
}


def _to_dec(v: Any) -> Decimal | None:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:  # pragma: no cover — defensive
        return None


def _normalise_status(raw: Any) -> str | None:
    if raw is None:
        return None
    s = str(raw).strip().lower()
    return s if s in _KNOWN_STATUSES else "unknown"


def _row_from_body(mlb_id: str, sku: str | None, body: dict[str, Any]) -> dict[str, Any]:
    """Linha de ml_catalog_status a partir do /price_to_win (HTTP 200)."""
    catalog_product_id = body.get("catalog_product_id")
    winner = body.get("winner") or {}
    return {
        "mlb_id": mlb_id,
        "sku": sku,
        "catalog_listing": catalog_product_id is not None,
        "catalog_product_id": catalog_product_id,
        "status": _normalise_status(body.get("status")),
        "visit_share": body.get("visit_share"),
        "current_price": _to_dec(body.get("current_price")),
        "price_to_win": _to_dec(body.get("price_to_win")),
        "winner_item_id": winner.get("item_id"),
        "winner_price": _to_dec(winner.get("price")),
        "competitors_sharing_first_place": body.get("competitors_sharing_first_place"),
        "boosts": body.get("boosts"),
    }


def _not_listed_row(mlb_id: str, sku: str | None) -> dict[str, Any]:
    """Linha pra anúncio fora de catálogo (HTTP 404 no /price_to_win)."""
    return {
        "mlb_id": mlb_id,
        "sku": sku,
        "catalog_listing": False,
        "catalog_product_id": None,
        "status": "not_listed",
        "visit_share": None,
        "current_price": None,
        "price_to_win": None,
        "winner_item_id": None,
        "winner_price": None,
        "competitors_sharing_first_place": None,
        "boosts": None,
    }


class CatalogStatusSyncService:
    """Reads /items/{MLB}/price_to_win for every active MLB and upserts
    the resulting row into ``ml_catalog_status``.

    Stateless aside from the injected HTTP client + token service.
    """

    def __init__(
        self,
        *,
        token_service: MercadoLivreTokenService,
        http_client: httpx.AsyncClient,
    ) -> None:
        self._token_service = token_service
        self._http = http_client

    async def _fetch_one(self, mlb_id: str) -> tuple[dict[str, Any] | None, int]:
        """Call /items/{MLB}/price_to_win. Returns (payload, http_status).

        404 is common and means "this item is not in any catalog listing" —
        we record that explicitly so the engine knows there's no competitor
        signal (vs. never fetched).
        """
        token = await self._token_service.get_valid_access_token()
        url = f"{ML_API_BASE}/items/{mlb_id}/price_to_win"
        try:
            resp = await self._http.get(
                url, headers={"Authorization": f"Bearer {token}"}, timeout=15.0
            )
            if resp.status_code == 401:
                token = await self._token_service.handle_unauthorized()
                resp = await self._http.get(
                    url, headers={"Authorization": f"Bearer {token}"}, timeout=15.0
                )
            if resp.status_code == 200:
                body = resp.json()
                return (body if isinstance(body, dict) else None), 200
            return None, resp.status_code
        except (httpx.RequestError, ValueError) as exc:
            logger.warning("catalog_status_fetch_failed", mlb_id=mlb_id, error=str(exc))
            return None, -1

    async def refresh_all(self, session: AsyncSession) -> dict[str, int]:
        """Iterate every active MLB in ml_listings and upsert its catalog
        status. Returns counts for telemetry.
        """
        result = await session.execute(
            select(MLListingORM.mlb_id, MLListingORM.sku).where(MLListingORM.status == "active")
        )
        listings = result.all()

        stats = {
            "total_mlbs": len(listings),
            "in_catalog": 0,
            "not_listed_404": 0,
            "errors": 0,
            "winning": 0,
            "sharing_first_place": 0,
            "competing": 0,
            "losing": 0,
            "other_status": 0,
        }

        batch_size = 50
        for i, (mlb_id, sku) in enumerate(listings):
            body, http_status = await self._fetch_one(mlb_id)
            if http_status == 200 and body:
                row = _row_from_body(mlb_id, sku, body)
                if row["catalog_listing"]:
                    stats["in_catalog"] += 1
                ns = row["status"]
                if ns in {"winning", "sharing_first_place", "competing", "losing"}:
                    stats[ns] += 1
                else:
                    stats["other_status"] += 1
            elif http_status == 404:
                stats["not_listed_404"] += 1
                row = _not_listed_row(mlb_id, sku)
            else:
                stats["errors"] += 1
                # Don't touch the existing row on transient errors — skip.
                continue

            update_set = {k: v for k, v in row.items() if k != "mlb_id"}
            update_set["fetched_at"] = func.now()
            stmt = (
                pg_insert(MLCatalogStatusORM)
                .values(**row)
                .on_conflict_do_update(index_elements=["mlb_id"], set_=update_set)
            )
            await session.execute(stmt)
            if (i + 1) % batch_size == 0:
                await session.commit()

        await session.commit()
        logger.info("catalog_status_sync_completed", **stats)
        return stats

    async def refresh_one(self, session: AsyncSession, mlb_id: str, sku: str | None) -> str | None:
        """Atualiza o status de catálogo de UM anúncio (usado pelo webhook em
        tempo real, no tópico de competição do buy-box). Faz upsert e commit.
        Retorna o status ('winning'/'losing'/'competing'/'not_listed'/...) ou
        None se o ML deu erro transitório (mantém a linha existente)."""
        body, http_status = await self._fetch_one(mlb_id)
        if http_status == 200 and body:
            row = _row_from_body(mlb_id, sku, body)
        elif http_status == 404:
            row = _not_listed_row(mlb_id, sku)
        else:
            return None
        update_set = {k: v for k, v in row.items() if k != "mlb_id"}
        update_set["fetched_at"] = func.now()
        await session.execute(
            pg_insert(MLCatalogStatusORM)
            .values(**row)
            .on_conflict_do_update(index_elements=["mlb_id"], set_=update_set)
        )
        await session.commit()
        status = row["status"]
        return status if isinstance(status, str) else None
