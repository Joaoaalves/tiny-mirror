"""Tiny ERP webhook receivers.

Both endpoints follow the same contract:
- HTTP 200 in under ~100ms.
- Never raise on RabbitMQ failure — Tiny would retry up to 15 times and
  the regular sync would catch the change anyway.
- Optionally reject mismatched CNPJ (via ``TINY_EXPECTED_CNPJ``) — the
  request is logged + acknowledged with 200 to avoid Tiny retries.
- Validation errors return 200 + WARNING log so unknown ``tipo`` values
  added by Tiny in the future don't trigger retry storms.
"""

from __future__ import annotations

import hmac
import re
from datetime import UTC, datetime
from typing import Any

import structlog
from fastapi import APIRouter, Depends, Header, Request, status
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.api.dependencies import db_session, get_queue_publisher
from tiny_mirror.api.schemas import OrderWebhookPayload, StockWebhookPayload
from tiny_mirror.config import settings
from tiny_mirror.exceptions import QueueException
from tiny_mirror.infrastructure.orm.models import MLWebhookNotificationORM
from tiny_mirror.queue.publisher import QueuePublisher

logger = structlog.get_logger(__name__)

router = APIRouter()

EXPECTED_TIPO_ORDERS = "situacao_pedido"
EXPECTED_TIPO_STOCK = "estoque"


@router.post("/orders", status_code=status.HTTP_200_OK, tags=["Webhooks"])
async def receive_order_webhook(
    request: Request,
    publisher: QueuePublisher = Depends(get_queue_publisher),
) -> JSONResponse:
    raw = await _read_json(request)
    if raw is None:
        logger.warning("Order webhook: invalid JSON, acknowledged anyway")
        return _ack()

    try:
        payload = OrderWebhookPayload.model_validate(raw)
    except ValidationError as exc:
        logger.warning(
            "Order webhook: payload validation failed, acknowledged anyway",
            errors=exc.errors(include_url=False, include_input=False),
        )
        return _ack()

    if not _cnpj_matches(payload.cnpj):
        logger.warning(
            "Order webhook: cnpj mismatch, acknowledged anyway",
            cnpj_received=payload.cnpj,
            cnpj_expected=settings.tiny_expected_cnpj,
        )
        return _ack()

    if payload.tipo != EXPECTED_TIPO_ORDERS:
        logger.warning(
            "Order webhook: unexpected 'tipo', acknowledged anyway",
            tipo_received=payload.tipo,
            endpoint="/webhooks/orders",
        )
        return _ack()

    logger.info(
        "Order webhook received",
        order_tiny_id=payload.dados.id_venda_tiny,
        situacao=payload.dados.situacao,
    )

    message = {
        "cnpj": payload.cnpj,
        "id_ecommerce": str(payload.id_ecommerce),
        "tipo": payload.tipo,
        "versao": payload.versao,
        "dados": {
            "id_pedido_ecommerce": payload.dados.id_pedido_ecommerce,
            "id_venda_tiny": payload.dados.id_venda_tiny,
            "situacao": payload.dados.situacao,
            "descricao_situacao": payload.dados.descricao_situacao,
        },
        "request_id": _current_request_id(),
        "published_at": datetime.now(UTC).isoformat(),
    }

    try:
        await publisher.publish_sync_message("webhooks.orders", message)
    except QueueException as exc:
        # The order will be picked up by the regular hourly sync — never
        # propagate the error to Tiny.
        logger.error(
            "Failed to publish order webhook to queue, " "will be handled by regular sync",
            order_tiny_id=payload.dados.id_venda_tiny,
            error=str(exc),
        )

    return _ack()


@router.post("/stock", status_code=status.HTTP_200_OK, tags=["Webhooks"])
async def receive_stock_webhook(
    request: Request,
    publisher: QueuePublisher = Depends(get_queue_publisher),
) -> JSONResponse:
    raw = await _read_json(request)
    if raw is None:
        logger.warning("Stock webhook: invalid JSON, acknowledged anyway")
        return _ack()

    try:
        payload = StockWebhookPayload.model_validate(raw)
    except ValidationError as exc:
        logger.warning(
            "Stock webhook: payload validation failed, acknowledged anyway",
            errors=exc.errors(include_url=False, include_input=False),
        )
        return _ack()

    if not _cnpj_matches(payload.cnpj):
        logger.warning(
            "Stock webhook: cnpj mismatch, acknowledged anyway",
            cnpj_received=payload.cnpj,
            cnpj_expected=settings.tiny_expected_cnpj,
        )
        return _ack()

    if payload.tipo != EXPECTED_TIPO_STOCK:
        logger.warning(
            "Stock webhook: unexpected 'tipo', acknowledged anyway",
            tipo_received=payload.tipo,
            endpoint="/webhooks/stock",
        )
        return _ack()

    logger.info(
        "Stock webhook received",
        product_tiny_id=payload.dados.id_produto,
        sku=payload.dados.sku,
        stock_type=payload.dados.tipo_estoque,
        balance=payload.dados.saldo,
    )

    message = {
        "cnpj": payload.cnpj,
        # Tiny v3 stock webhooks omit idEcommerce; preserve None instead of
        # serializing it to the literal string "None".
        "id_ecommerce": (str(payload.id_ecommerce) if payload.id_ecommerce is not None else None),
        "tipo": payload.tipo,
        "versao": payload.versao,
        "dados": {
            "tipo_estoque": payload.dados.tipo_estoque,
            "saldo": payload.dados.saldo,
            "id_produto": payload.dados.id_produto,
            "sku": payload.dados.sku,
            "sku_mapeamento": payload.dados.sku_mapeamento,
            "sku_mapeamento_pai": payload.dados.sku_mapeamento_pai,
        },
        "request_id": _current_request_id(),
        "published_at": datetime.now(UTC).isoformat(),
    }

    try:
        await publisher.publish_sync_message("webhooks.stock", message)
    except QueueException as exc:
        logger.error(
            "Failed to publish stock webhook to queue, " "will be handled by regular sync",
            product_tiny_id=payload.dados.id_produto,
            error=str(exc),
        )

    return _ack()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
@router.post("/ml-notifications", tags=["Webhooks"])
async def receive_ml_notification(
    request: Request,
    session: AsyncSession = Depends(db_session),
    x_webhook_token: str | None = Header(default=None),
) -> JSONResponse:
    """Recebe as notificações push do Mercado Livre (tópicos public_offers,
    public_candidates, catalog_item_competition_status, ...).

    Segurança: o ML NÃO assina as notificações, então a URL pública carrega um
    token secreto (validado aqui em tempo constante) — sem token correto, 401.
    Fail-closed: se o segredo não está configurado, recusa tudo. O payload é
    tratado como NÃO confiável: só dispara um re-sync do anúncio afetado, cujos
    dados reais vêm da API autenticada do ML (não do corpo da notificação).

    Contrato com o ML: responder 200 rápido. Aqui só gravamos a notificação
    (1 insert) e devolvemos 200; o processamento (re-sync) roda num job à parte.
    """
    expected = (settings.ml_webhook_token or "").strip()
    if not expected or not x_webhook_token or not hmac.compare_digest(x_webhook_token, expected):
        logger.warning("ml_webhook.unauthorized", has_token=bool(x_webhook_token))
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED, content={"error": "unauthorized"}
        )

    raw = await _read_json(request)
    if raw is None:
        logger.warning("ml_webhook: invalid JSON, acknowledged anyway")
        return _ack()
    topic = str(raw.get("topic") or "").strip()
    resource = str(raw.get("resource") or "").strip()
    if not topic or not resource:
        logger.warning("ml_webhook: missing topic/resource", raw=raw)
        return _ack()

    user_id = raw.get("user_id")
    # Defesa extra: ignora notificações de OUTRO vendedor (payload spoofado).
    own = not settings.ml_user_id or user_id is None or str(user_id) == str(settings.ml_user_id)
    row = MLWebhookNotificationORM(
        topic=topic[:60],
        resource=resource,
        mlb_id=_parse_mlb_from_resource(resource),
        ml_user_id=str(user_id) if user_id is not None else None,
        application_id=str(raw["application_id"])
        if raw.get("application_id") is not None
        else None,
        attempts=raw.get("attempts") if isinstance(raw.get("attempts"), int) else None,
        sent_at=_parse_iso(raw.get("sent")),
        raw=raw,
        status="pending" if own else "ignored",
    )
    try:
        session.add(row)
        await session.commit()
    except Exception as exc:
        # Falha ao gravar → 503 pro ML reenviar (não perdemos a notificação).
        logger.error("ml_webhook.store_failed", error=str(exc), topic=topic)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content={"error": "store_failed"}
        )
    logger.info("ml_webhook.received", topic=topic, resource=resource, own=own)
    return _ack()


_MLB_RE = re.compile(r"MLB\d{6,}")


def _parse_mlb_from_resource(resource: str) -> str | None:
    m = _MLB_RE.search(resource or "")
    return m.group(0) if m else None


def _parse_iso(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


async def _read_json(request: Request) -> dict[str, Any] | None:
    try:
        body = await request.json()
    except Exception:
        return None
    return body if isinstance(body, dict) else None


def _ack() -> JSONResponse:
    return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "received"})


def _cnpj_matches(received: str) -> bool:
    expected = (settings.tiny_expected_cnpj or "").strip()
    if not expected:
        return True
    return _normalize_cnpj(received) == _normalize_cnpj(expected)


def _normalize_cnpj(value: str) -> str:
    return "".join(ch for ch in value if ch.isdigit())


def _current_request_id() -> str | None:
    ctx = structlog.contextvars.get_contextvars()
    return ctx.get("request_id")
