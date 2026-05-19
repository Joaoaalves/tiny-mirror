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

from datetime import UTC, datetime
from typing import Any

import structlog
from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from tiny_mirror.api.dependencies import get_queue_publisher
from tiny_mirror.api.schemas import OrderWebhookPayload, StockWebhookPayload
from tiny_mirror.config import settings
from tiny_mirror.exceptions import QueueException
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
