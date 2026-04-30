"""RabbitMQ connection and channel lifecycle helpers."""

from __future__ import annotations

import aio_pika
import structlog
from aio_pika.abc import AbstractChannel, AbstractRobustConnection

from tiny_mirror.config import settings
from tiny_mirror.exceptions import QueueException

logger = structlog.get_logger(__name__)

_connection: AbstractRobustConnection | None = None
_channel: AbstractChannel | None = None


async def initialize_rabbitmq() -> None:
    """Open a robust connection and a single shared channel.

    ``connect_robust`` reconnects automatically on transient failures.
    """
    global _connection, _channel
    try:
        _connection = await aio_pika.connect_robust(settings.rabbitmq_url)
        _channel = await _connection.channel()
    except Exception as exc:
        logger.error("RabbitMQ connectivity check failed", error=str(exc))
        raise QueueException(f"Failed to connect to RabbitMQ: {exc}") from exc


async def close_rabbitmq() -> None:
    """Close the shared channel and connection if they were initialized."""
    global _connection, _channel
    if _channel is not None:
        await _channel.close()
        _channel = None
    if _connection is not None:
        await _connection.close()
        _connection = None


def get_channel() -> AbstractChannel:
    """Return the initialized RabbitMQ channel.

    Raises :class:`QueueException` if :func:`initialize_rabbitmq` was not called.
    """
    if _channel is None:
        raise QueueException("RabbitMQ channel not initialized")
    return _channel
