"""Abstract consumer with the standard ack / nack-to-DLQ behavior.

Every concrete consumer subclass overrides :meth:`handle` (the business
logic) and inherits :meth:`process` and :meth:`start_consuming`.

Failure semantics: every uncaught exception inside ``handle`` results in a
``nack(requeue=False)`` which routes the message to the queue's
dead-letter exchange. We never requeue — that would create a tight retry
loop. Operators investigate DLQ messages manually via the management UI.
"""

from __future__ import annotations

import abc
import json
from typing import Any

import structlog
from aio_pika.abc import AbstractChannel, AbstractIncomingMessage

from tiny_mirror.exceptions import TinyAPIException
from tiny_mirror.queue.publisher import QueuePublisher

logger = structlog.get_logger(__name__)


class BaseConsumer(abc.ABC):
    QUEUE_NAME: str = ""
    PREFETCH_COUNT: int = 1

    def __init__(self, channel: AbstractChannel, queue_publisher: QueuePublisher) -> None:
        self._channel = channel
        self._publisher = queue_publisher

    @abc.abstractmethod
    async def handle(self, message_body: dict[str, Any]) -> None:
        """Process the decoded message. Must raise on failure."""

    async def process(self, message: AbstractIncomingMessage) -> None:
        async with message.process(ignore_processed=True):
            try:
                body = json.loads(message.body.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.error(
                    "Invalid message body, sending to DLQ",
                    queue=self.QUEUE_NAME,
                    error=str(exc),
                )
                await message.nack(requeue=False)
                return

            try:
                await self.handle(body)
            except (KeyError, TypeError, ValueError) as exc:
                logger.error(
                    "Invalid message structure, sending to DLQ",
                    queue=self.QUEUE_NAME,
                    error=str(exc),
                )
                await message.nack(requeue=False)
            except TinyAPIException as exc:
                logger.error(
                    "Tiny API error processing message, sending to DLQ",
                    queue=self.QUEUE_NAME,
                    error=str(exc),
                    status_code=exc.status_code,
                )
                await message.nack(requeue=False)
            except Exception as exc:
                logger.exception(
                    "Unexpected error processing message, sending to DLQ",
                    queue=self.QUEUE_NAME,
                    error=str(exc),
                )
                await message.nack(requeue=False)

    async def start_consuming(self) -> None:
        if not self.QUEUE_NAME:
            raise ValueError(f"{type(self).__name__} must define QUEUE_NAME")
        await self._channel.set_qos(prefetch_count=self.PREFETCH_COUNT)
        queue = await self._channel.get_queue(self.QUEUE_NAME)
        await queue.consume(self.process)
        logger.info("Consumer started", queue=self.QUEUE_NAME)
