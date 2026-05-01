"""Unit tests for :class:`tiny_mirror.queue.base_consumer.BaseConsumer`.

The interesting behavior is in ``process`` — the template that wraps
``handle`` and routes failures to the DLQ via ``message.nack`` (never
``requeue=True``). We exercise every error class with a synthetic
incoming message.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from tiny_mirror.exceptions import TinyAPIException
from tiny_mirror.queue.base_consumer import BaseConsumer

pytestmark = pytest.mark.unit


class _Recording(BaseConsumer):
    """BaseConsumer subclass whose ``handle`` does whatever the test
    configures via the ``handler`` attribute."""

    QUEUE_NAME = "test.queue"

    def __init__(self) -> None:
        super().__init__(channel=MagicMock(), queue_publisher=MagicMock())
        self.received: list[dict[str, Any]] = []
        self.handler = AsyncMock(return_value=None)

    async def handle(self, message_body: dict[str, Any]) -> None:
        self.received.append(message_body)
        await self.handler(message_body)


def _make_message(body: bytes, *, ack: AsyncMock | None = None, nack: AsyncMock | None = None) -> MagicMock:
    msg = MagicMock()
    msg.body = body
    msg.ack = AsyncMock() if ack is None else ack
    msg.nack = AsyncMock() if nack is None else nack

    @asynccontextmanager
    async def _process_ctx(*, ignore_processed: bool = False):
        yield None

    msg.process = _process_ctx
    return msg


# ---------------------------------------------------------------------------
async def test_process_decodes_json_and_calls_handle() -> None:
    consumer = _Recording()
    msg = _make_message(b'{"hello": "world"}')

    await consumer.process(msg)

    assert consumer.received == [{"hello": "world"}]
    msg.nack.assert_not_awaited()


async def test_process_invalid_json_nacks_without_requeue() -> None:
    consumer = _Recording()
    msg = _make_message(b"not-json")

    await consumer.process(msg)

    msg.nack.assert_awaited_once_with(requeue=False)
    assert consumer.received == []


async def test_process_handle_keyerror_nacks_to_dlq() -> None:
    consumer = _Recording()
    consumer.handler = AsyncMock(side_effect=KeyError("missing"))
    msg = _make_message(b'{"a": 1}')

    await consumer.process(msg)

    msg.nack.assert_awaited_once_with(requeue=False)


async def test_process_handle_typeerror_nacks_to_dlq() -> None:
    consumer = _Recording()
    consumer.handler = AsyncMock(side_effect=TypeError("bad shape"))
    msg = _make_message(b'{"a": 1}')

    await consumer.process(msg)

    msg.nack.assert_awaited_once_with(requeue=False)


async def test_process_handle_valueerror_nacks_to_dlq() -> None:
    consumer = _Recording()
    consumer.handler = AsyncMock(side_effect=ValueError("bad value"))
    msg = _make_message(b'{"a": 1}')

    await consumer.process(msg)

    msg.nack.assert_awaited_once_with(requeue=False)


async def test_process_tiny_api_exception_nacks_to_dlq() -> None:
    consumer = _Recording()
    consumer.handler = AsyncMock(
        side_effect=TinyAPIException("boom", status_code=500)
    )
    msg = _make_message(b'{"a": 1}')

    await consumer.process(msg)

    msg.nack.assert_awaited_once_with(requeue=False)


async def test_process_unexpected_exception_nacks_to_dlq() -> None:
    consumer = _Recording()
    consumer.handler = AsyncMock(side_effect=RuntimeError("kaboom"))
    msg = _make_message(b'{"a": 1}')

    await consumer.process(msg)

    msg.nack.assert_awaited_once_with(requeue=False)


async def test_start_consuming_requires_queue_name() -> None:
    class _NoName(BaseConsumer):
        QUEUE_NAME = ""  # default, must trip the guard

        async def handle(self, message_body: dict[str, Any]) -> None:
            return None

    c = _NoName(channel=MagicMock(), queue_publisher=MagicMock())
    with pytest.raises(ValueError):
        await c.start_consuming()
