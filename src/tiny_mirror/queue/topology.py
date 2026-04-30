"""RabbitMQ topology declaration. Implemented in stage 05."""

from __future__ import annotations

from aio_pika.abc import AbstractChannel


async def setup_topology(channel: AbstractChannel) -> None:
    """Declare exchanges and queues. No-op until stage 05."""
    return None
