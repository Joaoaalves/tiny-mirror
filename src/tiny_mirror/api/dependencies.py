"""Reusable FastAPI dependencies. Populated by stages 03+."""

from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.database import get_async_session


async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Re-export of :func:`tiny_mirror.database.get_async_session` as a Depends target."""
    async for session in get_async_session():
        yield session
