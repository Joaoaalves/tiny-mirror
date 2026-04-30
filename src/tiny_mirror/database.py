"""Async SQLAlchemy engine, session factory, and lifecycle helpers."""

from __future__ import annotations

from collections.abc import AsyncGenerator

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from tiny_mirror.config import settings
from tiny_mirror.exceptions import DatabaseException

logger = structlog.get_logger(__name__)


engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    echo=False,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    autoflush=False,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    """Declarative base for every ORM model in the project."""


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency that yields an :class:`AsyncSession`.

    The session is closed automatically on exit. Commits and rollbacks are the
    repository layer's responsibility.
    """
    async with AsyncSessionLocal() as session:
        yield session


async def initialize_database() -> None:
    """Verify connectivity by running ``SELECT 1`` on the configured engine."""
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as exc:
        logger.error("Database connectivity check failed", error=str(exc))
        raise DatabaseException(f"Failed to connect to database: {exc}") from exc


async def close_database() -> None:
    """Dispose of the engine and release every pooled connection."""
    await engine.dispose()
