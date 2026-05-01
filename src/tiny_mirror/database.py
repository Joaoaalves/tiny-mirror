"""Async SQLAlchemy engine, session factory, and lifecycle helpers."""

from __future__ import annotations

from collections.abc import AsyncGenerator

import structlog
from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from tiny_mirror.config import settings
from tiny_mirror.exceptions import DatabaseException

logger = structlog.get_logger(__name__)

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


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
    """Declarative base for every ORM model in the project.

    Constraints and indexes follow the project naming convention so that
    Alembic-generated migrations and hand-written SQL stay consistent.
    """

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


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
