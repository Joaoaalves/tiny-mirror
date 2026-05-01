"""Structured logging configuration based on structlog.

Call :func:`configure_logging` exactly once, at the start of ``create_app()``,
before any other initialization so that subsequent log entries use the JSON
format and the ``service`` context variable.
"""

from __future__ import annotations

import logging
import sys

import structlog

from tiny_mirror.config import settings
from tiny_mirror.observability.seq_handler import SeqHandler


def configure_logging(log_level: str) -> None:
    """Configure structlog and the stdlib logging module.

    The pipeline merges contextvars (e.g. ``request_id``), adds level/logger
    name/timestamp/exception info, and renders everything as a single JSON
    object per log entry. Third-party loggers (SQLAlchemy, httpx, uvicorn,
    aio_pika) are routed through structlog so they share the same format.

    When ``settings.seq_url`` is set, a parallel :class:`SeqHandler` ships
    every event to the Seq server in CLEF format; stdout remains the
    primary local sink.
    """

    level = getattr(logging, log_level.upper(), logging.INFO)

    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)

    pre_chain = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        timestamper,
    ]

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=pre_chain,  # type: ignore[arg-type]
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(stream_handler)
    root_logger.setLevel(level)

    if settings.seq_url:
        seq_handler = SeqHandler(
            server_url=settings.seq_url,
            api_key=settings.seq_api_key or None,
        )
        seq_handler.setFormatter(formatter)
        seq_handler.setLevel(level)
        root_logger.addHandler(seq_handler)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            timestamper,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    structlog.contextvars.bind_contextvars(
        service="tiny-mirror",
        env=settings.app_env,
    )
