"""HTTP middlewares: request id propagation and request/response logging."""

from __future__ import annotations

import time
import uuid
from collections.abc import Awaitable, Callable

import structlog
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = structlog.get_logger(__name__)


class RequestIdMiddleware(BaseHTTPMiddleware):
    """Generate or propagate a request id and bind it as a structlog contextvar."""

    HEADER_NAME = "X-Request-Id"

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request_id = request.headers.get(self.HEADER_NAME) or str(uuid.uuid4())
        structlog.contextvars.bind_contextvars(request_id=request_id)
        try:
            response = await call_next(request)
        finally:
            structlog.contextvars.unbind_contextvars("request_id")
        response.headers[self.HEADER_NAME] = request_id
        return response


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Log each request's method, path, status, and total elapsed time."""

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.exception(
                "Request failed",
                method=request.method,
                path=request.url.path,
                elapsed_ms=round(elapsed_ms, 2),
            )
            raise
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "Request completed",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            elapsed_ms=round(elapsed_ms, 2),
        )
        return response
