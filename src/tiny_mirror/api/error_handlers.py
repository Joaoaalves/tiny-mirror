"""Exception handlers registered on the FastAPI app."""

from __future__ import annotations

import structlog
from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from tiny_mirror.exceptions import TinyMirrorException

logger = structlog.get_logger(__name__)


async def _http_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, StarletteHTTPException)
    if exc.status_code == status.HTTP_404_NOT_FOUND:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={
                "error": "not_found",
                "message": "The requested resource was not found",
            },
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": "http_error", "message": exc.detail},
    )


async def _validation_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, RequestValidationError)
    # exc.errors() may include ctx['error'] = ValueError(...) on
    # `model_validator` failures; jsonable_encoder stringifies it.
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "validation_error",
            "message": "Request validation failed",
            "details": jsonable_encoder(exc.errors()),
        },
    )


async def _tiny_mirror_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, TinyMirrorException)
    logger.error(
        "tiny-mirror exception",
        exception_type=exc.__class__.__name__,
        message=exc.message,
        details=exc.details,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": "internal_error", "message": exc.message},
    )


async def _unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.critical(
        "Unhandled exception",
        exception_type=exc.__class__.__name__,
        message=str(exc),
        exc_info=exc,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": "internal_error", "message": "An unexpected error occurred"},
    )


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(StarletteHTTPException, _http_exception_handler)
    app.add_exception_handler(RequestValidationError, _validation_exception_handler)
    app.add_exception_handler(TinyMirrorException, _tiny_mirror_exception_handler)
    app.add_exception_handler(Exception, _unhandled_exception_handler)
