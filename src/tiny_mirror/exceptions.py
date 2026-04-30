"""Project-wide exception hierarchy.

All exceptions raised by tiny-mirror inherit from :class:`TinyMirrorException`
so that callers (FastAPI error handlers, queue consumers) can distinguish
project errors from unexpected ones with a single ``except`` clause.
"""

from __future__ import annotations


class TinyMirrorException(Exception):
    """Base class for every exception raised by tiny-mirror."""

    def __init__(self, message: str, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(message={self.message!r}, details={self.details!r})"


class TinyAPIException(TinyMirrorException):
    """Raised on errors when talking to the Tiny REST API."""

    def __init__(
        self,
        message: str,
        details: dict | None = None,
        status_code: int | None = None,
        response_body: str | None = None,
    ) -> None:
        super().__init__(message, details)
        self.status_code = status_code
        self.response_body = response_body


class RateLimitException(TinyAPIException):
    """Raised when the rate limit is exceeded after exhausting retries."""

    def __init__(
        self,
        message: str,
        details: dict | None = None,
        status_code: int | None = None,
        response_body: str | None = None,
        retry_after_seconds: int | None = None,
    ) -> None:
        super().__init__(message, details, status_code, response_body)
        self.retry_after_seconds = retry_after_seconds


class TokenExpiredException(TinyAPIException):
    """Raised when both access and refresh tokens are expired/invalid."""


class TinyNotFoundException(TinyAPIException):
    """Raised when the Tiny API returns 404 for a known resource."""

    def __init__(
        self,
        message: str,
        resource_type: str,
        resource_id: int | str,
        details: dict | None = None,
        status_code: int | None = 404,
        response_body: str | None = None,
    ) -> None:
        super().__init__(message, details, status_code, response_body)
        self.resource_type = resource_type
        self.resource_id = resource_id


class SyncException(TinyMirrorException):
    """Raised on errors during data synchronization operations."""

    def __init__(
        self,
        message: str,
        sync_type: str,
        details: dict | None = None,
        sync_log_id: int | None = None,
    ) -> None:
        super().__init__(message, details)
        self.sync_type = sync_type
        self.sync_log_id = sync_log_id


class QueueException(TinyMirrorException):
    """Raised on errors publishing or consuming RabbitMQ messages."""

    def __init__(
        self,
        message: str,
        details: dict | None = None,
        queue_name: str | None = None,
        routing_key: str | None = None,
    ) -> None:
        super().__init__(message, details)
        self.queue_name = queue_name
        self.routing_key = routing_key


class DatabaseException(TinyMirrorException):
    """Raised on unexpected database errors (constraint violation, lost connection, etc.)."""


class ConfigurationException(TinyMirrorException):
    """Raised on critical configuration errors detected at startup.

    Raising this from the lifespan startup must terminate the service with a
    non-zero exit code.
    """
