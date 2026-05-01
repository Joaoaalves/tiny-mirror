"""Pure domain models — no ORM, no framework dependencies."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass(frozen=True)
class OAuthToken:
    """In-memory representation of the active Tiny OAuth2 token pair.

    Every datetime field MUST be timezone-aware (UTC). Naive datetimes are
    rejected at construction time so the rest of the codebase can compare
    against ``datetime.now(UTC)`` without ambiguity.
    """

    access_token: str
    refresh_token: str
    expires_at: datetime
    refresh_expires_at: datetime

    def __post_init__(self) -> None:
        if self.expires_at.tzinfo is None or self.refresh_expires_at.tzinfo is None:
            raise ValueError("OAuthToken datetimes must be timezone-aware (UTC).")

    def is_expired(self) -> bool:
        return self.expires_at <= datetime.now(UTC)

    def is_refresh_expired(self) -> bool:
        return self.refresh_expires_at <= datetime.now(UTC)

    def is_expiring_soon(self, threshold_minutes: int = 30) -> bool:
        return self.expires_at - datetime.now(UTC) < timedelta(minutes=threshold_minutes)

    def seconds_until_expiry(self) -> int:
        delta = self.expires_at - datetime.now(UTC)
        return max(0, int(delta.total_seconds()))
