"""APScheduler job registration. Implemented in stage 12."""

from __future__ import annotations

from typing import Any


def setup_scheduler(app: Any) -> Any:
    """Configure AsyncIOScheduler and register jobs. No-op until stage 12."""
    return None


def shutdown_scheduler(scheduler: Any) -> None:
    """Stop APScheduler if it was started."""
    if scheduler is None:
        return
    scheduler.shutdown(wait=False)
