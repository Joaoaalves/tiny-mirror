"""Logging handler that ships structured events to a Seq server.

Seq accepts CLEF (Compact Log Event Format) over ``POST /api/events/raw``.
We buffer events in a bounded queue and flush them on a background thread,
so the request loop is never blocked by network I/O. Failures are
swallowed — log shipping must not break the application.

Wire it from :func:`logging_config.configure_logging` only when
``settings.seq_url`` is set; otherwise it is omitted entirely and stdout
remains the single sink.
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import time
from typing import Any

import httpx

_CLEF_LEVELS = {
    "debug": "Debug",
    "info": "Information",
    "warning": "Warning",
    "warn": "Warning",
    "error": "Error",
    "critical": "Fatal",
    "fatal": "Fatal",
}

_DEFAULT_BATCH_SIZE = 50
_DEFAULT_FLUSH_INTERVAL = 1.0
_DEFAULT_QUEUE_MAXSIZE = 10_000
_DEFAULT_HTTP_TIMEOUT = 5.0


def _to_clef_level(level: Any) -> str:
    if not isinstance(level, str):
        return "Information"
    return _CLEF_LEVELS.get(level.lower(), "Information")


class SeqHandler(logging.Handler):
    """Send structlog-rendered JSON records to a Seq endpoint as CLEF.

    The handler expects records that have already been formatted into a
    JSON string by :class:`structlog.stdlib.ProcessorFormatter` whose
    final processor is :class:`structlog.processors.JSONRenderer` — i.e.
    ``record.getMessage()`` returns valid JSON containing at least
    ``timestamp``, ``event`` and ``level`` keys.

    Records that cannot be parsed are dropped silently; broken log lines
    must never crash the producer.
    """

    def __init__(
        self,
        server_url: str,
        api_key: str | None = None,
        *,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        flush_interval_seconds: float = _DEFAULT_FLUSH_INTERVAL,
        queue_maxsize: int = _DEFAULT_QUEUE_MAXSIZE,
        http_timeout_seconds: float = _DEFAULT_HTTP_TIMEOUT,
    ) -> None:
        super().__init__()
        self._url = server_url.rstrip("/") + "/api/events/raw?clef"
        self._api_key = api_key
        self._batch_size = batch_size
        self._flush_interval = flush_interval_seconds
        self._queue: queue.Queue[str] = queue.Queue(maxsize=queue_maxsize)
        self._client = httpx.Client(timeout=http_timeout_seconds)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="seq-shipper", daemon=True)
        self._thread.start()

    # ------------------------------------------------------------------
    # logging.Handler API
    # ------------------------------------------------------------------
    def emit(self, record: logging.LogRecord) -> None:
        try:
            rendered = self.format(record)
            event = json.loads(rendered)
        except Exception:
            return

        clef = self._to_clef(event)
        try:
            self._queue.put_nowait(json.dumps(clef, default=str))
        except queue.Full:
            # Drop on overflow rather than block the producer; the
            # stdout handler still records everything.
            return

    def close(self) -> None:
        self._stop.set()
        try:
            self._thread.join(timeout=self._flush_interval * 3)
        finally:
            try:
                self._client.close()
            except Exception:
                pass
            super().close()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    @staticmethod
    def _to_clef(event: dict[str, Any]) -> dict[str, Any]:
        clef: dict[str, Any] = {}
        timestamp = event.pop("timestamp", None)
        message = event.pop("event", None)
        level = event.pop("level", None)

        if timestamp is not None:
            clef["@t"] = timestamp
        if message is not None:
            clef["@m"] = message
        clef["@l"] = _to_clef_level(level)

        # Keep stack/exception info as standard CLEF attributes.
        exc_info = event.pop("exception", None)
        if exc_info:
            clef["@x"] = exc_info

        for key, value in event.items():
            # CLEF reserves the @ prefix; keep custom fields unprefixed.
            clef[key] = value

        return clef

    def _run(self) -> None:
        batch: list[str] = []
        last_flush = time.monotonic()
        while not self._stop.is_set():
            try:
                timeout = max(
                    0.05,
                    self._flush_interval - (time.monotonic() - last_flush),
                )
                try:
                    item = self._queue.get(timeout=timeout)
                    batch.append(item)
                except queue.Empty:
                    pass

                should_flush = batch and (
                    len(batch) >= self._batch_size
                    or (time.monotonic() - last_flush) >= self._flush_interval
                )
                if should_flush:
                    self._flush(batch)
                    batch = []
                    last_flush = time.monotonic()
            except Exception:
                # Defensive: never let the shipper thread die.
                batch = []
                last_flush = time.monotonic()

        # Drain remaining items on shutdown.
        try:
            while True:
                batch.append(self._queue.get_nowait())
        except queue.Empty:
            pass
        if batch:
            self._flush(batch)

    def _flush(self, batch: list[str]) -> None:
        body = ("\n".join(batch)).encode("utf-8")
        headers = {"Content-Type": "application/vnd.serilog.clef"}
        if self._api_key:
            headers["X-Seq-ApiKey"] = self._api_key
        try:
            self._client.post(self._url, content=body, headers=headers)
        except Exception:
            # Network/server errors are never propagated; the stdout
            # handler is the source of truth for local diagnostics.
            return
