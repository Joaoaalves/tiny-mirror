"""Unit tests for the centralized ML-write logging (`_ml_write`).

Every seller-promotions write funnels through ``_ml_write`` so that a failed
ML call can be reconstructed end-to-end from Seq without a screenshot. These
tests pin the structured log shape — the exact request payload on the way out,
and the parsed ML error envelope + status + elapsed on the way back — because
that log IS the debugging contract once live writes are enabled.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from structlog.testing import capture_logs

from tiny_mirror.services.ml_promotion_service import MLPromotionService

pytestmark = pytest.mark.unit


def _resp(json_body: Any, status: int = 200) -> Any:
    resp = MagicMock()
    resp.status_code = status
    resp.content = (json.dumps(json_body) if json_body is not None else "").encode()
    resp.json = MagicMock(return_value=json_body if json_body is not None else {})
    resp.text = json.dumps(json_body) if json_body is not None else ""
    return resp


def _service(http: Any) -> MLPromotionService:
    token = MagicMock()
    token.get_valid_access_token = AsyncMock(return_value="tok")
    token.handle_unauthorized = AsyncMock(return_value="tok2")
    return MLPromotionService(token_service=token, http_client=http)


def _by_event(logs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {entry["event"]: entry for entry in logs}


@pytest.mark.asyncio
async def test_ml_write_logs_request_payload_and_ml_error() -> None:
    err = {
        "message": "Item already in promotion",
        "error": "item_already_in_promotion",
        "status": 400,
        "cause": [{"code": "x", "message": "y"}],
    }
    http = AsyncMock(spec=httpx.AsyncClient)
    http.post = AsyncMock(return_value=_resp(err, 400))
    svc = _service(http)

    body = {"promotion_type": "DEAL", "promotion_id": "P1", "deal_price": 10.5}
    with capture_logs() as logs:
        out = await svc._ml_write("POST", "MLB1", op="apply", body=body)

    assert out["ok"] is False
    assert out["status_code"] == 400
    assert out["error"] == "http"
    assert out["sent_body"] == body
    assert out["response"] == err

    events = _by_event(logs)
    # Request line carries the EXACT payload + correlation fields.
    req = events["ml_write.request"]
    assert req["op"] == "apply"
    assert req["mlb_id"] == "MLB1"
    assert req["ml_method"] == "POST"
    assert req["ml_body"] == body
    assert req["ml_params"] == {"app_version": "v2"}
    # Failure line surfaces the parsed ML error envelope as queryable fields.
    fail = events["ml_write.failed"]
    assert fail["status_code"] == 400
    assert fail["ml_error_code"] == "item_already_in_promotion"
    assert fail["ml_error_message"] == "Item already in promotion"
    assert fail["ml_error_status"] == 400
    assert fail["ml_error_cause"] == err["cause"]
    assert isinstance(fail["elapsed_ms"], int)


@pytest.mark.asyncio
async def test_ml_write_ok_logs_full_response() -> None:
    http = AsyncMock(spec=httpx.AsyncClient)
    http.post = AsyncMock(return_value=_resp({"id": "P1", "status": "started"}, 201))
    svc = _service(http)

    with capture_logs() as logs:
        out = await svc._ml_write(
            "POST", "MLB9", op="enroll_offer", body={"promotion_type": "SMART"}
        )

    assert out["ok"] is True
    assert out["status_code"] == 201
    events = _by_event(logs)
    assert "ml_write.request" in events
    ok = events["ml_write.ok"]
    assert ok["status_code"] == 201
    assert ok["ml_response"] == {"id": "P1", "status": "started"}


@pytest.mark.asyncio
async def test_ml_write_delete_merges_extra_params() -> None:
    http = AsyncMock(spec=httpx.AsyncClient)
    http.delete = AsyncMock(return_value=_resp({}, 200))
    svc = _service(http)

    await svc._ml_write(
        "DELETE", "MLB1", op="exit_promotion", extra_params={"promotion_type": "DEAL"}
    )
    _, kwargs = http.delete.call_args
    assert kwargs["params"] == {"app_version": "v2", "promotion_type": "DEAL"}


@pytest.mark.asyncio
async def test_ml_write_transport_error_is_logged_and_non_raising() -> None:
    http = AsyncMock(spec=httpx.AsyncClient)
    http.post = AsyncMock(side_effect=httpx.ConnectError("connpool boom"))
    svc = _service(http)

    with capture_logs() as logs:
        out = await svc._ml_write("POST", "MLB1", op="apply", body={"a": 1})

    assert out["ok"] is False
    assert out["status_code"] is None
    assert out["error"] == "transport"
    err = _by_event(logs)["ml_write.transport_error"]
    assert err["error_type"] == "ConnectError"
    assert "connpool boom" in err["error"]


@pytest.mark.asyncio
async def test_ml_write_refreshes_token_on_401_and_flags_retry() -> None:
    http = AsyncMock(spec=httpx.AsyncClient)
    http.post = AsyncMock(side_effect=[_resp({"m": "unauth"}, 401), _resp({"ok": True}, 200)])
    svc = _service(http)

    with capture_logs() as logs:
        out = await svc._ml_write("POST", "MLB1", op="apply", body={"a": 1})

    assert out["ok"] is True
    assert http.post.await_count == 2
    events = _by_event(logs)
    assert "ml_write.unauthorized_refresh" in events
    assert events["ml_write.ok"]["retried_auth"] is True
