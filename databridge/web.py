"""HTTP boundary: request identity, local-origin guard, error envelope, and request log."""

import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from time import perf_counter
from typing import Any
from urllib.parse import parse_qsl
from uuid import uuid4

from starlette.datastructures import Headers, MutableHeaders
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from databridge.errors import ErrorCode
from databridge.models import ErrorDetail, ErrorResponse, FieldProblem
from databridge.request_log import RequestLog

REQUEST_ID_HEADER = "X-Request-ID"
_STATE_CHANGING_METHODS = frozenset({"DELETE", "PATCH", "POST", "PUT"})
_LOGGED_PATH_PARAMS = ("name", "filename", "transfer_id")
_LOGGED_QUERY_PARAMS = ("limit", "status")
_logger = logging.getLogger(__name__)


def error_response(
    code: ErrorCode,
    message: str,
    *,
    transfer_id: str | None = None,
    details: list[FieldProblem] | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    body = ErrorResponse(
        error=ErrorDetail(code=code, message=message, transfer_id=transfer_id, details=details)
    )
    return JSONResponse(body.model_dump(mode="json"), status_code=code.http_status, headers=headers)


def log_fields(values: Mapping[str, object], allowed: tuple[str, ...]) -> dict[str, object]:
    """Keep only known, scalar request fields; never copy a raw request body to logs."""
    result: dict[str, object] = {}
    for key in allowed:
        value = values.get(key)
        if isinstance(value, str):
            result[key] = value[:512]
        elif isinstance(value, (int, bool)):
            result[key] = value
    return result


def body_log_fields(body: object, route: str) -> dict[str, object]:
    if not isinstance(body, dict):
        return {}
    if route.startswith("/connections"):
        common = ("name", "type")
        if body.get("type") == "local":
            return log_fields(body, (*common, "path"))
        if body.get("type") == "sftp":
            return log_fields(body, (*common, "host", "port", "username", "root"))
        return log_fields(body, common)
    if route == "/transfers":
        return log_fields(
            body, ("source", "source_file", "destination", "destination_file", "overwrite")
        )
    return {}


def _host_name(host: str) -> str:
    if host.startswith("["):
        end = host.find("]")
        return host[1:end].lower() if end > 0 else ""
    name, separator, _port = host.rpartition(":")
    return (name if separator and ":" not in name else host).lower()


class RequestContextMiddleware:
    """Assign a request ID, reject foreign and cross-site callers, and translate crashes.

    The service trusts every local caller, so the Host allowlist blocks DNS-rebinding pages and
    the same-origin check blocks cross-site forms and scripts from changing state.
    """

    def __init__(
        self, app: ASGIApp, *, allowed_hosts: frozenset[str], request_log: RequestLog
    ) -> None:
        self.app = app
        self._allowed_hosts = allowed_hosts
        self._request_log = request_log

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = perf_counter()
        request_id = str(uuid4())
        state: dict[str, Any] = scope.setdefault("state", {})
        state["request_id"] = request_id
        response_status: int | None = None

        async def send_with_request_id(message: Message) -> None:
            nonlocal response_status
            if message["type"] == "http.response.start":
                response_status = message["status"]
                MutableHeaders(scope=message).append(REQUEST_ID_HEADER, request_id)
            await send(message)

        try:
            rejection = self._rejection(scope)
            if rejection is not None:
                code, message = rejection
                state["error_code"] = code
                await error_response(code, message)(scope, receive, send_with_request_id)
            else:
                await self.app(scope, receive, send_with_request_id)
        except Exception as exc:
            state["error_type"] = type(exc).__name__
            _logger.error("unhandled_exception request_id=%s", request_id, exc_info=exc)
            if response_status is not None:
                raise
            state["error_code"] = ErrorCode.INTERNAL_ERROR
            response = error_response(
                ErrorCode.INTERNAL_ERROR,
                f"Unexpected server error; the service log has details for request {request_id}",
            )
            await response(scope, receive, send_with_request_id)
        finally:
            self._log(scope, state, request_id, response_status or 500, started)

    def _rejection(self, scope: Scope) -> tuple[ErrorCode, str] | None:
        headers = Headers(scope=scope)
        host = headers.get("host", "")
        if _host_name(host) not in self._allowed_hosts:
            return (
                ErrorCode.INVALID_HOST_HEADER,
                "The Host header must name this local service, for example 127.0.0.1",
            )
        if scope["method"] not in _STATE_CHANGING_METHODS:
            return None
        origin = headers.get("origin")
        own_origin = f"{scope['scheme']}://{host}".lower()
        cross_site = headers.get("sec-fetch-site") == "cross-site"
        if cross_site or (origin is not None and origin.lower() != own_origin):
            return (
                ErrorCode.CROSS_ORIGIN_REJECTED,
                "Requests from other web origins cannot change DataBridge state",
            )
        return None

    def _log(
        self, scope: Scope, state: dict[str, Any], request_id: str, status: int, started: float
    ) -> None:
        route = scope.get("route")
        event: dict[str, object] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "request_id": request_id,
            "method": scope["method"],
            "route": getattr(route, "path", None),
            "status_code": status,
            "duration_ms": round((perf_counter() - started) * 1000, 2),
        }
        if path_params := log_fields(scope.get("path_params", {}), _LOGGED_PATH_PARAMS):
            event["path_params"] = path_params
        query = dict(parse_qsl(scope.get("query_string", b"").decode("latin-1")))
        if query_params := log_fields(query, _LOGGED_QUERY_PARAMS):
            event["query_params"] = query_params
        for key in ("body_params", "error_code", "error_type", "transfer_id"):
            if value := state.get(key):
                event[key] = value
        try:
            self._request_log.write(event)
        except OSError:
            _logger.error("request_log_write_failed request_id=%s", request_id)
