"""HTTP application entry point and error boundary."""

import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Annotated
from uuid import uuid4

from fastapi import Body, FastAPI, Query, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from databridge.config import Settings
from databridge.connectors import connector_for
from databridge.errors import DataBridgeError
from databridge.models import (
    ConnectionInput,
    ConnectionView,
    ErrorResponse,
    FileList,
    HealthcheckResult,
    LocalConnection,
    PreviewResult,
    TransferRecord,
    TransferRequest,
)
from databridge.preview import preview
from databridge.store import ConnectionStore
from databridge.transfer import TransferService

_HTTP_STATUS = {
    "INVALID_CONNECTION_SETTINGS": 400,
    "INVALID_FILENAME": 400,
    "CONNECTION_NOT_FOUND": 404,
    "FILE_NOT_FOUND": 404,
    "CONNECTION_EXISTS": 409,
    "DESTINATION_EXISTS": 409,
    "CONNECTION_ROOT_UNAVAILABLE": 503,
    "LOCAL_IO_ERROR": 500,
    "SAME_FILE": 400,
    "UNSUPPORTED_PREVIEW_FORMAT": 400,
    "MALFORMED_FILE": 400,
    "PREVIEW_LIMIT_EXCEEDED": 400,
    "INVALID_REQUEST": 422,
    "TRANSFER_NOT_FOUND": 404,
    "SOURCE_READ_FAILED": 500,
    "DESTINATION_WRITE_FAILED": 500,
    "TRANSFER_INTERNAL_ERROR": 500,
    "SFTP_AUTH_FAILED": 502,
    "SFTP_HOST_KEY_REJECTED": 502,
    "SFTP_OPERATION_FAILED": 502,
    "SFTP_UNAVAILABLE": 503,
}


def _error_responses(*status_codes: int) -> dict[int, dict[str, object]]:
    return {
        code: {"model": ErrorResponse, "description": "DataBridge error"} for code in status_codes
    }


def _error_response(
    status_code: int, code: str, message: str, transfer_id: str | None = None
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "message": message, "transfer_id": transfer_id}},
    )


def _log_fields(values: dict[str, object], allowed: tuple[str, ...]) -> dict[str, object]:
    """Keep only known, scalar request fields; never copy a raw request body to logs."""
    result: dict[str, object] = {}
    for key in allowed:
        value = values.get(key)
        if isinstance(value, str):
            result[key] = value[:512]
        elif isinstance(value, (int, bool)):
            result[key] = value
    return result


def _body_log_fields(body: object, route: str) -> dict[str, object]:
    if not isinstance(body, dict):
        return {}
    if route == "/connections":
        common = ("name", "type")
        if body.get("type") == "local":
            return _log_fields(body, (*common, "path"))
        if body.get("type") == "sftp":
            return _log_fields(body, (*common, "host", "port", "username", "root"))
        return _log_fields(body, common)
    if route == "/transfers":
        return _log_fields(
            body, ("source", "source_file", "destination", "destination_file", "overwrite")
        )
    return {}


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build an app instance so tests can control its lifecycle and settings."""

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        effective = settings if settings is not None else Settings.from_env()
        store = ConnectionStore(effective.database_path, effective.encryption_key)
        store.initialize()
        application.state.store = store
        application.state.settings = effective
        log_path = effective.database_path.with_suffix(".requests.jsonl")
        descriptor = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(descriptor, "a", encoding="utf-8") as request_log:
            os.chmod(log_path, 0o600)
            application.state.request_log = request_log
            yield

    application = FastAPI(
        title="DataBridge",
        version="0.1.0",
        lifespan=lifespan,
        responses={422: {"model": ErrorResponse, "description": "Invalid request"}},
    )

    @application.middleware("http")
    async def log_request(request: Request, call_next):
        started = perf_counter()
        request_id = str(uuid4())
        request.state.request_id = request_id
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            response.headers["X-Request-ID"] = request_id
            return response
        except Exception as exc:
            request.state.error_type = type(exc).__name__
            raise
        finally:
            route = request.scope.get("route")
            event = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "request_id": request_id,
                "method": request.method,
                "route": route.path if route else None,
                "status_code": status_code,
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            }
            if request.path_params:
                event["path_params"] = _log_fields(
                    request.path_params, ("name", "filename", "transfer_id")
                )
            if "limit" in request.query_params:
                event["query_params"] = _log_fields(dict(request.query_params), ("limit",))
            if params := getattr(request.state, "body_params", None):
                event["body_params"] = params
            for key in ("error_code", "error_type", "transfer_id"):
                if value := getattr(request.state, key, None):
                    event[key] = value
            try:
                request.app.state.request_log.write(json.dumps(event, separators=(",", ":")) + "\n")
                request.app.state.request_log.flush()
            except OSError:
                logging.getLogger("uvicorn.error").error("request_log_write_failed")

    @application.exception_handler(DataBridgeError)
    def domain_error(request: Request, exc: DataBridgeError) -> JSONResponse:
        request.state.error_code = exc.code
        request.state.transfer_id = exc.transfer_id
        return _error_response(_HTTP_STATUS[exc.code], exc.code, exc.message, exc.transfer_id)

    @application.exception_handler(RequestValidationError)
    def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        request.state.error_code = "INVALID_REQUEST"
        route = request.scope.get("route")
        request.state.body_params = _body_log_fields(exc.body, route.path if route else "")
        first = exc.errors()[0]
        field = next(
            (part for part in reversed(first["loc"]) if isinstance(part, str) and part != "body"),
            "request",
        )
        problem = "is required" if first["type"] == "missing" else "is invalid"
        return _error_response(422, "INVALID_REQUEST", f"{field} {problem}")

    @application.post(
        "/connections",
        response_model=ConnectionView,
        status_code=status.HTTP_201_CREATED,
        responses=_error_responses(400, 409),
    )
    def create_connection(
        item: Annotated[
            ConnectionInput,
            Body(
                discriminator="type",
                description=(
                    "Local paths are relative to the server's working directory; "
                    "missing directories are created."
                ),
                openapi_examples={
                    "local_data": {
                        "summary": "Read supplied files",
                        "value": {"name": "local_data", "type": "local", "path": "data"},
                    },
                    "local_output": {
                        "summary": "Write local output",
                        "value": {"name": "local_output", "type": "local", "path": "output"},
                    },
                    "sftp": {
                        "summary": "Supplied SFTP fixture",
                        "value": {
                            "name": "remote_server",
                            "type": "sftp",
                            "host": "127.0.0.1",
                            "port": 2222,
                            "username": "testuser",
                            "password": "testpass",
                            "root": "data",
                        },
                    },
                },
            ),
        ],
        request: Request,
    ) -> ConnectionView:
        request.state.body_params = _body_log_fields(
            item.model_dump(exclude={"password"}), "/connections"
        )
        if isinstance(item, LocalConnection):
            try:
                requested_path = Path(item.path).expanduser()
                requested_path.mkdir(parents=True, exist_ok=True)
                path = requested_path.resolve(strict=True)
            except FileExistsError as exc:
                raise DataBridgeError(
                    "INVALID_CONNECTION_SETTINGS", "Local path is not a directory"
                ) from exc
            except (OSError, RuntimeError) as exc:
                raise DataBridgeError(
                    "INVALID_CONNECTION_SETTINGS", "Cannot create local directory"
                ) from exc
            if not path.is_dir():
                raise DataBridgeError(
                    "INVALID_CONNECTION_SETTINGS", "Local path is not a directory"
                )
            item = item.model_copy(update={"path": str(path)})
        return request.app.state.store.create(item)

    @application.get("/connections", response_model=list[ConnectionView])
    def list_connections(request: Request) -> list[ConnectionView]:
        return request.app.state.store.list_public()

    @application.get(
        "/connections/{name}", response_model=ConnectionView, responses=_error_responses(404)
    )
    def get_connection(name: str, request: Request) -> ConnectionView:
        return request.app.state.store.get_public(name)

    @application.get(
        "/connections/{name}/files",
        response_model=FileList,
        responses=_error_responses(404, 500, 502, 503),
    )
    def list_files(name: str, request: Request) -> FileList:
        item = request.app.state.store.get(name)
        connector = connector_for(item, request.app.state.settings.known_hosts_path)
        listing = connector.list_files()
        return FileList(connection=name, files=listing.files, truncated=listing.truncated)

    @application.post(
        "/connections/{name}/healthcheck",
        response_model=HealthcheckResult,
        responses=_error_responses(404, 500, 502, 503),
    )
    def healthcheck(name: str, request: Request) -> HealthcheckResult:
        item = request.app.state.store.get(name)
        connector = connector_for(item, request.app.state.settings.known_hosts_path)
        connector.list_files()
        return HealthcheckResult(connection=name, reachable=True)

    @application.get(
        "/connections/{name}/files/{filename}/head",
        response_model=PreviewResult,
        responses=_error_responses(400, 404, 500, 502, 503),
    )
    def preview_file(
        name: str, filename: str, request: Request, limit: int = Query(default=5, ge=1, le=100)
    ) -> PreviewResult:
        item = request.app.state.store.get(name)
        connector = connector_for(item, request.app.state.settings.known_hosts_path)
        return preview(connector, filename, limit)

    @application.post(
        "/transfers",
        response_model=TransferRecord,
        status_code=status.HTTP_201_CREATED,
        responses={
            **_error_responses(400, 404, 409, 500, 502, 503),
            201: {
                "description": (
                    "Transfer completed. The full response also includes null "
                    "failed_at, failure_phase, and error fields."
                ),
                "content": {
                    "application/json": {
                        "example": {
                            "id": "00000000-0000-4000-8000-000000000001",
                            "source": "local_data",
                            "source_file": "customers.csv",
                            "destination": "remote_server",
                            "destination_file": "customers.csv",
                            "status": "completed",
                            "started_at": "2026-09-24T15:00:00+00:00",
                            "completed_at": "2026-09-24T15:00:01+00:00",
                            "bytes_copied": 1005,
                        }
                    }
                },
            },
        },
    )
    def transfer_file(
        item: Annotated[
            TransferRequest,
            Body(
                description=(
                    "Create the named connections first. Source and destination files are names "
                    "at their configured roots. Run the upload before the download example."
                ),
                openapi_examples={
                    "upload": {
                        "summary": "Local to SFTP",
                        "value": {
                            "source": "local_data",
                            "source_file": "customers.csv",
                            "destination": "remote_server",
                            "destination_file": "customers.csv",
                            "overwrite": False,
                        },
                    },
                    "download": {
                        "summary": "SFTP to local",
                        "value": {
                            "source": "remote_server",
                            "source_file": "customers.csv",
                            "destination": "local_output",
                            "destination_file": "downloaded.csv",
                            "overwrite": False,
                        },
                    },
                },
            ),
        ],
        request: Request,
    ) -> TransferRecord:
        request.state.body_params = _body_log_fields(item.model_dump(), "/transfers")
        service = TransferService(
            request.app.state.store,
            lambda connection: connector_for(
                connection, request.app.state.settings.known_hosts_path
            ),
        )
        record = service.run(item)
        request.state.transfer_id = record.id
        return record

    @application.get(
        "/transfers/{transfer_id}",
        response_model=TransferRecord,
        responses=_error_responses(404),
    )
    def get_transfer(transfer_id: str, request: Request) -> TransferRecord:
        return request.app.state.store.get_transfer(transfer_id)

    return application


app = create_app()
