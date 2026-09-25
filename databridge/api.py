"""HTTP application: routes, error mapping, and the OpenAPI contract."""

import json
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any

from fastapi import Body, FastAPI, Query, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from databridge.config import Settings
from databridge.connectors import connector_for
from databridge.errors import DataBridgeError, ErrorCode
from databridge.models import (
    CONNECTION_TYPE_NAMES,
    ConnectionInput,
    ConnectionView,
    ErrorResponse,
    FieldProblem,
    FileList,
    HealthcheckResult,
    LocalConnection,
    PreviewResult,
    TransferRecord,
    TransferRequest,
)
from databridge.preview import preview
from databridge.request_log import RequestLog
from databridge.store import ConnectionStore
from databridge.transfer import TransferService
from databridge.web import RequestContextMiddleware, body_log_fields, error_response

_ALWAYS_POSSIBLE = (ErrorCode.INVALID_HOST_HEADER, ErrorCode.INTERNAL_ERROR)
_STATE_CHANGE = (ErrorCode.CROSS_ORIGIN_REJECTED,)
_SFTP_ACCESS = (
    ErrorCode.SFTP_AUTH_FAILED,
    ErrorCode.SFTP_HOST_KEY_REJECTED,
    ErrorCode.SFTP_OPERATION_FAILED,
    ErrorCode.SFTP_UNAVAILABLE,
)
_ROUTING_ERRORS = {
    404: (ErrorCode.ROUTE_NOT_FOUND, "No route matches this path; see /docs for the API"),
    405: (ErrorCode.METHOD_NOT_ALLOWED, "This route does not accept the request method"),
}
_LOCATION_PREFIXES = ("body", "query", "path")
_REQUIREMENT_PREFIXES = ("Input should", "String should", "Value should")


def _error_responses(
    *codes: ErrorCode, changes_state: bool = False
) -> dict[int | str, dict[str, Any]]:
    """Document every error status a route can return and the codes behind each status."""
    grouped: dict[int, set[str]] = {}
    for code in (*codes, *_ALWAYS_POSSIBLE, *(_STATE_CHANGE if changes_state else ())):
        grouped.setdefault(code.http_status, set()).add(code.value)
    return {
        status_code: {
            "model": ErrorResponse,
            "description": "Error codes: " + ", ".join(sorted(names)),
        }
        for status_code, names in sorted(grouped.items())
    }


def _problem_text(message: str) -> str:
    text = message.removeprefix("Value error, ")
    for prefix in _REQUIREMENT_PREFIXES:
        if text.startswith(prefix):
            return "must" + text[len(prefix) :]
    return text[:1].lower() + text[1:]


def _field_problem(error: Mapping[str, Any]) -> FieldProblem:
    kind = error["type"]
    if kind == "json_invalid":
        return FieldProblem(field="request body", problem="is not valid JSON")
    if kind in ("union_tag_invalid", "union_tag_not_found"):
        return FieldProblem(
            field="type", problem="must be one of: " + ", ".join(CONNECTION_TYPE_NAMES)
        )
    location = list(error["loc"])
    if location and location[0] in _LOCATION_PREFIXES:
        location = location[1:]
    if location and location[0] in CONNECTION_TYPE_NAMES:
        location = location[1:]
    field = ".".join(str(part) for part in location) or "request body"
    if kind == "missing":
        return FieldProblem(field=field, problem="is required")
    if kind == "extra_forbidden":
        return FieldProblem(field=field, problem="is not a recognized field")
    return FieldProblem(field=field, problem=_problem_text(str(error["msg"])))


def _openapi_without_default_validation(application: FastAPI) -> Callable[[], dict[str, Any]]:
    """Publish only declared responses; FastAPI adds a 422 to every route with parameters."""

    def openapi() -> dict[str, Any]:
        if application.openapi_schema is None:
            schema = get_openapi(
                title=application.title, version=application.version, routes=application.routes
            )
            for path_item in schema["paths"].values():
                for operation in path_item.values():
                    responses = operation["responses"]
                    if "HTTPValidationError" in json.dumps(responses.get("422", {})):
                        del responses["422"]
            component_schemas = schema.get("components", {}).get("schemas", {})
            component_schemas.pop("HTTPValidationError", None)
            component_schemas.pop("ValidationError", None)
            application.openapi_schema = schema
        return application.openapi_schema

    return openapi


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build an app instance so tests can control its lifecycle and settings."""

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        effective = settings if settings is not None else Settings.from_env()
        store = ConnectionStore(effective.database_path, effective.encryption_key)
        store.initialize()
        request_log = RequestLog(effective.request_log_path)
        request_log.open()
        application.state.store = store
        application.state.settings = effective
        application.state.request_log = request_log
        try:
            yield
        finally:
            request_log.close()

    application = FastAPI(
        title="DataBridge",
        version="0.1.0",
        lifespan=lifespan,
        strict_content_type=True,
    )
    application.add_middleware(RequestContextMiddleware)
    application.openapi = _openapi_without_default_validation(application)  # type: ignore[method-assign]

    @application.exception_handler(DataBridgeError)
    def domain_error(request: Request, exc: DataBridgeError) -> JSONResponse:
        request.state.error_code = exc.code
        request.state.transfer_id = exc.transfer_id
        return error_response(exc.code, exc.message, transfer_id=exc.transfer_id)

    @application.exception_handler(RequestValidationError)
    def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        route = request.scope.get("route")
        request.state.error_code = ErrorCode.INVALID_REQUEST
        request.state.body_params = body_log_fields(exc.body, route.path if route else "")
        details = [_field_problem(error) for error in exc.errors()]
        message = "; ".join(f"{detail.field} {detail.problem}" for detail in details)
        return error_response(ErrorCode.INVALID_REQUEST, message, details=details)

    @application.exception_handler(StarletteHTTPException)
    def routing_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code, message = _ROUTING_ERRORS.get(
            exc.status_code, (ErrorCode.INTERNAL_ERROR, "Unexpected HTTP error")
        )
        request.state.error_code = code
        return error_response(code, message, headers=exc.headers)

    @application.post(
        "/connections",
        response_model=ConnectionView,
        status_code=status.HTTP_201_CREATED,
        responses=_error_responses(
            ErrorCode.INVALID_REQUEST,
            ErrorCode.INVALID_CONNECTION_SETTINGS,
            ErrorCode.CONNECTION_EXISTS,
            changes_state=True,
        ),
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
        request.state.body_params = body_log_fields(
            item.model_dump(exclude={"password"}), "/connections"
        )
        if isinstance(item, LocalConnection):
            try:
                requested_path = Path(item.path).expanduser()
                requested_path.mkdir(parents=True, exist_ok=True)
                path = requested_path.resolve(strict=True)
            except FileExistsError as exc:
                raise DataBridgeError(
                    ErrorCode.INVALID_CONNECTION_SETTINGS, "Local path is not a directory"
                ) from exc
            except (OSError, RuntimeError) as exc:
                raise DataBridgeError(
                    ErrorCode.INVALID_CONNECTION_SETTINGS, "Cannot create local directory"
                ) from exc
            if not path.is_dir():
                raise DataBridgeError(
                    ErrorCode.INVALID_CONNECTION_SETTINGS, "Local path is not a directory"
                )
            item = item.model_copy(update={"path": str(path)})
        return request.app.state.store.create(item)

    @application.get(
        "/connections", response_model=list[ConnectionView], responses=_error_responses()
    )
    def list_connections(request: Request) -> list[ConnectionView]:
        return request.app.state.store.list_public()

    @application.get(
        "/connections/{name}",
        response_model=ConnectionView,
        responses=_error_responses(ErrorCode.CONNECTION_NOT_FOUND),
    )
    def get_connection(name: str, request: Request) -> ConnectionView:
        return request.app.state.store.get_public(name)

    listing_errors = (
        ErrorCode.CONNECTION_NOT_FOUND,
        ErrorCode.CONNECTION_ROOT_UNAVAILABLE,
        *_SFTP_ACCESS,
    )

    @application.get(
        "/connections/{name}/files",
        response_model=FileList,
        responses=_error_responses(*listing_errors),
    )
    def list_files(name: str, request: Request) -> FileList:
        item = request.app.state.store.get(name)
        connector = connector_for(item, request.app.state.settings.known_hosts_path)
        listing = connector.list_files()
        return FileList(connection=name, files=listing.files, truncated=listing.truncated)

    @application.post(
        "/connections/{name}/healthcheck",
        response_model=HealthcheckResult,
        responses=_error_responses(*listing_errors, changes_state=True),
    )
    def healthcheck(name: str, request: Request) -> HealthcheckResult:
        item = request.app.state.store.get(name)
        connector = connector_for(item, request.app.state.settings.known_hosts_path)
        connector.list_files()
        return HealthcheckResult(connection=name, reachable=True)

    @application.get(
        "/connections/{name}/files/{filename}/head",
        response_model=PreviewResult,
        responses=_error_responses(
            ErrorCode.INVALID_REQUEST,
            ErrorCode.CONNECTION_NOT_FOUND,
            ErrorCode.FILE_NOT_FOUND,
            ErrorCode.INVALID_FILENAME,
            ErrorCode.UNSUPPORTED_PREVIEW_FORMAT,
            ErrorCode.MALFORMED_FILE,
            ErrorCode.PREVIEW_LIMIT_EXCEEDED,
            ErrorCode.LOCAL_IO_ERROR,
            *_SFTP_ACCESS,
        ),
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
            **_error_responses(
                ErrorCode.INVALID_REQUEST,
                ErrorCode.INVALID_FILENAME,
                ErrorCode.SAME_FILE,
                ErrorCode.CONNECTION_NOT_FOUND,
                ErrorCode.FILE_NOT_FOUND,
                ErrorCode.DESTINATION_EXISTS,
                ErrorCode.CONNECTION_ROOT_UNAVAILABLE,
                ErrorCode.LOCAL_IO_ERROR,
                ErrorCode.SOURCE_READ_FAILED,
                ErrorCode.DESTINATION_WRITE_FAILED,
                ErrorCode.TRANSFER_INTERNAL_ERROR,
                *_SFTP_ACCESS,
                changes_state=True,
            ),
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
        request.state.body_params = body_log_fields(item.model_dump(), "/transfers")
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
        responses=_error_responses(ErrorCode.TRANSFER_NOT_FOUND),
    )
    def get_transfer(transfer_id: str, request: Request) -> TransferRecord:
        return request.app.state.store.get_transfer(transfer_id)

    return application


app = create_app()
