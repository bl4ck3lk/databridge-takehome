"""HTTP application: its startup resources, routes, error mapping, and the OpenAPI contract."""

import json
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from typing import Annotated, Any

from fastapi import Body, FastAPI, Query, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from starlette.exceptions import HTTPException as StarletteHTTPException

from databridge.config import Settings
from databridge.connectors import open_connector, prepare_connection
from databridge.connectors.base import Connector, ConnectorContext
from databridge.errors import DataBridgeError, ErrorCode
from databridge.models import (
    CONNECTION_TYPE_NAMES,
    ConnectionInput,
    ConnectionView,
    ErrorResponse,
    FieldProblem,
    FileList,
    HealthcheckResult,
    PreviewResult,
    TransferRecord,
    TransferRequest,
    TransferStatus,
)
from databridge.preview import DEFAULT_PREVIEW_ROWS, MAX_PREVIEW_ROWS, preview
from databridge.request_log import RequestLog
from databridge.store import ConnectionStore, DatabaseOwnerLock
from databridge.transfer import TransferService
from databridge.web import RequestContextMiddleware, body_log_fields, error_response

_ALWAYS_POSSIBLE = (ErrorCode.INVALID_HOST_HEADER, ErrorCode.INTERNAL_ERROR)
_STATE_CHANGE = (ErrorCode.CROSS_ORIGIN_REJECTED,)
_SFTP_ACCESS = (
    ErrorCode.INVALID_CONNECTION_SETTINGS,
    ErrorCode.SFTP_AUTH_FAILED,
    ErrorCode.SFTP_HOST_KEY_REJECTED,
    ErrorCode.SFTP_OPERATION_FAILED,
    ErrorCode.SFTP_UNAVAILABLE,
)
_ROUTING_ERRORS = {
    404: (ErrorCode.ROUTE_NOT_FOUND, "No route matches this path; see /docs for the API"),
    405: (ErrorCode.METHOD_NOT_ALLOWED, "This route does not accept the request method"),
}
TRANSFER_PAGE_LIMIT = 500
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


_CONNECTION_BODY = Body(
    discriminator="type",
    description=(
        "Local paths are relative to the server's working directory; missing directories are "
        "created. Unknown fields are rejected."
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
)


def _store(request: Request) -> ConnectionStore:
    store: ConnectionStore = request.app.state.store
    return store


def _transfers(request: Request) -> TransferService:
    transfers: TransferService = request.app.state.transfers
    return transfers


def _connector(request: Request, name: str) -> Connector:
    context: ConnectorContext = request.app.state.connector_context
    return open_connector(_store(request).get(name), context)


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


def _published_openapi(application: FastAPI) -> Callable[[], dict[str, Any]]:
    """Publish only declared responses, with their examples exactly as declared.

    FastAPI adds a 422 to every route with parameters, and it drops null values from the whole
    document, so an example would not show fields that the live response returns as null.
    """

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
            for route in application.routes:
                if not isinstance(route, APIRoute):
                    continue
                for method in route.methods or ():
                    published = schema["paths"][route.path_format][method.lower()]["responses"]
                    for status_code, declared in route.responses.items():
                        for media_type, content in declared.get("content", {}).items():
                            if "example" in content:
                                published[str(status_code)]["content"][media_type]["example"] = (
                                    content["example"]
                                )
            component_schemas = schema.get("components", {}).get("schemas", {})
            component_schemas.pop("HTTPValidationError", None)
            component_schemas.pop("ValidationError", None)
            application.openapi_schema = schema
        return application.openapi_schema

    return openapi


@contextmanager
def open_app(settings: Settings) -> Iterator[FastAPI]:
    """Take the database lock, verify the database, and open the request log. Then yield the
    app that uses them. Leaving the block closes the log and releases the lock.

    These steps run before a server starts, not in the ASGI lifespan. Thus a database problem
    is one StartupError, not a lifespan failure with a traceback.
    """
    with DatabaseOwnerLock(settings.lock_path):
        store = ConnectionStore(settings.database_path, settings.encryption_key)
        store.initialize()
        request_log = RequestLog(settings.request_log_path)
        request_log.open()
        try:
            yield _application(settings, store, request_log)
        finally:
            request_log.close()


def _application(settings: Settings, store: ConnectionStore, request_log: RequestLog) -> FastAPI:
    context = ConnectorContext(known_hosts_path=settings.known_hosts_path)
    application = FastAPI(title="DataBridge", version="0.1.0", strict_content_type=True)
    application.state.store = store
    application.state.connector_context = context
    application.state.transfers = TransferService(
        store, lambda connection: open_connector(connection, context)
    )
    application.add_middleware(
        RequestContextMiddleware, allowed_hosts=settings.allowed_hosts, request_log=request_log
    )
    application.openapi = _published_openapi(application)  # type: ignore[method-assign]

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
        if exc.status_code == 400:
            # FastAPI raises a plain 400 when a JSON body cannot be decoded (for example, bytes
            # that are not UTF-8); that is the client's malformed request, not a server fault.
            request.state.error_code = ErrorCode.INVALID_REQUEST
            body = FieldProblem(field="request body", problem="is not valid JSON")
            return error_response(
                ErrorCode.INVALID_REQUEST, "request body is not valid JSON", details=[body]
            )
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
        item: Annotated[ConnectionInput, _CONNECTION_BODY], request: Request
    ) -> ConnectionView:
        request.state.body_params = body_log_fields(
            item.model_dump(exclude={"password"}), "/connections"
        )
        return _store(request).create(prepare_connection(item))

    @application.put(
        "/connections/{name}",
        response_model=ConnectionView,
        responses=_error_responses(
            ErrorCode.INVALID_REQUEST,
            ErrorCode.INVALID_CONNECTION_SETTINGS,
            ErrorCode.CONNECTION_NOT_FOUND,
            changes_state=True,
        ),
    )
    def replace_connection(
        name: str, item: Annotated[ConnectionInput, _CONNECTION_BODY], request: Request
    ) -> ConnectionView:
        """Replace every setting of a connection; an SFTP password must be sent again."""
        request.state.body_params = body_log_fields(
            item.model_dump(exclude={"password"}), "/connections"
        )
        if item.name != name:
            raise DataBridgeError(
                ErrorCode.INVALID_CONNECTION_SETTINGS,
                f"The body names connection '{item.name}', but the path names '{name}'",
            )
        store = _store(request)
        store.get_public(name)  # a missing connection must not create a local directory
        return store.replace(prepare_connection(item))

    @application.delete(
        "/connections/{name}",
        status_code=status.HTTP_204_NO_CONTENT,
        response_class=Response,
        responses=_error_responses(ErrorCode.CONNECTION_NOT_FOUND, changes_state=True),
    )
    def delete_connection(name: str, request: Request) -> Response:
        """Remove a connection; its files and its transfer records stay."""
        _store(request).delete(name)
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @application.get(
        "/connections", response_model=list[ConnectionView], responses=_error_responses()
    )
    def list_connections(request: Request) -> list[ConnectionView]:
        return _store(request).list_public()

    @application.get(
        "/connections/{name}",
        response_model=ConnectionView,
        responses=_error_responses(ErrorCode.CONNECTION_NOT_FOUND),
    )
    def get_connection(name: str, request: Request) -> ConnectionView:
        return _store(request).get_public(name)

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
        listing = _connector(request, name).list_files()
        return FileList(connection=name, files=listing.files, truncated=listing.truncated)

    @application.post(
        "/connections/{name}/healthcheck",
        response_model=HealthcheckResult,
        responses=_error_responses(*listing_errors, ErrorCode.LOCAL_IO_ERROR, changes_state=True),
    )
    def healthcheck(name: str, request: Request) -> HealthcheckResult:
        """Check credentials, list access to the root, and write access with a probe file."""
        access = _connector(request, name).check_access()
        return HealthcheckResult(connection=name, reachable=True, writable=access.writable)

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
            ErrorCode.FILE_NOT_READABLE,
            ErrorCode.SOURCE_CHANGED,
            ErrorCode.CONNECTION_ROOT_UNAVAILABLE,
            ErrorCode.LOCAL_IO_ERROR,
            *_SFTP_ACCESS,
        ),
    )
    def preview_file(
        name: str,
        filename: str,
        request: Request,
        limit: Annotated[
            int, Query(ge=1, le=MAX_PREVIEW_ROWS, description="Rows to return")
        ] = DEFAULT_PREVIEW_ROWS,
    ) -> PreviewResult:
        return preview(_connector(request, name), filename, limit)

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
                ErrorCode.DESTINATION_NOT_WRITABLE,
                ErrorCode.FILE_NOT_READABLE,
                ErrorCode.PUBLISH_UNSUPPORTED,
                ErrorCode.DESTINATION_FULL,
                ErrorCode.CONNECTION_ROOT_UNAVAILABLE,
                ErrorCode.LOCAL_IO_ERROR,
                ErrorCode.SOURCE_READ_FAILED,
                ErrorCode.SOURCE_CHANGED,
                ErrorCode.DESTINATION_WRITE_FAILED,
                ErrorCode.TRANSFER_CAPACITY_EXCEEDED,
                ErrorCode.TRANSFER_INTERNAL_ERROR,
                ErrorCode.TRANSFER_RECORD_FAILED,
                ErrorCode.TRANSFER_STATE_CONFLICT,
                *_SFTP_ACCESS,
                changes_state=True,
            ),
            201: {
                "description": "Transfer completed; every byte is published at the destination",
                "content": {
                    "application/json": {
                        "example": {
                            "id": "00000000-0000-4000-8000-000000000001",
                            "source": "local_data",
                            "source_file": "customers.csv",
                            "destination": "remote_server",
                            "destination_file": "customers.csv",
                            "overwrite": False,
                            "status": "completed",
                            "started_at": "2026-09-24T15:00:00Z",
                            "updated_at": "2026-09-24T15:00:01Z",
                            "completed_at": "2026-09-24T15:00:01Z",
                            "failed_at": None,
                            "bytes_copied": 1005,
                            "failure_phase": None,
                            "error_code": None,
                            "error": None,
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
        record = _transfers(request).run(item)
        request.state.transfer_id = record.id
        return record

    @application.get(
        "/transfers",
        response_model=list[TransferRecord],
        responses=_error_responses(ErrorCode.INVALID_REQUEST),
    )
    def list_transfers(
        request: Request,
        status_filter: Annotated[
            TransferStatus | None,
            Query(alias="status", description="Return only transfers in this status"),
        ] = None,
        limit: Annotated[
            int, Query(ge=1, le=TRANSFER_PAGE_LIMIT, description="Newest transfers to return")
        ] = 50,
    ) -> list[TransferRecord]:
        """List transfers newest first; a running transfer shows its latest progress."""
        return _store(request).list_transfers(status_filter, limit)

    @application.get(
        "/transfers/{transfer_id}",
        response_model=TransferRecord,
        responses=_error_responses(ErrorCode.TRANSFER_NOT_FOUND),
    )
    def get_transfer(transfer_id: str, request: Request) -> TransferRecord:
        return _store(request).get_transfer(transfer_id)

    return application
