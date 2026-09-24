"""HTTP application entry point and error boundary."""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Query, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from databridge.config import Settings
from databridge.connectors import connector_for
from databridge.errors import DataBridgeError
from databridge.models import (
    ConnectionInput,
    ConnectionView,
    FileList,
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


def _error_response(
    status_code: int, code: str, message: str, transfer_id: str | None = None
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "message": message, "transfer_id": transfer_id}},
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build an app instance so tests can control its lifecycle and settings."""

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        effective = settings if settings is not None else Settings.from_env()
        store = ConnectionStore(effective.database_path, effective.encryption_key)
        store.initialize()
        application.state.store = store
        application.state.settings = effective
        yield

    application = FastAPI(title="DataBridge", version="0.1.0", lifespan=lifespan)

    @application.exception_handler(DataBridgeError)
    def domain_error(_request: Request, exc: DataBridgeError) -> JSONResponse:
        return _error_response(_HTTP_STATUS[exc.code], exc.code, exc.message, exc.transfer_id)

    @application.exception_handler(RequestValidationError)
    def validation_error(_request: Request, exc: RequestValidationError) -> JSONResponse:
        first = exc.errors()[0]
        field = next(
            (part for part in reversed(first["loc"]) if isinstance(part, str) and part != "body"),
            "request",
        )
        problem = "is required" if first["type"] == "missing" else "is invalid"
        return _error_response(422, "INVALID_REQUEST", f"{field} {problem}")

    @application.post(
        "/connections", response_model=ConnectionView, status_code=status.HTTP_201_CREATED
    )
    def create_connection(item: ConnectionInput, request: Request) -> ConnectionView:
        if isinstance(item, LocalConnection):
            try:
                path = Path(item.path).expanduser().resolve(strict=True)
            except OSError as exc:
                raise DataBridgeError(
                    "INVALID_CONNECTION_SETTINGS", "Local directory does not exist"
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

    @application.get("/connections/{name}", response_model=ConnectionView)
    def get_connection(name: str, request: Request) -> ConnectionView:
        return request.app.state.store.get_public(name)

    @application.get("/connections/{name}/files", response_model=FileList)
    def list_files(name: str, request: Request) -> FileList:
        item = request.app.state.store.get(name)
        connector = connector_for(item, request.app.state.settings.known_hosts_path)
        listing = connector.list_files()
        return FileList(connection=name, files=listing.files, truncated=listing.truncated)

    @application.get("/connections/{name}/files/{filename}/head", response_model=PreviewResult)
    def preview_file(
        name: str, filename: str, request: Request, limit: int = Query(default=5, ge=1, le=100)
    ) -> PreviewResult:
        item = request.app.state.store.get(name)
        connector = connector_for(item, request.app.state.settings.known_hosts_path)
        return preview(connector, filename, limit)

    @application.post(
        "/transfers", response_model=TransferRecord, status_code=status.HTTP_201_CREATED
    )
    def transfer_file(item: TransferRequest, request: Request) -> TransferRecord:
        service = TransferService(
            request.app.state.store,
            lambda connection: connector_for(
                connection, request.app.state.settings.known_hosts_path
            ),
        )
        return service.run(item)

    @application.get("/transfers/{transfer_id}", response_model=TransferRecord)
    def get_transfer(transfer_id: str, request: Request) -> TransferRecord:
        return request.app.state.store.get_transfer(transfer_id)

    return application


app = create_app()
