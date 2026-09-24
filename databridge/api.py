"""HTTP application entry point and error boundary."""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from databridge.config import Settings
from databridge.connectors.local import LocalConnector
from databridge.errors import DataBridgeError
from databridge.models import (
    ConnectionInput,
    ConnectionView,
    FileList,
    LocalConnection,
)
from databridge.store import ConnectionStore

_HTTP_STATUS = {
    "INVALID_CONNECTION_SETTINGS": 400,
    "INVALID_FILENAME": 400,
    "CONNECTION_NOT_FOUND": 404,
    "FILE_NOT_FOUND": 404,
    "CONNECTION_EXISTS": 409,
    "DESTINATION_EXISTS": 409,
    "CONNECTION_ROOT_UNAVAILABLE": 503,
    "LOCAL_IO_ERROR": 500,
}


def _error_response(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "message": message, "transfer_id": None}},
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
        return _error_response(_HTTP_STATUS[exc.code], exc.code, exc.message)

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
        if item.type != "local":
            raise DataBridgeError("CONNECTION_ROOT_UNAVAILABLE", "SFTP file access is not ready")
        listing = LocalConnector(Path(item.path)).list_files()
        return FileList(connection=name, files=listing.files, truncated=listing.truncated)

    return application


app = create_app()
