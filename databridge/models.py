"""The public request, response, and record models."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Annotated, Any, Literal, get_args

from pydantic import AwareDatetime, BaseModel, Field, SecretStr, StringConstraints

from databridge.errors import ErrorCode

ConnectionName = Annotated[str, StringConstraints(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")]


class LocalConnection(BaseModel):
    name: ConnectionName
    type: Literal["local"]
    path: str = Field(min_length=1)


class SFTPConnection(BaseModel):
    name: ConnectionName
    type: Literal["sftp"]
    host: str = Field(min_length=1)
    port: int = Field(gt=0, le=65535)
    username: str = Field(min_length=1)
    password: SecretStr = Field(min_length=1)
    root: str = Field(min_length=1)


Connection = LocalConnection | SFTPConnection
ConnectionInput = Annotated[Connection, Field(discriminator="type")]


class LocalConnectionView(BaseModel):
    name: str
    type: Literal["local"]
    path: str


class SFTPConnectionView(BaseModel):
    name: str
    type: Literal["sftp"]
    host: str
    port: int
    username: str
    root: str


ConnectionView = LocalConnectionView | SFTPConnectionView


def _type_name(model: type[BaseModel]) -> str:
    return str(get_args(model.model_fields["type"].annotation)[0])


CONNECTION_MODELS: Mapping[str, tuple[type[Connection], type[ConnectionView]]] = MappingProxyType(
    {
        _type_name(LocalConnection): (LocalConnection, LocalConnectionView),
        _type_name(SFTPConnection): (SFTPConnection, SFTPConnectionView),
    }
)
CONNECTION_TYPE_NAMES: tuple[str, ...] = tuple(CONNECTION_MODELS)


class FileList(BaseModel):
    connection: str
    files: list[str]
    truncated: bool


class HealthcheckResult(BaseModel):
    connection: str
    reachable: Literal[True]


class FieldProblem(BaseModel):
    field: str
    problem: str


class ErrorDetail(BaseModel):
    code: ErrorCode
    message: str
    transfer_id: str | None = None
    details: list[FieldProblem] | None = None


class ErrorResponse(BaseModel):
    error: ErrorDetail


class PreviewResult(BaseModel):
    filename: str
    format: Literal["csv", "json"]
    rows: list[dict[str, Any]]
    schema_: dict[str, Literal["string", "integer", "float", "boolean", "date"]] = Field(
        alias="schema"
    )


class TransferRequest(BaseModel):
    source: ConnectionName
    source_file: str = Field(min_length=1)
    destination: ConnectionName
    destination_file: str = Field(min_length=1)
    overwrite: bool = False


TransferStatus = Literal["running", "publishing", "completed", "failed"]
FailurePhase = Literal[
    "source_lookup",
    "destination_lookup",
    "source_open",
    "destination_open",
    "source_read",
    "destination_write",
    "publication",
    "interruption",
]


class TransferRecord(BaseModel):
    id: str
    source: str
    source_file: str
    destination: str
    destination_file: str
    overwrite: bool
    status: TransferStatus = Field(
        description=(
            "running: copying to a staging file; publishing: every byte is staged and the "
            "destination is being replaced; completed; failed"
        )
    )
    started_at: AwareDatetime
    updated_at: AwareDatetime = Field(
        description="Last change, including progress checkpoints about once per second"
    )
    completed_at: AwareDatetime | None
    failed_at: AwareDatetime | None
    bytes_copied: int = Field(
        ge=0,
        description=(
            "Bytes written to the staging file; after a failure, a nonzero count does not mean "
            "the destination changed"
        ),
    )
    failure_phase: FailurePhase | None
    error_code: ErrorCode | None
    error: str | None
