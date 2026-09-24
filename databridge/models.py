"""The small public connection contract."""

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, SecretStr, StringConstraints

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


ConnectionInput = Annotated[LocalConnection | SFTPConnection, Field(discriminator="type")]


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


class FileList(BaseModel):
    connection: str
    files: list[str]
    truncated: bool


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


class TransferRecord(BaseModel):
    id: str
    source: str
    source_file: str
    destination: str
    destination_file: str
    status: Literal["running", "completed", "failed"]
    started_at: str
    completed_at: str | None
    failed_at: str | None
    bytes_copied: int
    failure_phase: str | None
    error: str | None
