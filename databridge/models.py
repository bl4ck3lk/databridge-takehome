"""The public request, response, and record models."""

import ipaddress
import re
import unicodedata
from collections.abc import Mapping
from types import MappingProxyType
from typing import Annotated, Any, Literal, get_args

from pydantic import (
    AfterValidator,
    AliasChoices,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    StringConstraints,
)

from databridge.errors import ErrorCode

MAX_SETTING_LENGTH = 4096
_HOST_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")


def _plain_text(value: str) -> str:
    """Refuse control characters, which would corrupt paths and log lines."""
    if any(unicodedata.category(character) == "Cc" for character in value):
        raise ValueError("must not contain control characters")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError("must be valid Unicode text") from None
    return value


def _host(value: str) -> str:
    """Accept an IP address or an ASCII DNS name, in lower case as OpenSSH records hosts."""
    try:
        return str(ipaddress.ip_address(value))
    except ValueError:
        pass
    name = value.lower()
    if len(name) > 253 or not all(_HOST_LABEL.fullmatch(label) for label in name.split(".")):
        raise ValueError(
            "must be an IP address or a host name made of ASCII letters, digits, and hyphens; "
            "use the punycode form of an internationalized name"
        )
    return name


ConnectionName = Annotated[str, StringConstraints(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")]
SettingText = Annotated[
    str,
    StringConstraints(min_length=1, max_length=MAX_SETTING_LENGTH),
    AfterValidator(_plain_text),
]
HostName = Annotated[str, StringConstraints(min_length=1), AfterValidator(_host)]


class _ConnectionInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


class LocalConnection(_ConnectionInput):
    name: ConnectionName
    type: Literal["local"]
    path: SettingText = Field(
        description=(
            "Directory on the service host; a relative path starts at the service's working "
            "directory, and a missing directory is created"
        )
    )


class SFTPConnection(_ConnectionInput):
    name: ConnectionName
    type: Literal["sftp"]
    host: HostName = Field(
        description="IP address or host name; a host name is stored in lower case"
    )
    port: int = Field(gt=0, le=65535)
    username: SettingText = Field(
        validation_alias=AliasChoices("username", "user"),
        description="SFTP login name; the brief's `user` field is accepted too",
    )
    password: SecretStr = Field(min_length=1)
    root: SettingText = Field(
        description=(
            "Directory on the SFTP server that holds the files, relative to the login directory "
            "unless it starts with '/'; the supplied test server uses 'data'"
        )
    )


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
    writable: bool = Field(
        description="Whether DataBridge could create and remove a probe file at the root"
    )


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
