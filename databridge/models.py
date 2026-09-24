"""The small public connection contract."""

from typing import Annotated, Literal

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
