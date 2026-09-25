"""Backend-independent file operations used by preview and transfer."""

import re
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import BinaryIO, Protocol

from databridge.errors import DataBridgeError, ErrorCode

MAX_LIST_RESULTS = 1_000
MAX_LIST_SCAN = 10_000
CHUNK_SIZE = 1_048_576
_STAGE_NAME = re.compile(r"^\..+\.databridge-[0-9a-f-]{36}\.part$")


@dataclass(frozen=True)
class Listing:
    files: list[str]
    truncated: bool


def validate_filename(filename: str) -> str:
    if (
        not filename
        or filename in {".", ".."}
        or "/" in filename
        or "\\" in filename
        or "\x00" in filename
        or _STAGE_NAME.fullmatch(filename)
    ):
        raise DataBridgeError(ErrorCode.INVALID_FILENAME, "Filename must name one file at the root")
    return filename


def is_stage_name(filename: str) -> bool:
    return _STAGE_NAME.fullmatch(filename) is not None


class Connector(Protocol):
    def list_files(self) -> Listing: ...

    def read(self, filename: str) -> AbstractContextManager[BinaryIO]: ...

    def write(
        self, filename: str, transfer_id: str, overwrite: bool
    ) -> AbstractContextManager[BinaryIO]: ...
