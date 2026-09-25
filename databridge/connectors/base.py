"""The connector contract, the filename policy, and bounded listing shared by every backend."""

from collections.abc import Iterable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from uuid import UUID

from databridge.errors import DataBridgeError, ErrorCode

MAX_LIST_RESULTS = 1_000
MAX_LIST_SCAN = 10_000
CHUNK_SIZE = 1_048_576
MAX_FILENAME_BYTES = 255
RESERVED_PREFIX = ".databridge-"


@dataclass(frozen=True)
class Listing:
    files: list[str]
    truncated: bool


@dataclass(frozen=True)
class AccessCheck:
    writable: bool


@dataclass(frozen=True)
class ConnectorContext:
    """Service-wide settings a connector may need when a stored connection is opened."""

    known_hosts_path: Path


class ByteSource(Protocol):
    """A readable byte stream; `read` may return fewer bytes than requested, and b"" means EOF."""

    def read(self, size: int, /) -> bytes: ...


class ByteSink(Protocol):
    """A writable byte stream; `write` stores every byte or raises DataBridgeError."""

    def write(self, data: bytes, /) -> None: ...


class Connector(Protocol):
    """File operations at one connection root.

    Error contract: every exception that leaves these methods, or the streams they yield, is a
    DataBridgeError with a stable code. `write` publishes the destination only when its context
    exits without an error; after any error the destination is absent or unchanged, and the
    staging file named by `staging_id` has been removed or, if the process stopped, left behind.
    """

    def list_files(self) -> Listing: ...

    def check_access(self) -> AccessCheck: ...

    def read(self, filename: str) -> AbstractContextManager[ByteSource]: ...

    def write(
        self, filename: str, staging_id: str, overwrite: bool
    ) -> AbstractContextManager[ByteSink]: ...


def validate_filename(filename: str) -> str:
    if (
        not filename
        or filename in {".", ".."}
        or "/" in filename
        or "\\" in filename
        or "\x00" in filename
    ):
        raise DataBridgeError(
            ErrorCode.INVALID_FILENAME, "Filename must name one file at the connection root"
        )
    try:
        size = len(filename.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise DataBridgeError(
            ErrorCode.INVALID_FILENAME, "Filename must be valid Unicode text"
        ) from exc
    if size > MAX_FILENAME_BYTES:
        raise DataBridgeError(
            ErrorCode.INVALID_FILENAME,
            f"Filename must be at most {MAX_FILENAME_BYTES} bytes when encoded as UTF-8",
        )
    if is_reserved_name(filename):
        raise DataBridgeError(
            ErrorCode.INVALID_FILENAME,
            f"Names starting with {RESERVED_PREFIX!r} are reserved for DataBridge staging files",
        )
    return filename


def is_reserved_name(filename: str) -> bool:
    return filename.lower().startswith(RESERVED_PREFIX)


def stage_name(staging_id: str) -> str:
    """Name the staging file for one transfer; the fixed length never exceeds NAME_MAX."""
    return f"{RESERVED_PREFIX}{UUID(staging_id)}.part"


def probe_name(probe_id: str) -> str:
    return f"{RESERVED_PREFIX}check-{UUID(probe_id)}"


def bounded_listing(entries: Iterable[tuple[str, bool]]) -> Listing:
    """Collect regular, valid file names from `(name, is_regular_file)` pairs within the caps."""
    files: list[str] = []
    for scanned, (name, regular) in enumerate(entries, start=1):
        if scanned > MAX_LIST_SCAN:
            return Listing(sorted(files), True)
        if not regular:
            continue
        try:
            validate_filename(name)
        except DataBridgeError:
            continue
        if len(files) >= MAX_LIST_RESULTS:
            return Listing(sorted(files), True)
        files.append(name)
    return Listing(sorted(files), False)
