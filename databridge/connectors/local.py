"""Local filesystem connector: a flat root, regular files only, and crash-safe publication."""

import errno
import logging
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from uuid import uuid4

from databridge.connectors.base import (
    AccessCheck,
    ByteSink,
    ByteSource,
    ConnectorContext,
    Listing,
    bounded_listing,
    destination_exists,
    probe_name,
    source_changed,
    stage_name,
    validate_filename,
)
from databridge.errors import DataBridgeError, ErrorCode
from databridge.models import Connection, LocalConnection

_logger = logging.getLogger(__name__)
_NOT_WRITABLE = frozenset({errno.EACCES, errno.EPERM, errno.EROFS})
_FULL = frozenset({errno.ENOSPC, errno.EDQUOT})
_MISSING = frozenset({errno.ENOENT, errno.ENOTDIR})
_NO_HARD_LINKS = frozenset(
    {errno.EPERM, errno.ENOTSUP, errno.EOPNOTSUPP, errno.EXDEV, errno.EMLINK}
)


def _root_unavailable() -> DataBridgeError:
    return DataBridgeError(
        ErrorCode.CONNECTION_ROOT_UNAVAILABLE, "The local connection directory is unavailable"
    )


def _write_error(exc: OSError, action: str) -> DataBridgeError:
    if exc.errno in _FULL:
        return DataBridgeError(
            ErrorCode.DESTINATION_FULL, f"The destination has no space left to {action}"
        )
    if exc.errno in _NOT_WRITABLE:
        return DataBridgeError(
            ErrorCode.DESTINATION_NOT_WRITABLE,
            f"The connection directory does not allow DataBridge to {action}",
        )
    return DataBridgeError(ErrorCode.DESTINATION_WRITE_FAILED, f"Cannot {action}")


class _FileSource:
    def __init__(self, descriptor: int, size: int, filename: str) -> None:
        self._descriptor = descriptor
        self._remaining = size
        self._filename = filename

    def read(self, size: int, /) -> bytes:
        if not self._remaining:
            return b""
        try:
            data = os.read(self._descriptor, min(size, self._remaining))
        except OSError as exc:
            raise DataBridgeError(
                ErrorCode.SOURCE_READ_FAILED, "Cannot read the local source file"
            ) from exc
        if not data:
            raise source_changed(self._filename)
        self._remaining -= len(data)
        return data


class _FileSink:
    def __init__(self, descriptor: int, existing: os.stat_result | None) -> None:
        self._descriptor = descriptor
        self._existing = existing
        self.bytes_written = 0

    def write(self, data: bytes, /) -> None:
        view = memoryview(data)
        while view:
            try:
                written = os.write(self._open(), view)
            except OSError as exc:
                raise _write_error(exc, "write the staging file") from exc
            view = view[written:]
            self.bytes_written += written

    def finish(self) -> None:
        """Sync, verify, and close the staging file; a second call does nothing."""
        if self._descriptor < 0:
            return
        descriptor, self._descriptor = self._descriptor, -1
        try:
            _persist(descriptor, self.bytes_written, self._existing)
        except BaseException:
            with suppress(OSError):
                os.close(descriptor)
            raise
        try:
            os.close(descriptor)
        except OSError as exc:
            raise _write_error(exc, "close the staging file") from exc

    def release(self) -> None:
        """Close an unfinished staging file after a failure."""
        if self._descriptor >= 0:
            descriptor, self._descriptor = self._descriptor, -1
            with suppress(OSError):
                os.close(descriptor)

    def _open(self) -> int:
        if self._descriptor < 0:
            raise RuntimeError("The staging file is already closed")
        return self._descriptor


class LocalConnector:
    def __init__(self, root: Path) -> None:
        self.root = root

    @classmethod
    def open(cls, connection: Connection, _context: ConnectorContext) -> "LocalConnector":
        if not isinstance(connection, LocalConnection):
            raise TypeError("LocalConnector requires a local connection")
        return cls(Path(connection.path))

    @staticmethod
    def prepare(connection: Connection) -> Connection:
        """Create a missing root and store its absolute, resolved path."""
        if not isinstance(connection, LocalConnection):
            raise TypeError("LocalConnector requires a local connection")
        try:
            requested = Path(connection.path).expanduser()
            requested.mkdir(parents=True, exist_ok=True)
            root = requested.resolve(strict=True)
        except FileExistsError as exc:
            raise DataBridgeError(
                ErrorCode.INVALID_CONNECTION_SETTINGS, "Local path exists and is not a directory"
            ) from exc
        except (OSError, RuntimeError, ValueError) as exc:
            raise DataBridgeError(
                ErrorCode.INVALID_CONNECTION_SETTINGS,
                "Cannot create or resolve the local directory",
            ) from exc
        if not root.is_dir():
            raise DataBridgeError(
                ErrorCode.INVALID_CONNECTION_SETTINGS, "Local path is not a directory"
            )
        return connection.model_copy(update={"path": str(root)})

    def _root(self) -> Path:
        try:
            root = self.root.resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise _root_unavailable() from exc
        if not root.is_dir():
            raise _root_unavailable()
        return root

    def _resolve(self, filename: str) -> tuple[Path, Path]:
        validate_filename(filename)
        root = self._root()
        try:
            resolved = (root / filename).resolve()
        except (OSError, RuntimeError) as exc:
            raise DataBridgeError(
                ErrorCode.INVALID_FILENAME, "Filename cannot be resolved inside the connection root"
            ) from exc
        if not resolved.is_relative_to(root):
            raise DataBridgeError(
                ErrorCode.INVALID_FILENAME, "Filename escapes the connection root"
            )
        return root, resolved

    def list_files(self) -> Listing:
        root = self._root()
        try:
            with os.scandir(root) as entries:
                return bounded_listing(
                    (entry.name, entry.is_file(follow_symlinks=False)) for entry in entries
                )
        except OSError as exc:
            raise _root_unavailable() from exc

    def check_access(self) -> AccessCheck:
        self.list_files()
        probe = self._root() / probe_name(str(uuid4()))
        try:
            descriptor = os.open(probe, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        except OSError as exc:
            if exc.errno in _NOT_WRITABLE | _FULL:
                return AccessCheck(writable=False)
            raise DataBridgeError(
                ErrorCode.LOCAL_IO_ERROR, "Cannot test write access to the connection directory"
            ) from exc
        with suppress(OSError):  # the probe exists, so the root is writable either way
            os.close(descriptor)
        _remove(probe)
        return AccessCheck(writable=True)

    @contextmanager
    def read(self, filename: str, limit: int | None = None) -> Iterator[ByteSource]:
        _root, path = self._resolve(filename)
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC)
        except FileNotFoundError as exc:
            raise DataBridgeError(
                ErrorCode.FILE_NOT_FOUND, f"No file named '{filename}' at the connection root"
            ) from exc
        except PermissionError as exc:
            raise DataBridgeError(
                ErrorCode.FILE_NOT_READABLE, f"The service user may not read '{filename}'"
            ) from exc
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise DataBridgeError(
                    ErrorCode.INVALID_FILENAME, f"'{filename}' changed while it was being opened"
                ) from exc
            raise DataBridgeError(ErrorCode.LOCAL_IO_ERROR, "Cannot open the local file") from exc
        try:
            try:
                opened = os.fstat(descriptor)
                if stat.S_ISREG(opened.st_mode):
                    os.set_blocking(descriptor, True)
            except OSError as exc:
                raise DataBridgeError(
                    ErrorCode.LOCAL_IO_ERROR, f"Cannot inspect the opened file '{filename}'"
                ) from exc
            if not stat.S_ISREG(opened.st_mode):
                raise DataBridgeError(
                    ErrorCode.FILE_NOT_FOUND, f"'{filename}' is not a regular file"
                )
            size = opened.st_size if limit is None else min(opened.st_size, limit)
            yield _FileSource(descriptor, size, filename)
        finally:
            with suppress(OSError):  # every byte was read; closing cannot change the result
                os.close(descriptor)

    @contextmanager
    def write(self, filename: str, staging_id: str, overwrite: bool) -> Iterator[ByteSink]:
        root, destination = self._resolve(filename)
        existing = _existing_file(destination, filename, overwrite)
        stage = root / stage_name(staging_id)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
        try:
            descriptor = os.open(stage, flags, 0o666)
        except OSError as exc:
            if exc.errno in _MISSING:
                raise _root_unavailable() from exc
            raise _write_error(exc, "create the staging file") from exc
        sink = _FileSink(descriptor, existing)
        try:
            yield sink
            sink.finish()
            _publish(stage, destination, filename, overwrite)
            _sync_directory(root)
        finally:
            sink.release()
            _remove(stage)


def _existing_file(destination: Path, filename: str, overwrite: bool) -> os.stat_result | None:
    try:
        existing = os.stat(destination)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise _write_error(exc, "inspect the destination") from exc
    if not overwrite:
        raise destination_exists(filename)
    if not stat.S_ISREG(existing.st_mode):
        raise DataBridgeError(
            ErrorCode.DESTINATION_EXISTS, f"'{filename}' exists and is not a regular file"
        )
    return existing


def _persist(descriptor: int, expected: int, existing: os.stat_result | None) -> None:
    """Make the staged bytes durable and prove they are all there before publication."""
    try:
        os.fsync(descriptor)
        size = os.fstat(descriptor).st_size
        if existing is not None:
            os.fchmod(descriptor, stat.S_IMODE(existing.st_mode))
    except OSError as exc:
        raise _write_error(exc, "persist the staging file") from exc
    if size != expected:
        raise DataBridgeError(
            ErrorCode.DESTINATION_WRITE_FAILED,
            f"The staging file holds {size} of {expected} bytes",
        )


def _publish(stage: Path, destination: Path, filename: str, overwrite: bool) -> None:
    try:
        if overwrite:
            os.replace(stage, destination)
        else:
            os.link(stage, destination)
    except FileExistsError as exc:
        raise destination_exists(filename) from exc
    except OSError as exc:
        if not overwrite and exc.errno in _NO_HARD_LINKS:
            raise DataBridgeError(
                ErrorCode.PUBLISH_UNSUPPORTED,
                "The destination filesystem cannot create hard links, which publishing without "
                'overwrite requires; set "overwrite": true to replace atomically instead',
            ) from exc
        raise _write_error(exc, "publish the destination file") from exc


def _sync_directory(directory: Path) -> None:
    """Persist the new directory entry; the file is already published if this fails."""
    try:
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        _logger.warning("directory_sync_failed path=%r", str(directory))


def _remove(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        _logger.warning("staging_cleanup_failed path=%r", str(path))
