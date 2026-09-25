"""SFTP connector: explicit host trust, per-operation sessions, and checked, pipelined requests."""

import logging
import posixpath
import shlex
import socket
import stat
import threading
import time
from collections import deque
from collections.abc import Generator, Iterator
from contextlib import AbstractContextManager, closing, contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import paramiko
from paramiko.sftp import (
    CMD_ATTRS,
    CMD_CLOSE,
    CMD_DATA,
    CMD_EXTENDED,
    CMD_FSTAT,
    CMD_HANDLE,
    CMD_LSTAT,
    CMD_NAME,
    CMD_OPEN,
    CMD_OPENDIR,
    CMD_READ,
    CMD_READDIR,
    CMD_REMOVE,
    CMD_RENAME,
    CMD_SETSTAT,
    CMD_STAT,
    CMD_STATUS,
    CMD_WRITE,
    SFTP_BAD_MESSAGE,
    SFTP_CONNECTION_LOST,
    SFTP_DESC,
    SFTP_EOF,
    SFTP_FLAG_CREATE,
    SFTP_FLAG_EXCL,
    SFTP_FLAG_READ,
    SFTP_FLAG_WRITE,
    SFTP_NO_CONNECTION,
    SFTP_NO_SUCH_FILE,
    SFTP_OK,
    SFTP_OP_UNSUPPORTED,
    SFTP_PERMISSION_DENIED,
    SFTPError,
    int64,
)

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
from databridge.models import Connection, SFTPConnection

_logger = logging.getLogger(__name__)

REQUEST_SIZE = 32_768
"""The largest read or write request that every SFTP server must accept."""
WINDOW = 64
"""Requests in flight per file, as in OpenSSH sftp: 64 x 32 KiB = 2 MiB, one SSH channel window."""
MAX_REPLY = 262_144
"""The largest reply accepted, as in OpenSSH sftp; a larger one is a protocol error."""
_OVERSIZED = -1
_CONNECT_TIMEOUT = 10
_REPLY_TIMEOUT = 30
"""Seconds one request may take to send, or its reply to arrive, before the session is closed."""
_WATCH_INTERVAL = 0.25
SETTLE_RATE = 10 * 1_048_576
"""The slowest rate, in bytes per second, at which a server is assumed to sync or close an
upload; fsync and close get _REPLY_TIMEOUT plus the upload's size at this rate."""
_LOST = (OSError, EOFError, paramiko.SSHException)
"""Transport failures: every one of them means the session is gone."""


class _Status(Exception):
    """A reply other than success; `code` is the SFTP status code."""

    def __init__(self, code: int, text: str = "") -> None:
        super().__init__(f"{_describe(code)}: {text}")
        self.code = code


def _describe(code: int) -> str:
    return SFTP_DESC[code] if 0 <= code < len(SFTP_DESC) else f"status {code}"


@dataclass(frozen=True)
class _Attributes:
    size: int | None
    mode: int | None


class _Reader:
    """The fields of one reply; a field that would pass its end is a protocol error.

    Paramiko's Message pads a short read with up to 1 MiB of zeros, so a hostile count or
    length would make it parse phantom data for as long as the count says.
    """

    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._offset = 0

    def uint32(self) -> int:
        return int.from_bytes(self._take(4), "big")

    def uint64(self) -> int:
        return int.from_bytes(self._take(8), "big")

    def string(self) -> bytes:
        return self._take(self.uint32())

    def attributes(self) -> _Attributes:
        flags = self.uint32()
        size = self.uint64() if flags & paramiko.SFTPAttributes.FLAG_SIZE else None
        if flags & paramiko.SFTPAttributes.FLAG_UIDGID:
            self._take(8)
        mode = self.uint32() if flags & paramiko.SFTPAttributes.FLAG_PERMISSIONS else None
        if flags & paramiko.SFTPAttributes.FLAG_AMTIME:
            self._take(8)
        if flags & paramiko.SFTPAttributes.FLAG_EXTENDED:
            for _ in range(self.uint32()):
                self.string()
                self.string()
        return _Attributes(size, mode)

    def _take(self, size: int) -> bytes:
        end = self._offset + size
        if end > len(self._payload):
            raise _Status(SFTP_BAD_MESSAGE, "a reply shorter than its fields")
        field = self._payload[self._offset : end]
        self._offset = end
        return field


class _Requests:
    """Paramiko's SFTP request layer with every reply delivered to the caller.

    Paramiko's SFTPFile ignores the reply to CMD_CLOSE and the replies to pipelined writes still in
    flight at close; its directory iterator handles a closed connection like the end of the
    directory and fails on the first name that is not UTF-8. This class sends requests with
    `SFTPClient._async_request` and receives replies with `SFTPClient._read_response`, the layer
    SFTPFile itself uses, unchanged across the paramiko 5.x range that pyproject.toml allows, and
    parses each reply with `_Reader`. A reply other than success raises `_Status`; any exception
    in `_LOST` comes from the transport.
    """

    def __init__(self, client: paramiko.SFTPClient, watchdog: "_Watchdog") -> None:
        self._client = client
        self._watchdog = watchdog
        self._replies: dict[int, tuple[int, bytes]] = {}
        self._ignored: set[int] = set()

    def _async_response(self, kind: int, message: paramiko.Message, number: int) -> None:
        """Paramiko delivers each reply to a request that `send` registered here."""
        if number in self._ignored:
            self._ignored.remove(number)
        elif message.packet.getbuffer().nbytes > MAX_REPLY:
            self._replies[number] = (_OVERSIZED, b"")
        else:
            self._replies[number] = (kind, message.get_remainder())

    # The stubs omit paramiko's private request layer; the class docstring explains the use.
    def send(self, kind: int, *args: object) -> int:
        with self._watched():
            return int(self._client._async_request(self, kind, *args))  # type: ignore[attr-defined]

    def receive(
        self, number: int, expected: int = CMD_STATUS, *, timeout: float | None = None
    ) -> _Reader:
        with self._watched(timeout):
            while number not in self._replies:
                self._client._read_response()  # type: ignore[attr-defined]
        kind, payload = self._replies.pop(number)
        reply = _Reader(payload)
        if kind == _OVERSIZED:
            raise _Status(SFTP_BAD_MESSAGE, f"a reply larger than {MAX_REPLY} bytes")
        if kind == CMD_STATUS:
            code = reply.uint32()
            if code != SFTP_OK:
                raise _Status(code, reply.string().decode("utf-8", "replace"))
            if expected != CMD_STATUS:
                raise _Status(SFTP_BAD_MESSAGE, "a success status instead of a result")
        elif kind != expected:
            raise _Status(SFTP_BAD_MESSAGE, f"reply type {kind} instead of {expected}")
        return reply

    def call(
        self,
        kind: int,
        *args: object,
        expected: int = CMD_STATUS,
        timeout: float | None = None,
    ) -> _Reader:
        return self.receive(self.send(kind, *args), expected, timeout=timeout)

    @contextmanager
    def _watched(self, timeout: float | None = None) -> Iterator[None]:
        """Bound one send or one wait for a reply (by default _REPLY_TIMEOUT); report an
        abandoned step as a timeout."""
        try:
            with self._watchdog.step(_REPLY_TIMEOUT if timeout is None else timeout):
                yield
        except _LOST as exc:
            if self._watchdog.expired:
                raise TimeoutError("The SFTP server did not respond in time") from exc
            raise

    def ignore(self, number: int) -> None:
        """Drop the reply to a request whose outcome no longer matters."""
        if self._replies.pop(number, None) is None:
            self._ignored.add(number)

    def attributes(self, kind: int, path: str) -> _Attributes:
        return self.call(kind, path, expected=CMD_ATTRS).attributes()

    def open(self, path: str, flags: int) -> bytes:
        attributes = paramiko.SFTPAttributes()
        return self.call(CMD_OPEN, path, flags, attributes, expected=CMD_HANDLE).string()

    def open_directory(self, path: str) -> bytes:
        return self.call(CMD_OPENDIR, path, expected=CMD_HANDLE).string()

    def close_quietly(self, handle: bytes) -> None:
        """Release a handle without waiting; its reply cannot change the outcome."""
        with suppress(*_LOST):
            self.ignore(self.send(CMD_CLOSE, handle))

    def remove_quietly(self, path: str) -> None:
        """Remove a staging file; a leftover keeps its reserved, hidden name."""
        try:
            self.call(CMD_REMOVE, path)
        except _Status as status:
            if status.code != SFTP_NO_SUCH_FILE:
                _logger.warning("staging_cleanup_failed path=%r status=%s", path, status.code)
        except (SFTPError, *_LOST):
            _logger.warning("staging_cleanup_failed path=%r status=connection_lost", path)


def _unavailable(action: str) -> DataBridgeError:
    return DataBridgeError(
        ErrorCode.SFTP_UNAVAILABLE, f"The SFTP connection was lost while trying to {action}"
    )


def _root_unavailable(root: str) -> DataBridgeError:
    return DataBridgeError(
        ErrorCode.CONNECTION_ROOT_UNAVAILABLE,
        f"The SFTP root '{root}' does not exist or is not a directory",
    )


def _not_writable(root: str) -> DataBridgeError:
    return DataBridgeError(
        ErrorCode.DESTINATION_NOT_WRITABLE,
        f"The SFTP user may not create or replace files in '{root}'",
    )


@contextmanager
def _failures(
    action: str,
    *,
    missing: DataBridgeError | None = None,
    denied: DataBridgeError | None = None,
    unsupported: DataBridgeError | None = None,
    lost: DataBridgeError | None = None,
) -> Iterator[None]:
    """Translate SFTP failures in the block; `action` completes "while trying to ..."."""
    try:
        yield
    except _Status as status:
        classified = {
            SFTP_NO_SUCH_FILE: missing,
            SFTP_PERMISSION_DENIED: denied,
            SFTP_OP_UNSUPPORTED: unsupported,
        }.get(status.code)
        if classified is not None:
            raise classified from status
        if status.code in {SFTP_NO_CONNECTION, SFTP_CONNECTION_LOST}:
            raise (lost or _unavailable(action)) from status
        raise DataBridgeError(
            ErrorCode.SFTP_OPERATION_FAILED,
            f"The SFTP server refused to {action} ({_describe(status.code)})",
        ) from status
    except UnicodeEncodeError as exc:
        raise DataBridgeError(
            ErrorCode.INVALID_CONNECTION_SETTINGS, "The SFTP root must be valid UTF-8 text"
        ) from exc
    except SFTPError as exc:
        raise DataBridgeError(
            ErrorCode.SFTP_OPERATION_FAILED,
            f"The SFTP server sent an invalid reply while trying to {action}",
        ) from exc
    except _LOST as exc:
        if lost is not None:
            raise lost from exc
        if isinstance(exc, TimeoutError):
            raise DataBridgeError(
                ErrorCode.SFTP_UNAVAILABLE,
                f"The SFTP server did not respond within the time allowed while trying to {action}",
            ) from exc
        raise _unavailable(action) from exc


class _Watchdog:
    """Closes a session's transport when one step outlives its deadline.

    Paramiko's timeouts bound each socket wait, and some channel requests wait without one, so
    a server that sends one byte before every timeout could hold a worker thread forever.
    Closing the transport wakes every blocked paramiko call, which then fails as a lost session.
    """

    def __init__(self, transport: paramiko.Transport) -> None:
        self._transport = transport
        self._condition = threading.Condition()
        self._deadline: float | None = None
        self._stopped = False
        self.expired = False
        self._thread = threading.Thread(target=self._watch, name="sftp-watchdog", daemon=True)
        self._thread.start()

    @contextmanager
    def step(self, seconds: float) -> Iterator[None]:
        with self._condition:
            self._deadline = time.monotonic() + seconds
        try:
            yield
        finally:
            with self._condition:
                self._deadline = None

    def stop(self) -> None:
        with self._condition:
            self._stopped = True
            self._condition.notify()
        self._thread.join()

    def _watch(self) -> None:
        with self._condition:
            while not self._stopped and not self._overdue():
                self._condition.wait(_WATCH_INTERVAL)
            if self._stopped:
                return
            self.expired = True
        self._transport.close()

    def _overdue(self) -> bool:
        return self._deadline is not None and time.monotonic() >= self._deadline


def _host_key_name(host: str, port: int) -> str:
    """The name OpenSSH and paramiko look up in known_hosts."""
    return host if port == 22 else f"[{host}]:{port}"


def _presented(key: paramiko.PKey) -> str:
    return f"a {key.get_name()} host key with fingerprint {key.fingerprint}"


class _RejectUnknownHost(paramiko.MissingHostKeyPolicy):
    """Refuse a server whose key is not trusted; the check precedes authentication."""

    def __init__(self, connector: "SFTPConnector") -> None:
        self._connector = connector

    def missing_host_key(
        self, _client: paramiko.SSHClient, _hostname: str, key: paramiko.PKey
    ) -> None:
        connector = self._connector
        raise DataBridgeError(
            ErrorCode.SFTP_HOST_KEY_REJECTED,
            f"The SFTP server {connector.host_key_name} presented {_presented(key)}, which is not "
            f"trusted in {connector.known_hosts_path}. After verifying that fingerprint with the "
            f"server's administrator, trust it with: {connector.keyscan_command}",
        )


class _Download:
    """The first `size` bytes of a remote file, read in order with up to WINDOW requests in
    flight (a ByteSource)."""

    def __init__(self, requests: _Requests, handle: bytes, size: int, name: str) -> None:
        self._requests = requests
        self._handle = handle
        self._name = name
        self._size = size
        self._next_offset = 0
        self._pending: deque[tuple[int, int, int]] = deque()
        self._leftover = b""

    def read(self, size: int, /) -> bytes:
        with _failures(
            f"read '{self._name}'",
            denied=DataBridgeError(
                ErrorCode.FILE_NOT_READABLE, f"The SFTP user may not read '{self._name}'"
            ),
        ):
            buffer = bytearray()
            while len(buffer) < size and (self._leftover or self._pending or self._unrequested):
                if not self._leftover:
                    self._leftover = self._next_reply()
                wanted = size - len(buffer)
                buffer += self._leftover[:wanted]
                self._leftover = self._leftover[wanted:]
            return bytes(buffer)

    def release(self) -> None:
        for number, _offset, _length in self._pending:
            self._requests.ignore(number)
        self._pending.clear()
        self._requests.close_quietly(self._handle)

    @property
    def _unrequested(self) -> bool:
        return self._next_offset < self._size

    def _next_reply(self) -> bytes:
        """Receive the next range in file order; a short reply re-requests its remainder first."""
        while len(self._pending) < WINDOW and self._unrequested:
            length = min(REQUEST_SIZE, self._size - self._next_offset)
            self._pending.append(self._request(self._next_offset, length))
            self._next_offset += length
        number, offset, length = self._pending.popleft()
        try:
            data = self._requests.receive(number, CMD_DATA).string()
        except _Status as status:
            if status.code != SFTP_EOF:
                raise
            data = b""
        if not data:
            raise source_changed(self._name)
        if len(data) > length:
            raise _Status(SFTP_BAD_MESSAGE, f"{len(data)} bytes for a {length}-byte request")
        if len(data) < length:
            self._pending.appendleft(self._request(offset + len(data), length - len(data)))
        return data

    def _request(self, offset: int, length: int) -> tuple[int, int, int]:
        number = self._requests.send(CMD_READ, self._handle, int64(offset), length)
        return number, offset, length


class _Upload:
    """A staging file written with up to WINDOW unacknowledged requests (a ByteSink)."""

    def __init__(
        self, requests: _Requests, handle: bytes, root: str, stage: str, mode: int | None
    ) -> None:
        self._requests = requests
        self._handle: bytes | None = handle
        self._root = root
        self._stage = stage
        self._mode = mode
        self._unacknowledged: deque[int] = deque()
        self._finished = False
        self.size = 0

    def write(self, data: bytes, /) -> None:
        with _failures("write the staging file", denied=_not_writable(self._root)):
            view = memoryview(data)
            for start in range(0, len(view), REQUEST_SIZE):
                if len(self._unacknowledged) >= WINDOW:
                    self._requests.receive(self._unacknowledged.popleft())
                chunk = bytes(view[start : start + REQUEST_SIZE])
                number = self._requests.send(CMD_WRITE, self._open(), int64(self.size), chunk)
                self._unacknowledged.append(number)
                self.size += len(chunk)

    def finish(self) -> None:
        """Collect every write status, sync where the server can, close the file, prove every
        byte is stored, and give the stage the replaced file's mode. A second call does nothing.

        Syncing and closing can take time in proportion to the upload, so they get the reply
        timeout plus the upload's size at SETTLE_RATE.
        """
        if self._finished:
            return
        settle = _REPLY_TIMEOUT + self.size / SETTLE_RATE
        with _failures("finish the staging file", denied=_not_writable(self._root)):
            while self._unacknowledged:
                self._requests.receive(self._unacknowledged.popleft())
            try:
                self._requests.call(CMD_EXTENDED, "fsync@openssh.com", self._open(), timeout=settle)
            except _Status as status:
                if status.code != SFTP_OP_UNSUPPORTED:
                    raise
            handle, self._handle = self._open(), None
            self._requests.call(CMD_CLOSE, handle, timeout=settle)
            stored = self._requests.attributes(CMD_STAT, self._stage).size
        if stored is None:
            raise DataBridgeError(
                ErrorCode.SFTP_OPERATION_FAILED,
                "The SFTP server did not report the staging file's size, so the upload cannot "
                "be verified; nothing was published",
            )
        if stored != self.size:
            raise DataBridgeError(
                ErrorCode.SFTP_OPERATION_FAILED,
                f"The SFTP server stored {stored} of {self.size} bytes of the staging file; "
                "nothing was published",
            )
        if self._mode is not None:
            attributes = paramiko.SFTPAttributes()
            attributes.st_mode = self._mode
            with _failures("copy the replaced file's permissions to the staging file"):
                self._requests.call(CMD_SETSTAT, self._stage, attributes)
        self._finished = True

    def release(self) -> None:
        for number in self._unacknowledged:
            self._requests.ignore(number)
        self._unacknowledged.clear()
        if self._handle is not None:
            handle, self._handle = self._handle, None
            self._requests.close_quietly(handle)

    def _open(self) -> bytes:
        if self._handle is None:
            raise RuntimeError("The staging file is already closed")
        return self._handle


class SFTPConnector:
    def __init__(self, connection: SFTPConnection, known_hosts_path: Path) -> None:
        self.connection = connection
        self.known_hosts_path = known_hosts_path
        self.root = connection.root

    @classmethod
    def open(cls, connection: Connection, context: ConnectorContext) -> "SFTPConnector":
        if not isinstance(connection, SFTPConnection):
            raise TypeError("SFTPConnector requires an SFTP connection")
        return cls(connection, context.known_hosts_path)

    @staticmethod
    def prepare(connection: Connection) -> Connection:
        """SFTP settings are validated by the model; live access is checked by the healthcheck."""
        return connection

    @property
    def host_key_name(self) -> str:
        return _host_key_name(self.connection.host, self.connection.port)

    @property
    def keyscan_command(self) -> str:
        return (
            f"ssh-keyscan -p {self.connection.port} {shlex.quote(self.connection.host)}"
            f" >> {shlex.quote(str(self.known_hosts_path))}"
        )

    def list_files(self) -> Listing:
        with self._session() as requests, self._root_failures():
            with closing(self._entries(requests)) as entries:
                return bounded_listing(entries)

    def check_access(self) -> AccessCheck:
        name = probe_name(str(uuid4()))
        probe = posixpath.join(self.root, name)
        with self._session() as requests:
            with self._root_failures():
                requests.close_quietly(requests.open_directory(self.root))
            with _failures(
                f"test write access to '{self.root}'", missing=_root_unavailable(self.root)
            ):
                try:
                    handle = requests.open(
                        probe, SFTP_FLAG_WRITE | SFTP_FLAG_CREATE | SFTP_FLAG_EXCL
                    )
                except _Status as status:
                    if status.code == SFTP_PERMISSION_DENIED:
                        return AccessCheck(writable=False)
                    raise
            requests.close_quietly(handle)
            # The check leaves nothing behind, so a probe it cannot remove fails the check.
            with _failures(
                f"remove the write-access probe '{name}' from '{self.root}'",
                missing=_root_unavailable(self.root),
            ):
                requests.call(CMD_REMOVE, probe)
        return AccessCheck(writable=True)

    @contextmanager
    def read(self, filename: str, limit: int | None = None) -> Iterator[ByteSource]:
        path = self._path(filename)
        with self._session() as requests:
            with _failures(
                f"open '{filename}'",
                missing=DataBridgeError(
                    ErrorCode.FILE_NOT_FOUND, f"'{filename}' was removed while it was opened"
                ),
                denied=DataBridgeError(
                    ErrorCode.FILE_NOT_READABLE, f"The SFTP user may not read '{filename}'"
                ),
            ):
                # STAT first so the server never opens a FIFO or a directory; the size comes
                # from FSTAT on the opened handle, since the path may name another file by then.
                self._check_regular(requests, path, filename)
                handle = requests.open(path, SFTP_FLAG_READ)
                try:
                    opened = requests.call(CMD_FSTAT, handle, expected=CMD_ATTRS).attributes()
                    size = _readable_size(opened, filename)
                except BaseException:
                    requests.close_quietly(handle)
                    raise
            download = _Download(
                requests, handle, size if limit is None else min(size, limit), filename
            )
            try:
                yield download
            finally:
                download.release()

    @contextmanager
    def write(self, filename: str, staging_id: str, overwrite: bool) -> Iterator[ByteSink]:
        destination = self._path(filename)
        stage = posixpath.join(self.root, stage_name(staging_id))
        with self._session() as requests:
            with _failures(f"inspect '{filename}'", denied=_not_writable(self.root)):
                mode = self._replaceable_mode(requests, destination, filename, overwrite)
            with _failures(
                "create the staging file",
                missing=_root_unavailable(self.root),
                denied=_not_writable(self.root),
            ):
                flags = SFTP_FLAG_WRITE | SFTP_FLAG_CREATE | SFTP_FLAG_EXCL
                upload = _Upload(requests, requests.open(stage, flags), self.root, stage, mode)
            published = False
            try:
                yield upload
                upload.finish()
                self._publish(requests, stage, destination, filename, overwrite)
                published = True
            finally:
                if not published:
                    upload.release()
                    requests.remove_quietly(stage)

    def _path(self, filename: str) -> str:
        validate_filename(filename)
        return posixpath.join(self.root, filename)

    def _root_failures(self) -> AbstractContextManager[None]:
        return _failures(
            f"list '{self.root}'",
            missing=_root_unavailable(self.root),
            denied=DataBridgeError(
                ErrorCode.CONNECTION_ROOT_UNAVAILABLE,
                f"The SFTP user may not list the SFTP root '{self.root}'",
            ),
        )

    def _entries(self, requests: _Requests) -> Generator[tuple[str, bool]]:
        """Yield `(name, is_regular_file)` for every raw entry, one READDIR reply at a time; an
        entry the API cannot address yields an empty name, so it still counts as scanned."""
        handle = requests.open_directory(self.root)
        try:
            while True:
                try:
                    reply = requests.call(CMD_READDIR, handle, expected=CMD_NAME)
                except _Status as status:
                    if status.code == SFTP_EOF:
                        return
                    raise
                yield from self._classified(requests, _names(reply))
        finally:
            requests.close_quietly(handle)

    def _classified(
        self, requests: _Requests, entries: list[tuple[str | None, _Attributes]]
    ) -> Iterator[tuple[str, bool]]:
        """READDIR may omit permissions; lstat those entries, at most WINDOW at a time."""
        for start in range(0, len(entries), WINDOW):
            batch = entries[start : start + WINDOW]
            lookups = {
                index: requests.send(CMD_LSTAT, posixpath.join(self.root, name))
                for index, (name, attributes) in enumerate(batch)
                if name is not None and attributes.mode is None
            }
            try:
                for index, (name, attributes) in enumerate(batch):
                    if name is None:
                        yield "", False
                        continue
                    mode = attributes.mode
                    if index in lookups:
                        try:
                            reply = requests.receive(lookups.pop(index), CMD_ATTRS)
                            mode = reply.attributes().mode
                        except _Status:
                            mode = None  # removed or hidden since READDIR
                    yield name, mode is not None and stat.S_ISREG(mode)
            finally:
                for number in lookups.values():
                    requests.ignore(number)

    def _check_regular(self, requests: _Requests, path: str, filename: str) -> None:
        try:
            attributes = requests.attributes(CMD_STAT, path)
        except _Status as status:
            if status.code != SFTP_NO_SUCH_FILE:
                raise
            raise self._missing(requests, filename) from status
        if attributes.mode is not None and not stat.S_ISREG(attributes.mode):
            raise DataBridgeError(ErrorCode.FILE_NOT_FOUND, f"'{filename}' is not a regular file")

    def _missing(self, requests: _Requests, filename: str) -> DataBridgeError:
        try:
            requests.attributes(CMD_STAT, self.root)
        except _Status:
            return _root_unavailable(self.root)
        return DataBridgeError(
            ErrorCode.FILE_NOT_FOUND, f"No file named '{filename}' at the SFTP root '{self.root}'"
        )

    @staticmethod
    def _replaceable_mode(
        requests: _Requests, destination: str, filename: str, overwrite: bool
    ) -> int | None:
        """Refuse an existing destination unless replacing it; return the mode to preserve."""
        try:
            attributes = requests.attributes(CMD_STAT, destination)
        except _Status as status:
            if status.code == SFTP_NO_SUCH_FILE:
                return None
            raise
        if not overwrite:
            raise destination_exists(filename)
        if attributes.mode is None:
            return None
        if not stat.S_ISREG(attributes.mode):
            raise DataBridgeError(
                ErrorCode.DESTINATION_EXISTS, f"'{filename}' exists and is not a regular file"
            )
        return stat.S_IMODE(attributes.mode)

    def _publish(
        self, requests: _Requests, stage: str, destination: str, filename: str, overwrite: bool
    ) -> None:
        lost = DataBridgeError(
            ErrorCode.SFTP_UNAVAILABLE,
            f"The SFTP connection was lost while publishing '{filename}'; "
            "the destination may already contain the new file",
        )
        unsupported = DataBridgeError(
            ErrorCode.PUBLISH_UNSUPPORTED,
            "The SFTP server cannot replace files atomically (it lacks the "
            f"posix-rename@openssh.com extension), so '{filename}' was left unchanged; "
            "choose another destination file name",
        )
        if not overwrite:
            # OpenSSH's RENAME never replaces a file; this check also protects servers whose
            # RENAME does, except against a file created between the check and the rename.
            with _failures(f"inspect '{filename}'", denied=_not_writable(self.root)):
                if self._exists(requests, destination):
                    raise destination_exists(filename)
        try:
            with _failures(
                f"publish '{filename}'",
                denied=_not_writable(self.root),
                unsupported=unsupported,
                lost=lost,
            ):
                if overwrite:
                    requests.call(CMD_EXTENDED, "posix-rename@openssh.com", stage, destination)
                else:
                    requests.call(CMD_RENAME, stage, destination)
        except DataBridgeError as error:
            if not overwrite and error.code == ErrorCode.SFTP_OPERATION_FAILED:
                # The server answered, so nothing was published; OpenSSH answers a RENAME onto
                # an existing file with a plain failure.
                with _failures(f"inspect '{filename}'"):
                    if self._exists(requests, destination):
                        raise destination_exists(filename) from error
            raise

    @staticmethod
    def _exists(requests: _Requests, path: str) -> bool:
        try:
            requests.attributes(CMD_STAT, path)
        except _Status as status:
            if status.code == SFTP_NO_SUCH_FILE:
                return False
            raise
        return True

    @contextmanager
    def _session(self) -> Iterator[_Requests]:
        client = paramiko.SSHClient()
        try:
            self._trust(client)
            transport = self._connect(client)
            watchdog = _Watchdog(transport)
            try:
                with (
                    _failures(
                        "open an SFTP session",
                        lost=DataBridgeError(
                            ErrorCode.SFTP_UNAVAILABLE,
                            "The SFTP server did not open an SFTP session",
                        ),
                    ),
                    watchdog.step(_CONNECT_TIMEOUT),
                ):
                    sftp = client.open_sftp()
                try:
                    yield _Requests(sftp, watchdog)
                finally:
                    # Closing the channel of a dropped session raises; the outcome is known.
                    with suppress(*_LOST):
                        sftp.close()
            finally:
                watchdog.stop()
        finally:
            client.close()

    def _trust(self, client: paramiko.SSHClient) -> None:
        try:
            client.load_host_keys(str(self.known_hosts_path))
        except FileNotFoundError as exc:
            raise self._trust_file_unusable("does not exist") from exc
        except Exception as exc:  # the known_hosts parser raises undocumented types
            raise self._trust_file_unusable("is not a readable known_hosts file") from exc
        client.set_missing_host_key_policy(_RejectUnknownHost(self))

    def _trust_file_unusable(self, reason: str) -> DataBridgeError:
        return DataBridgeError(
            ErrorCode.SFTP_HOST_KEY_REJECTED,
            f"The trusted SFTP host key file {self.known_hosts_path} {reason}; after verifying "
            f"the server's fingerprint, trust it with: {self.keyscan_command}",
        )

    def _connect(self, client: paramiko.SSHClient) -> paramiko.Transport:
        connection = self.connection
        try:
            client.connect(
                hostname=connection.host,
                port=connection.port,
                username=connection.username,
                password=connection.password.get_secret_value(),
                allow_agent=False,
                look_for_keys=False,
                timeout=_CONNECT_TIMEOUT,
                banner_timeout=_CONNECT_TIMEOUT,
                auth_timeout=_CONNECT_TIMEOUT,
                channel_timeout=_CONNECT_TIMEOUT,
            )
        except paramiko.BadHostKeyException as exc:
            name = shlex.quote(self.host_key_name)
            raise DataBridgeError(
                ErrorCode.SFTP_HOST_KEY_REJECTED,
                f"The SFTP server {self.host_key_name} presented {_presented(exc.key)}, which "
                f"does not match the key trusted in {self.known_hosts_path}. If the server's key "
                "was replaced on purpose, verify the new fingerprint, remove the old entry with: "
                f"ssh-keygen -R {name} -f {shlex.quote(str(self.known_hosts_path))}, then trust "
                f"the new key with: {self.keyscan_command}",
            ) from exc
        except paramiko.AuthenticationException as exc:
            raise DataBridgeError(
                ErrorCode.SFTP_AUTH_FAILED, "The SFTP server rejected the username or password"
            ) from exc
        except UnicodeError as exc:
            raise DataBridgeError(
                ErrorCode.INVALID_CONNECTION_SETTINGS,
                "The SFTP host name, username, or password cannot be sent to an SSH server",
            ) from exc
        except socket.gaierror as exc:
            raise DataBridgeError(
                ErrorCode.SFTP_UNAVAILABLE, f"Cannot resolve the SFTP host '{connection.host}'"
            ) from exc
        except _LOST as exc:
            raise DataBridgeError(
                ErrorCode.SFTP_UNAVAILABLE,
                f"Cannot connect to the SFTP server at {self.host_key_name}",
            ) from exc
        transport = client.get_transport()
        if transport is None or not transport.is_active():
            raise DataBridgeError(
                ErrorCode.SFTP_UNAVAILABLE,
                f"The SFTP server at {self.host_key_name} closed the connection",
            )
        return transport


def _names(reply: _Reader) -> list[tuple[str | None, _Attributes]]:
    """Parse a NAME reply. "." and "..", and names that are not UTF-8 (the API names files with
    text), get None: the API cannot address them, but they still count as scanned. A directory
    ends with an end-of-file status, so a page without entries could repeat forever and is a
    protocol error."""
    count = reply.uint32()
    if count == 0:
        raise _Status(SFTP_BAD_MESSAGE, "a directory page without entries")
    entries: list[tuple[str | None, _Attributes]] = []
    for _ in range(count):
        raw_name = reply.string()
        reply.string()  # the "ls -l" style long name
        attributes = reply.attributes()
        try:
            name: str | None = raw_name.decode("utf-8")
        except UnicodeDecodeError:
            name = None
        entries.append((None if name in {".", ".."} else name, attributes))
    return entries


def _readable_size(attributes: _Attributes, filename: str) -> int:
    """The size of an opened regular file, which bounds how much a read may return."""
    if attributes.mode is not None and not stat.S_ISREG(attributes.mode):
        raise DataBridgeError(ErrorCode.FILE_NOT_FOUND, f"'{filename}' is not a regular file")
    if attributes.size is None:
        raise DataBridgeError(
            ErrorCode.SFTP_OPERATION_FAILED,
            f"The SFTP server did not report the size of '{filename}', so it cannot be read",
        )
    return attributes.size
