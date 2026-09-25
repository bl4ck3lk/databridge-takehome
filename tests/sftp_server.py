"""Loopback Paramiko SFTP server with scripted faults, so SFTP behavior is testable without Docker.

The request handling follows OpenSSH sftp-server where DataBridge depends on it: RENAME never
replaces an existing file, posix-rename@openssh.com and fsync@openssh.com are extensions, and
READDIR returns lstat attributes.
"""

import os
import socket
import struct
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Any, Self

import paramiko
from paramiko import Message, SFTPAttributes, SFTPHandle, SFTPServer, SFTPServerInterface
from paramiko.sftp import (
    CMD_EXTENDED,
    CMD_NAME,
    CMD_STATUS,
    SFTP_BAD_MESSAGE,
    SFTP_EOF,
    SFTP_FAILURE,
    SFTP_OK,
    SFTP_OP_UNSUPPORTED,
)

USERNAME = "testuser"
PASSWORD = "testpass"
HOST_KEY = paramiko.ECDSAKey.generate()
UNDECODABLE_NAME = b"\xff\xfe.bin"


@dataclass
class Scenario:
    """Faults to inject and the requests the server observed.

    Operation names: auth, list_folder, readdir, stat, lstat, open, read, write, fsync, close,
    remove, rename, posix_rename, chattr. `malformed_names` replaces each READDIR reply with a
    hostile one: "count" claims 2**31-1 entries, "oversized" exceeds 256 KiB, and "extended"
    claims 2**31-1 extended attributes. `chatter_seconds` delays each READDIR reply while the
    server sends replies to requests that were never made. `trickle_readdir` answers READDIR
    with an end-of-directory status sent one byte every 0.2 seconds.
    """

    drop_before: str | None = None
    drop_after: str | None = None
    fail_write_at: int | None = None
    fail_close: bool = False
    truncate_on_close: bool = False
    short_reads: bool = False
    service_time: float = 0.0
    no_posix_rename: bool = False
    no_fsync: bool = False
    omit_permissions: bool = False
    undecodable_name: bool = False
    omit_sizes: bool = False
    malformed_names: str | None = None
    chatter_seconds: float = 0.0
    trickle_readdir: bool = False
    calls: Counter[str] = field(default_factory=Counter)
    pipelined: Counter[str] = field(default_factory=Counter)


class _Auth(paramiko.ServerInterface):
    def __init__(self, transport: paramiko.Transport, scenario: Scenario) -> None:
        self.transport = transport
        self.scenario = scenario

    def check_auth_password(self, username: str, password: str) -> int:
        self.scenario.calls["auth"] += 1
        if (username, password) == (USERNAME, PASSWORD):
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self, username: str) -> str:
        return "password"

    def check_channel_request(self, kind: str, chanid: int) -> int:
        return paramiko.OPEN_SUCCEEDED


class _Interface(SFTPServerInterface):
    def __init__(self, server: _Auth, *args: Any, root: Path, **kwargs: Any) -> None:
        super().__init__(server, *args, **kwargs)
        self.auth = server
        self.scenario = server.scenario
        self.root = root
        self.channel: paramiko.Channel | None = None

    def begin(self, operation: str) -> None:
        """Record one request and drop the connection first when the scenario says so.

        `pipelined` counts requests that found another request already queued, which happens
        only when the client sends without waiting for each reply; `service_time` makes each
        read and write slow enough for a pipelining client to queue requests reliably.
        """
        self.scenario.calls[operation] += 1
        if self.channel is not None and self.channel.recv_ready():
            self.scenario.pipelined[operation] += 1
        if operation in {"read", "write"}:
            time.sleep(self.scenario.service_time)
        if self.scenario.drop_before == operation:
            self.drop()

    def end(self, operation: str) -> None:
        """Drop the connection after the request took effect, before its reply is sent."""
        if self.scenario.drop_after == operation:
            self.drop()

    def drop(self) -> None:
        """Close the connection once; cleanup after the drop then runs without faults."""
        if self.auth.transport.is_active():
            self.auth.transport.close()
            raise EOFError("simulated connection drop")

    def real(self, path: str) -> Path:
        return self.root / path.lstrip("/")

    def list_folder(self, path: str) -> list[SFTPAttributes] | int:
        self.begin("list_folder")
        try:
            entries = [_attributes(entry, follow=False) for entry in self.real(path).iterdir()]
        except OSError as exc:
            return SFTPServer.convert_errno(exc.errno)
        if self.scenario.omit_permissions:
            for entry in entries:
                entry.st_mode = None
        if self.scenario.undecodable_name:
            undecodable = SFTPAttributes()
            undecodable.filename = UNDECODABLE_NAME  # type: ignore[assignment]
            undecodable.st_size = 0
            undecodable.st_mode = 0o100644
            entries.append(undecodable)
        return entries

    def stat(self, path: str) -> SFTPAttributes | int:
        self.begin("stat")
        return self._attributes(path, follow=True)

    def lstat(self, path: str) -> SFTPAttributes | int:
        self.begin("lstat")
        return self._attributes(path, follow=False)

    def _attributes(self, path: str, *, follow: bool) -> SFTPAttributes | int:
        try:
            attributes = _attributes(self.real(path), follow=follow)
        except OSError as exc:
            return SFTPServer.convert_errno(exc.errno)
        if self.scenario.omit_sizes:
            attributes.st_size = None
        return attributes

    def open(self, path: str, flags: int, attr: SFTPAttributes) -> SFTPHandle | int:
        self.begin("open")
        real = self.real(path)
        try:
            descriptor = os.open(real, flags, 0o666)
        except OSError as exc:
            return SFTPServer.convert_errno(exc.errno)
        mode = "wb" if flags & os.O_WRONLY else "r+b" if flags & os.O_RDWR else "rb"
        try:
            stream = os.fdopen(descriptor, mode)
        except OSError as exc:
            os.close(descriptor)
            return SFTPServer.convert_errno(exc.errno)
        return _Handle(self, real, flags, stream)

    def remove(self, path: str) -> int:
        self.begin("remove")
        try:
            self.real(path).unlink()
        except OSError as exc:
            return SFTPServer.convert_errno(exc.errno)
        return SFTP_OK

    def rename(self, oldpath: str, newpath: str) -> int:
        """Like OpenSSH sftp-server: link, then unlink, so an existing destination is kept."""
        self.begin("rename")
        try:
            os.link(self.real(oldpath), self.real(newpath))
            os.unlink(self.real(oldpath))
        except FileExistsError:
            return SFTP_FAILURE
        except OSError as exc:
            return SFTPServer.convert_errno(exc.errno)
        self.end("rename")
        return SFTP_OK

    def posix_rename(self, oldpath: str, newpath: str) -> int:
        self.begin("posix_rename")
        if self.scenario.no_posix_rename:
            return SFTP_OP_UNSUPPORTED
        try:
            os.replace(self.real(oldpath), self.real(newpath))
        except OSError as exc:
            return SFTPServer.convert_errno(exc.errno)
        self.end("posix_rename")
        return SFTP_OK

    def chattr(self, path: str, attr: SFTPAttributes) -> int:
        self.begin("chattr")
        try:
            SFTPServer.set_file_attr(str(self.real(path)), attr)
        except OSError as exc:
            return SFTPServer.convert_errno(exc.errno)
        return SFTP_OK


class _Handle(SFTPHandle):
    def __init__(self, interface: _Interface, path: Path, flags: int, stream: Any) -> None:
        super().__init__(flags)
        self.interface = interface
        self.path = path
        self.readfile = stream
        self.writefile = stream
        self.closed = False

    def read(self, offset: int, length: int) -> bytes | int:
        self.interface.begin("read")
        if self.interface.scenario.short_reads:
            length = max(1, length // 3)
        return super().read(offset, length)

    def write(self, offset: int, data: bytes) -> int:
        self.interface.begin("write")
        if offset == self.interface.scenario.fail_write_at:
            return SFTP_FAILURE
        return super().write(offset, data)

    def fsync(self) -> None:
        self.interface.begin("fsync")
        self.writefile.flush()
        os.fsync(self.writefile.fileno())

    def close(self) -> None:
        """Close once; the server also closes every handle still open when a session ends."""
        if self.closed:
            return
        self.closed = True
        try:
            self.interface.begin("close")
        finally:
            super().close()
        if not self.path.name.startswith(".databridge-"):
            return
        if self.interface.scenario.fail_close:
            raise OSError("simulated deferred write failure reported at close")
        if self.interface.scenario.truncate_on_close:
            self.path.write_bytes(b"")


class _Server(SFTPServer):
    server: _Interface

    def __init__(self, channel: paramiko.Channel, name: str, server: _Auth, *args: Any, **kw: Any):
        super().__init__(channel, name, server, *args, **kw)
        self.server.channel = channel

    def _read_folder(self, request_number: int, folder: SFTPHandle) -> None:
        self.server.begin("readdir")
        if self.server.scenario.trickle_readdir:
            reply = Message()
            reply.add_int(request_number)
            reply.add_int(SFTP_EOF)
            reply.add_string("")
            reply.add_string("")
            payload = reply.asbytes()
            packet = struct.pack(">I", len(payload) + 1) + bytes([CMD_STATUS]) + payload
            for index in range(len(packet)):
                self.sock.send(packet[index : index + 1])
                time.sleep(0.2)
            return
        chatter_until = time.monotonic() + self.server.scenario.chatter_seconds
        while time.monotonic() < chatter_until:
            self._send_status(0x7FFFFFF0, SFTP_OK)
            time.sleep(0.01)
        malformed = self.server.scenario.malformed_names
        if malformed is None:
            super()._read_folder(request_number, folder)
            return
        reply = Message()
        reply.add_int(request_number)
        if malformed == "count":
            reply.add_int(0x7FFFFFFF)  # entries claimed, none sent
        elif malformed == "oversized":
            reply.add_int(1)
            reply.add_string(b"x" * 300_000)
            reply.add_string(b"")
            reply.add_int(0)
        elif malformed == "extended":
            reply.add_int(1)
            reply.add_string(b"a")
            reply.add_string(b"")
            reply.add_int(SFTPAttributes.FLAG_EXTENDED)
            reply.add_int(0x7FFFFFFF)  # extended attributes claimed, none sent
        self._send_packet(CMD_NAME, reply)

    def _process(self, t: int, request_number: int, msg: Message) -> None:
        if t == CMD_EXTENDED and not self.server.scenario.no_fsync:
            if msg.get_text() == "fsync@openssh.com":
                handle = self.file_table.get(msg.get_binary())
                if not isinstance(handle, _Handle):
                    self._send_status(request_number, SFTP_BAD_MESSAGE, "Invalid handle")
                    return
                handle.fsync()
                self._send_status(request_number, SFTP_OK)
                return
            msg.rewind()
            msg.get_int()
        super()._process(t, request_number, msg)


def _attributes(path: Path, *, follow: bool) -> SFTPAttributes:
    attributes = SFTPAttributes.from_stat(os.stat(path) if follow else os.lstat(path))
    attributes.filename = path.name
    return attributes


class FakeSFTPServer:
    """Serve `root` over SFTP on an ephemeral loopback port for the duration of a `with` block."""

    def __init__(self, root: Path, scenario: Scenario | None = None) -> None:
        self.root = root
        self.scenario = scenario or Scenario()
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._stopped = threading.Event()
        self._transports: list[paramiko.Transport] = []
        self._thread = threading.Thread(target=self._serve, daemon=True)

    @property
    def port(self) -> int:
        return int(self._listener.getsockname()[1])

    def known_hosts_line(self, host: str = "127.0.0.1", key: paramiko.PKey = HOST_KEY) -> str:
        return f"[{host}]:{self.port} {key.get_name()} {key.get_base64()}\n"

    def __enter__(self) -> Self:
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(16)
        self._listener.settimeout(0.05)
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._stopped.set()
        self._thread.join(timeout=5)
        self._listener.close()
        for transport in self._transports:
            transport.close()

    def _serve(self) -> None:
        while not self._stopped.is_set():
            try:
                connection, _address = self._listener.accept()
            except TimeoutError:
                continue
            transport = paramiko.Transport(connection)
            transport.add_server_key(HOST_KEY)
            transport.set_subsystem_handler("sftp", _Server, _Interface, root=self.root)
            self._transports.append(transport)
            transport.start_server(threading.Event(), _Auth(transport, self.scenario))
