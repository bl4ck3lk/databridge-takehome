"""SFTP connector against a loopback server: host trust, error classes, integrity, pipelining."""

import os
import socket
import stat
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import paramiko
import pytest
from paramiko.sftp import CMD_LSTAT
from pydantic import SecretStr
from sftp_server import PASSWORD, USERNAME, FakeSFTPServer, Scenario
from support import read_all

from databridge.config import Settings
from databridge.connectors import base, sftp
from databridge.connectors.base import Listing
from databridge.connectors.local import LocalConnector
from databridge.connectors.sftp import SFTPConnector
from databridge.errors import DataBridgeError, ErrorCode
from databridge.models import LocalConnection, SFTPConnection, TransferRequest
from databridge.store import ConnectionStore
from databridge.transfer import TransferService

MIB = 1_048_576
PAYLOAD = bytes(range(256)) * (4 * MIB // 256) + b"tail"


@dataclass
class Remote:
    server: FakeSFTPServer
    files: Path
    known_hosts: Path

    @property
    def scenario(self) -> Scenario:
        return self.server.scenario

    def connector(
        self, *, host: str = "127.0.0.1", root: str = "files", password: str = PASSWORD
    ) -> SFTPConnector:
        connection = SFTPConnection(
            name="remote",
            type="sftp",
            host=host,
            port=self.server.port,
            username=USERNAME,
            password=password,
            root=root,
        )
        return SFTPConnector(connection, self.known_hosts)


@pytest.fixture
def serve(tmp_path: Path) -> Iterator[Callable[..., Remote]]:
    """Start a loopback server whose connection root "files" is an empty directory."""
    with ExitStack() as stack:

        def start(scenario: Scenario | None = None) -> Remote:
            served = tmp_path / "served"
            (served / "files").mkdir(parents=True)
            server = stack.enter_context(FakeSFTPServer(served, scenario))
            known_hosts = tmp_path / "known_hosts"
            known_hosts.write_text(server.known_hosts_line())
            return Remote(server, served / "files", known_hosts)

        yield start


@pytest.fixture
def remote(serve: Callable[..., Remote]) -> Remote:
    return serve()


def _error(action: Callable[[], object]) -> DataBridgeError:
    with pytest.raises(DataBridgeError) as error:
        action()
    return error.value


def _write(connector: SFTPConnector, name: str, data: bytes, *, overwrite: bool = False) -> None:
    with connector.write(name, str(uuid4()), overwrite) as sink:
        sink.write(data)


def _read(connector: SFTPConnector, name: str) -> bytes:
    with connector.read(name) as source:
        return read_all(source)


def _stages(files: Path) -> list[Path]:
    return list(files.glob(".databridge-*"))


def test_round_trip_pipelines_reads_and_writes(serve: Callable[..., Remote]) -> None:
    remote = serve(Scenario(service_time=0.002))
    connector = remote.connector()
    _write(connector, "data.bin", PAYLOAD)
    assert (remote.files / "data.bin").read_bytes() == PAYLOAD
    assert _read(connector, "data.bin") == PAYLOAD
    # 129 requests each way; a client that waits for every reply leaves nothing queued.
    assert remote.scenario.pipelined["write"] >= 64
    assert remote.scenario.pipelined["read"] >= 64


def test_short_read_replies_are_completed(serve: Callable[..., Remote]) -> None:
    remote = serve(Scenario(short_reads=True))
    (remote.files / "data.bin").write_bytes(PAYLOAD[:MIB])
    assert _read(remote.connector(), "data.bin") == PAYLOAD[:MIB]


def test_read_returns_the_size_the_file_had_when_opened(remote: Remote) -> None:
    growing = remote.files / "growing.bin"
    growing.write_bytes(b"a" * 100)
    with remote.connector().read("growing.bin") as source:
        with growing.open("ab") as appended:
            appended.write(b"b" * 50)
        assert read_all(source) == b"a" * 100


def test_file_that_shrinks_while_read_is_reported(remote: Remote) -> None:
    shrinking = remote.files / "shrinking.bin"
    shrinking.write_bytes(PAYLOAD)
    with remote.connector().read("shrinking.bin") as source:
        assert source.read(10) == PAYLOAD[:10]
        os.truncate(shrinking, 3 * MIB + 7)
        error = _error(lambda: read_all(source))
    assert error.code == ErrorCode.SOURCE_CHANGED


def test_sizes_the_server_does_not_report_fail_closed(serve: Callable[..., Remote]) -> None:
    remote = serve(Scenario(omit_sizes=True))
    (remote.files / "file.csv").write_text("a\n")
    connector = remote.connector()
    assert _error(lambda: _read(connector, "file.csv")).code == ErrorCode.SFTP_OPERATION_FAILED
    error = _error(lambda: _write(connector, "target.bin", b"data"))
    assert error.code == ErrorCode.SFTP_OPERATION_FAILED
    assert not (remote.files / "target.bin").exists()
    assert not _stages(remote.files)


def test_unrelated_replies_do_not_extend_the_reply_timeout(
    serve: Callable[..., Remote], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sftp, "_REPLY_TIMEOUT", 0.3)
    remote = serve(Scenario(chatter_seconds=3))
    started = time.monotonic()
    error = _error(remote.connector().list_files)
    assert error.code == ErrorCode.SFTP_UNAVAILABLE
    assert "did not respond within" in error.message
    assert time.monotonic() - started < 2


def test_a_reply_trickled_byte_by_byte_is_abandoned(
    serve: Callable[..., Remote], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sftp, "_REPLY_TIMEOUT", 0.3)
    remote = serve(Scenario(trickle_readdir=True))
    started = time.monotonic()
    error = _error(remote.connector().list_files)
    assert error.code == ErrorCode.SFTP_UNAVAILABLE
    assert "did not respond within" in error.message
    assert time.monotonic() - started < 2


@pytest.mark.parametrize("malformed", ["count", "oversized", "extended"])
def test_hostile_listing_reply_fails_fast(serve: Callable[..., Remote], malformed: str) -> None:
    remote = serve(Scenario(malformed_names=malformed))
    (remote.files / "file.csv").write_text("a\n")
    started = time.monotonic()
    error = _error(remote.connector().list_files)
    assert error.code == ErrorCode.SFTP_OPERATION_FAILED
    assert time.monotonic() - started < 5


@pytest.mark.parametrize(
    ("operation", "action"),
    [
        ("list_folder", "list"),
        ("readdir", "list"),
        ("stat", "read"),
        ("open", "read"),
        ("read", "read"),
        ("open", "write"),
        ("write", "write"),
        ("fsync", "write"),
        ("close", "write"),
        ("list_folder", "check"),
        ("open", "check"),
        ("remove", "check"),
    ],
)
def test_connection_drop_is_unavailable(
    serve: Callable[..., Remote], operation: str, action: str
) -> None:
    remote = serve(Scenario(drop_before=operation))
    (remote.files / "source.bin").write_bytes(PAYLOAD)
    connector = remote.connector()
    actions: dict[str, Callable[[], object]] = {
        "list": connector.list_files,
        "read": lambda: _read(connector, "source.bin"),
        "write": lambda: _write(connector, "target.bin", PAYLOAD),
        "check": connector.check_access,
    }
    assert _error(actions[action]).code == ErrorCode.SFTP_UNAVAILABLE
    assert not (remote.files / "target.bin").exists()


def test_failed_close_publishes_nothing(serve: Callable[..., Remote]) -> None:
    remote = serve(Scenario(fail_close=True))
    error = _error(lambda: _write(remote.connector(), "target.bin", PAYLOAD))
    assert error.code == ErrorCode.SFTP_OPERATION_FAILED
    assert not (remote.files / "target.bin").exists()
    assert not _stages(remote.files)


def test_stage_truncated_by_the_server_publishes_nothing(serve: Callable[..., Remote]) -> None:
    remote = serve(Scenario(truncate_on_close=True))
    error = _error(lambda: _write(remote.connector(), "target.bin", PAYLOAD))
    assert error.code == ErrorCode.SFTP_OPERATION_FAILED
    assert f"stored 0 of {len(PAYLOAD)} bytes" in error.message
    assert not (remote.files / "target.bin").exists()
    assert not _stages(remote.files)


@pytest.mark.parametrize(
    "offset",
    [MIB, 3 * MIB],
    ids=["status-read-while-writing", "status-read-when-finishing"],
)
def test_failed_pipelined_write_publishes_nothing(
    serve: Callable[..., Remote], offset: int
) -> None:
    # Later writes still extend the file, so only the failed request's status reveals the hole.
    remote = serve(Scenario(fail_write_at=offset))
    error = _error(lambda: _write(remote.connector(), "target.bin", PAYLOAD))
    assert error.code == ErrorCode.SFTP_OPERATION_FAILED
    assert not (remote.files / "target.bin").exists()
    assert not _stages(remote.files)


@pytest.mark.parametrize(("overwrite", "operation"), [(False, "rename"), (True, "posix_rename")])
def test_lost_publish_reply_reports_a_possible_publication(
    serve: Callable[..., Remote], overwrite: bool, operation: str
) -> None:
    remote = serve(Scenario(drop_after=operation))
    error = _error(lambda: _write(remote.connector(), "target.bin", b"new", overwrite=overwrite))
    assert error.code == ErrorCode.SFTP_UNAVAILABLE
    assert "may already contain" in error.message
    assert (remote.files / "target.bin").read_bytes() == b"new"


def test_overwrite_without_posix_rename_fails_closed(serve: Callable[..., Remote]) -> None:
    remote = serve(Scenario(no_posix_rename=True))
    (remote.files / "target.bin").write_bytes(b"old")
    error = _error(lambda: _write(remote.connector(), "target.bin", b"new", overwrite=True))
    assert error.code == ErrorCode.PUBLISH_UNSUPPORTED
    assert (remote.files / "target.bin").read_bytes() == b"old"
    assert not _stages(remote.files)


def test_existing_destination_requires_overwrite(remote: Remote) -> None:
    (remote.files / "target.bin").write_bytes(b"old")
    error = _error(lambda: _write(remote.connector(), "target.bin", b"new"))
    assert error.code == ErrorCode.DESTINATION_EXISTS
    assert '"overwrite": true' in error.message
    assert (remote.files / "target.bin").read_bytes() == b"old"


def test_publish_time_collision_keeps_the_winner(remote: Remote) -> None:
    winner = remote.files / "target.bin"
    with pytest.raises(DataBridgeError) as error:
        with remote.connector().write("target.bin", str(uuid4()), overwrite=False) as sink:
            sink.write(b"staged")
            winner.write_bytes(b"winner")
    assert error.value.code == ErrorCode.DESTINATION_EXISTS
    assert winner.read_bytes() == b"winner"
    assert not _stages(remote.files)


def test_failed_copy_removes_the_stage(remote: Remote) -> None:
    with pytest.raises(ValueError, match="injected"):
        with remote.connector().write("target.bin", str(uuid4()), overwrite=False) as sink:
            sink.write(PAYLOAD[:MIB])
            raise ValueError("injected")
    assert not (remote.files / "target.bin").exists()
    assert not _stages(remote.files)


def test_successful_write_is_synced_and_removes_nothing(remote: Remote) -> None:
    _write(remote.connector(), "target.bin", b"data")
    assert remote.scenario.calls["fsync"] == 1
    assert remote.scenario.calls["remove"] == 0


def test_write_succeeds_on_servers_without_fsync(serve: Callable[..., Remote]) -> None:
    remote = serve(Scenario(no_fsync=True))
    _write(remote.connector(), "target.bin", b"data")
    assert (remote.files / "target.bin").read_bytes() == b"data"


def test_overwrite_preserves_the_destination_mode(remote: Remote) -> None:
    target = remote.files / "target.bin"
    target.write_bytes(b"old")
    target.chmod(0o640)
    _write(remote.connector(), "target.bin", b"new", overwrite=True)
    assert target.read_bytes() == b"new"
    assert stat.S_IMODE(target.stat().st_mode) == 0o640


def test_listing_without_permission_bits_classifies_each_entry(
    serve: Callable[..., Remote],
) -> None:
    remote = serve(Scenario(omit_permissions=True))
    (remote.files / "file.csv").write_text("a\n")
    (remote.files / "folder").mkdir()
    (remote.files / "link.csv").symlink_to(remote.files / "file.csv")
    assert remote.connector().list_files().files == ["file.csv"]


def test_listing_skips_names_that_are_not_utf8(serve: Callable[..., Remote]) -> None:
    remote = serve(Scenario(undecodable_name=True))
    (remote.files / "file.csv").write_text("a\n")
    assert remote.connector().list_files().files == ["file.csv"]


def test_listing_stops_reading_at_the_scan_cap(
    remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    for index in range(100):
        (remote.files / f"file-{index:03}.csv").write_text("a\n")
    monkeypatch.setattr(base, "MAX_LIST_SCAN", 20)
    listing = remote.connector().list_files()
    assert listing.truncated is True
    assert len(listing.files) == 20
    assert remote.scenario.calls["readdir"] == 2


def test_missing_root_is_unavailable(remote: Remote) -> None:
    connector = remote.connector(root="missing")
    for action in (
        connector.list_files,
        connector.check_access,
        lambda: _read(connector, "file.csv"),
        lambda: _write(connector, "file.csv", b"data"),
    ):
        assert _error(action).code == ErrorCode.CONNECTION_ROOT_UNAVAILABLE


def test_missing_and_non_regular_files_are_not_found(remote: Remote) -> None:
    (remote.files / "folder").mkdir()
    connector = remote.connector()
    assert _error(lambda: _read(connector, "absent.csv")).code == ErrorCode.FILE_NOT_FOUND
    assert _error(lambda: _read(connector, "folder")).code == ErrorCode.FILE_NOT_FOUND


@pytest.mark.skipif(os.geteuid() == 0, reason="root bypasses file permissions")
def test_unreadable_file_is_not_readable(remote: Remote) -> None:
    secret = remote.files / "secret.csv"
    secret.write_text("a\n")
    secret.chmod(0)
    try:
        error = _error(lambda: _read(remote.connector(), "secret.csv"))
    finally:
        secret.chmod(0o600)
    assert error.code == ErrorCode.FILE_NOT_READABLE


@pytest.mark.skipif(os.geteuid() == 0, reason="root bypasses file permissions")
def test_read_only_root_is_not_writable(remote: Remote) -> None:
    remote.files.chmod(0o555)
    try:
        connector = remote.connector()
        assert connector.check_access().writable is False
        error = _error(lambda: _write(connector, "target.bin", b"data"))
    finally:
        remote.files.chmod(0o755)
    assert error.code == ErrorCode.DESTINATION_NOT_WRITABLE


def test_check_access_leaves_no_probe(remote: Remote) -> None:
    assert remote.connector().check_access().writable is True
    assert list(remote.files.iterdir()) == []


def test_a_probe_that_cannot_be_removed_fails_the_access_check(
    serve: Callable[..., Remote],
) -> None:
    remote = serve(Scenario(deny_remove=True))
    error = _error(remote.connector().check_access)
    assert error.code == ErrorCode.SFTP_OPERATION_FAILED
    [probe] = list(remote.files.iterdir())
    assert probe.name in error.message


@pytest.mark.parametrize(
    "contents",
    [None, b"[127.0.0.1]:22 ssh-ed25519 !!!not-base64!!!\n", b"\xff\xfe\x00 not text\n"],
    ids=["missing", "invalid-key", "not-utf8"],
)
def test_unusable_trust_file_is_rejected_and_named(remote: Remote, contents: bytes | None) -> None:
    if contents is None:
        remote.known_hosts.unlink()
    else:
        remote.known_hosts.write_bytes(contents)
    error = _error(remote.connector().list_files)
    assert error.code == ErrorCode.SFTP_HOST_KEY_REJECTED
    assert str(remote.known_hosts) in error.message
    assert remote.scenario.calls["auth"] == 0


def test_untrusted_host_alias_names_the_key_and_the_fix(remote: Remote) -> None:
    error = _error(remote.connector(host="localhost").list_files)
    assert error.code == ErrorCode.SFTP_HOST_KEY_REJECTED
    assert f"[localhost]:{remote.server.port}" in error.message
    assert str(remote.known_hosts) in error.message
    assert "ssh-keyscan" in error.message
    assert "SHA256:" in error.message
    assert remote.scenario.calls["auth"] == 0


def test_changed_host_key_names_the_fix(remote: Remote) -> None:
    impostor = paramiko.ECDSAKey.generate()
    remote.known_hosts.write_text(remote.server.known_hosts_line(key=impostor))
    error = _error(remote.connector().list_files)
    assert error.code == ErrorCode.SFTP_HOST_KEY_REJECTED
    assert "ssh-keygen -R" in error.message
    assert remote.scenario.calls["auth"] == 0


def test_rejected_password_is_an_authentication_failure(remote: Remote) -> None:
    error = _error(remote.connector(password="incorrect").list_files)
    assert error.code == ErrorCode.SFTP_AUTH_FAILED


def test_closed_port_is_unavailable(remote: Remote) -> None:
    with socket.socket() as unused:
        unused.bind(("127.0.0.1", 0))
        port = unused.getsockname()[1]
    connection = SFTPConnection(
        name="remote",
        type="sftp",
        host="127.0.0.1",
        port=port,
        username=USERNAME,
        password=PASSWORD,
        root="files",
    )
    error = _error(SFTPConnector(connection, remote.known_hosts).list_files)
    assert error.code == ErrorCode.SFTP_UNAVAILABLE


@pytest.mark.parametrize(("host", "root"), [("a" * 64, "files"), ("127.0.0.1", "\udcff")])
def test_settings_that_cannot_be_encoded_are_invalid(remote: Remote, host: str, root: str) -> None:
    # model_construct skips validation, as a connector must not trust its input's origin.
    connection = SFTPConnection.model_construct(
        name="remote",
        type="sftp",
        host=host,
        port=remote.server.port,
        username=USERNAME,
        password=SecretStr(PASSWORD),
        root=root,
    )
    error = _error(SFTPConnector(connection, remote.known_hosts).list_files)
    assert error.code == ErrorCode.INVALID_CONNECTION_SETTINGS


@pytest.mark.parametrize("pages", ["empty", "dots"])
def test_a_directory_that_never_ends_is_bounded(
    serve: Callable[..., Remote], monkeypatch: pytest.MonkeyPatch, pages: str
) -> None:
    monkeypatch.setattr(base, "MAX_LIST_SCAN", 50)
    remote = serve(Scenario(endless_readdir=pages))
    outcome: list[object] = []

    def list_files() -> None:
        try:
            outcome.append(remote.connector().list_files())
        except DataBridgeError as error:
            outcome.append(error)

    # A regression would loop forever, so the listing runs where the test can stop waiting.
    worker = threading.Thread(target=list_files, daemon=True)
    worker.start()
    worker.join(timeout=5)
    assert outcome, "the listing did not end within 5 seconds"
    if pages == "empty":
        assert isinstance(outcome[0], DataBridgeError)
        assert outcome[0].code == ErrorCode.SFTP_OPERATION_FAILED
    else:
        # "." and ".." still count as scanned entries, so the scan cap ends the listing.
        assert isinstance(outcome[0], Listing)
        assert (outcome[0].files, outcome[0].truncated) == ([], True)


def test_read_uses_the_size_of_the_file_it_opened(serve: Callable[..., Remote]) -> None:
    replaced: list[str] = []

    def replace_after_stat(path: str) -> None:
        if path.endswith("report.csv") and not replaced:
            replaced.append(path)
            staged = remote.files / "report.tmp"
            staged.write_bytes(b"b" * 200)
            os.replace(staged, remote.files / "report.csv")

    remote = serve(Scenario(after_stat=replace_after_stat))
    (remote.files / "report.csv").write_bytes(b"a" * 100)
    assert _read(remote.connector(), "report.csv") == b"b" * 200
    assert replaced


def test_sync_and_close_get_time_in_proportion_to_the_upload(
    serve: Callable[..., Remote], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sftp, "_REPLY_TIMEOUT", 0.3)
    monkeypatch.setattr(sftp, "SETTLE_RATE", MIB)
    remote = serve(Scenario(fsync_seconds=1.0))
    _write(remote.connector(), "target.bin", PAYLOAD)
    assert (remote.files / "target.bin").read_bytes() == PAYLOAD


def test_a_limited_read_requests_only_what_it_may_return(remote: Remote) -> None:
    (remote.files / "big.bin").write_bytes(PAYLOAD)
    with remote.connector().read("big.bin", limit=100) as source:
        assert read_all(source) == PAYLOAD[:100]
    assert remote.scenario.calls["read"] == 1


def test_permission_lookups_stay_within_the_request_window(
    serve: Callable[..., Remote], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sftp, "WINDOW", 4)
    remote = serve(Scenario(omit_permissions=True))
    for index in range(40):
        (remote.files / f"file-{index:02}.csv").write_text("a\n")
    outstanding: set[int] = set()
    peak = [0]
    send, receive, ignore = sftp._Requests.send, sftp._Requests.receive, sftp._Requests.ignore

    def counting_send(self: sftp._Requests, kind: int, *args: object) -> int:
        number = send(self, kind, *args)
        if kind == CMD_LSTAT:
            outstanding.add(number)
            peak[0] = max(peak[0], len(outstanding))
        return number

    def counting_receive(self: sftp._Requests, number: int, *args: Any, **kw: Any) -> Any:
        outstanding.discard(number)
        return receive(self, number, *args, **kw)

    def counting_ignore(self: sftp._Requests, number: int) -> None:
        outstanding.discard(number)
        ignore(self, number)

    monkeypatch.setattr(sftp._Requests, "send", counting_send)
    monkeypatch.setattr(sftp._Requests, "receive", counting_receive)
    monkeypatch.setattr(sftp._Requests, "ignore", counting_ignore)
    assert len(remote.connector().list_files().files) == 40
    assert 0 < peak[0] <= 4


def test_a_failed_tail_write_is_a_write_failure_not_a_publication(
    serve: Callable[..., Remote],
    settings: Settings,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote = serve(Scenario(fail_write_at=3 * MIB))
    source_root = tmp_path / "source"
    source_root.mkdir()
    (source_root / "data.bin").write_bytes(PAYLOAD)
    store = ConnectionStore(settings.database_path, settings.encryption_key)
    store.initialize()
    store.create(LocalConnection(name="local", type="local", path=str(source_root)))
    sftp_connector = remote.connector()
    store.create(sftp_connector.connection)
    connectors: dict[str, Any] = {"local": LocalConnector(source_root), "remote": sftp_connector}
    marked: list[str] = []
    mark_publishing = store.mark_publishing

    def spy(transfer_id: str, bytes_copied: int) -> None:
        marked.append(transfer_id)
        mark_publishing(transfer_id, bytes_copied)

    monkeypatch.setattr(store, "mark_publishing", spy)
    service = TransferService(store, lambda connection: connectors[connection.name])
    request = TransferRequest(
        source="local", source_file="data.bin", destination="remote", destination_file="t.bin"
    )
    error = _error(lambda: service.run(request))
    assert error.code == ErrorCode.SFTP_OPERATION_FAILED
    assert error.transfer_id is not None
    record = store.get_transfer(error.transfer_id)
    assert (record.status, record.failure_phase) == ("failed", "destination_write")
    assert marked == []
    assert not (remote.files / "t.bin").exists()
