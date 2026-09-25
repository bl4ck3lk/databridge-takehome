"""Local connector: name policy, non-regular files, publication safety, and error classes."""

import errno
import os
import stat
import threading
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

import pytest
from support import read_all

from databridge.connectors import CONNECTOR_TYPES, open_connector
from databridge.connectors.base import ConnectorContext
from databridge.connectors.local import LocalConnector
from databridge.errors import DataBridgeError, ErrorCode
from databridge.models import CONNECTION_MODELS, LocalConnection


def _code(action: Callable[[], object]) -> ErrorCode:
    with pytest.raises(DataBridgeError) as error:
        action()
    return error.value.code


def _write(connector: LocalConnector, name: str, data: bytes, *, overwrite: bool = False) -> None:
    with connector.write(name, str(uuid4()), overwrite) as sink:
        sink.write(data)


def _read(connector: LocalConnector, name: str) -> bytes:
    with connector.read(name) as source:
        return source.read(1_048_576)


def _stages(root: Path) -> list[Path]:
    return list(root.glob(".databridge-*"))


def test_every_connection_type_has_a_connector() -> None:
    assert set(CONNECTOR_TYPES) == set(CONNECTION_MODELS)


def test_unknown_connection_type_has_no_connector() -> None:
    impostor = LocalConnection.model_construct(name="impostor", type="ftp", path="/tmp")
    with pytest.raises(RuntimeError, match="Unsupported connection type 'ftp'"):
        open_connector(impostor, ConnectorContext(known_hosts_path=Path("/tmp/known_hosts")))


@pytest.mark.parametrize(
    "name",
    [".databridge-anything", ".DataBridge-" + str(uuid4()) + ".part", "a" * 256, "é" * 128],
)
def test_reserved_and_overlong_names_are_invalid(tmp_path: Path, name: str) -> None:
    connector = LocalConnector(tmp_path)
    assert _code(lambda: _write(connector, name, b"x")) == ErrorCode.INVALID_FILENAME


@pytest.mark.parametrize("length", [202, 255])
def test_names_up_to_the_filesystem_limit_can_be_published(tmp_path: Path, length: int) -> None:
    connector = LocalConnector(tmp_path)
    name = "n" * length
    _write(connector, name, b"payload")
    assert _read(connector, name) == b"payload"
    assert name in connector.list_files().files


def test_reads_of_non_regular_files_fail_fast(tmp_path: Path) -> None:
    os.mkfifo(tmp_path / "pipe.csv")
    (tmp_path / "folder.csv").mkdir()
    connector = LocalConnector(tmp_path)
    codes: list[ErrorCode] = []

    def read_all() -> None:
        for name in ("pipe.csv", "folder.csv"):
            codes.append(_code(lambda name=name: _read(connector, name)))  # type: ignore[misc]

    reader = threading.Thread(target=read_all, daemon=True)
    reader.start()
    reader.join(timeout=5)

    assert not reader.is_alive(), "reading a FIFO blocked"
    assert codes == [ErrorCode.FILE_NOT_FOUND, ErrorCode.FILE_NOT_FOUND]


def test_device_files_are_not_readable_as_data() -> None:
    connector = LocalConnector(Path("/dev"))
    assert _code(lambda: _read(connector, "zero")) == ErrorCode.FILE_NOT_FOUND


def test_symlink_loop_is_an_invalid_filename(tmp_path: Path) -> None:
    (tmp_path / "loop.csv").symlink_to("loop.csv")
    connector = LocalConnector(tmp_path)
    assert _code(lambda: _read(connector, "loop.csv")) == ErrorCode.INVALID_FILENAME


def test_read_returns_the_size_the_file_had_when_opened(tmp_path: Path) -> None:
    growing = tmp_path / "growing.bin"
    growing.write_bytes(b"a" * 100)
    with LocalConnector(tmp_path).read("growing.bin") as source:
        with growing.open("ab") as appended:
            appended.write(b"b" * 50)
        assert read_all(source) == b"a" * 100


def test_file_that_shrinks_while_read_is_reported(tmp_path: Path) -> None:
    shrinking = tmp_path / "shrinking.bin"
    shrinking.write_bytes(b"a" * 100)
    with LocalConnector(tmp_path).read("shrinking.bin") as source:
        assert source.read(10) == b"a" * 10
        os.truncate(shrinking, 50)
        assert _code(lambda: read_all(source)) == ErrorCode.SOURCE_CHANGED


@pytest.mark.skipif(os.geteuid() == 0, reason="root bypasses file permissions")
def test_unreadable_file_is_reported_as_not_readable(tmp_path: Path) -> None:
    secret = tmp_path / "secret.csv"
    secret.write_text("a\n1\n")
    secret.chmod(0)
    try:
        assert _code(lambda: _read(LocalConnector(tmp_path), "secret.csv")) == (
            ErrorCode.FILE_NOT_READABLE
        )
    finally:
        secret.chmod(0o600)


def test_publish_time_collision_preserves_the_winner(tmp_path: Path) -> None:
    connector = LocalConnector(tmp_path)

    def race() -> None:
        with connector.write("report.csv", str(uuid4()), False) as sink:
            sink.write(b"staged")
            (tmp_path / "report.csv").write_bytes(b"winner")

    assert _code(race) == ErrorCode.DESTINATION_EXISTS
    assert (tmp_path / "report.csv").read_bytes() == b"winner"
    assert _stages(tmp_path) == []


def test_overwrite_replaces_content_and_keeps_the_destination_mode(tmp_path: Path) -> None:
    destination = tmp_path / "report.csv"
    destination.write_bytes(b"old")
    destination.chmod(0o640)
    connector = LocalConnector(tmp_path)

    _write(connector, "report.csv", b"new", overwrite=True)

    assert destination.read_bytes() == b"new"
    assert stat.S_IMODE(destination.stat().st_mode) == 0o640


def test_existing_destination_is_refused_with_an_overwrite_hint(tmp_path: Path) -> None:
    (tmp_path / "report.csv").write_bytes(b"old")
    with pytest.raises(DataBridgeError) as error:
        _write(LocalConnector(tmp_path), "report.csv", b"new")
    assert error.value.code == ErrorCode.DESTINATION_EXISTS
    assert "overwrite" in error.value.message


def test_partial_os_writes_still_store_every_byte(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_write = os.write
    monkeypatch.setattr(os, "write", lambda descriptor, data: real_write(descriptor, data[:3]))
    connector = LocalConnector(tmp_path)

    _write(connector, "slow.bin", b"0123456789")

    assert (tmp_path / "slow.bin").read_bytes() == b"0123456789"


@pytest.mark.parametrize(
    ("patched", "error_number", "expected"),
    [
        ("write", errno.ENOSPC, ErrorCode.DESTINATION_FULL),
        ("fsync", errno.ENOSPC, ErrorCode.DESTINATION_FULL),
        ("fsync", errno.EIO, ErrorCode.DESTINATION_WRITE_FAILED),
        ("link", errno.EPERM, ErrorCode.PUBLISH_UNSUPPORTED),
        ("link", errno.EXDEV, ErrorCode.PUBLISH_UNSUPPORTED),
    ],
)
def test_write_failures_are_classified_and_leave_no_trace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    patched: str,
    error_number: int,
    expected: ErrorCode,
) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        raise OSError(error_number, os.strerror(error_number))

    connector = LocalConnector(tmp_path)
    monkeypatch.setattr(os, patched, fail)

    assert _code(lambda: _write(connector, "report.csv", b"payload")) == expected
    monkeypatch.undo()
    assert not (tmp_path / "report.csv").exists()
    assert _stages(tmp_path) == []


@pytest.mark.skipif(os.geteuid() == 0, reason="root bypasses file permissions")
def test_read_only_root_is_reported_as_not_writable(tmp_path: Path) -> None:
    root = tmp_path / "readonly"
    root.mkdir()
    root.chmod(0o555)
    connector = LocalConnector(root)
    try:
        assert _code(lambda: _write(connector, "report.csv", b"x")) == (
            ErrorCode.DESTINATION_NOT_WRITABLE
        )
        assert connector.check_access().writable is False
    finally:
        root.chmod(0o755)


def test_missing_root_is_reported_as_unavailable_for_every_operation(tmp_path: Path) -> None:
    connector = LocalConnector(tmp_path / "gone")
    assert _code(connector.list_files) == ErrorCode.CONNECTION_ROOT_UNAVAILABLE
    assert _code(connector.check_access) == ErrorCode.CONNECTION_ROOT_UNAVAILABLE
    assert _code(lambda: _write(connector, "report.csv", b"x")) == (
        ErrorCode.CONNECTION_ROOT_UNAVAILABLE
    )


def test_cleanup_failure_after_publication_does_not_fail_the_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_unlink = os.unlink

    def refuse_stage_removal(path: str, *args: object, **kwargs: object) -> None:
        if Path(path).name.startswith(".databridge-"):
            raise OSError(errno.EBUSY, "busy")
        real_unlink(path)

    monkeypatch.setattr(os, "unlink", refuse_stage_removal)
    _write(LocalConnector(tmp_path), "report.csv", b"payload")

    assert (tmp_path / "report.csv").read_bytes() == b"payload"


def test_failure_to_inspect_an_opened_file_is_an_io_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "data.csv").write_text("a\n")
    opened: list[int] = []
    closed: list[int] = []
    real_open, real_close = os.open, os.close

    def tracking_open(path: object, *args: object, **kwargs: object) -> int:
        descriptor = real_open(path, *args, **kwargs)  # type: ignore[arg-type]
        opened.append(descriptor)
        return descriptor

    def tracking_close(descriptor: int) -> None:
        closed.append(descriptor)
        real_close(descriptor)

    def broken_fstat(descriptor: int) -> os.stat_result:
        raise OSError(errno.EIO, "I/O error")

    monkeypatch.setattr(os, "open", tracking_open)
    monkeypatch.setattr(os, "close", tracking_close)
    monkeypatch.setattr(os, "fstat", broken_fstat)
    assert _code(lambda: _read(LocalConnector(tmp_path), "data.csv")) == ErrorCode.LOCAL_IO_ERROR
    assert opened and closed == opened


def test_failed_close_of_the_stage_is_reported_and_closes_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stage_descriptors: list[int] = []
    closes: list[int] = []
    real_open, real_close = os.open, os.close

    def tracking_open(path: object, *args: object, **kwargs: object) -> int:
        descriptor = real_open(path, *args, **kwargs)  # type: ignore[arg-type]
        if Path(str(path)).name.startswith(".databridge-"):
            stage_descriptors.append(descriptor)
        return descriptor

    def failing_close(descriptor: int) -> None:
        closes.append(descriptor)
        real_close(descriptor)
        if descriptor in stage_descriptors:
            raise OSError(errno.EIO, "I/O error")

    monkeypatch.setattr(os, "open", tracking_open)
    monkeypatch.setattr(os, "close", failing_close)
    code = _code(lambda: _write(LocalConnector(tmp_path), "report.csv", b"payload"))
    assert code == ErrorCode.DESTINATION_WRITE_FAILED
    assert [closes.count(descriptor) for descriptor in stage_descriptors] == [1]
    assert not (tmp_path / "report.csv").exists()


def test_cleanup_failure_is_logged_on_one_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    root = tmp_path / "forged\nstaging_cleanup_failed path=elsewhere"
    root.mkdir()
    monkeypatch.setattr(os, "unlink", lambda path, *args, **kwargs: _refuse(errno.EBUSY))
    _write(LocalConnector(root), "report.csv", b"payload")

    messages = [record.getMessage() for record in caplog.records]
    assert any(message.startswith("staging_cleanup_failed") for message in messages)
    assert not any("\n" in message for message in messages)


def _refuse(code: int) -> None:
    raise OSError(code, os.strerror(code))


def test_writable_root_passes_the_access_check_without_leaving_files(tmp_path: Path) -> None:
    assert LocalConnector(tmp_path).check_access().writable is True
    assert list(tmp_path.iterdir()) == []


def test_a_probe_that_cannot_be_removed_fails_the_access_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_unlink = os.unlink

    def refuse_probe_removal(path: str, *args: object, **kwargs: object) -> None:
        if Path(path).name.startswith(".databridge-check-"):
            raise OSError(errno.EBUSY, "busy")
        real_unlink(path)

    monkeypatch.setattr(os, "unlink", refuse_probe_removal)
    with pytest.raises(DataBridgeError) as caught:
        LocalConnector(tmp_path).check_access()

    assert caught.value.code == ErrorCode.LOCAL_IO_ERROR
    [probe] = list(tmp_path.iterdir())
    assert probe.name in caught.value.message


def test_prepare_creates_and_resolves_the_root(tmp_path: Path) -> None:
    requested = LocalConnection(
        name="output", type="local", path=str(tmp_path / "new" / ".." / "out")
    )
    prepared = LocalConnector.prepare(requested)
    assert isinstance(prepared, LocalConnection)
    assert prepared.path == str((tmp_path / "out").resolve())
    assert (tmp_path / "out").is_dir()


@pytest.mark.parametrize("problem", ["file", "nul"])
def test_prepare_rejects_unusable_roots(tmp_path: Path, problem: str) -> None:
    target = tmp_path / "file.txt"
    target.write_text("content")
    path = str(target) if problem == "file" else str(tmp_path / "a\x00b")
    requested = LocalConnection.model_construct(name="bad", type="local", path=path)
    assert _code(lambda: LocalConnector.prepare(requested)) == (
        ErrorCode.INVALID_CONNECTION_SETTINGS
    )
