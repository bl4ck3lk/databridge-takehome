"""Transfer orchestration, its persisted record, and its HTTP routes, without a network fixture."""

import itertools
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from support import client_for

from databridge.config import Settings
from databridge.connectors.base import CHUNK_SIZE, AccessCheck, Listing
from databridge.errors import DataBridgeError, ErrorCode
from databridge.models import Connection, LocalConnection, TransferRequest
from databridge.store import ConnectionStore
from databridge.transfer import TransferService

PAYLOAD = b"x" * (CHUNK_SIZE + 1)


def _connection(client: TestClient, name: str, path: Path) -> None:
    response = client.post("/connections", json={"name": name, "type": "local", "path": str(path)})
    assert response.status_code == 201


def _transfer(
    source: str, destination: str, *, overwrite: bool = False, target: str = "target.bin"
) -> dict[str, object]:
    return {
        "source": source,
        "source_file": "source.bin",
        "destination": destination,
        "destination_file": target,
        "overwrite": overwrite,
    }


def _error(action: Callable[[], object]) -> DataBridgeError:
    with pytest.raises(DataBridgeError) as error:
        action()
    return error.value


class _Scripted:
    """A third connector, unrelated to the production backends, that fails or pauses on cue.

    Steps: source_open, source_read, source_close, destination_open, destination_write,
    publication. `fail_on` selects which occurrence of `fail_at` raises `failure`.
    """

    def __init__(
        self,
        files: dict[str, bytes],
        *,
        fail_at: str | None = None,
        fail_on: int = 1,
        failure: Exception | None = None,
        hooks: dict[str, Callable[[], None]] | None = None,
    ) -> None:
        self.files = files
        self.fail_at = fail_at
        self.fail_on = fail_on
        self.failure = failure or DataBridgeError(ErrorCode.SFTP_UNAVAILABLE, "injected failure")
        self.hooks = hooks or {}
        self.seen: dict[str, int] = {}

    def step(self, name: str) -> None:
        self.seen[name] = self.seen.get(name, 0) + 1
        if name in self.hooks:
            self.hooks[name]()
        if name == self.fail_at and self.seen[name] == self.fail_on:
            raise self.failure

    def list_files(self) -> Listing:
        return Listing(sorted(self.files), False)

    def check_access(self) -> AccessCheck:
        return AccessCheck(writable=True)

    @contextmanager
    def read(self, filename: str) -> Iterator["_Source"]:
        self.step("source_open")
        yield _Source(self, self.files[filename])
        self.step("source_close")

    @contextmanager
    def write(self, filename: str, _staging_id: str, _overwrite: bool) -> Iterator["_Sink"]:
        self.step("destination_open")
        sink = _Sink(self)
        yield sink
        self.step("publication")
        self.files[filename] = bytes(sink.data)


class _Source:
    def __init__(self, connector: _Scripted, data: bytes) -> None:
        self.connector = connector
        self.data = data
        self.offset = 0

    def read(self, size: int, /) -> bytes:
        self.connector.step("source_read")
        chunk = self.data[self.offset : self.offset + size]
        self.offset += len(chunk)
        return chunk


class _Sink:
    def __init__(self, connector: _Scripted) -> None:
        self.connector = connector
        self.data = bytearray()

    def write(self, data: bytes, /) -> None:
        self.connector.step("destination_write")
        self.data += data


@pytest.fixture
def store(settings: Settings, tmp_path: Path) -> ConnectionStore:
    store = ConnectionStore(settings.database_path, settings.encryption_key)
    store.initialize()
    for name in ("memory_source", "memory_destination"):
        store.create(LocalConnection(name=name, type="local", path=str(tmp_path)))
    return store


def _service(
    store: ConnectionStore, source: _Scripted, destination: _Scripted, **options: object
) -> TransferService:
    def connector(connection: Connection) -> _Scripted:
        return source if connection.name == "memory_source" else destination

    return TransferService(store, connector, **options)  # type: ignore[arg-type]


def _request(target: str = "target.bin") -> TransferRequest:
    return TransferRequest.model_validate(
        _transfer("memory_source", "memory_destination", target=target)
    )


def test_local_transfer_bytes_status_collision_and_overwrite(
    settings: Settings, tmp_path: Path
) -> None:
    source = tmp_path / "source"
    target = tmp_path / "target"
    source.mkdir()
    target.mkdir()
    payload = bytes(range(256)) * (CHUNK_SIZE // 256) + b"last-byte"
    (source / "source.bin").write_bytes(payload)

    with client_for(settings) as client:
        _connection(client, "source", source)
        _connection(client, "target", target)
        completed = client.post("/transfers", json=_transfer("source", "target"))
        assert completed.status_code == 201
        record = completed.json()
        assert record["status"] == "completed"
        assert record["bytes_copied"] == len(payload)
        assert record["overwrite"] is False
        assert record["started_at"] and record["completed_at"] == record["updated_at"]
        assert record["failed_at"] is None and record["error_code"] is None
        assert client.get(f"/transfers/{record['id']}").json() == record
        assert (target / "target.bin").read_bytes() == payload

        collision = client.post("/transfers", json=_transfer("source", "target"))
        assert collision.status_code == 409
        error = collision.json()["error"]
        assert error["code"] == "DESTINATION_EXISTS"
        assert error["message"].startswith("Destination connection 'target': ")
        failed = client.get(f"/transfers/{error['transfer_id']}").json()
        assert failed["status"] == "failed" and failed["bytes_copied"] == 0
        assert failed["failure_phase"] == "destination_open"
        assert failed["error_code"] == "DESTINATION_EXISTS"
        assert failed["failed_at"] and failed["completed_at"] is None
        assert (target / "target.bin").read_bytes() == payload

        (source / "source.bin").write_bytes(b"replacement")
        overwritten = client.post("/transfers", json=_transfer("source", "target", overwrite=True))
        assert overwritten.status_code == 201
        assert overwritten.json()["overwrite"] is True
        assert overwritten.json()["bytes_copied"] == len(b"replacement")
        assert (target / "target.bin").read_bytes() == b"replacement"


def test_transfer_rejects_same_file_and_reports_missing_source(
    settings: Settings, tmp_path: Path
) -> None:
    with client_for(settings) as client:
        _connection(client, "files", tmp_path)
        same = client.post(
            "/transfers",
            json={
                "source": "files",
                "source_file": "source.bin",
                "destination": "files",
                "destination_file": "source.bin",
            },
        )
        missing = client.post("/transfers", json=_transfer("files", "files"))
        missing_record = client.get(f"/transfers/{missing.json()['error']['transfer_id']}").json()
        absent = client.get("/transfers/not-a-transfer")

    assert same.status_code == 400
    assert same.json()["error"]["code"] == "SAME_FILE"
    assert same.json()["error"]["transfer_id"] is None
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "FILE_NOT_FOUND"
    assert missing.json()["error"]["message"].startswith("Source connection 'files': ")
    assert missing_record["failure_phase"] == "source_open"
    assert absent.status_code == 404
    assert absent.json()["error"]["code"] == "TRANSFER_NOT_FOUND"


@pytest.mark.parametrize(
    ("side", "phase"), [("source", "source_lookup"), ("destination", "destination_lookup")]
)
def test_missing_connection_names_its_side(
    settings: Settings, tmp_path: Path, side: str, phase: str
) -> None:
    with client_for(settings) as client:
        _connection(client, "files", tmp_path)
        names = {"source": "files", "destination": "files", side: "nowhere"}
        response = client.post("/transfers", json=_transfer(names["source"], names["destination"]))
        record = client.get(f"/transfers/{response.json()['error']['transfer_id']}").json()

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "CONNECTION_NOT_FOUND"
    assert response.json()["error"]["message"] == f"The {side} connection 'nowhere' does not exist"
    assert record["failure_phase"] == phase


@pytest.mark.parametrize(
    ("side", "step"),
    [
        ("source", "source_open"),
        ("source", "source_read"),
        ("destination", "destination_open"),
        ("destination", "destination_write"),
        ("destination", "publication"),
    ],
)
def test_each_failure_records_its_phase_code_and_side(
    store: ConnectionStore, side: str, step: str
) -> None:
    source = _Scripted({"source.bin": PAYLOAD}, fail_at=step if side == "source" else None)
    destination = _Scripted({}, fail_at=step if side == "destination" else None)

    error = _error(lambda: _service(store, source, destination).run(_request()))

    assert error.code == ErrorCode.SFTP_UNAVAILABLE
    assert error.message == f"{side.capitalize()} connection 'memory_{side}': injected failure"
    assert error.transfer_id is not None
    record = store.get_transfer(error.transfer_id)
    assert (record.status, record.failure_phase, record.error_code) == (
        "failed",
        step,
        ErrorCode.SFTP_UNAVAILABLE,
    )
    assert "target.bin" not in destination.files


def test_contract_violation_after_a_chunk_is_classified_by_phase(store: ConnectionStore) -> None:
    source = _Scripted({"source.bin": PAYLOAD})
    destination = _Scripted(
        {"target.bin": b"original"},
        fail_at="destination_write",
        fail_on=2,
        failure=OSError("injected destination failure"),
    )

    error = _error(lambda: _service(store, source, destination).run(_request()))

    assert error.code == ErrorCode.DESTINATION_WRITE_FAILED
    assert error.transfer_id is not None
    record = store.get_transfer(error.transfer_id)
    assert (record.status, record.bytes_copied, record.failure_phase) == (
        "failed",
        CHUNK_SIZE,
        "destination_write",
    )
    assert destination.files["target.bin"] == b"original"


@pytest.mark.parametrize(
    ("seconds_per_reading", "expected"),
    [(2.0, [0, CHUNK_SIZE, 2 * CHUNK_SIZE]), (0.0, [0, 0, 0])],
    ids=["every-chunk-after-an-interval", "throttled"],
)
def test_progress_is_checkpointed_at_most_once_per_interval(
    store: ConnectionStore, seconds_per_reading: float, expected: list[int]
) -> None:
    readings = itertools.count(step=seconds_per_reading)
    persisted: list[int] = []
    source = _Scripted({"source.bin": b"x" * (3 * CHUNK_SIZE)})
    destination = _Scripted(
        {},
        hooks={
            "destination_write": lambda: persisted.append(
                store.list_transfers("running", 1)[0].bytes_copied
            )
        },
    )

    service = _service(store, source, destination, clock=lambda: next(readings))
    record = service.run(_request())

    assert persisted == expected
    assert record.bytes_copied == 3 * CHUNK_SIZE


def test_publishing_is_recorded_before_publication(store: ConnectionStore) -> None:
    observed = []
    destination = _Scripted(
        {}, hooks={"publication": lambda: observed.append(store.list_transfers(None, 1)[0])}
    )

    record = _service(store, _Scripted({"source.bin": PAYLOAD}), destination).run(_request())

    assert [(item.status, item.bytes_copied) for item in observed] == [("publishing", len(PAYLOAD))]
    assert (record.status, record.bytes_copied) == ("completed", len(PAYLOAD))


def test_unrecorded_completion_is_reported_and_left_publishing(
    store: ConnectionStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(transfer_id: str) -> None:
        raise DataBridgeError(ErrorCode.TRANSFER_RECORD_FAILED, "disk full", transfer_id)

    monkeypatch.setattr(store, "finish_transfer", refuse)
    destination = _Scripted({})

    error = _error(
        lambda: _service(store, _Scripted({"source.bin": PAYLOAD}), destination).run(_request())
    )

    assert error.code == ErrorCode.TRANSFER_RECORD_FAILED
    assert "was published" in error.message
    assert destination.files["target.bin"] == PAYLOAD
    assert error.transfer_id is not None
    assert store.get_transfer(error.transfer_id).status == "publishing"


def test_source_close_failure_after_publication_still_completes(store: ConnectionStore) -> None:
    source = _Scripted({"source.bin": PAYLOAD}, fail_at="source_close")
    destination = _Scripted({})

    record = _service(store, source, destination).run(_request())

    assert record.status == "completed"
    assert destination.files["target.bin"] == PAYLOAD


def test_capacity_limit_rejects_extra_transfers_without_a_record(store: ConnectionStore) -> None:
    copying = threading.Barrier(3, timeout=5)
    release = threading.Event()

    def hold() -> None:
        if not release.is_set():
            copying.wait()
            release.wait(timeout=5)

    source = _Scripted({"source.bin": b"x"})
    destination = _Scripted({}, hooks={"destination_write": hold})
    service = _service(store, source, destination, max_concurrent=2)
    workers = [
        threading.Thread(target=service.run, args=(_request(f"held-{index}.bin"),))
        for index in range(2)
    ]
    for worker in workers:
        worker.start()
    copying.wait()

    error = _error(lambda: service.run(_request("extra.bin")))
    records = store.list_transfers(None, 10)
    release.set()
    for worker in workers:
        worker.join(timeout=5)

    assert error.code == ErrorCode.TRANSFER_CAPACITY_EXCEEDED
    assert error.transfer_id is None
    assert len(records) == 2
    assert service.run(_request("after.bin")).status == "completed"


def test_startup_marks_interrupted_transfer_failed(settings: Settings) -> None:
    store = ConnectionStore(settings.database_path, settings.encryption_key)
    store.initialize()
    transfer_id = "00000000-0000-0000-0000-000000000001"
    store.start_transfer(transfer_id, TransferRequest.model_validate(_transfer("a", "b")))

    with client_for(settings) as client:
        record = client.get(f"/transfers/{transfer_id}").json()
    assert record["status"] == "failed"
    assert record["failure_phase"] == "interruption"
    assert record["error_code"] == "TRANSFER_INTERRUPTED"
    assert record["failed_at"]
    assert "stopped" in record["error"]


def test_empty_file_transfer(settings: Settings, tmp_path: Path) -> None:
    source = tmp_path / "source"
    target = tmp_path / "target"
    source.mkdir()
    target.mkdir()
    (source / "source.bin").touch()
    with client_for(settings) as client:
        _connection(client, "source", source)
        _connection(client, "target", target)
        response = client.post("/transfers", json=_transfer("source", "target"))
    assert response.status_code == 201
    assert response.json()["bytes_copied"] == 0
    assert (target / "target.bin").read_bytes() == b""


def test_transfers_are_listed_newest_first_and_filtered_by_status(
    settings: Settings, tmp_path: Path
) -> None:
    (tmp_path / "source.bin").write_bytes(b"data")
    with client_for(settings) as client:
        _connection(client, "files", tmp_path)
        assert client.post("/transfers", json=_transfer("files", "files")).status_code == 201
        assert client.post("/transfers", json=_transfer("files", "files")).status_code == 409

        listing = client.get("/transfers")
        completed = client.get("/transfers", params={"status": "completed"})
        limited = client.get("/transfers", params={"limit": 1})
        bad_status = client.get("/transfers", params={"status": "paused"})
        bad_limit = client.get("/transfers", params={"limit": 0})

    assert [record["status"] for record in listing.json()] == ["failed", "completed"]
    assert [record["status"] for record in completed.json()] == ["completed"]
    assert len(limited.json()) == 1
    assert bad_status.status_code == bad_limit.status_code == 422
