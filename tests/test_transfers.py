"""Transfer behavior and persistence without requiring a network fixture."""

import io
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Iterator

import pytest
from fastapi.testclient import TestClient

from databridge.api import create_app
from databridge.config import Settings
from databridge.connectors.base import CHUNK_SIZE, Listing
from databridge.errors import DataBridgeError
from databridge.models import LocalConnection, TransferRequest
from databridge.store import ConnectionStore
from databridge.transfer import TransferService


def _connection(client: TestClient, name: str, path: Path) -> None:
    response = client.post("/connections", json={"name": name, "type": "local", "path": str(path)})
    assert response.status_code == 201


def _transfer(source: str, destination: str, *, overwrite: bool = False) -> dict[str, object]:
    return {
        "source": source,
        "source_file": "source.bin",
        "destination": destination,
        "destination_file": "target.bin",
        "overwrite": overwrite,
    }


def test_local_transfer_bytes_status_collision_and_overwrite(
    settings: Settings, tmp_path: Path
) -> None:
    source = tmp_path / "source"
    target = tmp_path / "target"
    source.mkdir()
    target.mkdir()
    payload = bytes(range(256)) * (CHUNK_SIZE // 256) + b"last-byte"
    (source / "source.bin").write_bytes(payload)

    with TestClient(create_app(settings)) as client:
        _connection(client, "source", source)
        _connection(client, "target", target)
        completed = client.post("/transfers", json=_transfer("source", "target"))
        assert completed.status_code == 201
        record = completed.json()
        assert record["status"] == "completed"
        assert record["bytes_copied"] == len(payload)
        assert record["started_at"] and record["completed_at"]
        assert record["failed_at"] is None
        assert client.get(f"/transfers/{record['id']}").json() == record
        assert (target / "target.bin").read_bytes() == payload

        collision = client.post("/transfers", json=_transfer("source", "target"))
        assert collision.status_code == 409
        assert collision.json()["error"]["code"] == "DESTINATION_EXISTS"
        failed_id = collision.json()["error"]["transfer_id"]
        failed = client.get(f"/transfers/{failed_id}").json()
        assert failed["status"] == "failed" and failed["bytes_copied"] == 0
        assert failed["failed_at"] and failed["completed_at"] is None
        assert (target / "target.bin").read_bytes() == payload

        (source / "source.bin").write_bytes(b"replacement")
        overwritten = client.post("/transfers", json=_transfer("source", "target", overwrite=True))
        assert overwritten.status_code == 201
        assert overwritten.json()["bytes_copied"] == len(b"replacement")
        assert (target / "target.bin").read_bytes() == b"replacement"


def test_transfer_rejects_same_file_and_reports_missing_source(
    settings: Settings, tmp_path: Path
) -> None:
    with TestClient(create_app(settings)) as client:
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
        absent = client.get("/transfers/not-a-transfer")

    assert same.status_code == 400
    assert same.json()["error"]["code"] == "SAME_FILE"
    assert same.json()["error"]["transfer_id"] is None
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "FILE_NOT_FOUND"
    assert missing.json()["error"]["transfer_id"]
    assert absent.status_code == 404
    assert absent.json()["error"]["code"] == "TRANSFER_NOT_FOUND"


class _FailingWriter(io.BytesIO):
    def __init__(self) -> None:
        super().__init__()
        self.writes = 0

    def write(self, data: bytes) -> int:
        self.writes += 1
        if self.writes == 2:
            raise OSError("injected destination failure")
        return super().write(data)


class _MemoryConnector:
    """A third connector shape, intentionally unrelated to either production backend."""

    def __init__(self, files: dict[str, bytes], *, fail_second_write: bool = False) -> None:
        self.files = files
        self.fail_second_write = fail_second_write

    def list_files(self) -> Listing:
        return Listing(sorted(self.files), False)

    @contextmanager
    def read(self, filename: str) -> Iterator[BinaryIO]:
        with io.BytesIO(self.files[filename]) as stream:
            yield stream

    @contextmanager
    def write(self, filename: str, _transfer_id: str, _overwrite: bool) -> Iterator[BinaryIO]:
        with _FailingWriter() if self.fail_second_write else io.BytesIO() as stage:
            yield stage
            self.files[filename] = stage.getvalue()


def test_third_connector_fails_after_chunk_without_publishing(
    settings: Settings, tmp_path: Path
) -> None:
    store = ConnectionStore(settings.database_path, settings.encryption_key)
    store.initialize()
    store.create(LocalConnection(name="memory_source", type="local", path=str(tmp_path)))
    store.create(LocalConnection(name="memory_target", type="local", path=str(tmp_path)))
    payload = b"x" * (CHUNK_SIZE + 1)
    source = _MemoryConnector({"source.bin": payload})
    target = _MemoryConnector({"target.bin": b"original"}, fail_second_write=True)
    service = TransferService(
        store, lambda connection: source if connection.name == "memory_source" else target
    )

    with pytest.raises(DataBridgeError) as failure:
        service.run(TransferRequest.model_validate(_transfer("memory_source", "memory_target")))
    assert failure.value.code == "DESTINATION_WRITE_FAILED"
    transfer_id = failure.value.transfer_id
    assert transfer_id is not None

    record = store.get_transfer(transfer_id)
    assert record.status == "failed"
    assert record.bytes_copied == CHUNK_SIZE
    assert record.failure_phase == "destination_write"
    assert target.files["target.bin"] == b"original"


def test_startup_marks_interrupted_transfer_failed(settings: Settings, tmp_path: Path) -> None:
    store = ConnectionStore(settings.database_path, settings.encryption_key)
    store.initialize()
    transfer_id = "00000000-0000-0000-0000-000000000001"
    request = TransferRequest.model_validate(_transfer("source", "destination"))
    store.start_transfer(transfer_id, request)

    with TestClient(create_app(settings)) as client:
        record = client.get(f"/transfers/{transfer_id}").json()
    assert record["status"] == "failed"
    assert record["failure_phase"] == "interruption"
    assert record["failed_at"]
    assert "stopped" in record["error"]


def test_empty_file_transfer(settings: Settings, tmp_path: Path) -> None:
    source = tmp_path / "source"
    target = tmp_path / "target"
    source.mkdir()
    target.mkdir()
    (source / "source.bin").touch()
    with TestClient(create_app(settings)) as client:
        _connection(client, "source", source)
        _connection(client, "target", target)
        response = client.post("/transfers", json=_transfer("source", "target"))
    assert response.status_code == 201
    assert response.json()["bytes_copied"] == 0
    assert (target / "target.bin").read_bytes() == b""
