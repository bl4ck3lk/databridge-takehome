import sqlite3
from pathlib import Path

import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient
from support import client_for

from databridge.api import create_app
from databridge.config import Settings
from databridge.connectors import local
from databridge.connectors.local import LocalConnector
from databridge.errors import DataBridgeError


def test_local_connection_persists_and_lists_files(settings: Settings, tmp_path: Path) -> None:
    root = tmp_path / "files"
    root.mkdir()
    (root / "customers.csv").write_text("id\n1\n")
    (root / "subdirectory").mkdir()
    with client_for(settings) as client:
        created = client.post(
            "/connections", json={"name": "local_data", "type": "local", "path": str(root)}
        )
        listed = client.get("/connections/local_data/files")
        health = client.post("/connections/local_data/healthcheck")

    assert created.status_code == 201
    assert created.json() == {"name": "local_data", "type": "local", "path": str(root)}
    assert listed.json() == {
        "connection": "local_data",
        "files": ["customers.csv"],
        "truncated": False,
    }
    assert health.json() == {"connection": "local_data", "reachable": True}
    with client_for(settings) as client:
        assert client.get("/connections/local_data").json() == created.json()


def test_encrypted_sftp_password_is_never_in_read_responses(settings: Settings) -> None:
    password = "unique-test-password"
    request = {
        "name": "remote_server",
        "type": "sftp",
        "host": "127.0.0.1",
        "port": 2222,
        "username": "testuser",
        "password": password,
        "root": "data",
    }
    with client_for(settings) as client:
        created = client.post("/connections", json=request)
        fetched = client.get("/connections/remote_server")
        listed = client.get("/connections")

    assert created.status_code == 201
    assert "password" not in created.json()
    assert fetched.json() == created.json()
    assert listed.json() == [created.json()]

    with sqlite3.connect(settings.database_path) as database:
        row = database.execute(
            "SELECT settings_json, password_ciphertext FROM connections WHERE name = ?",
            ("remote_server",),
        ).fetchone()
    assert password not in row[0]
    assert password not in row[1]
    assert row[1] != password

    with client_for(settings) as client:
        assert client.get("/connections/remote_server").json() == created.json()
        assert client.app.state.store.get("remote_server").password.get_secret_value() == password


def test_wrong_key_fails_startup_even_without_sftp_connections(settings: Settings) -> None:
    with client_for(settings):
        pass

    wrong = Settings(
        database_path=settings.database_path,
        encryption_key=Fernet.generate_key(),
        known_hosts_path=settings.known_hosts_path,
    )
    with pytest.raises(RuntimeError, match="does not match the database"):
        with client_for(wrong):
            pass


def test_missing_key_fails_at_startup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABRIDGE_ENCRYPTION_KEY", raising=False)
    with pytest.raises(RuntimeError, match="DATABRIDGE_ENCRYPTION_KEY is required"):
        with TestClient(create_app()):
            pass


def test_connection_errors_are_structured(settings: Settings, tmp_path: Path) -> None:
    not_a_directory = tmp_path / "file.txt"
    not_a_directory.write_text("content")
    with client_for(settings) as client:
        missing = client.get("/connections/no_such_connection")
        invalid_path = client.post(
            "/connections",
            json={"name": "invalid", "type": "local", "path": str(not_a_directory)},
        )
        invalid_shape = client.post(
            "/connections", json={"name": "not valid", "type": "local", "path": str(tmp_path)}
        )
        valid = client.post(
            "/connections", json={"name": "local_data", "type": "local", "path": str(tmp_path)}
        )
        duplicate = client.post(
            "/connections", json={"name": "local_data", "type": "local", "path": str(tmp_path)}
        )

    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "CONNECTION_NOT_FOUND"
    assert invalid_path.status_code == 400
    assert invalid_path.json()["error"]["code"] == "INVALID_CONNECTION_SETTINGS"
    assert invalid_shape.status_code == 422
    assert invalid_shape.json()["error"]["code"] == "INVALID_REQUEST"
    assert "name" in invalid_shape.json()["error"]["message"]
    assert valid.status_code == 201
    assert duplicate.status_code == 409
    assert duplicate.json()["error"]["code"] == "CONNECTION_EXISTS"


def test_local_connection_creates_missing_directory(settings: Settings, tmp_path: Path) -> None:
    root = tmp_path / "new" / "nested"
    with client_for(settings) as client:
        created = client.post(
            "/connections", json={"name": "output", "type": "local", "path": str(root)}
        )

    assert created.status_code == 201
    assert root.is_dir()
    assert created.json()["path"] == str(root)


def test_local_write_is_staged_and_preserves_destination_on_failure(tmp_path: Path) -> None:
    connector = LocalConnector(tmp_path)
    transfer_id = "00000000-0000-0000-0000-000000000001"
    with connector.write("file.bin", transfer_id, overwrite=False) as destination:
        destination.write(b"original")
    assert (tmp_path / "file.bin").read_bytes() == b"original"
    with connector.read("file.bin") as source:
        assert source.read() == b"original"

    with pytest.raises(DataBridgeError) as collision:
        with connector.write("file.bin", transfer_id, overwrite=False):
            pass
    assert collision.value.code == "DESTINATION_EXISTS"

    with pytest.raises(ValueError, match="injected failure"):
        with connector.write("file.bin", transfer_id, overwrite=True) as destination:
            destination.write(b"partial")
            raise ValueError("injected failure")
    assert (tmp_path / "file.bin").read_bytes() == b"original"
    assert list(tmp_path.glob(".*.part")) == []


def test_local_connector_rejects_path_escape_and_reserved_stage_name(tmp_path: Path) -> None:
    connector = LocalConnector(tmp_path)
    outside = tmp_path.parent / "outside.bin"
    outside.write_bytes(b"outside")
    (tmp_path / "escape.bin").symlink_to(outside)
    for name in (
        "../outside.bin",
        "escape.bin",
        ".x.databridge-00000000-0000-0000-0000-000000000001.part",
    ):
        with pytest.raises(DataBridgeError) as error:
            with connector.read(name):
                pass
        assert error.value.code == "INVALID_FILENAME"


def test_local_listing_reports_result_and_scan_caps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connector = LocalConnector(tmp_path)
    for number in range(1_000):
        (tmp_path / f"file-{number:04}.bin").touch()
    assert len(connector.list_files().files) == 1_000
    assert connector.list_files().truncated is False

    (tmp_path / "extra.bin").touch()
    listing = connector.list_files()
    assert len(listing.files) == 1_000
    assert listing.truncated is True

    monkeypatch.setattr(local, "MAX_LIST_SCAN", 2)
    listing = connector.list_files()
    assert listing.truncated is True
    assert len(listing.files) <= 2
