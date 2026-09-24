"""Real SFTP tests; run with the documented Docker fixture and known-hosts bootstrap."""

import shutil
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from databridge.api import create_app
from databridge.config import Settings
from databridge.connectors.sftp import SFTPConnector
from databridge.errors import DataBridgeError

pytestmark = pytest.mark.integration
PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def trusted_settings(settings: Settings) -> Settings:
    trust_file = PROJECT_ROOT / "known_hosts"
    assert trust_file.is_file(), (
        "Run ssh-keyscan bootstrap from the README before integration tests"
    )
    shutil.copyfile(trust_file, settings.known_hosts_path)
    return settings


def _request(name: str, *, password: str = "testpass", root: str = "data", port: int = 2222):
    return {
        "name": name,
        "type": "sftp",
        "host": "127.0.0.1",
        "port": port,
        "username": "testuser",
        "password": password,
        "root": root,
    }


def test_sftp_roundtrip_collision_and_explicit_overwrite(trusted_settings: Settings) -> None:
    filename = f"integration-{uuid4().hex}.bin"
    host_file = PROJECT_ROOT / "sftp_data" / filename
    try:
        with TestClient(create_app(trusted_settings)) as client:
            created = client.post("/connections", json=_request("remote_server"))
            assert created.status_code == 201
            assert client.get("/connections/remote_server/files").status_code == 200

            connection = client.app.state.store.get("remote_server")
            connector = SFTPConnector(connection, trusted_settings.known_hosts_path)
            first_id = str(uuid4())
            with connector.write(filename, first_id, overwrite=False) as destination:
                destination.write(bytes(range(256)) * 16)
                assert filename not in connector.list_files().files
            with connector.read(filename) as source:
                assert source.read() == bytes(range(256)) * 16
            assert filename in connector.list_files().files

            with pytest.raises(DataBridgeError) as missing:
                with connector.read(f"missing-{uuid4().hex}.bin"):
                    pass
            assert missing.value.code == "FILE_NOT_FOUND"

            with pytest.raises(DataBridgeError) as collision:
                with connector.write(filename, str(uuid4()), overwrite=False):
                    pass
            assert collision.value.code == "DESTINATION_EXISTS"

            with connector.write(filename, str(uuid4()), overwrite=True) as destination:
                destination.write(b"replacement")
            with connector.read(filename) as source:
                assert source.read() == b"replacement"
            with pytest.raises(ValueError, match="injected failure"):
                with connector.write(filename, str(uuid4()), overwrite=True) as destination:
                    destination.write(b"partial")
                    raise ValueError("injected failure")
            with connector.read(filename) as source:
                assert source.read() == b"replacement"
            assert not any((PROJECT_ROOT / "sftp_data").glob(f".{filename}.databridge-*.part"))
        with TestClient(create_app(trusted_settings)) as restarted:
            assert restarted.get("/connections/remote_server/files").status_code == 200
    finally:
        host_file.unlink(missing_ok=True)


def test_sftp_publish_time_collision_preserves_existing_file(trusted_settings: Settings) -> None:
    filename = f"collision-{uuid4().hex}.bin"
    host_file = PROJECT_ROOT / "sftp_data" / filename
    try:
        with TestClient(create_app(trusted_settings)) as client:
            client.post("/connections", json=_request("remote_server"))
            connection = client.app.state.store.get("remote_server")
            connector = SFTPConnector(connection, trusted_settings.known_hosts_path)
            with pytest.raises(DataBridgeError) as collision:
                with connector.write(filename, str(uuid4()), overwrite=False) as destination:
                    destination.write(b"staged")
                    host_file.write_bytes(b"winner")
            assert collision.value.code == "DESTINATION_EXISTS"
            assert host_file.read_bytes() == b"winner"
            assert not any((PROJECT_ROOT / "sftp_data").glob(f".{filename}.databridge-*.part"))
    finally:
        host_file.unlink(missing_ok=True)


def test_sftp_failures_have_distinct_safe_codes(trusted_settings: Settings) -> None:
    with TestClient(create_app(trusted_settings)) as client:
        for request in (
            _request("bad_password", password="incorrect"),
            _request("bad_root", root="missing-directory"),
            _request("server_down", port=22345),
        ):
            assert client.post("/connections", json=request).status_code == 201
        assert client.get("/connections/bad_password/files").json()["error"]["code"] == (
            "SFTP_AUTH_FAILED"
        )
        assert client.get("/connections/bad_root/files").json()["error"]["code"] == (
            "CONNECTION_ROOT_UNAVAILABLE"
        )
        assert client.get("/connections/server_down/files").json()["error"]["code"] == (
            "SFTP_UNAVAILABLE"
        )

    untrusted = Settings(
        database_path=trusted_settings.database_path,
        encryption_key=trusted_settings.encryption_key,
        known_hosts_path=trusted_settings.known_hosts_path.parent / "missing_known_hosts",
    )
    with TestClient(create_app(untrusted)) as client:
        assert client.get("/connections/bad_root/files").json()["error"]["code"] == (
            "SFTP_HOST_KEY_REJECTED"
        )
