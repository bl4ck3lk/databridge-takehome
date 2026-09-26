"""Real SFTP tests against the Docker fixture; run `make integration` after `make quickstart`."""

import hashlib
import shutil
from pathlib import Path
from uuid import uuid4

import pytest
import sftp_fixture
from sftp_fixture import DATA_DIR, PROJECT_ROOT
from support import client_for, read_all

from databridge.config import Settings
from databridge.connectors import base
from databridge.connectors.base import CHUNK_SIZE
from databridge.connectors.sftp import SFTPConnector
from databridge.errors import DataBridgeError
from databridge.models import TransferRequest

pytestmark = pytest.mark.integration


@pytest.fixture
def trusted_settings(settings: Settings) -> Settings:
    shutil.copyfile(sftp_fixture.known_hosts(), settings.known_hosts_path)
    return settings


def test_sftp_localhost_alias_is_trusted(trusted_settings: Settings) -> None:
    with client_for(trusted_settings) as client:
        created = client.post(
            "/connections", json=sftp_fixture.connection("aliased", host="localhost")
        )
        listed = client.get("/connections/aliased/files")
    assert created.status_code == 201
    assert listed.status_code == 200


def test_sftp_roundtrip_collision_and_explicit_overwrite(trusted_settings: Settings) -> None:
    filename = f"integration-{uuid4().hex}.bin"
    host_file = DATA_DIR / filename
    try:
        with client_for(trusted_settings) as client:
            created = client.post("/connections", json=sftp_fixture.connection("remote_server"))
            assert created.status_code == 201
            assert client.get("/connections/remote_server/files").status_code == 200
            assert client.post("/connections/remote_server/healthcheck").json() == {
                "connection": "remote_server",
                "reachable": True,
                "writable": True,
            }

            connection = client.app.state.store.get("remote_server")
            connector = SFTPConnector(connection, trusted_settings.known_hosts_path)
            first_id = str(uuid4())
            with connector.write(filename, first_id, overwrite=False) as destination:
                destination.write(bytes(range(256)) * 16)
                assert filename not in connector.list_files().files
            with connector.read(filename) as source:
                assert read_all(source) == bytes(range(256)) * 16
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
                assert read_all(source) == b"replacement"
            with pytest.raises(ValueError, match="injected failure"):
                with connector.write(filename, str(uuid4()), overwrite=True) as destination:
                    destination.write(b"partial")
                    raise ValueError("injected failure")
            with connector.read(filename) as source:
                assert read_all(source) == b"replacement"
            assert not any(DATA_DIR.glob(".databridge-*.part"))
        with client_for(trusted_settings) as restarted:
            assert restarted.get("/connections/remote_server/files").status_code == 200
    finally:
        host_file.unlink(missing_ok=True)


def test_sftp_publish_time_collision_preserves_existing_file(trusted_settings: Settings) -> None:
    filename = f"collision-{uuid4().hex}.bin"
    host_file = DATA_DIR / filename
    try:
        with client_for(trusted_settings) as client:
            client.post("/connections", json=sftp_fixture.connection("remote_server"))
            connection = client.app.state.store.get("remote_server")
            connector = SFTPConnector(connection, trusted_settings.known_hosts_path)
            with pytest.raises(DataBridgeError) as collision:
                with connector.write(filename, str(uuid4()), overwrite=False) as destination:
                    destination.write(b"staged")
                    host_file.write_bytes(b"winner")
            assert collision.value.code == "DESTINATION_EXISTS"
            assert host_file.read_bytes() == b"winner"
            assert not any(DATA_DIR.glob(".databridge-*.part"))
    finally:
        host_file.unlink(missing_ok=True)


def test_sftp_failures_have_distinct_safe_codes(trusted_settings: Settings) -> None:
    with client_for(trusted_settings) as client:
        for request in (
            sftp_fixture.connection("bad_password", password="incorrect"),
            sftp_fixture.connection("bad_root", root="missing-directory"),
            sftp_fixture.connection("server_down", port=22345),
        ):
            assert client.post("/connections", json=request).status_code == 201
        assert client.get("/connections/bad_password/files").json()["error"]["code"] == (
            "SFTP_AUTH_FAILED"
        )
        assert (
            client.post("/connections/bad_password/healthcheck").json()["error"]["code"]
            == "SFTP_AUTH_FAILED"
        )
        assert client.get("/connections/bad_root/files").json()["error"]["code"] == (
            "CONNECTION_ROOT_UNAVAILABLE"
        )
        assert (
            client.post("/connections/bad_root/healthcheck").json()["error"]["code"]
            == "CONNECTION_ROOT_UNAVAILABLE"
        )
        assert client.get("/connections/server_down/files").json()["error"]["code"] == (
            "SFTP_UNAVAILABLE"
        )
        assert (
            client.post("/connections/server_down/healthcheck").json()["error"]["code"]
            == "SFTP_UNAVAILABLE"
        )

    untrusted = Settings(
        database_path=trusted_settings.database_path,
        encryption_key=trusted_settings.encryption_key,
        known_hosts_path=trusted_settings.known_hosts_path.parent / "missing_known_hosts",
    )
    with client_for(untrusted) as client:
        assert client.get("/connections/bad_root/files").json()["error"]["code"] == (
            "SFTP_HOST_KEY_REJECTED"
        )


def test_api_transfers_binary_both_directions_and_preserves_collision(
    trusted_settings: Settings, tmp_path: Path
) -> None:
    source_root = tmp_path / "source"
    target_root = tmp_path / "target"
    source_root.mkdir()
    target_root.mkdir()
    payload = bytes(range(256)) * (CHUNK_SIZE // 256) + b"tail"
    (source_root / "source.bin").write_bytes(payload)
    remote_name = f"api-{uuid4().hex}.bin"
    host_file = DATA_DIR / remote_name
    try:
        with client_for(trusted_settings) as client:
            assert (
                client.post(
                    "/connections",
                    json={"name": "source", "type": "local", "path": str(source_root)},
                ).status_code
                == 201
            )
            assert (
                client.post(
                    "/connections",
                    json={"name": "target", "type": "local", "path": str(target_root)},
                ).status_code
                == 201
            )
            assert (
                client.post("/connections", json=sftp_fixture.connection("remote")).status_code
                == 201
            )

            upload = {
                "source": "source",
                "source_file": "source.bin",
                "destination": "remote",
                "destination_file": remote_name,
            }
            uploaded = client.post("/transfers", json=upload)
            assert uploaded.status_code == 201
            assert uploaded.json()["bytes_copied"] == len(payload)
            assert uploaded.json()["status"] == "completed"
            assert (
                hashlib.sha256(host_file.read_bytes()).digest() == hashlib.sha256(payload).digest()
            )

            collision = client.post("/transfers", json=upload)
            assert collision.status_code == 409
            assert collision.json()["error"]["code"] == "DESTINATION_EXISTS"
            assert host_file.read_bytes() == payload

            downloaded = client.post(
                "/transfers",
                json={
                    "source": "remote",
                    "source_file": remote_name,
                    "destination": "target",
                    "destination_file": "download.bin",
                },
            )
            assert downloaded.status_code == 201
            assert downloaded.json()["bytes_copied"] == len(payload)
            assert (target_root / "download.bin").read_bytes() == payload
            assert client.get(f"/transfers/{downloaded.json()['id']}").json() == (downloaded.json())
    finally:
        host_file.unlink(missing_ok=True)


def test_api_failed_sftp_transfer_has_retrievable_record(
    trusted_settings: Settings, tmp_path: Path
) -> None:
    (tmp_path / "source.bin").write_bytes(b"contents")
    with client_for(trusted_settings) as client:
        assert (
            client.post(
                "/connections", json={"name": "source", "type": "local", "path": str(tmp_path)}
            ).status_code
            == 201
        )
        assert (
            client.post(
                "/connections", json=sftp_fixture.connection("server_down", port=22345)
            ).status_code
            == 201
        )
        failed = client.post(
            "/transfers",
            json={
                "source": "source",
                "source_file": "source.bin",
                "destination": "server_down",
                "destination_file": "unpublished.bin",
            },
        )
        assert failed.status_code == 503
        assert failed.json()["error"]["code"] == "SFTP_UNAVAILABLE"
        transfer_id = failed.json()["error"]["transfer_id"]
        assert transfer_id
        record = client.get(f"/transfers/{transfer_id}").json()
        assert record["status"] == "failed"
        assert record["failed_at"] and record["bytes_copied"] == 0


def test_sftp_preview_uses_shared_parser(trusted_settings: Settings) -> None:
    filename = f"preview-{uuid4().hex}.json"
    host_file = DATA_DIR / filename
    host_file.write_bytes((PROJECT_ROOT / "instructions" / "products.json").read_bytes())
    try:
        with client_for(trusted_settings) as client:
            assert (
                client.post("/connections", json=sftp_fixture.connection("remote")).status_code
                == 201
            )
            response = client.get(f"/connections/remote/files/{filename}/head?limit=2")
        assert response.status_code == 200
        assert len(response.json()["rows"]) == 2
        assert response.json()["schema"]["price"] == "float"
    finally:
        host_file.unlink(missing_ok=True)


def test_interrupted_transfer_stage_stays_hidden_after_restart(
    trusted_settings: Settings,
) -> None:
    transfer_id = str(uuid4())
    destination_name = f"interrupted-{uuid4().hex}.bin"
    stage_name = f".databridge-{transfer_id}.part"
    stage_file = DATA_DIR / stage_name
    try:
        with client_for(trusted_settings) as client:
            assert (
                client.post("/connections", json=sftp_fixture.connection("remote")).status_code
                == 201
            )
            client.app.state.store.start_transfer(
                transfer_id,
                TransferRequest(
                    source="remote",
                    source_file="source.bin",
                    destination="remote",
                    destination_file=destination_name,
                ),
            )
        stage_file.write_bytes(b"unpublished")
        with client_for(trusted_settings) as restarted:
            record = restarted.get(f"/transfers/{transfer_id}").json()
            listing = restarted.get("/connections/remote/files").json()
        assert record["status"] == "failed"
        assert record["failure_phase"] == "interruption"
        assert stage_name not in listing["files"]
        assert not (DATA_DIR / destination_name).exists()
    finally:
        stage_file.unlink(missing_ok=True)


def test_sftp_listing_reports_result_and_scan_caps(
    trusted_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    names = [f"listing-{uuid4().hex}.bin" for _ in range(3)]
    files = [DATA_DIR / name for name in names]
    try:
        for file in files:
            file.write_bytes(b"x")
        with client_for(trusted_settings) as client:
            client.post("/connections", json=sftp_fixture.connection("remote"))
            monkeypatch.setattr(base, "MAX_LIST_RESULTS", 2)
            result_cap = client.get("/connections/remote/files").json()
            assert result_cap["truncated"] is True
            assert len(result_cap["files"]) == 2
            monkeypatch.setattr(base, "MAX_LIST_SCAN", 1)
            scan_cap = client.get("/connections/remote/files").json()
            assert scan_cap["truncated"] is True
            assert len(scan_cap["files"]) <= 1
    finally:
        for file in files:
            file.unlink(missing_ok=True)


def test_sftp_healthcheck_reports_unreadable_root(trusted_settings: Settings) -> None:
    dirname = f"blocked-{uuid4().hex}"
    host_dir = DATA_DIR / dirname
    host_dir.mkdir()
    host_dir.chmod(0)
    try:
        with client_for(trusted_settings) as client:
            assert (
                client.post(
                    "/connections", json=sftp_fixture.connection("blocked", root=f"data/{dirname}")
                ).status_code
                == 201
            )
            response = client.post("/connections/blocked/healthcheck")
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "CONNECTION_ROOT_UNAVAILABLE"
    finally:
        host_dir.chmod(0o755)
        host_dir.rmdir()
