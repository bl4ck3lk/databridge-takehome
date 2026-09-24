import json
import stat
from pathlib import Path

from fastapi.testclient import TestClient

from databridge.api import create_app
from databridge.config import Settings


def test_openapi_is_served(settings: Settings) -> None:
    with TestClient(create_app(settings)) as client:
        response = client.get("/openapi.json")

    assert response.status_code == 200
    document = response.json()
    assert document["info"]["title"] == "DataBridge"
    assert {
        "/connections",
        "/connections/{name}",
        "/connections/{name}/files",
        "/connections/{name}/files/{filename}/head",
        "/connections/{name}/healthcheck",
        "/transfers",
        "/transfers/{transfer_id}",
    } <= set(document["paths"])
    assert "password" not in document["components"]["schemas"]["SFTPConnectionView"]["properties"]
    assert "password" in document["components"]["schemas"]["SFTPConnection"]["properties"]


def test_request_log_records_error_params_without_secrets(
    settings: Settings, tmp_path: Path
) -> None:
    invalid_path = tmp_path / "file.txt"
    invalid_path.write_text("content")
    password = "do-not-log-this-password"
    with TestClient(create_app(settings)) as client:
        failed = client.post(
            "/connections",
            json={"name": "bad", "type": "local", "path": str(invalid_path)},
        )
        validated = client.post(
            "/connections",
            json={
                "name": "remote",
                "type": "sftp",
                "host": "127.0.0.1",
                "port": -1,
                "username": "testuser",
                "password": password,
                "root": "data",
            },
        )

    log_path = settings.database_path.with_suffix(".requests.jsonl")
    events = [json.loads(line) for line in log_path.read_text().splitlines()]
    assert failed.status_code == 400
    assert validated.status_code == 422
    assert failed.headers["X-Request-ID"] == events[0]["request_id"]
    assert events[0]["route"] == "/connections"
    assert events[0]["body_params"]["path"] == str(invalid_path)
    assert events[0]["error_code"] == "INVALID_CONNECTION_SETTINGS"
    assert events[1]["body_params"]["host"] == "127.0.0.1"
    assert events[1]["error_code"] == "INVALID_REQUEST"
    assert password not in log_path.read_text()
    assert stat.S_IMODE(log_path.stat().st_mode) == 0o600
