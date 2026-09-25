import json
import stat
from pathlib import Path

from fastapi.testclient import TestClient

from databridge.api import create_app
from databridge.config import Settings
from databridge.models import ErrorResponse


def test_openapi_is_served(settings: Settings) -> None:
    with TestClient(create_app(settings)) as client:
        response = client.get("/openapi.json")
        invalid = client.post("/transfers", json={})

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
    connection_body = document["paths"]["/connections"]["post"]["requestBody"]["content"][
        "application/json"
    ]
    assert connection_body["schema"]["discriminator"]["propertyName"] == "type"
    assert connection_body["examples"]["local_data"]["value"] == {
        "name": "local_data",
        "type": "local",
        "path": "data",
    }
    assert connection_body["examples"]["local_output"]["value"]["path"] == "output"
    assert connection_body["examples"]["sftp"]["value"]["root"] == "data"
    transfer_post = document["paths"]["/transfers"]["post"]
    transfer_body = transfer_post["requestBody"]["content"]["application/json"]
    assert transfer_body["examples"]["upload"]["value"] == {
        "source": "local_data",
        "source_file": "customers.csv",
        "destination": "remote_server",
        "destination_file": "customers.csv",
        "overwrite": False,
    }
    assert transfer_body["examples"]["download"]["value"] == {
        "source": "remote_server",
        "source_file": "customers.csv",
        "destination": "local_output",
        "destination_file": "downloaded.csv",
        "overwrite": False,
    }
    created = transfer_post["responses"]["201"]["content"]["application/json"]
    assert created["schema"]["$ref"] == "#/components/schemas/TransferRecord"
    assert created["example"]["status"] == "completed"
    assert transfer_post["responses"]["422"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/ErrorResponse"
    }
    assert transfer_post["responses"]["409"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/ErrorResponse"
    }
    assert "HTTPValidationError" not in document["components"]["schemas"]
    assert invalid.status_code == 422
    assert ErrorResponse.model_validate(invalid.json()).error.code == "INVALID_REQUEST"


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
