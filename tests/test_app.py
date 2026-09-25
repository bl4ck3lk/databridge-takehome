"""HTTP boundary: error envelope, request identity, request log, OpenAPI, and browser guard."""

import json
import stat
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from support import BASE_URL, client_for, request_events

from databridge.api import create_app
from databridge.config import Settings
from databridge.errors import DataBridgeError, ErrorCode
from databridge.models import ErrorResponse
from databridge.request_log import RequestLog

GLOBAL_STATUSES = {"400", "500"}
UNSAFE_STATUSES = {"403"}
EXPECTED_STATUSES = {
    ("post", "/connections"): {"201", "400", "409", "422"} | GLOBAL_STATUSES | UNSAFE_STATUSES,
    ("get", "/connections"): {"200"} | GLOBAL_STATUSES,
    ("get", "/connections/{name}"): {"200", "404"} | GLOBAL_STATUSES,
    ("put", "/connections/{name}"): (
        {"200", "400", "404", "422"} | GLOBAL_STATUSES | UNSAFE_STATUSES
    ),
    ("delete", "/connections/{name}"): {"204", "404"} | GLOBAL_STATUSES | UNSAFE_STATUSES,
    ("get", "/connections/{name}/files"): {"200", "404", "502", "503"} | GLOBAL_STATUSES,
    ("post", "/connections/{name}/healthcheck"): (
        {"200", "404", "502", "503"} | GLOBAL_STATUSES | UNSAFE_STATUSES
    ),
    ("get", "/connections/{name}/files/{filename}/head"): (
        {"200", "404", "409", "422", "502", "503"} | GLOBAL_STATUSES
    ),
    ("post", "/transfers"): (
        {"201", "404", "409", "422", "502", "503", "507"} | GLOBAL_STATUSES | UNSAFE_STATUSES
    ),
    ("get", "/transfers"): {"200", "422"} | GLOBAL_STATUSES,
    ("get", "/transfers/{transfer_id}"): {"200", "404"} | GLOBAL_STATUSES,
}


def _local(name: str, path: Path) -> dict[str, str]:
    return {"name": name, "type": "local", "path": str(path)}


def test_every_error_code_has_an_http_error_status() -> None:
    assert all(400 <= code.http_status < 600 for code in ErrorCode)


def test_unhandled_exception_returns_error_envelope_with_request_id(settings: Settings) -> None:
    app = create_app(settings)

    def explode() -> None:
        raise RuntimeError("internal detail that must not leak")

    app.add_api_route("/explode", explode)
    with TestClient(app, base_url=BASE_URL) as client:
        response = client.get("/explode")

    assert response.status_code == 500
    assert response.headers["content-type"] == "application/json"
    assert ErrorResponse.model_validate(response.json()).error.code == ErrorCode.INTERNAL_ERROR
    assert "internal detail" not in response.text
    event = request_events(settings)[-1]
    assert response.headers["X-Request-ID"] == event["request_id"]
    assert event["status_code"] == 500
    assert event["error_code"] == "INTERNAL_ERROR"
    assert event["error_type"] == "RuntimeError"


def test_domain_error_status_follows_its_code(settings: Settings) -> None:
    app = create_app(settings)

    def unavailable() -> None:
        raise DataBridgeError(ErrorCode.CONNECTION_ROOT_UNAVAILABLE, "Root is unavailable")

    app.add_api_route("/unavailable", unavailable)
    with TestClient(app, base_url=BASE_URL) as client:
        response = client.get("/unavailable")

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "CONNECTION_ROOT_UNAVAILABLE"


def test_unknown_route_and_method_use_error_envelope(settings: Settings) -> None:
    with client_for(settings) as client:
        missing = client.get("/no-such-route")
        wrong_method = client.delete("/transfers")

    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "ROUTE_NOT_FOUND"
    assert missing.headers["X-Request-ID"]
    assert wrong_method.status_code == 405
    assert wrong_method.json()["error"]["code"] == "METHOD_NOT_ALLOWED"
    assert "POST" in wrong_method.headers["allow"]


def test_validation_error_lists_every_problem_with_its_constraint(settings: Settings) -> None:
    with client_for(settings) as client:
        response = client.post(
            "/connections",
            json={
                "name": "remote",
                "type": "sftp",
                "host": "127.0.0.1",
                "port": 0,
                "password": "p",
            },
        )

    assert response.status_code == 422
    error = response.json()["error"]
    problems = {detail["field"]: detail["problem"] for detail in error["details"]}
    assert problems["username"] == "is required"
    assert problems["root"] == "is required"
    assert problems["port"] == "must be greater than 0"
    assert "username is required" in error["message"] and "root is required" in error["message"]


def test_validation_error_names_the_supported_connection_types(settings: Settings) -> None:
    with client_for(settings) as client:
        response = client.post("/connections", json={"name": "x", "type": "ftp"})

    detail = response.json()["error"]["details"][0]
    assert detail["field"] == "type"
    assert "local" in detail["problem"] and "sftp" in detail["problem"]


def test_malformed_json_is_reported_as_such(settings: Settings) -> None:
    with client_for(settings) as client:
        response = client.post(
            "/connections", content=b"{bad", headers={"Content-Type": "application/json"}
        )

    assert response.status_code == 422
    assert response.json()["error"]["details"] == [
        {"field": "request body", "problem": "is not valid JSON"}
    ]


def test_request_log_records_error_params_without_secrets(
    settings: Settings, tmp_path: Path
) -> None:
    invalid_path = tmp_path / "file.txt"
    invalid_path.write_text("content")
    password = "do-not-log-this-password"
    with client_for(settings) as client:
        failed = client.post("/connections", json=_local("bad", invalid_path))
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

    events = request_events(settings)
    assert failed.status_code == 400
    assert validated.status_code == 422
    assert failed.headers["X-Request-ID"] == events[0]["request_id"]
    assert events[0]["route"] == "/connections"
    assert events[0]["body_params"]["path"] == str(invalid_path)
    assert events[0]["error_code"] == "INVALID_CONNECTION_SETTINGS"
    assert events[1]["body_params"]["host"] == "127.0.0.1"
    assert events[1]["error_code"] == "INVALID_REQUEST"
    assert password not in settings.request_log_path.read_text()
    assert stat.S_IMODE(settings.request_log_path.stat().st_mode) == 0o600


def test_request_log_rotates_and_keeps_every_file_private(tmp_path: Path) -> None:
    path = tmp_path / "service.requests.jsonl"
    log = RequestLog(path, max_bytes=200, backups=2)
    log.open()
    try:
        for number in range(40):
            log.write({"request_id": f"request-{number:04}", "status_code": 200})
    finally:
        log.close()

    files = sorted(tmp_path.glob("service.requests.jsonl*"))
    assert [file.name for file in files] == [
        "service.requests.jsonl",
        "service.requests.jsonl.1",
        "service.requests.jsonl.2",
    ]
    assert all(stat.S_IMODE(file.stat().st_mode) == 0o600 for file in files)
    assert all(file.stat().st_size <= 200 for file in files)
    newest = [json.loads(line) for line in path.read_text().splitlines()]
    assert newest[-1]["request_id"] == "request-0039"


def test_openapi_documents_reachable_statuses_and_error_codes(settings: Settings) -> None:
    with client_for(settings) as client:
        document = client.get("/openapi.json").json()

    documented = {
        (method, path): set(operation["responses"])
        for path, item in document["paths"].items()
        for method, operation in item.items()
    }
    assert documented == EXPECTED_STATUSES
    schemas = document["components"]["schemas"]
    assert "HTTPValidationError" not in schemas
    assert set(schemas["ErrorCode"]["enum"]) == {code.value for code in ErrorCode}
    assert "password" not in schemas["SFTPConnectionView"]["properties"]
    connection_body = document["paths"]["/connections"]["post"]["requestBody"]["content"][
        "application/json"
    ]
    assert connection_body["schema"]["discriminator"]["propertyName"] == "type"
    assert connection_body["examples"]["local_data"]["value"] == {
        "name": "local_data",
        "type": "local",
        "path": "data",
    }
    assert connection_body["examples"]["sftp"]["value"]["root"] == "data"
    transfer_post = document["paths"]["/transfers"]["post"]
    examples = transfer_post["requestBody"]["content"]["application/json"]["examples"]
    assert examples["upload"]["value"]["destination"] == "remote_server"
    assert examples["download"]["value"]["source"] == "remote_server"
    created = transfer_post["responses"]["201"]["content"]["application/json"]
    assert created["schema"]["$ref"] == "#/components/schemas/TransferRecord"
    assert created["example"]["status"] == "completed"
    record = schemas["TransferRecord"]["properties"]
    for timestamp in ("started_at", "updated_at"):
        assert record[timestamp]["format"] == "date-time"
    for timestamp in ("completed_at", "failed_at"):
        assert {"type": "string", "format": "date-time"} in record[timestamp]["anyOf"]
    assert set(record["status"]["enum"]) == {"running", "publishing", "completed", "failed"}
    assert set(created["example"]) == set(record)


@pytest.mark.parametrize(
    "base_url", ["http://127.0.0.1:8080", "http://localhost:8080", "http://[::1]:8080"]
)
def test_loopback_host_names_are_served(settings: Settings, base_url: str) -> None:
    with TestClient(create_app(settings), base_url=base_url) as client:
        assert client.get("/connections").status_code == 200


def test_foreign_host_header_is_rejected_before_any_side_effect(
    settings: Settings, tmp_path: Path
) -> None:
    target = tmp_path / "rebound"
    with TestClient(create_app(settings), base_url="http://attacker.example:8080") as client:
        response = client.post("/connections", json=_local("rebound", target))
        listing = client.get("/connections")

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_HOST_HEADER"
    assert response.headers["X-Request-ID"]
    assert listing.status_code == 400
    assert not target.exists()


@pytest.mark.parametrize(
    "headers",
    [
        {"Origin": "http://attacker.example"},
        {"Origin": "null"},
        {"Origin": "http://localhost:3000"},
        {"Sec-Fetch-Site": "cross-site"},
    ],
)
def test_cross_site_state_change_is_rejected(
    settings: Settings, tmp_path: Path, headers: dict[str, str]
) -> None:
    target = tmp_path / "forged"
    with client_for(settings) as client:
        response = client.post("/connections", json=_local("forged", target), headers=headers)

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "CROSS_ORIGIN_REJECTED"
    assert not target.exists()


def test_same_origin_state_change_is_allowed(settings: Settings, tmp_path: Path) -> None:
    with client_for(settings) as client:
        response = client.post(
            "/connections",
            json=_local("docs_ui", tmp_path / "docs"),
            headers={"Origin": BASE_URL, "Sec-Fetch-Site": "same-origin"},
        )

    assert response.status_code == 201


@pytest.mark.parametrize("content_type", ["text/plain", None])
def test_json_body_requires_json_content_type(
    settings: Settings, tmp_path: Path, content_type: str | None
) -> None:
    target = tmp_path / "simple-request"
    headers = {"Content-Type": content_type} if content_type else {}
    with client_for(settings) as client:
        response = client.post(
            "/connections", content=json.dumps(_local("simple", target)), headers=headers
        )

    assert response.status_code == 422
    assert not target.exists()
