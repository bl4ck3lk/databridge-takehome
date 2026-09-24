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
