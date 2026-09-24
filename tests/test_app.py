from fastapi.testclient import TestClient

from databridge.api import create_app


def test_openapi_is_served() -> None:
    with TestClient(create_app()) as client:
        response = client.get("/openapi.json")

    assert response.status_code == 200
    assert response.json()["info"]["title"] == "DataBridge"
