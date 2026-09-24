from fastapi.testclient import TestClient

from databridge.api import create_app
from databridge.config import Settings


def test_openapi_is_served(settings: Settings) -> None:
    with TestClient(create_app(settings)) as client:
        response = client.get("/openapi.json")

    assert response.status_code == 200
    assert response.json()["info"]["title"] == "DataBridge"
