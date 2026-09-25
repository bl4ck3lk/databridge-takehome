"""Shared helpers for API tests."""

import json
from typing import Any

from fastapi.testclient import TestClient

from databridge.api import create_app
from databridge.config import Settings

BASE_URL = "http://127.0.0.1:8080"


def client_for(settings: Settings) -> TestClient:
    """Build a client that sends an allowed loopback Host header, as a local caller does."""
    return TestClient(create_app(settings), base_url=BASE_URL)


def request_events(settings: Settings) -> list[dict[str, Any]]:
    return [json.loads(line) for line in settings.request_log_path.read_text().splitlines()]
