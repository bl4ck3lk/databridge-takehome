"""Shared helpers for API tests."""

import json
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from fastapi.testclient import TestClient

from databridge.api import open_app
from databridge.config import Settings
from databridge.connectors.base import ByteSource

BASE_URL = "http://127.0.0.1:8080"


@contextmanager
def client_for(settings: Settings, base_url: str = BASE_URL) -> Iterator[TestClient]:
    """Open the service and a client for it. The default base URL sends an allowed loopback
    Host header, as a local caller does. Leaving the block stops the service."""
    with open_app(settings) as app, TestClient(app, base_url=base_url) as client:
        yield client


def read_all(source: ByteSource) -> bytes:
    """Drain a connector stream; the contract allows short reads."""
    chunks = []
    while chunk := source.read(65_536):
        chunks.append(chunk)
    return b"".join(chunks)


def request_events(settings: Settings) -> list[dict[str, Any]]:
    return [json.loads(line) for line in settings.request_log_path.read_text().splitlines()]
