from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from databridge.config import Settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        database_path=tmp_path / "databridge.sqlite3",
        encryption_key=Fernet.generate_key(),
        known_hosts_path=tmp_path / "known_hosts",
    )
