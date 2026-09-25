"""Startup configuration fails clearly and never depends on the working directory."""

from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from databridge.config import Settings

KEY = Fernet.generate_key().decode()


def _environment(monkeypatch: pytest.MonkeyPatch, **values: str) -> None:
    for name in (
        "DATABRIDGE_ENCRYPTION_KEY",
        "DATABRIDGE_DB_PATH",
        "DATABRIDGE_KNOWN_HOSTS",
        "DATABRIDGE_ALLOWED_HOSTS",
    ):
        monkeypatch.delenv(name, raising=False)
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def test_settings_read_absolute_paths_and_allowed_hosts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _environment(
        monkeypatch,
        DATABRIDGE_ENCRYPTION_KEY=KEY,
        DATABRIDGE_DB_PATH=str(tmp_path / "state" / "databridge.sqlite3"),
        DATABRIDGE_KNOWN_HOSTS=str(tmp_path / "known_hosts"),
        DATABRIDGE_ALLOWED_HOSTS=" 127.0.0.1 , Example.Internal ",
    )

    settings = Settings.from_env()

    assert settings.database_path == tmp_path / "state" / "databridge.sqlite3"
    assert settings.request_log_path == tmp_path / "state" / "databridge.requests.jsonl"
    assert settings.lock_path == tmp_path / "state" / "databridge.owner.lock"
    assert settings.allowed_hosts == frozenset({"127.0.0.1", "example.internal"})


@pytest.mark.parametrize("missing", ["DATABRIDGE_DB_PATH", "DATABRIDGE_KNOWN_HOSTS"])
def test_settings_require_every_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, missing: str
) -> None:
    values = {
        "DATABRIDGE_ENCRYPTION_KEY": KEY,
        "DATABRIDGE_DB_PATH": str(tmp_path / "databridge.sqlite3"),
        "DATABRIDGE_KNOWN_HOSTS": str(tmp_path / "known_hosts"),
    }
    del values[missing]
    _environment(monkeypatch, **values)

    with pytest.raises(RuntimeError, match=f"{missing} is required"):
        Settings.from_env()


def test_settings_reject_relative_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _environment(
        monkeypatch,
        DATABRIDGE_ENCRYPTION_KEY=KEY,
        DATABRIDGE_DB_PATH="state/databridge.sqlite3",
        DATABRIDGE_KNOWN_HOSTS=str(tmp_path / "known_hosts"),
    )

    with pytest.raises(RuntimeError, match="DATABRIDGE_DB_PATH must be an absolute path"):
        Settings.from_env()


def test_database_path_must_name_a_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _environment(
        monkeypatch,
        DATABRIDGE_ENCRYPTION_KEY=KEY,
        DATABRIDGE_DB_PATH=str(tmp_path),
        DATABRIDGE_KNOWN_HOSTS=str(tmp_path / "known_hosts"),
    )

    with pytest.raises(RuntimeError, match="DATABRIDGE_DB_PATH names a directory"):
        Settings.from_env()


def test_non_ascii_key_error_does_not_echo_the_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    secret = "secretprefixé" + "x" * 31
    _environment(
        monkeypatch,
        DATABRIDGE_ENCRYPTION_KEY=secret,
        DATABRIDGE_DB_PATH=str(tmp_path / "databridge.sqlite3"),
        DATABRIDGE_KNOWN_HOSTS=str(tmp_path / "known_hosts"),
    )

    with pytest.raises(RuntimeError) as error:
        Settings.from_env()

    message = str(error.value)
    assert "DATABRIDGE_ENCRYPTION_KEY must be the URL-safe base64 Fernet key" in message
    assert "secretprefix" not in message and "é" not in message
