"""Startup configuration names every problem at once and never depends on the working
directory."""

from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from databridge.config import Settings
from databridge.errors import StartupError

KEY = Fernet.generate_key().decode()
REQUIRED = ("DATABRIDGE_ENCRYPTION_KEY", "DATABRIDGE_DB_PATH", "DATABRIDGE_KNOWN_HOSTS")


def _environment(monkeypatch: pytest.MonkeyPatch, **values: str) -> None:
    for name in (*REQUIRED, "DATABRIDGE_ALLOWED_HOSTS"):
        monkeypatch.delenv(name, raising=False)
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def _valid(tmp_path: Path) -> dict[str, str]:
    return {
        "DATABRIDGE_ENCRYPTION_KEY": KEY,
        "DATABRIDGE_DB_PATH": str(tmp_path / "databridge.sqlite3"),
        "DATABRIDGE_KNOWN_HOSTS": str(tmp_path / "known_hosts"),
    }


def _problems(monkeypatch: pytest.MonkeyPatch, **values: str) -> str:
    _environment(monkeypatch, **values)
    with pytest.raises(StartupError) as error:
        Settings.from_env()
    return str(error.value)


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

    assert settings.encryption_key == KEY.encode("ascii")
    assert settings.database_path == tmp_path / "state" / "databridge.sqlite3"
    assert settings.request_log_path == tmp_path / "state" / "databridge.requests.jsonl"
    assert settings.lock_path == tmp_path / "state" / "databridge.owner.lock"
    assert settings.allowed_hosts == frozenset({"127.0.0.1", "example.internal"})


def test_every_missing_setting_is_named_at_once(monkeypatch: pytest.MonkeyPatch) -> None:
    assert _problems(monkeypatch) == (
        "DATABRIDGE_ENCRYPTION_KEY is required; DATABRIDGE_DB_PATH is required; "
        "DATABRIDGE_KNOWN_HOSTS is required; make quickstart writes missing settings to .env"
    )


@pytest.mark.parametrize("missing", REQUIRED)
def test_a_missing_setting_names_the_command_that_writes_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, missing: str
) -> None:
    values = _valid(tmp_path)
    del values[missing]

    assert _problems(monkeypatch, **values) == (
        f"{missing} is required; make quickstart writes missing settings to .env"
    )


def test_every_invalid_setting_is_named_at_once(monkeypatch: pytest.MonkeyPatch) -> None:
    # Quickstart never replaces an existing value, so an invalid one gets no quickstart hint.
    assert _problems(
        monkeypatch,
        DATABRIDGE_ENCRYPTION_KEY="not-a-fernet-key",
        DATABRIDGE_DB_PATH="state/databridge.sqlite3",
        DATABRIDGE_KNOWN_HOSTS="known_hosts",
        DATABRIDGE_ALLOWED_HOSTS=" , ",
    ) == (
        "DATABRIDGE_ENCRYPTION_KEY must be the URL-safe base64 Fernet key that make quickstart "
        "writes to .env; DATABRIDGE_DB_PATH must be an absolute path; DATABRIDGE_KNOWN_HOSTS "
        "must be an absolute path; DATABRIDGE_ALLOWED_HOSTS must name at least one host"
    )


def test_database_path_must_name_a_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    values = {**_valid(tmp_path), "DATABRIDGE_DB_PATH": str(tmp_path)}

    assert _problems(monkeypatch, **values) == (
        "DATABRIDGE_DB_PATH names a directory; it must name the SQLite database file"
    )


@pytest.mark.parametrize(
    "secret",
    # 44 base64 characters decode to 33 bytes: valid padding, wrong Fernet key length.
    ["secretprefixé" + "x" * 31, "secretprefix" + "A" * 32, "secretprefix$$bad$$"],
    ids=["non-ascii", "wrong-length", "not-base64"],
)
def test_invalid_key_error_does_not_echo_the_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, secret: str
) -> None:
    message = _problems(monkeypatch, **{**_valid(tmp_path), "DATABRIDGE_ENCRYPTION_KEY": secret})

    assert message == (
        "DATABRIDGE_ENCRYPTION_KEY must be the URL-safe base64 Fernet key that make quickstart "
        "writes to .env"
    )
