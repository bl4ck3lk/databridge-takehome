"""Runtime settings from the environment; every path is absolute so the working directory never
changes which database, log, or trust file the service uses."""

import os
from dataclasses import dataclass
from pathlib import Path

from cryptography.fernet import Fernet

from databridge.errors import StartupError

DEFAULT_ALLOWED_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
_KEY_ENV = "DATABRIDGE_ENCRYPTION_KEY"
_DATABASE_ENV = "DATABRIDGE_DB_PATH"
_KNOWN_HOSTS_ENV = "DATABRIDGE_KNOWN_HOSTS"
_ALLOWED_HOSTS_ENV = "DATABRIDGE_ALLOWED_HOSTS"
_REQUIRED_ENV = (_KEY_ENV, _DATABASE_ENV, _KNOWN_HOSTS_ENV)


@dataclass(frozen=True)
class Settings:
    database_path: Path
    encryption_key: bytes
    known_hosts_path: Path
    allowed_hosts: frozenset[str] = DEFAULT_ALLOWED_HOSTS

    @property
    def request_log_path(self) -> Path:
        return self.database_path.with_suffix(".requests.jsonl")

    @property
    def lock_path(self) -> Path:
        return self.database_path.with_suffix(".owner.lock")

    @classmethod
    def from_env(cls) -> "Settings":
        """Read every setting before failing, so one StartupError names each value to fix."""
        problems: list[str] = []
        encryption_key = _encryption_key(problems)
        database_path = _database_path(problems)
        known_hosts_path = _absolute_path(_KNOWN_HOSTS_ENV, problems)
        allowed_hosts = _allowed_hosts(problems)
        if (
            encryption_key is None
            or database_path is None
            or known_hosts_path is None
            or allowed_hosts is None
        ):
            # Quickstart adds a missing setting but never replaces a setting. Thus only a
            # missing setting refers to quickstart.
            if any(_value(name) is None for name in _REQUIRED_ENV):
                problems.append("make quickstart writes missing settings to .env")
            raise StartupError("; ".join(problems))
        return cls(database_path, encryption_key, known_hosts_path, allowed_hosts)


def _value(name: str) -> str | None:
    """Read a setting; an empty value counts as unset."""
    return os.environ.get(name) or None


def _required(name: str, problems: list[str]) -> str | None:
    value = _value(name)
    if value is None:
        problems.append(f"{name} is required")
    return value


def _encryption_key(problems: list[str]) -> bytes | None:
    value = _required(_KEY_ENV, problems)
    if value is None:
        return None
    try:
        key = value.encode("ascii")
        # The store makes its own cipher. This check is here so that one message can name
        # every problem with the settings.
        Fernet(key)  # raises ValueError for a wrong alphabet, padding, or length
    except ValueError:
        # UnicodeEncodeError is a ValueError too. The message never repeats the key.
        problems.append(
            f"{_KEY_ENV} must be the URL-safe base64 Fernet key that make quickstart writes to .env"
        )
        return None
    return key


def _absolute_path(name: str, problems: list[str]) -> Path | None:
    value = _required(name, problems)
    if value is None:
        return None
    path = Path(value).expanduser()
    if not path.is_absolute():
        problems.append(f"{name} must be an absolute path")
        return None
    return path


def _database_path(problems: list[str]) -> Path | None:
    path = _absolute_path(_DATABASE_ENV, problems)
    if path is not None and path.is_dir():
        problems.append(f"{_DATABASE_ENV} names a directory; it must name the SQLite database file")
        return None
    return path


def _allowed_hosts(problems: list[str]) -> frozenset[str] | None:
    value = _value(_ALLOWED_HOSTS_ENV)
    if value is None:
        return DEFAULT_ALLOWED_HOSTS
    hosts = frozenset(host.strip().lower() for host in value.split(",") if host.strip())
    if not hosts:
        problems.append(f"{_ALLOWED_HOSTS_ENV} must name at least one host")
        return None
    return hosts
