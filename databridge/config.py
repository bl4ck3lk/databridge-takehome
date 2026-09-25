"""Runtime settings from the environment; every path is absolute so the working directory never
changes which database, log, or trust file the service uses."""

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_ALLOWED_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
_SETUP_HINT = "make quickstart writes it to .env"


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
        key = os.environ.get("DATABRIDGE_ENCRYPTION_KEY")
        if not key:
            raise RuntimeError(f"DATABRIDGE_ENCRYPTION_KEY is required at startup; {_SETUP_HINT}")
        if not key.isascii():
            raise RuntimeError(
                "DATABRIDGE_ENCRYPTION_KEY must be the URL-safe base64 Fernet key that "
                "make quickstart writes to .env"
            )
        database_path = _absolute_path("DATABRIDGE_DB_PATH")
        if database_path.is_dir():
            raise RuntimeError(
                "DATABRIDGE_DB_PATH names a directory; it must name the SQLite database file"
            )
        allowed_hosts = os.environ.get("DATABRIDGE_ALLOWED_HOSTS")
        return cls(
            database_path=database_path,
            encryption_key=key.encode("ascii"),
            known_hosts_path=_absolute_path("DATABRIDGE_KNOWN_HOSTS"),
            allowed_hosts=(
                frozenset(host.strip().lower() for host in allowed_hosts.split(",") if host.strip())
                if allowed_hosts
                else DEFAULT_ALLOWED_HOSTS
            ),
        )


def _absolute_path(name: str) -> Path:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required at startup; {_SETUP_HINT}")
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise RuntimeError(f"{name} must be an absolute path")
    return path
