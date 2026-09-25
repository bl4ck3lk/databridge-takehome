"""Runtime settings; secrets are supplied by the environment."""

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_ALLOWED_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


@dataclass(frozen=True)
class Settings:
    database_path: Path
    encryption_key: bytes
    known_hosts_path: Path
    allowed_hosts: frozenset[str] = DEFAULT_ALLOWED_HOSTS

    @property
    def request_log_path(self) -> Path:
        return self.database_path.with_suffix(".requests.jsonl")

    @classmethod
    def from_env(cls) -> "Settings":
        key = os.environ.get("DATABRIDGE_ENCRYPTION_KEY")
        if not key:
            raise RuntimeError("DATABRIDGE_ENCRYPTION_KEY is required at startup")
        allowed_hosts = os.environ.get("DATABRIDGE_ALLOWED_HOSTS")
        return cls(
            database_path=Path(os.environ.get("DATABRIDGE_DB_PATH", "state/databridge.sqlite3")),
            encryption_key=key.encode("ascii"),
            known_hosts_path=Path(os.environ.get("DATABRIDGE_KNOWN_HOSTS", "known_hosts")),
            allowed_hosts=(
                frozenset(host.strip().lower() for host in allowed_hosts.split(",") if host.strip())
                if allowed_hosts
                else DEFAULT_ALLOWED_HOSTS
            ),
        )
