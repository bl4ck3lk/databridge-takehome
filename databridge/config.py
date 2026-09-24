"""Runtime settings; secrets are supplied by the environment."""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    database_path: Path
    encryption_key: bytes
    known_hosts_path: Path

    @classmethod
    def from_env(cls) -> "Settings":
        key = os.environ.get("DATABRIDGE_ENCRYPTION_KEY")
        if not key:
            raise RuntimeError("DATABRIDGE_ENCRYPTION_KEY is required at startup")
        return cls(
            database_path=Path(os.environ.get("DATABRIDGE_DB_PATH", "state/databridge.sqlite3")),
            encryption_key=key.encode("ascii"),
            known_hosts_path=Path(os.environ.get("DATABRIDGE_KNOWN_HOSTS", "known_hosts")),
        )
