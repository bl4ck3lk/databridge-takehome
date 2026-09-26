"""Where the local Docker SFTP fixture listens, and the trust file its host keys are saved in.

`make quickstart` writes DATABRIDGE_SFTP_PORT and DATABRIDGE_KNOWN_HOSTS to .env; the make
targets that use the fixture load .env, so every check reaches the same container. Neither
setting has a default. A guessed port could reach the container of another checkout, and the
check would then fail far from the missing setting.
"""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HOST = "127.0.0.1"
DATA_DIR = PROJECT_ROOT / "sftp_data"


def port() -> int:
    return int(_setting("DATABRIDGE_SFTP_PORT"))


def known_hosts() -> Path:
    path = Path(_setting("DATABRIDGE_KNOWN_HOSTS"))
    if not path.is_file():
        raise SystemExit(
            f"DATABRIDGE_KNOWN_HOSTS names {path}, which is not a file; run make quickstart, "
            "which trusts the local SFTP fixture"
        )
    return path


def connection(name: str, **changes: object) -> dict[str, object]:
    """A connection request for the fixture's documented test account."""
    return {
        "name": name,
        "type": "sftp",
        "host": HOST,
        "port": port(),
        "username": "testuser",
        "password": "testpass",
        "root": "data",
        **changes,
    }


def _setting(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"{name} is not set; run make quickstart, which writes it to .env")
    return value
