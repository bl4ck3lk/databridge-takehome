"""Where the local Docker SFTP fixture listens, and the trust file its host keys are saved in.

`make quickstart` writes DATABRIDGE_SFTP_PORT and DATABRIDGE_KNOWN_HOSTS to .env; the make
targets that use the fixture load .env, so every check reaches the same container.
"""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HOST = "127.0.0.1"
PORT = int(os.environ.get("DATABRIDGE_SFTP_PORT", "2222"))
KNOWN_HOSTS = Path(os.environ.get("DATABRIDGE_KNOWN_HOSTS", str(PROJECT_ROOT / "known_hosts")))
DATA_DIR = PROJECT_ROOT / "sftp_data"


def connection(name: str, **changes: object) -> dict[str, object]:
    """A connection request for the fixture's documented test account."""
    return {
        "name": name,
        "type": "sftp",
        "host": HOST,
        "port": PORT,
        "username": "testuser",
        "password": "testpass",
        "root": "data",
        **changes,
    }
