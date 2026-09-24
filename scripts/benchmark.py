"""Reproducible end-to-end transfer probe against the local Docker SFTP fixture.

Run `make benchmark` after the README's Docker and known-hosts bootstrap.
The measured duration includes connection setup and destination finalization.
"""

import argparse
import hashlib
import json
import platform
import resource
import sys
import tempfile
import time
from pathlib import Path
from uuid import uuid4

from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from databridge.api import create_app
from databridge.config import Settings
from databridge.connectors.base import CHUNK_SIZE

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MIB = 1_048_576


def _peak_rss_mib() -> float:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(value / MIB if sys.platform == "darwin" else value / 1024, 2)


def _create_payload(path: Path, size: int) -> None:
    block = bytes(range(256)) * (CHUNK_SIZE // 256)
    with path.open("wb") as output:
        remaining = size
        while remaining:
            count = min(remaining, len(block))
            output.write(block[:count])
            remaining -= count


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def _create_connections(client: TestClient, source: Path, target: Path) -> None:
    for body in (
        {"name": "source", "type": "local", "path": str(source)},
        {"name": "target", "type": "local", "path": str(target)},
        {
            "name": "remote",
            "type": "sftp",
            "host": "127.0.0.1",
            "port": 2222,
            "username": "testuser",
            "password": "testpass",
            "root": "data",
        },
    ):
        response = client.post("/connections", json=body)
        if response.status_code != 201:
            raise RuntimeError(f"Connection setup failed: {response.json()}")


def _copy(client: TestClient, request: dict[str, object], size: int) -> dict[str, object]:
    started = time.perf_counter()
    response = client.post("/transfers", json=request)
    elapsed = time.perf_counter() - started
    if response.status_code != 201:
        raise RuntimeError(f"Transfer failed: {response.json()}")
    record = response.json()
    if record["bytes_copied"] != size:
        raise RuntimeError("Transfer byte count differs from fixture size")
    return {
        "duration_seconds": round(elapsed, 3),
        "throughput_mib_s": round((size / MIB) / elapsed, 2),
        "peak_process_rss_mib": _peak_rss_mib(),
        "transfer_id": record["id"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes-mib", type=int, nargs="+", default=[16, 256])
    args = parser.parse_args()
    trust_file = PROJECT_ROOT / "known_hosts"
    remote_root = PROJECT_ROOT / "sftp_data"
    if not trust_file.is_file() or not remote_root.is_dir():
        parser.error("Run the README's Docker and known-hosts bootstrap first")
    if any(size < 1 for size in args.sizes_mib):
        parser.error("All sizes must be positive")

    results: list[dict[str, object]] = []
    baseline_peak_rss_mib = 0.0
    with tempfile.TemporaryDirectory(prefix="databridge-benchmark-") as temp:
        base = Path(temp)
        source_root = base / "source"
        target_root = base / "target"
        source_root.mkdir()
        target_root.mkdir()
        settings = Settings(base / "state.sqlite3", Fernet.generate_key(), trust_file)
        with TestClient(create_app(settings)) as client:
            _create_connections(client, source_root, target_root)
            baseline_peak_rss_mib = _peak_rss_mib()
            for size_mib in args.sizes_mib:
                size = size_mib * MIB
                name = f"benchmark-{size_mib}-{uuid4().hex}.bin"
                source_file = source_root / name
                remote_file = remote_root / name
                target_file = target_root / name
                _create_payload(source_file, size)
                expected_hash = _hash(source_file)
                try:
                    upload = _copy(
                        client,
                        {
                            "source": "source",
                            "source_file": name,
                            "destination": "remote",
                            "destination_file": name,
                        },
                        size,
                    )
                    if _hash(remote_file) != expected_hash:
                        raise RuntimeError("Upload hash mismatch")
                    download = _copy(
                        client,
                        {
                            "source": "remote",
                            "source_file": name,
                            "destination": "target",
                            "destination_file": name,
                        },
                        size,
                    )
                    if _hash(target_file) != expected_hash:
                        raise RuntimeError("Download hash mismatch")
                    results.append(
                        {
                            "size_mib": size_mib,
                            "bytes": size,
                            "sha256": expected_hash,
                            "upload": upload,
                            "download": download,
                        }
                    )
                finally:
                    remote_file.unlink(missing_ok=True)
                    source_file.unlink(missing_ok=True)
                    target_file.unlink(missing_ok=True)

    print(
        json.dumps(
            {
                "host_platform": platform.platform(),
                "host_architecture": platform.machine(),
                "python": platform.python_version(),
                "sftp_image": "atmoz/sftp:alpine-3.7",
                "sftp_image_platform": "linux/amd64",
                "measurement": (
                    "HTTP TestClient round trip; duration includes SSH connection and publish"
                ),
                "memory_note": (
                    "ru_maxrss is a process lifetime peak and is cumulative across cases"
                ),
                "baseline_peak_process_rss_mib": baseline_peak_rss_mib,
                "results": results,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
