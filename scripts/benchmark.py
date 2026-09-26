"""Reproducible end-to-end transfer probe against the local Docker SFTP fixture.

Run `make benchmark` after `make quickstart` has started and trusted the Docker fixture.
The measured duration includes connection setup and destination finalization.
"""

import argparse
import contextlib
import hashlib
import json
import platform
import queue
import resource
import socket
import sys
import tempfile
import threading
import time
from pathlib import Path
from uuid import uuid4

import sftp_fixture
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from databridge.api import open_app
from databridge.config import Settings
from databridge.connectors.base import CHUNK_SIZE

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
        sftp_fixture.connection("remote"),
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


class _DelayRelay:
    """A loopback TCP relay to the fixture that delays each direction by half a round trip.

    Loopback hides round-trip cost, so this makes one request per round trip visible in
    throughput without limiting bandwidth.
    """

    def __init__(self, round_trip_ms: int) -> None:
        self._delay = round_trip_ms / 2000
        self._listener = socket.create_server((sftp_fixture.HOST, 0))
        self.port = int(self._listener.getsockname()[1])
        threading.Thread(target=self._accept, daemon=True).start()

    def close(self) -> None:
        self._listener.close()

    def _accept(self) -> None:
        while True:
            try:
                client, _address = self._listener.accept()
            except OSError:
                return
            upstream = socket.create_connection((sftp_fixture.HOST, sftp_fixture.PORT))
            self._relay(client, upstream)
            self._relay(upstream, client)

    def _relay(self, source: socket.socket, sink: socket.socket) -> None:
        pending: queue.Queue[tuple[float, bytes]] = queue.Queue()

        def receive() -> None:
            with contextlib.suppress(OSError):
                while data := source.recv(262_144):
                    pending.put((time.monotonic() + self._delay, data))
            pending.put((0.0, b""))

        def deliver() -> None:
            with contextlib.suppress(OSError):
                while (item := pending.get())[1]:
                    due, data = item
                    time.sleep(max(0.0, due - time.monotonic()))
                    sink.sendall(data)
                sink.shutdown(socket.SHUT_WR)

        threading.Thread(target=receive, daemon=True).start()
        threading.Thread(target=deliver, daemon=True).start()


def _trust_relays(trust_file: Path, relay_ports: list[int], path: Path) -> None:
    """Trust each relay port with the fixture's already-trusted keys; no new key is accepted."""
    fixture = f"[{sftp_fixture.HOST}]:{sftp_fixture.PORT} "
    lines = trust_file.read_text().splitlines()
    trusted = [line for line in lines if line.startswith(fixture)]
    if not trusted:
        raise RuntimeError(f"{trust_file} has no entry for {fixture.strip()}")
    aliases = [
        line.replace(fixture, f"[{sftp_fixture.HOST}]:{port} ", 1)
        for port in relay_ports
        for line in trusted
    ]
    path.write_text("\n".join([*lines, *aliases]) + "\n")


def _measure(
    client: TestClient,
    roots: tuple[Path, Path, Path],
    remote: str,
    size_mib: int,
    round_trip_ms: int,
) -> dict[str, object]:
    source_root, remote_root, target_root = roots
    size = size_mib * MIB
    name = f"benchmark-{size_mib}-{round_trip_ms}-{uuid4().hex}.bin"
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
                "destination": remote,
                "destination_file": name,
            },
            size,
        )
        if _hash(remote_file) != expected_hash:
            raise RuntimeError("Upload hash mismatch")
        download = _copy(
            client,
            {
                "source": remote,
                "source_file": name,
                "destination": "target",
                "destination_file": name,
            },
            size,
        )
        if _hash(target_file) != expected_hash:
            raise RuntimeError("Download hash mismatch")
        return {
            "size_mib": size_mib,
            "round_trip_ms": round_trip_ms,
            "bytes": size,
            "sha256": expected_hash,
            "upload": upload,
            "download": download,
        }
    finally:
        remote_file.unlink(missing_ok=True)
        source_file.unlink(missing_ok=True)
        target_file.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes-mib", type=int, nargs="+", default=[16, 256])
    parser.add_argument(
        "--round-trip-ms",
        type=int,
        nargs="*",
        default=[20],
        help="added round-trip times, each measured with the first size through a delaying relay",
    )
    args = parser.parse_args()
    trust_file = sftp_fixture.KNOWN_HOSTS
    remote_root = sftp_fixture.DATA_DIR
    if not trust_file.is_file() or not remote_root.is_dir():
        parser.error("Run make quickstart first to start and trust the local SFTP fixture")
    if any(size < 1 for size in args.sizes_mib):
        parser.error("All sizes must be positive")
    if any(round_trip < 1 for round_trip in args.round_trip_ms):
        parser.error("All round-trip times must be positive")

    results: list[dict[str, object]] = []
    baseline_peak_rss_mib = 0.0
    relays = {round_trip: _DelayRelay(round_trip) for round_trip in args.round_trip_ms}
    try:
        with tempfile.TemporaryDirectory(prefix="databridge-benchmark-") as temp:
            base = Path(temp)
            source_root = base / "source"
            target_root = base / "target"
            source_root.mkdir()
            target_root.mkdir()
            roots = (source_root, remote_root, target_root)
            known_hosts = base / "known_hosts"
            _trust_relays(trust_file, [relay.port for relay in relays.values()], known_hosts)
            settings = Settings(base / "state.sqlite3", Fernet.generate_key(), known_hosts)
            with (
                open_app(settings) as app,
                TestClient(app, base_url="http://127.0.0.1") as client,
            ):
                _create_connections(client, source_root, target_root)
                for round_trip, relay in relays.items():
                    response = client.post(
                        "/connections",
                        json=sftp_fixture.connection(f"remote_rtt{round_trip}", port=relay.port),
                    )
                    if response.status_code != 201:
                        raise RuntimeError(f"Relay connection setup failed: {response.json()}")
                baseline_peak_rss_mib = _peak_rss_mib()
                for size_mib in args.sizes_mib:
                    results.append(_measure(client, roots, "remote", size_mib, 0))
                for round_trip in relays:
                    results.append(
                        _measure(
                            client, roots, f"remote_rtt{round_trip}", args.sizes_mib[0], round_trip
                        )
                    )
    finally:
        for relay in relays.values():
            relay.close()

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
                "latency_note": (
                    "round_trip_ms > 0 routes through a loopback relay that delays each "
                    "direction by half the round trip without limiting bandwidth"
                ),
                "baseline_peak_process_rss_mib": baseline_peak_rss_mib,
                "results": results,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
