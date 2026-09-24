"""Exercise the reviewer path through live HTTP and the Docker SFTP fixture."""

import hashlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

from cryptography.fernet import Fernet

ROOT = Path(__file__).resolve().parents[1]


def _port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def _request(base: str, method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    request = Request(
        base + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data is not None else {},
    )
    try:
        with urlopen(request, timeout=45) as response:
            return json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"{method} {path} returned {exc.code}: {exc.read().decode()}") from exc


def _wait_for_server(base: str, process: subprocess.Popen, seconds: float = 15) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Uvicorn stopped before the smoke check could connect")
        try:
            _request(base, "GET", "/openapi.json")
            return
        except URLError:
            time.sleep(0.1)
    raise RuntimeError("Uvicorn did not become ready within 15 seconds")


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1_048_576), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _transfer(base: str, source: str, source_file: str, target: str, target_file: str) -> dict:
    record = _request(
        base,
        "POST",
        "/transfers",
        {
            "source": source,
            "source_file": source_file,
            "destination": target,
            "destination_file": target_file,
        },
    )
    if record["status"] != "completed":
        raise RuntimeError("Transfer did not complete")
    if _request(base, "GET", f"/transfers/{record['id']}") != record:
        raise RuntimeError("Persisted transfer record differs from creation response")
    return record


def main() -> None:
    trust_file = ROOT / "known_hosts"
    remote_root = ROOT / "sftp_data"
    if not trust_file.is_file() or not remote_root.is_dir():
        raise SystemExit("Run the README's Docker and known-hosts bootstrap first")
    if not (ROOT / "data" / "customers.csv").is_file():
        raise SystemExit("The committed data/customers.csv fixture is missing")
    if not (ROOT / "data" / "products.json").is_file():
        raise SystemExit("The committed data/products.json fixture is missing")

    csv_name = f"smoke-{uuid4().hex}.csv"
    json_name = f"smoke-{uuid4().hex}.json"
    remote_files = [remote_root / csv_name, remote_root / json_name]
    with tempfile.TemporaryDirectory(prefix="databridge-smoke-") as temp:
        output_root = Path(temp) / "output"
        output_root.mkdir()
        port = _port()
        base = f"http://127.0.0.1:{port}"
        env = os.environ.copy()
        env.update(
            {
                "DATABRIDGE_ENCRYPTION_KEY": Fernet.generate_key().decode(),
                "DATABRIDGE_DB_PATH": str(Path(temp) / "state.sqlite3"),
                "DATABRIDGE_KNOWN_HOSTS": str(trust_file),
            }
        )
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "databridge.api:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--workers",
                "1",
                "--log-level",
                "warning",
            ],
            cwd=ROOT,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        try:
            _wait_for_server(base, process)
            for body in (
                {"name": "local_data", "type": "local", "path": str(ROOT / "data")},
                {"name": "local_output", "type": "local", "path": str(output_root)},
                {
                    "name": "remote_server",
                    "type": "sftp",
                    "host": "127.0.0.1",
                    "port": 2222,
                    "username": "testuser",
                    "password": "testpass",
                    "root": "data",
                },
            ):
                _request(base, "POST", "/connections", body)
            _request(base, "POST", "/connections/remote_server/healthcheck")
            local_preview = _request(
                base, "GET", "/connections/local_data/files/customers.csv/head?limit=5"
            )
            if len(local_preview["rows"]) != 5:
                raise RuntimeError("Local CSV preview returned the wrong row count")

            csv_upload = _transfer(base, "local_data", "customers.csv", "remote_server", csv_name)
            _transfer(base, "local_data", "products.json", "remote_server", json_name)
            remote_preview = _request(
                base, "GET", f"/connections/remote_server/files/{json_name}/head?limit=2"
            )
            if len(remote_preview["rows"]) != 2 or remote_preview["schema"]["price"] != "float":
                raise RuntimeError("Remote JSON preview did not infer the expected schema")
            csv_download = _transfer(
                base, "remote_server", csv_name, "local_output", "downloaded.csv"
            )
            source_hash = _hash(ROOT / "data" / "customers.csv")
            if not all(
                _hash(path) == source_hash
                for path in (remote_root / csv_name, output_root / "downloaded.csv")
            ):
                raise RuntimeError("CSV hashes differ across upload and download")
            print(
                json.dumps(
                    {
                        "result": "passed",
                        "http": base,
                        "local_csv_preview_rows": len(local_preview["rows"]),
                        "remote_json_preview_rows": len(remote_preview["rows"]),
                        "csv_sha256": source_hash,
                        "upload_bytes": csv_upload["bytes_copied"],
                        "download_bytes": csv_download["bytes_copied"],
                    },
                    indent=2,
                )
            )
        finally:
            process.terminate()
            try:
                process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
            for file in remote_files:
                file.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
