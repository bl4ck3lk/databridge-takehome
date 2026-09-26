"""The service command line: a setting or database problem stops startup with one line on
stderr and exit status 1, never a traceback, and a started service answers HTTP."""

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import pytest
from cryptography.fernet import Fernet

from databridge.api import open_app
from databridge.config import Settings
from databridge.store import DatabaseOwnerLock

_MISSING = (
    "DATABRIDGE_ENCRYPTION_KEY is required; DATABRIDGE_DB_PATH is required; "
    "DATABRIDGE_KNOWN_HOSTS is required; make quickstart writes missing settings to .env"
)


def _environment(settings: Settings | None = None, **changes: str) -> dict[str, str]:
    """A child environment with PATH and only the given DataBridge settings.

    The child inherits nothing else, so a failed test cannot print the developer's variables.
    """
    environment = {"PATH": os.environ["PATH"]}
    if settings is not None:
        environment |= {
            "DATABRIDGE_ENCRYPTION_KEY": settings.encryption_key.decode(),
            "DATABRIDGE_DB_PATH": str(settings.database_path),
            "DATABRIDGE_KNOWN_HOSTS": str(settings.known_hosts_path),
        }
    return environment | changes


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port: int = listener.getsockname()[1]
        return port


def _databridge(environment: dict[str, str], *arguments: str) -> subprocess.CompletedProcess[str]:
    # Fixed arguments: this interpreter runs the package's own command line.
    return subprocess.run(
        [sys.executable, "-m", "databridge", *arguments],
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
    )


def _refused_start(environment: dict[str, str]) -> tuple[int, str]:
    """Run `serve` where it must refuse to start; a server that starts hits the timeout."""
    result = _databridge(environment, "serve", "--port", str(_free_port()))
    return result.returncode, result.stderr


def test_serve_names_every_missing_setting_in_one_line() -> None:
    refused = _refused_start(_environment())

    assert refused == (1, f"DataBridge cannot start: {_MISSING}\n")


@pytest.mark.parametrize("port", ["0", "65536", "http", "٨٠"])
def test_serve_refuses_a_port_outside_the_tcp_range(port: str) -> None:
    # No settings are given: a port check made after the settings would exit with status 1.
    result = _databridge(_environment(), "serve", "--port", port)

    assert result.returncode == 2
    assert result.stderr.endswith("argument --port: must be a TCP port from 1 to 65535\n")


def test_serve_refuses_a_key_that_does_not_match_the_database(settings: Settings) -> None:
    with open_app(settings):
        pass
    wrong_key = Fernet.generate_key().decode()

    refused = _refused_start(_environment(settings, DATABRIDGE_ENCRYPTION_KEY=wrong_key))

    assert refused == (
        1,
        "DataBridge cannot start: DATABRIDGE_ENCRYPTION_KEY does not match the database "
        f"{settings.database_path}; start with the key that created it, or move the database "
        "aside to start with no saved connections\n",
    )


def test_serve_refuses_a_second_process_for_the_same_database(settings: Settings) -> None:
    with DatabaseOwnerLock(settings.lock_path):
        refused = _refused_start(_environment(settings))

    assert refused == (
        1,
        "DataBridge cannot start: Another DataBridge process is using the database locked by "
        f"{settings.lock_path}; stop that process before starting another\n",
    )


def _first_status(url: str, process: subprocess.Popen[bytes]) -> int:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and process.poll() is None:
        try:
            # The URL is loopback HTTP on a port this test chose.
            with urlopen(url, timeout=5) as response:  # noqa: S310
                status: int = response.status
                return status
        except URLError:
            time.sleep(0.1)
    pytest.fail(f"The service never answered {url}; its exit status is {process.poll()}")


def test_serve_answers_http_and_stops_cleanly_on_interrupt(
    settings: Settings, tmp_path: Path
) -> None:
    port = _free_port()
    with (tmp_path / "service.log").open("wb") as log:
        process = subprocess.Popen(
            [sys.executable, "-m", "databridge", "serve", "--port", str(port)],
            env=_environment(settings),
            stdout=log,
            stderr=log,
        )
    try:
        status = _first_status(f"http://127.0.0.1:{port}/connections", process)
    finally:
        process.send_signal(signal.SIGINT)
        returncode = process.wait(timeout=30)

    assert (status, returncode) == (200, 0)
    with open_app(settings):  # the stopped service left its database usable with the same key
        pass


def test_request_log_path_prints_the_configured_log(settings: Settings) -> None:
    result = _databridge(_environment(settings), "request-log-path")

    assert (result.returncode, result.stdout, result.stderr) == (
        0,
        f"{settings.request_log_path}\n",
        "",
    )


def test_request_log_path_names_missing_settings_in_one_line() -> None:
    result = _databridge(_environment(), "request-log-path")

    assert (result.returncode, result.stdout, result.stderr) == (
        1,
        "",
        f"Cannot locate the request log: {_MISSING}\n",
    )
