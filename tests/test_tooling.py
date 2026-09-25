"""Reviewer tooling: trusting the local SFTP fixture's host key under both loopback names."""

import os
import shutil
import stat
import subprocess
from pathlib import Path

import paramiko
import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "trust_sftp_fixture.sh"
PORT = "2299"
KEY = paramiko.ECDSAKey.generate()
TRUSTED = f"[127.0.0.1]:{PORT} {KEY.get_name()} {KEY.get_base64()}"

pytestmark = pytest.mark.skipif(shutil.which("ssh-keygen") is None, reason="needs ssh-keygen")


def _trust(tmp_path: Path, known_hosts: Path, *, scanned: str | None) -> Path:
    """Run the script with a stub ssh-keyscan; return the file the stub marks when called."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    called = tmp_path / "keyscan-called"
    stub = bin_dir / "ssh-keyscan"
    # OpenSSH's ssh-keyscan also prints "# host:port SSH-2.0-..." banner comments.
    banner = f"# 127.0.0.1:{PORT} SSH-2.0-OpenSSH_9.6"
    output = f"printf '%s\\n%s\\n' '{banner}' '{scanned}'" if scanned else "exit 1"
    stub.write_text(f"#!/bin/sh\ntouch '{called}'\n{output}\n")
    stub.chmod(0o755)
    subprocess.run(
        ["sh", str(SCRIPT), str(known_hosts), PORT],
        env={**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"},
        check=True,
        capture_output=True,
        timeout=60,
    )
    return called


def _lines(known_hosts: Path) -> list[str]:
    return known_hosts.read_text().splitlines()


def test_first_run_trusts_the_scanned_key_under_both_names(tmp_path: Path) -> None:
    known_hosts = tmp_path / "known_hosts"
    _trust(tmp_path, known_hosts, scanned=TRUSTED)
    # Only key lines are kept; the scanner's banner comments are dropped.
    assert sorted(_lines(known_hosts)) == sorted(
        [TRUSTED, TRUSTED.replace("[127.0.0.1]", "[localhost]")]
    )
    assert stat.S_IMODE(known_hosts.stat().st_mode) == 0o600


def test_localhost_reuses_the_trusted_key_without_scanning(tmp_path: Path) -> None:
    known_hosts = tmp_path / "known_hosts"
    other = "[example.test]:22 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIA0000000000000000000000"
    known_hosts.write_text(f"{other}\n{TRUSTED}\n")
    called = _trust(tmp_path, known_hosts, scanned=None)
    assert not called.exists()
    assert _lines(known_hosts) == [
        other,
        TRUSTED,
        TRUSTED.replace("[127.0.0.1]", "[localhost]"),
    ]


def test_rerunning_adds_nothing(tmp_path: Path) -> None:
    known_hosts = tmp_path / "known_hosts"
    _trust(tmp_path, known_hosts, scanned=TRUSTED)
    before = known_hosts.read_text()
    called = tmp_path / "keyscan-called"
    called.unlink()
    _trust(tmp_path, known_hosts, scanned=TRUSTED)
    assert known_hosts.read_text() == before
    assert not called.exists()
