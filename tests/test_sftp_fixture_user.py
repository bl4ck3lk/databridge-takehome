"""Quickstart's SFTP fixture user: never UID 0, and it owns the bind-mounted data directory."""

import os
import subprocess
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def _prepare(tmp_path: Path, host_uid: int) -> subprocess.CompletedProcess[str]:
    """Run prepare_fixture_data as a user with `host_uid`; a stub chown records its arguments."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    (bin_dir / "id").write_text(f"#!/bin/sh\necho {host_uid}\n")
    (bin_dir / "chown").write_text(f"#!/bin/sh\necho \"$*\" >> '{tmp_path / 'chown-calls'}'\n")
    for stub in bin_dir.iterdir():
        stub.chmod(0o755)
    return subprocess.run(
        [
            "sh",
            "-c",
            '. "$0"; . "$1"; prepare_fixture_data .env sftp_data',
            str(SCRIPTS / "env_file.sh"),
            str(SCRIPTS / "sftp_fixture_user.sh"),
        ],
        cwd=tmp_path,
        env={**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"},
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_a_user_gives_the_fixture_their_own_uid(tmp_path: Path) -> None:
    result = _prepare(tmp_path, 501)
    assert result.returncode == 0, result.stderr
    assert (tmp_path / ".env").read_text() == "DATABRIDGE_SFTP_UID='501'\n"
    assert (tmp_path / "sftp_data").is_dir()
    assert not (tmp_path / "chown-calls").exists()


def test_root_keeps_the_image_uid_and_hands_it_the_data_directory(tmp_path: Path) -> None:
    result = _prepare(tmp_path, 0)
    assert result.returncode == 0, result.stderr
    assert (tmp_path / ".env").read_text() == "DATABRIDGE_SFTP_UID='1001'\n"
    assert (tmp_path / "sftp_data").is_dir()
    assert (tmp_path / "chown-calls").read_text() == "1001 sftp_data\n"


def test_root_hands_the_data_directory_to_a_stored_uid(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("DATABRIDGE_SFTP_UID='501'\n")
    result = _prepare(tmp_path, 0)
    assert result.returncode == 0, result.stderr
    assert (tmp_path / ".env").read_text() == "DATABRIDGE_SFTP_UID='501'\n"
    assert (tmp_path / "chown-calls").read_text() == "501 sftp_data\n"


def test_a_double_quoted_stored_uid_stops_before_the_data_directory(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text('DATABRIDGE_SFTP_UID="501"\n')
    result = _prepare(tmp_path, 501)
    assert result.returncode != 0
    assert "DATABRIDGE_SFTP_UID='value'" in result.stderr
    assert not (tmp_path / "sftp_data").exists()


def test_a_stored_uid_of_zero_is_refused(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("DATABRIDGE_SFTP_UID='0'\n")
    result = _prepare(tmp_path, 501)
    assert result.returncode != 0
    assert "refuses UID 0 logins" in result.stderr
    assert not (tmp_path / "sftp_data").exists()
