"""The fixture helper reads the port and trust file that quickstart wrote, and never guesses."""

from pathlib import Path

import pytest
import sftp_fixture


def test_fixture_settings_have_no_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABRIDGE_SFTP_PORT", raising=False)
    monkeypatch.delenv("DATABRIDGE_KNOWN_HOSTS", raising=False)

    with pytest.raises(SystemExit) as port:
        sftp_fixture.port()
    with pytest.raises(SystemExit) as known_hosts:
        sftp_fixture.known_hosts()

    assert str(port.value) == (
        "DATABRIDGE_SFTP_PORT is not set; run make quickstart, which writes it to .env"
    )
    assert str(known_hosts.value) == (
        "DATABRIDGE_KNOWN_HOSTS is not set; run make quickstart, which writes it to .env"
    )


def test_trust_file_must_exist(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    missing = tmp_path / "known_hosts"
    monkeypatch.setenv("DATABRIDGE_KNOWN_HOSTS", str(missing))

    with pytest.raises(SystemExit) as known_hosts:
        sftp_fixture.known_hosts()

    assert str(known_hosts.value) == (
        f"DATABRIDGE_KNOWN_HOSTS names {missing}, which is not a file; run make quickstart, "
        "which trusts the local SFTP fixture"
    )


def test_fixture_settings_come_from_the_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    trusted = tmp_path / "known_hosts"
    trusted.write_text("[127.0.0.1]:2299 ssh-ed25519 AAAA\n")
    monkeypatch.setenv("DATABRIDGE_SFTP_PORT", "2299")
    monkeypatch.setenv("DATABRIDGE_KNOWN_HOSTS", str(trusted))

    assert sftp_fixture.port() == 2299
    assert sftp_fixture.known_hosts() == trusted
    assert sftp_fixture.connection("remote")["port"] == 2299
