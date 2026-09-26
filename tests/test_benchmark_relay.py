"""The benchmark's latency relay: it must forward a real SFTP session and add the delay."""

import time
from pathlib import Path

import benchmark
import pytest
from sftp_server import PASSWORD, USERNAME, FakeSFTPServer

from databridge.connectors.sftp import SFTPConnector
from databridge.models import SFTPConnection


def test_relay_forwards_sftp_with_added_round_trips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "served" / "files").mkdir(parents=True)
    (tmp_path / "served" / "files" / "data.csv").write_text("a\n")
    with FakeSFTPServer(tmp_path / "served") as server:
        monkeypatch.setenv("DATABRIDGE_SFTP_PORT", str(server.port))
        trusted = tmp_path / "known_hosts"
        trusted.write_text(server.known_hosts_line())
        relay = benchmark._DelayRelay(100)
        try:
            derived = tmp_path / "relay_known_hosts"
            benchmark._trust_relays(trusted, [relay.port], derived)
            connection = SFTPConnection(
                name="relayed",
                type="sftp",
                host="127.0.0.1",
                port=relay.port,
                username=USERNAME,
                password=PASSWORD,
                root="files",
            )
            started = time.monotonic()
            files = SFTPConnector(connection, derived).list_files().files
            elapsed = time.monotonic() - started
        finally:
            relay.close()
    assert files == ["data.csv"]
    # The SSH handshake, authentication, and listing need several round trips of 100 ms.
    assert elapsed >= 0.3


def test_relay_trust_requires_an_entry_for_the_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATABRIDGE_SFTP_PORT", "2299")
    trusted = tmp_path / "known_hosts"
    trusted.write_text("[example.test]:22 ssh-ed25519 AAAA\n")
    with pytest.raises(RuntimeError, match=r"no entry for \[127.0.0.1\]:2299"):
        benchmark._trust_relays(trusted, [4000], tmp_path / "derived")
