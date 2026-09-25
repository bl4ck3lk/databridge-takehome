"""The same observable file contract exercised on every production connector."""

from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from sftp_server import PASSWORD, USERNAME, FakeSFTPServer
from support import read_all

from databridge.connectors.base import CHUNK_SIZE, Connector
from databridge.connectors.local import LocalConnector
from databridge.connectors.sftp import SFTPConnector
from databridge.errors import DataBridgeError
from databridge.models import SFTPConnection

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _sftp(port: int, root: str, known_hosts: Path) -> SFTPConnector:
    connection = SFTPConnection(
        name="remote",
        type="sftp",
        host="127.0.0.1",
        port=port,
        username=USERNAME,
        password=PASSWORD,
        root=root,
    )
    return SFTPConnector(connection, known_hosts)


@pytest.fixture(
    params=["local", "sftp-loopback", pytest.param("sftp-docker", marks=pytest.mark.integration)]
)
def backend(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[tuple[Connector, Path]]:
    """Yield a connector and the host directory that holds its root."""
    if request.param == "local":
        yield LocalConnector(tmp_path), tmp_path
    elif request.param == "sftp-loopback":
        (tmp_path / "files").mkdir()
        with FakeSFTPServer(tmp_path) as server:
            known_hosts = tmp_path / "known_hosts"
            known_hosts.write_text(server.known_hosts_line())
            yield _sftp(server.port, "files", known_hosts), tmp_path / "files"
    else:
        assert (PROJECT_ROOT / "known_hosts").is_file()
        yield _sftp(2222, "data", PROJECT_ROOT / "known_hosts"), PROJECT_ROOT / "sftp_data"


@pytest.mark.parametrize("size", [0, CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 1])
def test_connector_contract_at_chunk_boundaries(backend: tuple[Connector, Path], size: int) -> None:
    connector, host_root = backend
    filename = f"contract-{uuid4().hex}.bin"
    host_file = host_root / filename
    payload = b"a" * size
    try:
        with connector.write(filename, str(uuid4()), overwrite=False) as writer:
            writer.write(payload)
        with connector.read(filename) as reader:
            assert read_all(reader) == payload
        assert filename in connector.list_files().files

        with pytest.raises(DataBridgeError) as collision:
            with connector.write(filename, str(uuid4()), overwrite=False):
                pass
        assert collision.value.code == "DESTINATION_EXISTS"

        with connector.write(filename, str(uuid4()), overwrite=True) as writer:
            writer.write(b"replacement")
        with pytest.raises(ValueError, match="injected failure"):
            with connector.write(filename, str(uuid4()), overwrite=True) as writer:
                writer.write(b"x" * CHUNK_SIZE)
                raise ValueError("injected failure")
        with connector.read(filename) as reader:
            assert read_all(reader) == b"replacement"
        assert not list(host_root.glob(".databridge-*"))

        with pytest.raises(DataBridgeError) as escape:
            with connector.read("../outside.bin"):
                pass
        assert escape.value.code == "INVALID_FILENAME"
    finally:
        host_file.unlink(missing_ok=True)
