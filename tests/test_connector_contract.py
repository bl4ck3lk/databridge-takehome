"""The same observable file contract exercised on both production connectors."""

from pathlib import Path
from uuid import uuid4

import pytest
from support import read_all

from databridge.connectors.base import CHUNK_SIZE
from databridge.connectors.local import LocalConnector
from databridge.connectors.sftp import SFTPConnector
from databridge.errors import DataBridgeError
from databridge.models import SFTPConnection

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("size", [0, CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 1])
@pytest.mark.parametrize("backend", ["local", pytest.param("sftp", marks=pytest.mark.integration)])
def test_connector_contract_at_chunk_boundaries(backend: str, size: int, tmp_path: Path) -> None:
    filename = f"contract-{uuid4().hex}.bin"
    if backend == "local":
        connector = LocalConnector(tmp_path)
        host_file = tmp_path / filename
    else:
        assert (PROJECT_ROOT / "known_hosts").is_file()
        connector = SFTPConnector(
            SFTPConnection(
                name="remote",
                type="sftp",
                host="127.0.0.1",
                port=2222,
                username="testuser",
                password="testpass",
                root="data",
            ),
            PROJECT_ROOT / "known_hosts",
        )
        host_file = PROJECT_ROOT / "sftp_data" / filename
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
        assert not list(host_file.parent.glob(".databridge-*.part"))

        with pytest.raises(DataBridgeError) as escape:
            with connector.read("../outside.bin"):
                pass
        assert escape.value.code == "INVALID_FILENAME"
    finally:
        host_file.unlink(missing_ok=True)
