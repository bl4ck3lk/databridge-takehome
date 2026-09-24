"""Select a file connector when opening a stored connection."""

from pathlib import Path

from databridge.connectors.base import Connector
from databridge.connectors.local import LocalConnector
from databridge.connectors.sftp import SFTPConnector
from databridge.models import LocalConnection, SFTPConnection


def connector_for(
    connection: LocalConnection | SFTPConnection, known_hosts_path: Path
) -> Connector:
    if isinstance(connection, LocalConnection):
        return LocalConnector(Path(connection.path))
    return SFTPConnector(connection, known_hosts_path)
