"""Connector registry: each connection type names how to open and prepare its connector."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from databridge.connectors.base import Connector, ConnectorContext
from databridge.connectors.local import LocalConnector
from databridge.connectors.sftp import SFTPConnector
from databridge.models import CONNECTION_MODELS, Connection


@dataclass(frozen=True)
class ConnectorType:
    open: Callable[[Connection, ConnectorContext], Connector]
    prepare: Callable[[Connection], Connection]


CONNECTOR_TYPES: Mapping[str, ConnectorType] = MappingProxyType(
    {
        "local": ConnectorType(open=LocalConnector.open, prepare=LocalConnector.prepare),
        "sftp": ConnectorType(open=SFTPConnector.open, prepare=SFTPConnector.prepare),
    }
)
if set(CONNECTOR_TYPES) != set(CONNECTION_MODELS):
    raise RuntimeError("Every connection model needs exactly one registered connector type")


def _connector_type(kind: str) -> ConnectorType:
    try:
        return CONNECTOR_TYPES[kind]
    except KeyError:
        raise RuntimeError(f"Unsupported connection type {kind!r}") from None


def open_connector(connection: Connection, context: ConnectorContext) -> Connector:
    return _connector_type(connection.type).open(connection, context)


def prepare_connection(connection: Connection) -> Connection:
    """Validate and normalize type-specific settings before a connection is stored."""
    return _connector_type(connection.type).prepare(connection)
