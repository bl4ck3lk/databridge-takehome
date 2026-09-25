"""SQLite persistence for connections and transfer records, with secrets encrypted at rest."""

import fcntl
import json
import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Self

from cryptography.fernet import Fernet, InvalidToken
from pydantic import SecretStr

from databridge.errors import DataBridgeError, ErrorCode
from databridge.models import (
    CONNECTION_MODELS,
    Connection,
    ConnectionView,
    TransferRecord,
    TransferRequest,
)

SCHEMA_VERSION = "1"
_KEY_CHECK = b"databridge-key-check-v1"
_SCHEMA = (
    "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    """CREATE TABLE connections (
        name TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        settings_json TEXT NOT NULL,
        secrets_ciphertext TEXT
    )""",
    """CREATE TABLE transfers (
        id TEXT PRIMARY KEY,
        source TEXT NOT NULL,
        source_file TEXT NOT NULL,
        destination TEXT NOT NULL,
        destination_file TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
        started_at TEXT NOT NULL,
        completed_at TEXT,
        failed_at TEXT,
        bytes_copied INTEGER NOT NULL DEFAULT 0,
        failure_phase TEXT,
        error TEXT
    )""",
)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _private_directory(directory: Path) -> None:
    """Create a missing state directory as owner-only; leave an existing directory unchanged."""
    if not directory.exists():
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)


def _connection_models(kind: str) -> tuple[type[Connection], type[ConnectionView]]:
    try:
        return CONNECTION_MODELS[kind]
    except KeyError:
        raise RuntimeError(f"Unsupported connection type {kind!r}") from None


class DatabaseOwnerLock:
    """Hold an exclusive advisory lock so exactly one service process owns the database.

    Startup recovery marks every running transfer as interrupted, which is correct only for the
    process that owns the database. The operating system releases the lock when the process
    exits, so a crash never leaves the database unusable.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._descriptor: int | None = None

    def __enter__(self) -> Self:
        _private_directory(self.path.parent)
        descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(descriptor)
            raise RuntimeError(
                f"Another DataBridge process is using the database locked by {self.path}; "
                "stop that process before starting another"
            ) from None
        os.fchmod(descriptor, 0o600)
        self._descriptor = descriptor
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._descriptor is not None:
            fcntl.flock(self._descriptor, fcntl.LOCK_UN)
            os.close(self._descriptor)
            self._descriptor = None


class ConnectionStore:
    def __init__(self, path: Path, encryption_key: bytes) -> None:
        self.path = path
        try:
            self.cipher = Fernet(encryption_key)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("DATABRIDGE_ENCRYPTION_KEY is not a valid Fernet key") from exc

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA busy_timeout=5000")
            yield connection
            connection.commit()
        finally:
            connection.close()

    def initialize(self) -> None:
        _private_directory(self.path.parent)
        descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        os.fchmod(descriptor, 0o600)
        os.close(descriptor)
        with self._connect() as connection:
            tables = {
                row["name"]
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            if tables:
                self._verify_existing(connection, tables)
            else:
                for statement in _SCHEMA:
                    connection.execute(statement)
                connection.executemany(
                    "INSERT INTO metadata (key, value) VALUES (?, ?)",
                    (
                        ("schema_version", SCHEMA_VERSION),
                        ("key_check", self.cipher.encrypt(_KEY_CHECK).decode("ascii")),
                    ),
                )
            connection.execute(
                """UPDATE transfers
                SET status = 'failed', failed_at = ?, failure_phase = 'interruption',
                    error = 'Service stopped before transfer completed'
                WHERE status = 'running'""",
                (_now(),),
            )

    def _verify_existing(self, connection: sqlite3.Connection, tables: set[str]) -> None:
        metadata = (
            dict(connection.execute("SELECT key, value FROM metadata").fetchall())
            if "metadata" in tables
            else {}
        )
        version = metadata.get("schema_version")
        if version is None:
            raise RuntimeError(
                f"{self.path} has no DataBridge schema version; move it aside so the service "
                "can create a new database"
            )
        if version != SCHEMA_VERSION:
            raise RuntimeError(
                f"{self.path} uses schema version {version}, but this service needs version "
                f"{SCHEMA_VERSION}; move it aside so the service can create a new database"
            )
        try:
            marker = self.cipher.decrypt(metadata["key_check"].encode("ascii"))
        except (KeyError, InvalidToken) as exc:
            raise RuntimeError("DATABRIDGE_ENCRYPTION_KEY does not match the database") from exc
        if marker != _KEY_CHECK:
            raise RuntimeError("DATABRIDGE_ENCRYPTION_KEY does not match the database")

    def create(self, item: Connection) -> ConnectionView:
        _connection_models(item.type)
        secret_fields = {
            name for name, field in type(item).model_fields.items() if field.annotation is SecretStr
        }
        settings = item.model_dump(mode="json", exclude={"name", "type", *secret_fields})
        secrets = {name: getattr(item, name).get_secret_value() for name in sorted(secret_fields)}
        ciphertext = (
            self.cipher.encrypt(json.dumps(secrets).encode()).decode("ascii") if secrets else None
        )
        try:
            with self._connect() as connection:
                connection.execute(
                    "INSERT INTO connections (name, type, settings_json, secrets_ciphertext) "
                    "VALUES (?, ?, ?, ?)",
                    (item.name, item.type, json.dumps(settings), ciphertext),
                )
        except sqlite3.IntegrityError as exc:
            if exc.sqlite_errorname != "SQLITE_CONSTRAINT_PRIMARYKEY":
                raise
            raise DataBridgeError(
                ErrorCode.CONNECTION_EXISTS, f"Connection '{item.name}' already exists"
            ) from exc
        return self._view(item.name, item.type, settings)

    def get(self, name: str) -> Connection:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT type, settings_json, secrets_ciphertext FROM connections WHERE name = ?",
                (name,),
            ).fetchone()
        if row is None:
            raise DataBridgeError(ErrorCode.CONNECTION_NOT_FOUND, f"Connection '{name}' not found")
        input_model, _view_model = _connection_models(row["type"])
        secrets: dict[str, str] = {}
        if row["secrets_ciphertext"] is not None:
            try:
                secrets = json.loads(self.cipher.decrypt(row["secrets_ciphertext"].encode()))
            except InvalidToken as exc:
                raise RuntimeError("Stored credential cannot be decrypted") from exc
        return input_model.model_validate(
            {"name": name, "type": row["type"], **json.loads(row["settings_json"]), **secrets}
        )

    def get_public(self, name: str) -> ConnectionView:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT name, type, settings_json FROM connections WHERE name = ?", (name,)
            ).fetchone()
        if row is None:
            raise DataBridgeError(ErrorCode.CONNECTION_NOT_FOUND, f"Connection '{name}' not found")
        return self._view(row["name"], row["type"], json.loads(row["settings_json"]))

    def list_public(self) -> list[ConnectionView]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT name, type, settings_json FROM connections ORDER BY name"
            ).fetchall()
        return [
            self._view(row["name"], row["type"], json.loads(row["settings_json"])) for row in rows
        ]

    @staticmethod
    def _view(name: str, kind: str, settings: dict[str, object]) -> ConnectionView:
        _input_model, view_model = _connection_models(kind)
        return view_model.model_validate({"name": name, "type": kind, **settings})

    def start_transfer(self, transfer_id: str, request: TransferRequest) -> TransferRecord:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO transfers (
                    id, source, source_file, destination, destination_file,
                    status, started_at, bytes_copied
                ) VALUES (?, ?, ?, ?, ?, 'running', ?, 0)""",
                (
                    transfer_id,
                    request.source,
                    request.source_file,
                    request.destination,
                    request.destination_file,
                    _now(),
                ),
            )
        return self.get_transfer(transfer_id)

    def finish_transfer(self, transfer_id: str, bytes_copied: int) -> TransferRecord:
        self._transition(
            transfer_id,
            """UPDATE transfers SET status = 'completed', completed_at = ?, bytes_copied = ?
            WHERE id = ? AND status = 'running'""",
            (_now(), bytes_copied, transfer_id),
        )
        return self.get_transfer(transfer_id)

    def fail_transfer(
        self, transfer_id: str, bytes_copied: int, phase: str, message: str
    ) -> TransferRecord:
        self._transition(
            transfer_id,
            """UPDATE transfers SET status = 'failed', failed_at = ?, bytes_copied = ?,
                failure_phase = ?, error = ?
            WHERE id = ? AND status = 'running'""",
            (_now(), bytes_copied, phase, message, transfer_id),
        )
        return self.get_transfer(transfer_id)

    def _transition(self, transfer_id: str, statement: str, parameters: tuple[object, ...]) -> None:
        """Apply a compare-and-set state change; a record that already left `running` is a
        conflict, never a silent no-op."""
        with self._connect() as connection:
            changed = connection.execute(statement, parameters).rowcount
        if changed != 1:
            raise DataBridgeError(
                ErrorCode.TRANSFER_STATE_CONFLICT,
                "The transfer record is no longer running; another operation already finalized it",
                transfer_id,
            )

    def get_transfer(self, transfer_id: str) -> TransferRecord:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM transfers WHERE id = ?", (transfer_id,)
            ).fetchone()
        if row is None:
            raise DataBridgeError(ErrorCode.TRANSFER_NOT_FOUND, "Transfer not found")
        return TransferRecord.model_validate(dict(row))
