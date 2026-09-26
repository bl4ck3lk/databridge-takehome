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

from databridge.errors import DataBridgeError, ErrorCode, StartupError
from databridge.models import (
    CONNECTION_MODELS,
    Connection,
    ConnectionView,
    FailurePhase,
    TransferRecord,
    TransferRequest,
    TransferStatus,
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
        overwrite INTEGER NOT NULL CHECK (overwrite IN (0, 1)),
        status TEXT NOT NULL CHECK (status IN ('running', 'publishing', 'completed', 'failed')),
        started_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        completed_at TEXT,
        failed_at TEXT,
        bytes_copied INTEGER NOT NULL CHECK (bytes_copied >= 0),
        failure_phase TEXT,
        error_code TEXT,
        error TEXT
    )""",
    "CREATE INDEX transfers_by_start ON transfers (started_at)",
)
_INTERRUPTED = {
    "copying": (
        "The service stopped before the transfer finished; the destination is unchanged, "
        "but its staging file may remain"
    ),
    "publishing": (
        "The service stopped while publishing; the destination may already contain the new file"
    ),
}
# SQLite errors that make the file itself unusable at startup, and how each is reported.
_UNUSABLE_FILE = {
    "SQLITE_NOTADB": "is not a SQLite database",
    "SQLITE_CORRUPT": "is damaged",
}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _private_directory(directory: Path) -> None:
    """Create a missing state directory as owner-only; leave an existing directory unchanged."""
    if not directory.exists():
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)


def _change(
    connection: sqlite3.Connection,
    transfer_id: str,
    statement: str,
    parameters: tuple[object, ...],
) -> None:
    """Apply a compare-and-set state change; a record in another state is a conflict, never a
    silent no-op, and the enclosing transaction commits nothing."""
    if connection.execute(statement, parameters).rowcount != 1:
        raise DataBridgeError(
            ErrorCode.TRANSFER_STATE_CONFLICT,
            "The transfer record is not in the state this change requires; another operation "
            "already changed it",
            transfer_id,
        )


def _connection_not_found(name: str) -> DataBridgeError:
    return DataBridgeError(ErrorCode.CONNECTION_NOT_FOUND, f"Connection '{name}' not found")


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
            raise StartupError(
                f"Another DataBridge process is using the database locked by {self.path}; "
                "stop that process before starting another"
            ) from None
        except BaseException:
            os.close(descriptor)
            raise
        try:
            os.fchmod(descriptor, 0o600)
        except BaseException:
            os.close(descriptor)
            raise
        self._descriptor = descriptor
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._descriptor is not None:
            descriptor = self._descriptor
            self._descriptor = None
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)


class ConnectionStore:
    def __init__(self, path: Path, encryption_key: bytes) -> None:
        self.path = path
        self.cipher = Fernet(encryption_key)

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
        try:
            os.fchmod(descriptor, 0o600)
        finally:
            os.close(descriptor)
        try:
            with self._connect() as connection:
                tables = {
                    row["name"]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
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
                    SET status = 'failed', updated_at = :now, failed_at = :now,
                        failure_phase = 'interruption', error_code = :code,
                        error = CASE status WHEN 'publishing' THEN :publishing ELSE :copying END
                    WHERE status IN ('running', 'publishing')""",
                    {"now": _now(), "code": ErrorCode.TRANSFER_INTERRUPTED.value, **_INTERRUPTED},
                )
        except sqlite3.DatabaseError as exc:
            # Errors that the Python driver raises itself have no SQLite error name.
            problem = _UNUSABLE_FILE.get(getattr(exc, "sqlite_errorname", ""))
            if problem is None:
                raise
            raise StartupError(
                f"{self.path} {problem}; move it aside so the service can create a new database"
            ) from exc

    def _verify_existing(self, connection: sqlite3.Connection, tables: set[str]) -> None:
        # Read as bytes: a damaged value may not be UTF-8, and the driver raises when it decodes.
        metadata: dict[bytes, bytes | None] = (
            dict(
                connection.execute(
                    "SELECT CAST(key AS BLOB), CAST(value AS BLOB) FROM metadata"
                ).fetchall()
            )
            if "metadata" in tables
            else {}
        )
        version = metadata.get(b"schema_version")
        if version is None:
            raise StartupError(
                f"{self.path} has no DataBridge schema version; move it aside so the service "
                "can create a new database"
            )
        if version != SCHEMA_VERSION.encode("ascii"):
            # The stored value is quoted, so a damaged value cannot break the one-line message.
            shown = version.decode("utf-8", "replace")
            raise StartupError(
                f"{self.path} uses schema version {shown!r}, but this service needs version "
                f"{SCHEMA_VERSION!r}; move it aside so the service can create a new database"
            )
        marker = metadata.get(b"key_check")
        # DataBridge writes the marker as an ASCII Fernet token. Any other value means a damaged
        # database, not a wrong key.
        if not marker or not marker.isascii():
            raise StartupError(
                f"{self.path} has no valid DataBridge key check; move it aside so the service "
                "can create a new database"
            )
        try:
            key_matches = self.cipher.decrypt(marker) == _KEY_CHECK
        except InvalidToken:
            key_matches = False
        if not key_matches:
            raise StartupError(
                f"DATABRIDGE_ENCRYPTION_KEY does not match the database {self.path}; start with "
                "the key that created it, or move the database aside to start with no saved "
                "connections"
            )

    def create(self, item: Connection) -> ConnectionView:
        settings, ciphertext = self._columns(item)
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

    def replace(self, item: Connection) -> ConnectionView:
        """Replace every setting of an existing connection, including its type."""
        settings, ciphertext = self._columns(item)
        with self._connect() as connection:
            changed = connection.execute(
                "UPDATE connections SET type = ?, settings_json = ?, secrets_ciphertext = ? "
                "WHERE name = ?",
                (item.type, json.dumps(settings), ciphertext, item.name),
            ).rowcount
        if changed != 1:
            raise _connection_not_found(item.name)
        return self._view(item.name, item.type, settings)

    def delete(self, name: str) -> None:
        """Remove a connection; transfer records keep its name as history."""
        with self._connect() as connection:
            deleted = connection.execute("DELETE FROM connections WHERE name = ?", (name,)).rowcount
        if deleted != 1:
            raise _connection_not_found(name)

    def _columns(self, item: Connection) -> tuple[dict[str, object], str | None]:
        """Split a connection into public settings and one encrypted value for its secrets."""
        _connection_models(item.type)
        secret_fields = {
            name for name, field in type(item).model_fields.items() if field.annotation is SecretStr
        }
        settings = item.model_dump(mode="json", exclude={"name", "type", *secret_fields})
        secrets = {name: getattr(item, name).get_secret_value() for name in sorted(secret_fields)}
        ciphertext = (
            self.cipher.encrypt(json.dumps(secrets).encode()).decode("ascii") if secrets else None
        )
        return settings, ciphertext

    def get(self, name: str) -> Connection:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT type, settings_json, secrets_ciphertext FROM connections WHERE name = ?",
                (name,),
            ).fetchone()
        if row is None:
            raise _connection_not_found(name)
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
            raise _connection_not_found(name)
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

    def start_transfer(self, transfer_id: str, request: TransferRequest) -> None:
        now = _now()
        with self._recording(transfer_id) as connection:
            connection.execute(
                """INSERT INTO transfers (
                    id, source, source_file, destination, destination_file, overwrite,
                    status, started_at, updated_at, bytes_copied
                ) VALUES (?, ?, ?, ?, ?, ?, 'running', ?, ?, 0)""",
                (
                    transfer_id,
                    request.source,
                    request.source_file,
                    request.destination,
                    request.destination_file,
                    int(request.overwrite),
                    now,
                    now,
                ),
            )

    def record_progress(self, transfer_id: str, bytes_copied: int) -> None:
        self._transition(
            transfer_id,
            """UPDATE transfers SET bytes_copied = ?, updated_at = ?
            WHERE id = ? AND status = 'running'""",
            (bytes_copied, _now(), transfer_id),
        )

    def mark_publishing(self, transfer_id: str, bytes_copied: int) -> None:
        self._transition(
            transfer_id,
            """UPDATE transfers SET status = 'publishing', bytes_copied = ?, updated_at = ?
            WHERE id = ? AND status = 'running'""",
            (bytes_copied, _now(), transfer_id),
        )

    def finish_transfer(self, transfer_id: str) -> TransferRecord:
        """Complete a publishing record and return it; if it cannot be read back, nothing
        changes, so a completed record is always one the caller could report."""
        now = _now()
        with self._recording(transfer_id) as connection:
            _change(
                connection,
                transfer_id,
                """UPDATE transfers SET status = 'completed', completed_at = ?, updated_at = ?
                WHERE id = ? AND status = 'publishing'""",
                (now, now, transfer_id),
            )
            row = connection.execute(
                "SELECT * FROM transfers WHERE id = ?", (transfer_id,)
            ).fetchone()
            return TransferRecord.model_validate(dict(row))

    def fail_transfer(
        self,
        transfer_id: str,
        bytes_copied: int,
        phase: FailurePhase,
        code: ErrorCode,
        message: str,
    ) -> None:
        now = _now()
        self._transition(
            transfer_id,
            """UPDATE transfers SET status = 'failed', failed_at = ?, updated_at = ?,
                bytes_copied = ?, failure_phase = ?, error_code = ?, error = ?
            WHERE id = ? AND status IN ('running', 'publishing')""",
            (now, now, bytes_copied, phase, code.value, message, transfer_id),
        )

    def _transition(self, transfer_id: str, statement: str, parameters: tuple[object, ...]) -> None:
        with self._recording(transfer_id) as connection:
            _change(connection, transfer_id, statement, parameters)

    @contextmanager
    def _recording(self, transfer_id: str) -> Iterator[sqlite3.Connection]:
        """Write a transfer record; a database failure becomes a typed, attributable error."""
        try:
            with self._connect() as connection:
                yield connection
        except sqlite3.Error as exc:
            raise DataBridgeError(
                ErrorCode.TRANSFER_RECORD_FAILED,
                "The transfer record could not be saved "
                f"({getattr(exc, 'sqlite_errorname', type(exc).__name__)})",
                transfer_id,
            ) from exc

    def get_transfer(self, transfer_id: str) -> TransferRecord:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM transfers WHERE id = ?", (transfer_id,)
            ).fetchone()
        if row is None:
            raise DataBridgeError(ErrorCode.TRANSFER_NOT_FOUND, "Transfer not found")
        return TransferRecord.model_validate(dict(row))

    def list_transfers(self, status: TransferStatus | None, limit: int) -> list[TransferRecord]:
        """Return the newest transfers first, optionally only those in one status."""
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT * FROM transfers WHERE :status IS NULL OR status = :status
                ORDER BY started_at DESC, rowid DESC LIMIT :limit""",
                {"status": status, "limit": limit},
            ).fetchall()
        return [TransferRecord.model_validate(dict(row)) for row in rows]
