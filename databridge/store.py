"""SQLite connection persistence with encryption at the write boundary."""

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from cryptography.fernet import Fernet, InvalidToken

from databridge.errors import DataBridgeError, ErrorCode
from databridge.models import (
    ConnectionView,
    LocalConnection,
    LocalConnectionView,
    SFTPConnection,
    SFTPConnectionView,
    TransferRecord,
    TransferRequest,
)

_KEY_CHECK = b"databridge-key-check-v1"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS connections (
                    name TEXT PRIMARY KEY,
                    type TEXT NOT NULL CHECK (type IN ('local', 'sftp')),
                    settings_json TEXT NOT NULL,
                    password_ciphertext TEXT
                )"""
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS transfers (
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
                )"""
            )
            marker = connection.execute(
                "SELECT value FROM metadata WHERE key = 'key_check'"
            ).fetchone()
            if marker is None:
                connection.execute(
                    "INSERT INTO metadata (key, value) VALUES ('key_check', ?)",
                    (self.cipher.encrypt(_KEY_CHECK).decode("ascii"),),
                )
            else:
                try:
                    value = self.cipher.decrypt(marker["value"].encode("ascii"))
                except InvalidToken as exc:
                    raise RuntimeError(
                        "DATABRIDGE_ENCRYPTION_KEY does not match the database"
                    ) from exc
                if value != _KEY_CHECK:
                    raise RuntimeError("DATABRIDGE_ENCRYPTION_KEY does not match the database")
            connection.execute(
                """UPDATE transfers
                SET status = 'failed', failed_at = ?, failure_phase = 'interruption',
                    error = 'Service stopped before transfer completed'
                WHERE status = 'running'""",
                (_now(),),
            )

    def create(self, item: LocalConnection | SFTPConnection) -> ConnectionView:
        if isinstance(item, LocalConnection):
            settings = {"path": item.path}
            ciphertext = None
        else:
            settings = item.model_dump(exclude={"name", "type", "password"})
            ciphertext = self.cipher.encrypt(item.password.get_secret_value().encode()).decode(
                "ascii"
            )
        try:
            with self._connect() as connection:
                connection.execute(
                    "INSERT INTO connections (name, type, settings_json, password_ciphertext) "
                    "VALUES (?, ?, ?, ?)",
                    (item.name, item.type, json.dumps(settings), ciphertext),
                )
        except sqlite3.IntegrityError as exc:
            raise DataBridgeError(
                ErrorCode.CONNECTION_EXISTS, "Connection name already exists"
            ) from exc
        return self._view(item.name, item.type, settings)

    def get(self, name: str) -> LocalConnection | SFTPConnection:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT name, type, settings_json, password_ciphertext "
                "FROM connections WHERE name = ?",
                (name,),
            ).fetchone()
        if row is None:
            raise DataBridgeError(ErrorCode.CONNECTION_NOT_FOUND, "Connection not found")
        settings = json.loads(row["settings_json"])
        if row["type"] == "local":
            return LocalConnection(name=name, type="local", **settings)
        try:
            password = self.cipher.decrypt(row["password_ciphertext"].encode()).decode()
        except InvalidToken as exc:
            raise RuntimeError("Stored credential cannot be decrypted") from exc
        return SFTPConnection(name=name, type="sftp", password=password, **settings)

    def get_public(self, name: str) -> ConnectionView:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT name, type, settings_json FROM connections WHERE name = ?", (name,)
            ).fetchone()
        if row is None:
            raise DataBridgeError(ErrorCode.CONNECTION_NOT_FOUND, "Connection not found")
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
        if kind == "local":
            return LocalConnectionView(name=name, type="local", **settings)
        return SFTPConnectionView(name=name, type="sftp", **settings)

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
        with self._connect() as connection:
            connection.execute(
                """UPDATE transfers SET status = 'completed', completed_at = ?, bytes_copied = ?
                WHERE id = ? AND status = 'running'""",
                (_now(), bytes_copied, transfer_id),
            )
        return self.get_transfer(transfer_id)

    def fail_transfer(
        self, transfer_id: str, bytes_copied: int, phase: str, message: str
    ) -> TransferRecord:
        with self._connect() as connection:
            connection.execute(
                """UPDATE transfers SET status = 'failed', failed_at = ?, bytes_copied = ?,
                    failure_phase = ?, error = ?
                WHERE id = ? AND status = 'running'""",
                (_now(), bytes_copied, phase, message, transfer_id),
            )
        return self.get_transfer(transfer_id)

    def get_transfer(self, transfer_id: str) -> TransferRecord:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM transfers WHERE id = ?", (transfer_id,)
            ).fetchone()
        if row is None:
            raise DataBridgeError(ErrorCode.TRANSFER_NOT_FOUND, "Transfer not found")
        return TransferRecord.model_validate(dict(row))
