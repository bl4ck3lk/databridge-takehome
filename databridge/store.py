"""SQLite connection persistence with encryption at the write boundary."""

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from cryptography.fernet import Fernet, InvalidToken

from databridge.errors import DataBridgeError
from databridge.models import (
    ConnectionView,
    LocalConnection,
    LocalConnectionView,
    SFTPConnection,
    SFTPConnectionView,
)

_KEY_CHECK = b"databridge-key-check-v1"


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
            raise DataBridgeError("CONNECTION_EXISTS", "Connection name already exists") from exc
        return self._view(item.name, item.type, settings)

    def get(self, name: str) -> LocalConnection | SFTPConnection:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT name, type, settings_json, password_ciphertext "
                "FROM connections WHERE name = ?",
                (name,),
            ).fetchone()
        if row is None:
            raise DataBridgeError("CONNECTION_NOT_FOUND", "Connection not found")
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
            raise DataBridgeError("CONNECTION_NOT_FOUND", "Connection not found")
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
