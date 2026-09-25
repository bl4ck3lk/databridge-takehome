"""Private, size-bounded JSON-line request log."""

import json
import os
from pathlib import Path
from typing import Any, TextIO

MAX_LOG_BYTES = 10 * 1024 * 1024
LOG_BACKUPS = 3


class RequestLog:
    """Append one JSON event per request, rotate by size, and keep every file owner-only."""

    def __init__(
        self, path: Path, *, max_bytes: int = MAX_LOG_BYTES, backups: int = LOG_BACKUPS
    ) -> None:
        if max_bytes < 1 or backups < 0:
            raise ValueError("max_bytes must be positive and backups must not be negative")
        self.path = path
        self.max_bytes = max_bytes
        self.backups = backups
        self._stream: TextIO | None = None
        self._size = 0

    def open(self) -> None:
        descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        os.fchmod(descriptor, 0o600)
        self._size = os.fstat(descriptor).st_size
        self._stream = os.fdopen(descriptor, "a", encoding="utf-8")

    def write(self, event: dict[str, Any]) -> None:
        line = json.dumps(event, separators=(",", ":")) + "\n"
        size = len(line.encode("utf-8"))
        if self._size and self._size + size > self.max_bytes:
            self._rotate()
        if self._stream is None:
            raise RuntimeError("Request log is not open")
        self._stream.write(line)
        self._stream.flush()
        self._size += size

    def close(self) -> None:
        if self._stream is not None:
            self._stream.close()
            self._stream = None

    def _rotate(self) -> None:
        self.close()
        if self.backups == 0:
            self.path.unlink(missing_ok=True)
        for number in range(self.backups, 0, -1):
            source = self.path if number == 1 else self._backup(number - 1)
            if source.exists():
                os.replace(source, self._backup(number))
        self.open()

    def _backup(self, number: int) -> Path:
        return self.path.with_name(f"{self.path.name}.{number}")
