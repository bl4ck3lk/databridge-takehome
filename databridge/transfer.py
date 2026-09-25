"""Connector-neutral, bounded-memory transfer orchestration with a phase-level record."""

import logging
import threading
import time
from collections.abc import Callable
from typing import NoReturn, Protocol
from uuid import uuid4

from databridge.connectors.base import (
    CHUNK_SIZE,
    ByteSink,
    ByteSource,
    Connector,
    validate_filename,
)
from databridge.errors import DataBridgeError, ErrorCode
from databridge.models import Connection, FailurePhase, TransferRecord, TransferRequest

logger = logging.getLogger(__name__)
ConnectorFactory = Callable[[Connection], Connector]
MAX_CONCURRENT_TRANSFERS = 4
"""Transfers that may copy at once, so worker threads stay free for every other request."""
PROGRESS_INTERVAL = 1.0
"""Seconds between persisted progress checkpoints."""
_SOURCE_PHASES = frozenset({"source_lookup", "source_open", "source_read"})
_UNATTRIBUTED = frozenset(
    {
        ErrorCode.TRANSFER_INTERNAL_ERROR,
        ErrorCode.TRANSFER_RECORD_FAILED,
        ErrorCode.TRANSFER_STATE_CONFLICT,
    }
)
_CONTRACT_VIOLATIONS: dict[str, tuple[ErrorCode, str]] = {
    "source_read": (ErrorCode.SOURCE_READ_FAILED, "Cannot read the source file"),
    "destination_write": (ErrorCode.DESTINATION_WRITE_FAILED, "Cannot write the destination file"),
}


class TransferLedger(Protocol):
    """The persistence a transfer needs: connection lookup and its own record."""

    def get(self, name: str) -> Connection: ...

    def start_transfer(self, transfer_id: str, request: TransferRequest) -> TransferRecord: ...

    def record_progress(self, transfer_id: str, bytes_copied: int) -> None: ...

    def mark_publishing(self, transfer_id: str, bytes_copied: int) -> None: ...

    def finish_transfer(self, transfer_id: str) -> TransferRecord: ...

    def fail_transfer(
        self,
        transfer_id: str,
        bytes_copied: int,
        phase: FailurePhase,
        code: ErrorCode,
        message: str,
    ) -> TransferRecord: ...


class TransferService:
    def __init__(
        self,
        ledger: TransferLedger,
        connector_factory: ConnectorFactory,
        *,
        max_concurrent: int = MAX_CONCURRENT_TRANSFERS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.ledger = ledger
        self.connector_factory = connector_factory
        self.clock = clock
        self._capacity = max_concurrent
        self._slots = threading.BoundedSemaphore(max_concurrent)

    def run(self, request: TransferRequest) -> TransferRecord:
        validate_filename(request.source_file)
        validate_filename(request.destination_file)
        if (
            request.source == request.destination
            and request.source_file == request.destination_file
        ):
            raise DataBridgeError(
                ErrorCode.SAME_FILE, "Source and destination identify the same file"
            )
        if not self._slots.acquire(blocking=False):
            raise DataBridgeError(
                ErrorCode.TRANSFER_CAPACITY_EXCEEDED,
                f"{self._capacity} transfers are already running; retry after one finishes",
            )
        try:
            return _Transfer(self, request).run()
        finally:
            self._slots.release()


class _Transfer:
    """One transfer: its record, the phase in progress, and the bytes copied so far."""

    def __init__(self, service: TransferService, request: TransferRequest) -> None:
        self.ledger = service.ledger
        self.connector_factory = service.connector_factory
        self.clock = service.clock
        self.request = request
        self.id = str(uuid4())
        self.phase: FailurePhase = "source_lookup"
        self.copied = 0
        self.checkpoint = 0.0

    def run(self) -> TransferRecord:
        try:
            self.ledger.start_transfer(self.id, self.request)
        except DataBridgeError as exc:
            raise DataBridgeError(exc.code, exc.message) from exc  # no record exists to cite
        self.checkpoint = self.clock()
        published = False
        try:
            source = self._connector(self.request.source)
            self.phase = "destination_lookup"
            destination = self._connector(self.request.destination)
            self.phase = "source_open"
            with source.read(self.request.source_file) as reader:
                self.phase = "destination_open"
                with destination.write(
                    self.request.destination_file, self.id, self.request.overwrite
                ) as writer:
                    self._copy(reader, writer)
                    self.phase = "publication"
                    self.ledger.mark_publishing(self.id, self.copied)
                published = True
        except Exception as exc:
            if not published:
                self._fail(exc)
            # Every byte is published; only releasing the source failed.
            logger.warning(
                "transfer_source_release_failed id=%s exception_type=%s",
                self.id,
                type(exc).__name__,
            )
        return self._finish()

    def _connector(self, name: str) -> Connector:
        return self.connector_factory(self.ledger.get(name))

    def _copy(self, reader: ByteSource, writer: ByteSink) -> None:
        while True:
            self.phase = "source_read"
            chunk = reader.read(CHUNK_SIZE)
            if not chunk:
                return
            self.phase = "destination_write"
            writer.write(chunk)
            self.copied += len(chunk)
            self._checkpoint()

    def _checkpoint(self) -> None:
        now = self.clock()
        if now - self.checkpoint < PROGRESS_INTERVAL:
            return
        self.checkpoint = now
        try:
            self.ledger.record_progress(self.id, self.copied)
        except DataBridgeError as exc:
            # Progress is informational; the final record still states the outcome.
            logger.warning("transfer_progress_not_saved id=%s code=%s", self.id, exc.code)

    def _finish(self) -> TransferRecord:
        try:
            return self.ledger.finish_transfer(self.id)
        except DataBridgeError as exc:
            raise DataBridgeError(
                ErrorCode.TRANSFER_RECORD_FAILED,
                f"'{self.request.destination_file}' was published, but its transfer record "
                f"could not be completed and stays 'publishing': {exc.message}",
                self.id,
            ) from exc

    def _fail(self, exc: Exception) -> NoReturn:
        if isinstance(exc, DataBridgeError):
            code, message = exc.code, exc.message
        else:
            code, message = _CONTRACT_VIOLATIONS.get(
                self.phase, (ErrorCode.TRANSFER_INTERNAL_ERROR, "The transfer could not finish")
            )
            logger.error(
                "transfer_failed id=%s phase=%s exception_type=%s",
                self.id,
                self.phase,
                type(exc).__name__,
            )
        if code not in _UNATTRIBUTED:
            message = self._attributed(code, message)
        try:
            self.ledger.fail_transfer(self.id, self.copied, self.phase, code, message)
        except DataBridgeError as record_error:
            logger.error("transfer_failure_not_saved id=%s code=%s", self.id, record_error.code)
        raise DataBridgeError(code, message, self.id) from exc

    def _attributed(self, code: ErrorCode, message: str) -> str:
        """Name the side and the connection behind a failure."""
        side = "source" if self.phase in _SOURCE_PHASES else "destination"
        name = self.request.source if side == "source" else self.request.destination
        if code == ErrorCode.CONNECTION_NOT_FOUND:
            return f"The {side} connection '{name}' does not exist"
        return f"{side.capitalize()} connection '{name}': {message}"
