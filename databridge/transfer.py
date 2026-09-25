"""Connector-neutral, bounded-memory transfer orchestration."""

import logging
from collections.abc import Callable
from uuid import uuid4

from databridge.connectors.base import CHUNK_SIZE, Connector, validate_filename
from databridge.errors import DataBridgeError, ErrorCode
from databridge.models import LocalConnection, SFTPConnection, TransferRecord, TransferRequest
from databridge.store import ConnectionStore

logger = logging.getLogger(__name__)
ConnectorFactory = Callable[[LocalConnection | SFTPConnection], Connector]


class TransferService:
    def __init__(self, store: ConnectionStore, connector_factory: ConnectorFactory) -> None:
        self.store = store
        self.connector_factory = connector_factory

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

        transfer_id = str(uuid4())
        self.store.start_transfer(transfer_id, request)
        copied = 0
        phase = "source_connection"
        try:
            source = self.connector_factory(self.store.get(request.source))
            phase = "destination_connection"
            destination = self.connector_factory(self.store.get(request.destination))
            phase = "source_read"
            with source.read(request.source_file) as reader:
                phase = "destination_write"
                with destination.write(
                    request.destination_file, transfer_id, request.overwrite
                ) as writer:
                    while True:
                        phase = "source_read"
                        chunk = reader.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        phase = "destination_write"
                        written = writer.write(chunk)
                        if written is not None and written != len(chunk):
                            raise DataBridgeError(
                                ErrorCode.DESTINATION_WRITE_FAILED,
                                "Destination accepted a partial chunk",
                            )
                        copied += len(chunk)
                    phase = "finalization"
            return self.store.finish_transfer(transfer_id, copied)
        except DataBridgeError as exc:
            self.store.fail_transfer(transfer_id, copied, phase, exc.message)
            raise DataBridgeError(exc.code, exc.message, transfer_id) from exc
        except Exception as exc:
            if phase == "source_read":
                code, message = ErrorCode.SOURCE_READ_FAILED, "Cannot read source file"
            elif phase == "destination_write":
                code, message = ErrorCode.DESTINATION_WRITE_FAILED, "Cannot write destination file"
            else:
                code, message = ErrorCode.TRANSFER_INTERNAL_ERROR, "Transfer could not be completed"
            self.store.fail_transfer(transfer_id, copied, phase, message)
            logger.error(
                "transfer_failed id=%s phase=%s exception_type=%s",
                transfer_id,
                phase,
                type(exc).__name__,
            )
            raise DataBridgeError(code, message, transfer_id) from exc
