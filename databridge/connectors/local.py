"""Flat-root local filesystem connector."""

import os
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Iterator

from databridge.connectors.base import (
    MAX_LIST_RESULTS,
    MAX_LIST_SCAN,
    Listing,
    is_stage_name,
    validate_filename,
)
from databridge.errors import DataBridgeError, ErrorCode


class LocalConnector:
    def __init__(self, root: Path) -> None:
        self.root = root

    def _file(self, filename: str) -> Path:
        validate_filename(filename)
        candidate = self.root / filename
        if not candidate.resolve().is_relative_to(self.root.resolve()):
            raise DataBridgeError(
                ErrorCode.INVALID_FILENAME, "Filename escapes the connection root"
            )
        return candidate

    def list_files(self) -> Listing:
        files: list[str] = []
        scanned = 0
        try:
            with os.scandir(self.root) as entries:
                for entry in entries:
                    scanned += 1
                    if scanned > MAX_LIST_SCAN:
                        return Listing(sorted(files), True)
                    if is_stage_name(entry.name) or not entry.is_file(follow_symlinks=False):
                        continue
                    try:
                        validate_filename(entry.name)
                    except DataBridgeError:
                        continue
                    if len(files) >= MAX_LIST_RESULTS:
                        return Listing(sorted(files), True)
                    files.append(entry.name)
        except OSError as exc:
            raise DataBridgeError(
                ErrorCode.CONNECTION_ROOT_UNAVAILABLE, "Local directory unavailable"
            ) from exc
        return Listing(sorted(files), False)

    @contextmanager
    def read(self, filename: str) -> Iterator[BinaryIO]:
        path = self._file(filename)
        try:
            stream = path.open("rb")
        except FileNotFoundError as exc:
            raise DataBridgeError(ErrorCode.FILE_NOT_FOUND, "File not found") from exc
        except OSError as exc:
            raise DataBridgeError(ErrorCode.LOCAL_IO_ERROR, "Cannot read local file") from exc
        try:
            yield stream
        finally:
            stream.close()

    @contextmanager
    def write(self, filename: str, transfer_id: str, overwrite: bool) -> Iterator[BinaryIO]:
        destination = self._file(filename)
        if destination.exists() and not overwrite:
            raise DataBridgeError(ErrorCode.DESTINATION_EXISTS, "Destination file already exists")
        stage = self.root / f".{filename}.databridge-{transfer_id}.part"
        try:
            stream = stage.open("xb")
        except OSError as exc:
            raise DataBridgeError(
                ErrorCode.LOCAL_IO_ERROR, "Cannot open destination staging file"
            ) from exc
        try:
            try:
                yield stream
            finally:
                stream.close()
            try:
                if overwrite:
                    os.replace(stage, destination)
                else:
                    os.link(stage, destination)
            except FileExistsError as exc:
                raise DataBridgeError(
                    ErrorCode.DESTINATION_EXISTS, "Destination file already exists"
                ) from exc
            except OSError as exc:
                raise DataBridgeError(
                    ErrorCode.LOCAL_IO_ERROR, "Cannot publish local file"
                ) from exc
        finally:
            stage.unlink(missing_ok=True)
