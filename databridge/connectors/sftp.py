"""SFTP connector with explicit host trust and per-operation sessions."""

import errno
import posixpath
import socket
import stat
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Iterator

import paramiko

from databridge.connectors.base import (
    MAX_LIST_RESULTS,
    MAX_LIST_SCAN,
    Listing,
    is_stage_name,
    validate_filename,
)
from databridge.errors import DataBridgeError, ErrorCode
from databridge.models import SFTPConnection


class _RejectUnknownHost(paramiko.MissingHostKeyPolicy):
    def missing_host_key(
        self, _client: paramiko.SSHClient, _hostname: str, _key: paramiko.PKey
    ) -> None:
        raise DataBridgeError(ErrorCode.SFTP_HOST_KEY_REJECTED, "SFTP host key is not trusted")


class SFTPConnector:
    def __init__(self, connection: SFTPConnection, known_hosts_path: Path) -> None:
        self.connection = connection
        self.known_hosts_path = known_hosts_path
        self.root = connection.root

    def _path(self, filename: str) -> str:
        validate_filename(filename)
        return posixpath.join(self.root, filename)

    @contextmanager
    def _session(self) -> Iterator[paramiko.SFTPClient]:
        client = paramiko.SSHClient()
        try:
            try:
                client.load_host_keys(str(self.known_hosts_path))
            except (OSError, paramiko.SSHException) as exc:
                raise DataBridgeError(
                    ErrorCode.SFTP_HOST_KEY_REJECTED, "SFTP known-hosts file is missing or invalid"
                ) from exc
            client.set_missing_host_key_policy(_RejectUnknownHost())
            try:
                client.connect(
                    hostname=self.connection.host,
                    port=self.connection.port,
                    username=self.connection.username,
                    password=self.connection.password.get_secret_value(),
                    allow_agent=False,
                    look_for_keys=False,
                    timeout=10,
                    banner_timeout=10,
                    auth_timeout=10,
                    channel_timeout=10,
                )
            except DataBridgeError:
                raise
            except paramiko.BadHostKeyException as exc:
                raise DataBridgeError(
                    ErrorCode.SFTP_HOST_KEY_REJECTED, "SFTP host key does not match known-hosts"
                ) from exc
            except paramiko.AuthenticationException as exc:
                raise DataBridgeError(
                    ErrorCode.SFTP_AUTH_FAILED, "SFTP credentials were rejected"
                ) from exc
            except (OSError, socket.timeout, paramiko.SSHException) as exc:
                raise DataBridgeError(
                    ErrorCode.SFTP_UNAVAILABLE, "SFTP server is unavailable"
                ) from exc
            try:
                sftp = client.open_sftp()
                sftp.get_channel().settimeout(30)
            except (OSError, paramiko.SSHException) as exc:
                raise DataBridgeError(
                    ErrorCode.SFTP_OPERATION_FAILED, "Cannot open SFTP session"
                ) from exc
            try:
                yield sftp
            finally:
                sftp.close()
        finally:
            client.close()

    def list_files(self) -> Listing:
        files: list[str] = []
        scanned = 0
        with self._session() as sftp:
            try:
                for entry in sftp.listdir_iter(self.root, read_aheads=1):
                    scanned += 1
                    if scanned > MAX_LIST_SCAN:
                        return Listing(sorted(files), True)
                    if is_stage_name(entry.filename) or not stat.S_ISREG(entry.st_mode or 0):
                        continue
                    try:
                        validate_filename(entry.filename)
                    except DataBridgeError:
                        continue
                    if len(files) >= MAX_LIST_RESULTS:
                        return Listing(sorted(files), True)
                    files.append(entry.filename)
            except OSError as exc:
                raise DataBridgeError(
                    ErrorCode.CONNECTION_ROOT_UNAVAILABLE, "SFTP root directory cannot be listed"
                ) from exc
        return Listing(sorted(files), False)

    @contextmanager
    def read(self, filename: str) -> Iterator[BinaryIO]:
        path = self._path(filename)
        with self._session() as sftp:
            try:
                stream = sftp.file(path, "rb")
            except OSError as exc:
                if exc.errno == errno.ENOENT:
                    raise DataBridgeError(ErrorCode.FILE_NOT_FOUND, "File not found") from exc
                raise DataBridgeError(
                    ErrorCode.SFTP_OPERATION_FAILED, "Cannot open SFTP file"
                ) from exc
            try:
                yield stream
            finally:
                stream.close()

    @staticmethod
    def _exists(sftp: paramiko.SFTPClient, path: str) -> bool:
        try:
            sftp.stat(path)
            return True
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                return False
            raise DataBridgeError(
                ErrorCode.SFTP_OPERATION_FAILED, "Cannot inspect SFTP file"
            ) from exc

    @contextmanager
    def write(self, filename: str, transfer_id: str, overwrite: bool) -> Iterator[BinaryIO]:
        destination = self._path(filename)
        stage = posixpath.join(self.root, f".{filename}.databridge-{transfer_id}.part")
        with self._session() as sftp:
            if not overwrite and self._exists(sftp, destination):
                raise DataBridgeError(
                    ErrorCode.DESTINATION_EXISTS, "Destination file already exists"
                )
            try:
                stream = sftp.file(stage, "wbx")
            except OSError as exc:
                raise DataBridgeError(
                    ErrorCode.SFTP_OPERATION_FAILED, "Cannot open SFTP staging file"
                ) from exc
            try:
                try:
                    yield stream
                finally:
                    stream.close()
                if not overwrite and self._exists(sftp, destination):
                    raise DataBridgeError(
                        ErrorCode.DESTINATION_EXISTS, "Destination file already exists"
                    )
                try:
                    if overwrite:
                        sftp.posix_rename(stage, destination)
                    else:
                        sftp.rename(stage, destination)
                except OSError as exc:
                    if not overwrite and self._exists(sftp, destination):
                        raise DataBridgeError(
                            ErrorCode.DESTINATION_EXISTS, "Destination file already exists"
                        ) from exc
                    raise DataBridgeError(
                        ErrorCode.SFTP_OPERATION_FAILED, "Cannot publish SFTP file"
                    ) from exc
            finally:
                try:
                    sftp.remove(stage)
                except OSError:
                    pass  # Published already, or cleanup is unavailable.
