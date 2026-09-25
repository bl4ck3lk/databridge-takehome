"""Persistence ownership, file privacy, schema identity, and transfer-state guards."""

import os
import sqlite3
import stat
from pathlib import Path

import pytest
from cryptography.fernet import Fernet
from support import client_for

from databridge.config import Settings
from databridge.errors import DataBridgeError, ErrorCode
from databridge.models import LocalConnection, TransferRequest
from databridge.store import ConnectionStore, DatabaseOwnerLock

FIRST = "00000000-0000-4000-8000-000000000001"
SECOND = "00000000-0000-4000-8000-000000000002"
THIRD = "00000000-0000-4000-8000-000000000003"


def _store(settings: Settings) -> ConnectionStore:
    store = ConnectionStore(settings.database_path, settings.encryption_key)
    store.initialize()
    return store


def _request() -> TransferRequest:
    return TransferRequest(
        source="a", source_file="in.bin", destination="b", destination_file="out.bin"
    )


def test_new_state_directory_and_database_are_owner_only(tmp_path: Path) -> None:
    database = tmp_path / "state" / "databridge.sqlite3"
    previous = os.umask(0o022)
    try:
        ConnectionStore(database, Fernet.generate_key()).initialize()
    finally:
        os.umask(previous)

    assert stat.S_IMODE(database.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(database.stat().st_mode) == 0o600


def test_second_process_cannot_own_the_database(settings: Settings) -> None:
    with client_for(settings):
        with pytest.raises(RuntimeError, match="Another DataBridge process is using"):
            with client_for(settings):
                pass

    with client_for(settings) as restarted:
        assert restarted.get("/connections").status_code == 200


def test_owner_lock_is_released_when_its_holder_exits(tmp_path: Path) -> None:
    lock_path = tmp_path / "state" / "databridge.owner.lock"
    with DatabaseOwnerLock(lock_path):
        assert stat.S_IMODE(lock_path.stat().st_mode) == 0o600
    with DatabaseOwnerLock(lock_path):
        pass


def test_database_from_another_schema_version_is_refused(settings: Settings) -> None:
    _store(settings)
    with sqlite3.connect(settings.database_path) as database:
        database.execute("UPDATE metadata SET value = '0' WHERE key = 'schema_version'")

    with pytest.raises(RuntimeError, match="schema version 0"):
        _store(settings)


def test_database_without_a_schema_version_is_refused(settings: Settings) -> None:
    with sqlite3.connect(settings.database_path) as database:
        database.execute("CREATE TABLE connections (name TEXT PRIMARY KEY)")

    with pytest.raises(RuntimeError, match="has no DataBridge schema version"):
        _store(settings)


def _completed(store: ConnectionStore, transfer_id: str, size: int) -> None:
    store.start_transfer(transfer_id, _request())
    store.mark_publishing(transfer_id, size)
    store.finish_transfer(transfer_id)


def test_startup_recovery_fails_only_unfinished_transfers(settings: Settings) -> None:
    store = _store(settings)
    _completed(store, FIRST, 7)
    store.start_transfer(SECOND, _request())
    store.record_progress(SECOND, 5)
    store.start_transfer(THIRD, _request())
    store.mark_publishing(THIRD, 9)

    restarted = _store(settings)

    completed = restarted.get_transfer(FIRST)
    copying = restarted.get_transfer(SECOND)
    publishing = restarted.get_transfer(THIRD)
    assert (completed.status, completed.bytes_copied, completed.failed_at) == ("completed", 7, None)
    for record in (copying, publishing):
        assert record.status == "failed"
        assert record.failure_phase == "interruption"
        assert record.error_code == ErrorCode.TRANSFER_INTERRUPTED
    assert copying.bytes_copied == 5
    assert "destination is unchanged" in (copying.error or "")
    assert publishing.bytes_copied == 9
    assert "may already contain" in (publishing.error or "")


def test_transfer_state_changes_follow_the_state_machine(settings: Settings) -> None:
    store = _store(settings)
    store.start_transfer(FIRST, _request())
    with pytest.raises(DataBridgeError) as unpublished_finish:
        store.finish_transfer(FIRST)
    store.mark_publishing(FIRST, 3)
    with pytest.raises(DataBridgeError) as progress_while_publishing:
        store.record_progress(FIRST, 4)
    store.finish_transfer(FIRST)

    late_changes = (
        lambda: store.record_progress(FIRST, 4),
        lambda: store.mark_publishing(FIRST, 4),
        lambda: store.finish_transfer(FIRST),
        lambda: store.fail_transfer(
            FIRST, 3, "publication", ErrorCode.SFTP_UNAVAILABLE, "late failure"
        ),
    )
    for change in late_changes:
        with pytest.raises(DataBridgeError) as conflict:
            change()
        assert conflict.value.code == ErrorCode.TRANSFER_STATE_CONFLICT

    assert unpublished_finish.value.code == ErrorCode.TRANSFER_STATE_CONFLICT
    assert progress_while_publishing.value.code == ErrorCode.TRANSFER_STATE_CONFLICT
    record = store.get_transfer(FIRST)
    assert (record.status, record.bytes_copied, record.failed_at) == ("completed", 3, None)
    assert record.completed_at is not None and record.completed_at >= record.started_at


def test_failed_transfer_records_phase_code_and_message(settings: Settings) -> None:
    store = _store(settings)
    store.start_transfer(FIRST, _request())
    failed = store.fail_transfer(
        FIRST, 2, "destination_write", ErrorCode.DESTINATION_NOT_WRITABLE, "read-only root"
    )
    assert (failed.status, failed.bytes_copied, failed.failure_phase) == (
        "failed",
        2,
        "destination_write",
    )
    assert failed.error_code == ErrorCode.DESTINATION_NOT_WRITABLE
    assert failed.error == "read-only root"
    assert failed.completed_at is None and failed.failed_at == failed.updated_at


def test_transfer_record_write_failure_is_a_typed_error(settings: Settings) -> None:
    store = _store(settings)
    store.start_transfer(FIRST, _request())
    settings.database_path.chmod(0o400)
    try:
        with pytest.raises(DataBridgeError) as failure:
            store.record_progress(FIRST, 1)
    finally:
        settings.database_path.chmod(0o600)
    assert failure.value.code == ErrorCode.TRANSFER_RECORD_FAILED
    assert failure.value.transfer_id == FIRST


def test_transfers_are_listed_newest_first_and_filtered(settings: Settings) -> None:
    store = _store(settings)
    _completed(store, FIRST, 1)
    store.start_transfer(SECOND, _request())
    store.start_transfer(THIRD, _request())
    store.fail_transfer(THIRD, 0, "source_open", ErrorCode.FILE_NOT_FOUND, "missing")

    assert [record.id for record in store.list_transfers(None, 10)] == [THIRD, SECOND, FIRST]
    assert [record.id for record in store.list_transfers("running", 10)] == [SECOND]
    assert [record.id for record in store.list_transfers(None, 2)] == [THIRD, SECOND]


def test_unknown_connection_type_fails_closed(settings: Settings) -> None:
    store = _store(settings)
    impostor = LocalConnection.model_construct(name="impostor", type="ftp", path="/tmp")
    with pytest.raises(RuntimeError, match="Unsupported connection type 'ftp'"):
        store.create(impostor)

    with sqlite3.connect(settings.database_path) as database:
        database.execute(
            "INSERT INTO connections (name, type, settings_json) VALUES ('legacy', 'ftp', '{}')"
        )
    with client_for(settings) as client:
        response = client.get("/connections/legacy")
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "INTERNAL_ERROR"
