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


def test_startup_recovery_fails_only_running_transfers(settings: Settings) -> None:
    store = _store(settings)
    store.start_transfer(FIRST, _request())
    store.finish_transfer(FIRST, 7)
    store.start_transfer(SECOND, _request())

    restarted = _store(settings)

    completed = restarted.get_transfer(FIRST)
    interrupted = restarted.get_transfer(SECOND)
    assert (completed.status, completed.bytes_copied, completed.failed_at) == ("completed", 7, None)
    assert interrupted.status == "failed"
    assert interrupted.failure_phase == "interruption"


def test_terminal_transfer_cannot_change_state_again(settings: Settings) -> None:
    store = _store(settings)
    store.start_transfer(FIRST, _request())
    store.finish_transfer(FIRST, 3)

    with pytest.raises(DataBridgeError) as late_failure:
        store.fail_transfer(FIRST, 3, "copy", "late failure")
    with pytest.raises(DataBridgeError) as second_finish:
        store.finish_transfer(FIRST, 4)

    assert late_failure.value.code == ErrorCode.TRANSFER_STATE_CONFLICT
    assert second_finish.value.code == ErrorCode.TRANSFER_STATE_CONFLICT
    record = store.get_transfer(FIRST)
    assert (record.status, record.bytes_copied, record.failed_at) == ("completed", 3, None)


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
