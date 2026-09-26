"""Persistence ownership, file privacy, schema identity, and transfer-state guards."""

import os
import sqlite3
import stat
from contextlib import closing
from pathlib import Path

import pytest
from cryptography.fernet import Fernet
from support import client_for

from databridge import store as store_module
from databridge.config import Settings
from databridge.errors import DataBridgeError, ErrorCode, StartupError
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
        with pytest.raises(StartupError) as refused:
            with client_for(settings):
                pass
    assert str(refused.value) == (
        f"Another DataBridge process is using the database locked by {settings.lock_path}; "
        "stop that process before starting another"
    )

    with client_for(settings) as restarted:
        assert restarted.get("/connections").status_code == 200


def test_owner_lock_is_released_when_its_holder_exits(tmp_path: Path) -> None:
    lock_path = tmp_path / "state" / "databridge.owner.lock"
    with DatabaseOwnerLock(lock_path):
        assert stat.S_IMODE(lock_path.stat().st_mode) == 0o600
    with DatabaseOwnerLock(lock_path):
        pass


def test_owner_lock_closes_descriptor_when_chmod_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state" / "databridge.owner.lock"
    chmod = os.fchmod

    def fail_once(descriptor: int, mode: int) -> None:
        monkeypatch.setattr(os, "fchmod", chmod)
        raise OSError("simulated chmod failure")

    monkeypatch.setattr(os, "fchmod", fail_once)
    with pytest.raises(OSError, match="simulated chmod failure"):
        with DatabaseOwnerLock(path):
            pass
    with DatabaseOwnerLock(path):
        pass


def test_owner_lock_closes_descriptor_when_unlock_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state" / "databridge.owner.lock"
    flock = store_module.fcntl.flock
    held: list[int] = []

    def fail_unlock(descriptor: int, operation: int) -> None:
        if operation == store_module.fcntl.LOCK_UN:
            held.append(descriptor)
            raise OSError("simulated unlock failure")
        flock(descriptor, operation)

    monkeypatch.setattr(store_module.fcntl, "flock", fail_unlock)
    with pytest.raises(OSError, match="simulated unlock failure"):
        with DatabaseOwnerLock(path):
            pass
    assert len(held) == 1
    with pytest.raises(OSError):
        os.fstat(held[0])


def test_store_initialization_closes_descriptor_when_chmod_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    descriptor: list[int] = []

    def fail_chmod(fd: int, mode: int) -> None:
        descriptor.append(fd)
        raise OSError("simulated chmod failure")

    monkeypatch.setattr(os, "fchmod", fail_chmod)
    store = ConnectionStore(tmp_path / "state" / "database.sqlite3", Fernet.generate_key())
    with pytest.raises(OSError, match="simulated chmod failure"):
        store.initialize()
    assert len(descriptor) == 1
    with pytest.raises(OSError):
        os.fstat(descriptor[0])


@pytest.mark.parametrize(
    ("stored", "shown"),
    [("'0'", "'0'"), ("CAST(x'80' AS TEXT)", "'�'"), ("'1' || char(10) || '2'", "'1\\n2'")],
    ids=["other-version", "undecodable", "newline"],
)
def test_database_from_another_schema_version_is_refused(
    settings: Settings, stored: str, shown: str
) -> None:
    _store(settings)
    with sqlite3.connect(settings.database_path) as database:
        # The parametrized values are fixed SQL expressions in this test, not input.
        database.execute(
            f"UPDATE metadata SET value = {stored} WHERE key = 'schema_version'"  # noqa: S608
        )

    with pytest.raises(StartupError) as refused:
        _store(settings)

    # The stored value is quoted, so a damaged value cannot break the one-line message.
    assert str(refused.value) == (
        f"{settings.database_path} uses schema version {shown}, but this service needs version "
        "'1'; move it aside so the service can create a new database"
    )


def test_database_without_a_schema_version_is_refused(settings: Settings) -> None:
    with sqlite3.connect(settings.database_path) as database:
        database.execute("CREATE TABLE connections (name TEXT PRIMARY KEY)")

    with pytest.raises(StartupError, match="has no DataBridge schema version"):
        _store(settings)


@pytest.mark.parametrize("offset", [120, 4096], ids=["schema-page", "table-page"])
def test_a_corrupted_database_is_refused_with_its_path(settings: Settings, offset: int) -> None:
    _store(settings)
    with settings.database_path.open("r+b") as database:
        database.seek(offset)
        database.write(b"\xff" * 400)

    with pytest.raises(StartupError) as refused:
        _store(settings)

    assert str(refused.value) == (
        f"{settings.database_path} is damaged; move it aside so the service can create a new "
        "database"
    )


def _rename_in_schema(settings: Settings, statement: str) -> None:
    with closing(sqlite3.connect(settings.database_path)) as database:
        database.execute("PRAGMA writable_schema = ON")
        database.execute(statement)
        database.commit()


def test_a_schema_error_that_quotes_a_damaged_name_is_refused(settings: Settings) -> None:
    _store(settings)
    # SQLite reports the renamed table in its error message, which the driver cannot decode.
    _rename_in_schema(
        settings, "UPDATE sqlite_master SET name = CAST(x'80' AS TEXT) WHERE name = 'connections'"
    )

    with pytest.raises(StartupError) as refused:
        _store(settings)

    assert str(refused.value) == (
        f"{settings.database_path} is damaged; move it aside so the service can create a new "
        "database"
    )


def test_an_extra_table_with_an_undecodable_name_does_not_stop_startup(
    settings: Settings,
) -> None:
    root = str(settings.database_path.parent)
    _store(settings).create(LocalConnection(name="kept", type="local", path=root))
    with closing(sqlite3.connect(settings.database_path)) as database:
        database.execute("CREATE TABLE extra (x)")
        database.commit()
    _rename_in_schema(
        settings,
        "UPDATE sqlite_master SET name = CAST(x'80' AS TEXT), tbl_name = CAST(x'80' AS TEXT), "
        "sql = 'CREATE TABLE \"' || CAST(x'80' AS TEXT) || '\" (x)' WHERE name = 'extra'",
    )

    assert [view.name for view in _store(settings).list_public()] == ["kept"]


def test_a_file_that_is_not_a_database_is_refused(settings: Settings) -> None:
    settings.database_path.write_bytes(b"not a SQLite database\n" * 64)

    with pytest.raises(StartupError) as refused:
        _store(settings)

    assert str(refused.value) == (
        f"{settings.database_path} is not a SQLite database; move it aside so the service can "
        "create a new database"
    )


@pytest.mark.parametrize(
    "damage",
    [
        "UPDATE metadata SET value = 'clé' WHERE key = 'key_check'",
        "UPDATE metadata SET value = CAST(x'80' AS TEXT) WHERE key = 'key_check'",
        "UPDATE metadata SET value = x'00ff' WHERE key = 'key_check'",
        "UPDATE metadata SET value = '' WHERE key = 'key_check'",
        "DELETE FROM metadata WHERE key = 'key_check'",
    ],
    ids=["non-ascii-text", "undecodable-text", "blob", "empty", "missing"],
)
def test_a_damaged_key_check_is_refused_with_the_database_path(
    settings: Settings, damage: str
) -> None:
    _store(settings)
    with sqlite3.connect(settings.database_path) as database:
        database.execute(damage)

    with pytest.raises(StartupError) as refused:
        _store(settings)

    assert str(refused.value) == (
        f"{settings.database_path} has no valid DataBridge key check; move it aside so the "
        "service can create a new database"
    )


def test_a_different_key_is_refused_with_the_database_it_does_not_match(
    settings: Settings,
) -> None:
    _store(settings)

    with pytest.raises(StartupError) as refused:
        ConnectionStore(settings.database_path, Fernet.generate_key()).initialize()

    assert str(refused.value) == (
        f"DATABRIDGE_ENCRYPTION_KEY does not match the database {settings.database_path}; start "
        "with the key that created it, or move the database aside to start with no saved "
        "connections"
    )


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
    store.fail_transfer(
        FIRST, 2, "destination_write", ErrorCode.DESTINATION_NOT_WRITABLE, "read-only root"
    )
    failed = store.get_transfer(FIRST)
    assert (failed.status, failed.bytes_copied, failed.failure_phase) == (
        "failed",
        2,
        "destination_write",
    )
    assert failed.error_code == ErrorCode.DESTINATION_NOT_WRITABLE
    assert failed.error == "read-only root"
    assert failed.completed_at is None and failed.failed_at == failed.updated_at


def test_completion_that_cannot_be_read_back_is_rolled_back(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(settings)
    store.start_transfer(FIRST, _request())
    store.mark_publishing(FIRST, 3)

    def unreadable(_row: object) -> None:
        raise sqlite3.OperationalError("disk I/O error")

    with monkeypatch.context() as patch:
        patch.setattr(store_module.TransferRecord, "model_validate", unreadable)
        with pytest.raises(DataBridgeError) as failure:
            store.finish_transfer(FIRST)

    assert failure.value.code == ErrorCode.TRANSFER_RECORD_FAILED
    assert store.get_transfer(FIRST).status == "publishing"


def test_transfer_record_write_failure_is_a_typed_error(settings: Settings) -> None:
    store = _store(settings)
    store.start_transfer(FIRST, _request())
    # A trigger refuses the write for every user; file permissions do not bind root.
    with closing(sqlite3.connect(settings.database_path)) as database:
        database.execute(
            "CREATE TRIGGER refuse_update BEFORE UPDATE ON transfers "
            "BEGIN SELECT RAISE(ABORT, 'refused'); END"
        )
        database.commit()
    with pytest.raises(DataBridgeError) as failure:
        store.record_progress(FIRST, 1)
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
