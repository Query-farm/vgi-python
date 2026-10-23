# Copyright 2026 Query Farm LLC - https://query.farm

"""Bounded completion and orphan-cleanup tests with real DuckDB transactions.

Stock DuckDB lacks the bundled transactor's ``enable_suspended_queries`` and
``subcursor()`` extensions. Setup therefore allocates ordinary DuckDB cursors
and installs them in the real transactor's existing registries. No completion,
shutdown, RPC, or engine method is replaced. These tests do not qualify the
bundled BEGIN or streaming-write paths, nor promise timed orphan reclamation.
"""

from __future__ import annotations

import socket
import tempfile
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from vgi_rpc.rpc import RpcError, RpcServer, UnixTransport

from vgi._duckdb import connect
from vgi.transactor.client import TransactorClient
from vgi.transactor.protocol import TransactorProtocol
from vgi.transactor.server import TransactorImpl


@pytest.fixture
def transactor(tmp_path: Path) -> Iterator[tuple[TransactorImpl, bytes]]:
    """Register a real database and close its cursors even after test failure."""
    implementation = TransactorImpl(str(tmp_path))
    attachment = uuid.uuid4().bytes
    implementation.register(
        attachment,
        ddl_statements=[
            "CREATE TABLE ledger (value BIGINT)",
            "INSERT INTO ledger VALUES (10)",
        ],
    )
    try:
        yield implementation, attachment
    finally:
        with pytest.raises(SystemExit) as exit_info:
            implementation.shutdown()
        assert exit_info.value.code == 0


def _allocate_cursor(implementation: TransactorImpl, attachment: bytes) -> tuple[bytes, Any]:
    """Seed an ordinary transaction without pretending to exercise custom BEGIN."""
    cursor = implementation._get_db_conn(attachment).cursor()
    cursor.begin()
    transaction = uuid.uuid4().bytes
    with implementation._lock:
        implementation._transactions.setdefault(attachment, {})[transaction] = cursor
        implementation._tx_locks.setdefault(attachment, {})[transaction] = threading.Lock()
    return transaction, cursor


def _rows(implementation: TransactorImpl, attachment: bytes) -> list[tuple[int]]:
    """Read committed data through the independent database connection."""
    return implementation._get_db_conn(attachment).execute("SELECT value FROM ledger ORDER BY value").fetchall()


@contextmanager
def _connected_client(implementation: TransactorImpl, monkeypatch: pytest.MonkeyPatch) -> Iterator[TransactorClient]:
    """Use the real client and RPC server on one isolated Unix connection."""
    # A short path avoids AF_UNIX's path limit under deeply nested pytest dirs.
    with tempfile.TemporaryDirectory(prefix="vgi-tx-") as directory:
        socket_path = str(Path(directory) / "rpc.sock")
        server = RpcServer(TransactorProtocol, implementation)
        failures: list[BaseException] = []
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(socket_path)
            listener.listen(1)
            listener.settimeout(5)

            def serve() -> None:
                try:
                    connection, _ = listener.accept()
                    transport = UnixTransport(connection)
                    try:
                        server.serve(transport)
                    finally:
                        transport.close()
                except BaseException as error:
                    failures.append(error)

            thread = threading.Thread(target=serve, daemon=True)
            thread.start()
            monkeypatch.setenv("VGI_TRANSACTOR_SOCKET", socket_path)
            client = TransactorClient()
            try:
                client.get_proxy().ping()
                assert client._process is None  # Connected to this server; never spawn a shared process.
                yield client
            finally:
                client.close()
                thread.join(timeout=5)
                assert not thread.is_alive(), "Transactor RPC did not finish after client disconnect"
                assert not failures


@pytest.mark.parametrize("completion", ["commit", "rollback"])
@pytest.mark.parametrize("repeated_completion", ["commit", "rollback"])
def test_completed_transaction_rejects_duplicate_and_opposite_completion_without_effects(
    transactor: tuple[TransactorImpl, bytes],
    monkeypatch: pytest.MonkeyPatch,
    completion: str,
    repeated_completion: str,
) -> None:
    """Duplicate RPCs cannot reapply writes, undo a commit, or revive a rollback."""
    implementation, attachment = transactor
    transaction, cursor = _allocate_cursor(implementation, attachment)
    cursor.execute("UPDATE ledger SET value = value + 1")
    cursor.execute("INSERT INTO ledger VALUES (20)")
    expected = [(11,), (20,)] if completion == "commit" else [(10,)]

    with _connected_client(implementation, monkeypatch) as client:
        proxy = client.get_proxy()
        getattr(proxy, completion)(attach_opaque_data=attachment, tx_id=transaction)
        assert _rows(implementation, attachment) == expected
        assert transaction not in implementation._transactions[attachment]
        assert transaction not in implementation._tx_locks[attachment]

        # Keep a different transaction active while stale completion calls arrive.
        other_transaction, other_cursor = _allocate_cursor(implementation, attachment)
        other_cursor.execute("INSERT INTO ledger VALUES (30)")
        for _ in range(3):
            with pytest.raises(RpcError, match="No active transaction"):
                getattr(proxy, repeated_completion)(attach_opaque_data=attachment, tx_id=transaction)
            assert _rows(implementation, attachment) == expected
            assert implementation._get_tx_conn(attachment, other_transaction) is other_cursor

        proxy.commit(attach_opaque_data=attachment, tx_id=other_transaction)
        assert _rows(implementation, attachment) == [*expected, (30,)]
        with pytest.raises(ValueError, match="No active transaction"):
            implementation.list_user_tables(attachment, transaction)


def test_client_disconnect_leaves_abandoned_transaction_active(
    transactor: tuple[TransactorImpl, bytes], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Connection closure is not transaction cleanup for the shared transactor."""
    implementation, attachment = transactor
    transaction, cursor = _allocate_cursor(implementation, attachment)
    cursor.execute("INSERT INTO ledger VALUES (99)")
    with _connected_client(implementation, monkeypatch) as client:
        assert client.get_proxy().list_user_tables(attach_opaque_data=attachment, tx_id=transaction) == ["ledger"]

    # The request connection and its serving thread are gone, but the shared
    # provider still owns the transaction. No wall-clock delay implies a lease.
    assert implementation._get_tx_conn(attachment, transaction) is cursor
    assert cursor.execute("SELECT value FROM ledger ORDER BY value").fetchall() == [(10,), (99,)]
    assert _rows(implementation, attachment) == [(10,)]
    implementation.rollback(attachment, transaction)
    assert _rows(implementation, attachment) == [(10,)]


def test_explicit_shutdown_rolls_back_abandoned_transactions_and_releases_databases(tmp_path: Path) -> None:
    """Existing explicit shutdown discards orphan DML/DDL in every attachment."""
    implementation = TransactorImpl(str(tmp_path))
    attachments = [uuid.uuid4().bytes, uuid.uuid4().bytes]
    cursors = []
    try:
        for attachment in attachments:
            implementation.register(attachment, ddl_statements=["CREATE TABLE ledger (value BIGINT)"])
            _, cursor = _allocate_cursor(implementation, attachment)
            cursors.append(cursor)
            cursor.execute("INSERT INTO ledger VALUES (99)")
            cursor.execute("CREATE TABLE abandoned (value BIGINT)")

        with pytest.raises(SystemExit) as exit_info:
            implementation.shutdown()
        assert exit_info.value.code == 0
        assert implementation._transactions == {}
        assert implementation._tx_locks == {}
        assert implementation._databases == {}
        for attachment in attachments:
            with connect(str(tmp_path / f"{attachment.hex()}.duckdb")) as reopened:
                assert reopened.execute("SELECT * FROM ledger").fetchall() == []
                assert reopened.execute("SHOW TABLES").fetchall() == [("ledger",)]
        for cursor in cursors:
            with pytest.raises(Exception, match="closed"):
                cursor.execute("SELECT 1")
    finally:
        with pytest.raises(SystemExit):
            implementation.shutdown()
