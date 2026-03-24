"""Tests for TransactorImpl — DuckDB transaction manager."""

from __future__ import annotations

import json
import uuid
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc import AnnotatedBatch, OutputCollector
from vgi_rpc.rpc import CallContext, ExchangeState, ProducerState

from vgi.schema_utils import schema
from vgi.transactor.server import TransactorImpl

# ============================================================================
# Helpers
# ============================================================================

_COUNT_SCHEMA = schema(count=pa.int64())
_FILTER_METADATA = {b"vgi_filter_version": b"1"}


def _make_filters(*filters: tuple[str, int, str, str, pa.Scalar[Any]]) -> pa.RecordBatch:
    """Build a pushdown filter RecordBatch.

    Each filter is (column_name, column_index, filter_type, op, value_scalar).
    For constant filters: ("id", 1, "constant", "eq", pa.scalar(2, pa.int64()))
    """
    specs = []
    value_arrays: list[pa.Array[Any]] = []
    for col_name, col_idx, ftype, op, value in filters:
        spec: dict[str, object] = {"column_name": col_name, "column_index": col_idx, "type": ftype}
        if ftype == "constant":
            spec["op"] = op
            spec["value_ref"] = len(value_arrays)
            value_arrays.append(pa.array([value.as_py()], type=value.type))
        specs.append(spec)

    spec_field = pa.field("filter_spec", pa.string(), metadata=_FILTER_METADATA)
    fields = [spec_field, *[pa.field(f"value_{i}", arr.type) for i, arr in enumerate(value_arrays)]]
    arrays: list[pa.Array[Any]] = [pa.array([json.dumps(specs)]), *value_arrays]
    return pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields))


def _tx() -> bytes:
    """Generate a unique transaction id."""
    return uuid.uuid4().bytes


def _ctx() -> CallContext:
    """Minimal CallContext for produce/exchange calls."""
    from vgi_rpc.rpc import AuthContext

    return CallContext(auth=AuthContext.anonymous(), emit_client_log=lambda *a, **kw: None)


def _insert_into(
    t: TransactorImpl, tx_id: bytes, table_name: str, rows: list[tuple[int, str]], *, returning: bool = False
) -> pa.RecordBatch:
    """Insert rows into an arbitrary table and return the response batch."""
    stream = t.insert(tx_id=tx_id, table_name=table_name, returning=returning)
    batch = pa.record_batch(
        {"id": [r[0] for r in rows], "name": [r[1] for r in rows]},
        schema=schema(id=pa.int64(), name=pa.string()),
    )
    out = OutputCollector(stream.output_schema, producer_mode=False)
    assert isinstance(stream.state, ExchangeState)
    stream.state.exchange(AnnotatedBatch(batch=batch), out, _ctx())
    return out.data_batch.batch


def _insert(t: TransactorImpl, tx_id: bytes, rows: list[tuple[int, str]], *, returning: bool = False) -> pa.RecordBatch:
    """Insert rows via the transactor and return the response batch."""
    stream = t.insert(tx_id=tx_id, table_name="data", returning=returning)
    batch = pa.record_batch(
        {"id": [r[0] for r in rows], "name": [r[1] for r in rows]},
        schema=schema(id=pa.int64(), name=pa.string()),
    )
    out = OutputCollector(stream.output_schema, producer_mode=False)
    assert isinstance(stream.state, ExchangeState)
    stream.state.exchange(AnnotatedBatch(batch=batch), out, _ctx())
    return out.data_batch.batch


def _scan(
    t: TransactorImpl,
    tx_id: bytes,
    columns: list[str] | None = None,
    pushdown_filters: pa.RecordBatch | None = None,
) -> pa.Table:
    """Scan the data table and return all rows as a pyarrow Table."""
    cols = columns or ["rowid", "id", "name"]
    pf_bytes = None
    if pushdown_filters is not None:
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, pushdown_filters.schema)
        writer.write_batch(pushdown_filters)
        writer.close()
        pf_bytes = sink.getvalue().to_pybytes()
    stream = t.scan(tx_id=tx_id, table_name="data", columns=cols, pushdown_filters=pf_bytes)
    assert isinstance(stream.state, ProducerState)
    batches: list[pa.RecordBatch] = []
    while True:
        out = OutputCollector(stream.output_schema, producer_mode=True)
        stream.state.produce(out, _ctx())
        if out.finished:
            break
        batches.append(out.data_batch.batch)
    return pa.Table.from_batches(batches, schema=stream.output_schema) if batches else pa.table({c: [] for c in cols})


def _delete(t: TransactorImpl, tx_id: bytes, rowids: list[int], *, returning: bool = False) -> pa.RecordBatch:
    """Delete rows by rowid, return the response batch."""
    stream = t.delete(tx_id=tx_id, table_name="data", returning=returning)
    batch = pa.record_batch({"rowid": rowids}, schema=schema(rowid=pa.int64()))
    out = OutputCollector(stream.output_schema, producer_mode=False)
    assert isinstance(stream.state, ExchangeState)
    stream.state.exchange(AnnotatedBatch(batch=batch), out, _ctx())
    return out.data_batch.batch


def _delete_count(t: TransactorImpl, tx_id: bytes, rowids: list[int]) -> int:
    """Delete rows by rowid, return deleted count."""
    return _delete(t, tx_id, rowids).column("count")[0].as_py()  # type: ignore[no-any-return]


def _update(
    t: TransactorImpl, tx_id: bytes, rowids: list[int], names: list[str], *, returning: bool = False
) -> pa.RecordBatch:
    """Update name column by rowid, return the response batch."""
    stream = t.update(tx_id=tx_id, table_name="data", columns=["name"], returning=returning)
    batch = pa.record_batch(
        {"name": names, "rowid": rowids},
        schema=schema(name=pa.string(), rowid=pa.int64()),
    )
    out = OutputCollector(stream.output_schema, producer_mode=False)
    assert isinstance(stream.state, ExchangeState)
    stream.state.exchange(AnnotatedBatch(batch=batch), out, _ctx())
    return out.data_batch.batch


def _update_count(t: TransactorImpl, tx_id: bytes, rowids: list[int], names: list[str]) -> int:
    """Update name column by rowid, return updated count."""
    return _update(t, tx_id, rowids, names).column("count")[0].as_py()  # type: ignore[no-any-return]


# ============================================================================
# Fixture
# ============================================================================


@pytest.fixture()
def transactor(tmp_path: object) -> TransactorImpl:
    """Create a TransactorImpl with a fresh test database."""
    from pathlib import Path

    db_path = Path(str(tmp_path)) / "test.duckdb"
    impl = TransactorImpl(str(db_path))
    impl.execute_ddl("CREATE TABLE data (  id BIGINT NOT NULL,  name VARCHAR NOT NULL)")
    return impl


# ============================================================================
# Transaction lifecycle
# ============================================================================


class TestTransactionLifecycle:
    """Transaction begin, commit, rollback semantics."""

    def test_begin_commit(self, transactor: TransactorImpl) -> None:
        """Committed data is visible in a subsequent transaction."""
        tx1 = _tx()
        transactor.begin(tx1)
        _insert(transactor, tx1, [(1, "hello")])
        transactor.commit(tx1)

        tx2 = _tx()
        transactor.begin(tx2)
        table = _scan(transactor, tx2)
        transactor.commit(tx2)

        assert table.num_rows == 1
        assert table.column("id")[0].as_py() == 1

    def test_begin_rollback(self, transactor: TransactorImpl) -> None:
        """Rolled-back data is not visible."""
        tx1 = _tx()
        transactor.begin(tx1)
        _insert(transactor, tx1, [(1, "gone")])
        transactor.rollback(tx1)

        tx2 = _tx()
        transactor.begin(tx2)
        table = _scan(transactor, tx2)
        transactor.commit(tx2)

        assert table.num_rows == 0

    def test_commit_unknown_tx_raises(self, transactor: TransactorImpl) -> None:
        """Committing an unknown transaction raises ValueError."""
        with pytest.raises(ValueError, match="No active transaction"):
            transactor.commit(_tx())

    def test_rollback_unknown_tx_raises(self, transactor: TransactorImpl) -> None:
        """Rolling back an unknown transaction raises ValueError."""
        with pytest.raises(ValueError, match="No active transaction"):
            transactor.rollback(_tx())

    def test_multiple_concurrent_transactions(self, transactor: TransactorImpl) -> None:
        """Two transactions can be active simultaneously."""
        tx_a, tx_b = _tx(), _tx()
        transactor.begin(tx_a)
        transactor.begin(tx_b)

        _insert(transactor, tx_a, [(1, "from_a")])
        _insert(transactor, tx_b, [(2, "from_b")])

        transactor.commit(tx_a)
        transactor.commit(tx_b)

        tx_c = _tx()
        transactor.begin(tx_c)
        table = _scan(transactor, tx_c)
        transactor.commit(tx_c)

        assert table.num_rows == 2

    def test_commit_cleans_up(self, transactor: TransactorImpl) -> None:
        """After commit, tx_id is removed from active transactions."""
        tx = _tx()
        transactor.begin(tx)
        transactor.commit(tx)
        assert tx not in transactor._transactions

    def test_rollback_cleans_up(self, transactor: TransactorImpl) -> None:
        """After rollback, tx_id is removed from active transactions."""
        tx = _tx()
        transactor.begin(tx)
        transactor.rollback(tx)
        assert tx not in transactor._transactions


# ============================================================================
# Insert
# ============================================================================


class TestInsert:
    """INSERT operations via the transactor."""

    def test_insert_single_row(self, transactor: TransactorImpl) -> None:
        """A single inserted row is scannable."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(1, "hello")])
        table = _scan(transactor, tx, columns=["id", "name"])
        transactor.commit(tx)

        assert table.num_rows == 1
        assert table.column("name")[0].as_py() == "hello"

    def test_insert_multiple_rows(self, transactor: TransactorImpl) -> None:
        """A batch with multiple rows inserts all of them."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(1, "a"), (2, "b"), (3, "c")])
        table = _scan(transactor, tx, columns=["id"])
        transactor.commit(tx)

        assert table.num_rows == 3

    def test_insert_returning(self, transactor: TransactorImpl) -> None:
        """INSERT with returning=True returns the inserted data."""
        tx = _tx()
        transactor.begin(tx)
        result = _insert(transactor, tx, [(42, "returned")], returning=True)
        transactor.commit(tx)

        assert result.num_rows == 1
        assert result.column("id")[0].as_py() == 42
        assert result.column("name")[0].as_py() == "returned"

    def test_insert_returning_schema(self, transactor: TransactorImpl) -> None:
        """RETURNING batch excludes rowid column."""
        tx = _tx()
        transactor.begin(tx)
        result = _insert(transactor, tx, [(1, "test")], returning=True)
        transactor.commit(tx)

        assert "rowid" not in result.schema.names
        assert "id" in result.schema.names
        assert "name" in result.schema.names


# ============================================================================
# Delete
# ============================================================================


class TestDelete:
    """DELETE operations via the transactor."""

    def test_delete_by_rowid(self, transactor: TransactorImpl) -> None:
        """Deleting by rowid removes the row."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(1, "del_me")])
        rows = _scan(transactor, tx, columns=["rowid"])
        rowid = rows.column("rowid")[0].as_py()
        _delete(transactor, tx, [rowid])

        remaining = _scan(transactor, tx, columns=["id"])
        transactor.commit(tx)

        assert remaining.num_rows == 0

    def test_delete_returns_count(self, transactor: TransactorImpl) -> None:
        """Delete returns the number of affected rows."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(1, "a"), (2, "b")])
        rows = _scan(transactor, tx, columns=["rowid"])
        rowids: list[int] = [r.as_py() for r in rows.column("rowid")]
        count = _delete_count(transactor, tx, rowids)
        transactor.commit(tx)

        assert count == 2

    def test_delete_nonexistent(self, transactor: TransactorImpl) -> None:
        """Deleting a non-existent rowid returns count=0."""
        tx = _tx()
        transactor.begin(tx)
        count = _delete_count(transactor, tx, [99999])
        transactor.commit(tx)

        assert count == 0

    def test_delete_returning(self, transactor: TransactorImpl) -> None:
        """DELETE with returning=True returns the deleted rows."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(1, "gone"), (2, "also_gone")])
        rows = _scan(transactor, tx, columns=["rowid"])
        rowids: list[int] = [r.as_py() for r in rows.column("rowid")]
        result = _delete(transactor, tx, rowids, returning=True)
        transactor.commit(tx)

        assert result.num_rows == 2
        assert set(result.column("id").to_pylist()) == {1, 2}

    def test_delete_returning_schema(self, transactor: TransactorImpl) -> None:
        """DELETE RETURNING batch excludes rowid column."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(1, "test")])
        rows = _scan(transactor, tx, columns=["rowid"])
        rowid = rows.column("rowid")[0].as_py()
        result = _delete(transactor, tx, [rowid], returning=True)
        transactor.commit(tx)

        assert "rowid" not in result.schema.names
        assert "id" in result.schema.names
        assert "name" in result.schema.names


# ============================================================================
# Update
# ============================================================================


class TestUpdate:
    """UPDATE operations via the transactor."""

    def test_update_single_column(self, transactor: TransactorImpl) -> None:
        """Updating a row changes its value."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(1, "old")])
        rows = _scan(transactor, tx, columns=["rowid"])
        rowid = rows.column("rowid")[0].as_py()
        _update_count(transactor, tx, [rowid], ["new"])

        table = _scan(transactor, tx, columns=["name"])
        transactor.commit(tx)

        assert table.column("name")[0].as_py() == "new"

    def test_update_returns_count(self, transactor: TransactorImpl) -> None:
        """Update returns the number of affected rows."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(1, "a"), (2, "b")])
        rows = _scan(transactor, tx, columns=["rowid"])
        rowids: list[int] = [r.as_py() for r in rows.column("rowid")]
        count = _update_count(transactor, tx, rowids, ["x", "y"])
        transactor.commit(tx)

        assert count == 2

    def test_update_nonexistent(self, transactor: TransactorImpl) -> None:
        """Updating a non-existent rowid returns count=0."""
        tx = _tx()
        transactor.begin(tx)
        count = _update_count(transactor, tx, [99999], ["nope"])
        transactor.commit(tx)

        assert count == 0

    def test_update_returning(self, transactor: TransactorImpl) -> None:
        """UPDATE with returning=True returns the updated rows."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(1, "old")])
        rows = _scan(transactor, tx, columns=["rowid"])
        rowid = rows.column("rowid")[0].as_py()
        result = _update(transactor, tx, [rowid], ["new"], returning=True)
        transactor.commit(tx)

        assert result.num_rows == 1
        assert result.column("name")[0].as_py() == "new"

    def test_update_returning_schema(self, transactor: TransactorImpl) -> None:
        """UPDATE RETURNING batch excludes rowid column."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(1, "test")])
        rows = _scan(transactor, tx, columns=["rowid"])
        rowid = rows.column("rowid")[0].as_py()
        result = _update(transactor, tx, [rowid], ["changed"], returning=True)
        transactor.commit(tx)

        assert "rowid" not in result.schema.names
        assert "id" in result.schema.names
        assert "name" in result.schema.names


# ============================================================================
# Scan
# ============================================================================


class TestScan:
    """Scan (SELECT) operations via the transactor."""

    def test_scan_empty_table(self, transactor: TransactorImpl) -> None:
        """Scanning an empty table returns zero rows."""
        tx = _tx()
        transactor.begin(tx)
        table = _scan(transactor, tx, columns=["id", "name"])
        transactor.commit(tx)

        assert table.num_rows == 0

    def test_scan_returns_data(self, transactor: TransactorImpl) -> None:
        """Scan returns all inserted rows."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(1, "a"), (2, "b"), (3, "c")])
        table = _scan(transactor, tx, columns=["id", "name"])
        transactor.commit(tx)

        assert table.num_rows == 3
        assert set(table.column("id").to_pylist()) == {1, 2, 3}

    def test_scan_column_projection(self, transactor: TransactorImpl) -> None:
        """Scan with a column subset only returns those columns."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(1, "hello")])
        table = _scan(transactor, tx, columns=["name"])
        transactor.commit(tx)

        assert table.schema.names == ["name"]
        assert table.column("name")[0].as_py() == "hello"

    def test_scan_streams_batches(self, transactor: TransactorImpl) -> None:
        """Scan uses a streaming reader (produce is called per batch, not all at once)."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(i, f"row_{i}") for i in range(100)])

        stream = transactor.scan(tx_id=tx, table_name="data", columns=["id"])
        # The _ScanState should have a _reader attribute (RecordBatchReader)
        assert hasattr(stream.state, "_reader")

        transactor.commit(tx)

    def test_scan_with_equality_filter(self, transactor: TransactorImpl) -> None:
        """Scan with id = value returns only matching rows."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(1, "a"), (2, "b"), (3, "c")])
        pf = _make_filters(("id", 1, "constant", "eq", pa.scalar(2, pa.int64())))
        table = _scan(transactor, tx, columns=["id", "name"], pushdown_filters=pf)
        transactor.commit(tx)

        assert table.num_rows == 1
        assert table.column("name")[0].as_py() == "b"

    def test_scan_with_range_filter(self, transactor: TransactorImpl) -> None:
        """Scan with id > value filters correctly."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(1, "a"), (2, "b"), (3, "c")])
        pf = _make_filters(("id", 1, "constant", "gt", pa.scalar(1, pa.int64())))
        table = _scan(transactor, tx, columns=["id"], pushdown_filters=pf)
        transactor.commit(tx)

        assert table.num_rows == 2
        assert set(table.column("id").to_pylist()) == {2, 3}

    def test_scan_with_multiple_filters(self, transactor: TransactorImpl) -> None:
        """Scan with multiple filters ANDed together."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(1, "a"), (2, "b"), (3, "a")])
        pf = _make_filters(
            ("id", 1, "constant", "ge", pa.scalar(2, pa.int64())),
            ("name", 2, "constant", "eq", pa.scalar("a", pa.string())),
        )
        table = _scan(transactor, tx, columns=["id"], pushdown_filters=pf)
        transactor.commit(tx)

        assert table.num_rows == 1
        assert table.column("id")[0].as_py() == 3

    def test_scan_filters_with_no_matches(self, transactor: TransactorImpl) -> None:
        """Scan with a filter that matches nothing returns empty."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(1, "a")])
        pf = _make_filters(("id", 1, "constant", "eq", pa.scalar(999, pa.int64())))
        table = _scan(transactor, tx, columns=["id"], pushdown_filters=pf)
        transactor.commit(tx)

        assert table.num_rows == 0


# ============================================================================
# Transaction isolation (read-your-writes)
# ============================================================================


class TestTransactionIsolation:
    """Read-your-writes and isolation between transactions."""

    def test_read_your_writes(self, transactor: TransactorImpl) -> None:
        """A scan within the same transaction sees previously inserted data."""
        tx = _tx()
        transactor.begin(tx)
        _insert(transactor, tx, [(1, "visible")])
        table = _scan(transactor, tx, columns=["id", "name"])
        transactor.commit(tx)

        assert table.num_rows == 1
        assert table.column("name")[0].as_py() == "visible"

    def test_rollback_undoes_insert(self, transactor: TransactorImpl) -> None:
        """After rollback, inserted rows are gone in a new transaction."""
        tx1 = _tx()
        transactor.begin(tx1)
        _insert(transactor, tx1, [(1, "will_vanish")])
        transactor.rollback(tx1)

        tx2 = _tx()
        transactor.begin(tx2)
        table = _scan(transactor, tx2)
        transactor.commit(tx2)

        assert table.num_rows == 0

    def test_rollback_undoes_delete(self, transactor: TransactorImpl) -> None:
        """After rollback, deleted rows reappear."""
        # Insert and commit
        tx1 = _tx()
        transactor.begin(tx1)
        _insert(transactor, tx1, [(1, "keep_me")])
        rows = _scan(transactor, tx1, columns=["rowid"])
        rowid = rows.column("rowid")[0].as_py()
        transactor.commit(tx1)

        # Delete and rollback
        tx2 = _tx()
        transactor.begin(tx2)
        _delete(transactor, tx2, [rowid])
        transactor.rollback(tx2)

        # Row should still be there
        tx3 = _tx()
        transactor.begin(tx3)
        table = _scan(transactor, tx3, columns=["id", "name"])
        transactor.commit(tx3)

        assert table.num_rows == 1
        assert table.column("name")[0].as_py() == "keep_me"

    def test_rollback_undoes_update(self, transactor: TransactorImpl) -> None:
        """After rollback, updated values revert."""
        tx1 = _tx()
        transactor.begin(tx1)
        _insert(transactor, tx1, [(1, "original")])
        rows = _scan(transactor, tx1, columns=["rowid"])
        rowid = rows.column("rowid")[0].as_py()
        transactor.commit(tx1)

        # Update and rollback
        tx2 = _tx()
        transactor.begin(tx2)
        _update(transactor, tx2, [rowid], ["changed"])
        transactor.rollback(tx2)

        # Value should be original
        tx3 = _tx()
        transactor.begin(tx3)
        table = _scan(transactor, tx3, columns=["name"])
        transactor.commit(tx3)

        assert table.column("name")[0].as_py() == "original"

    def test_isolation_between_transactions(self, transactor: TransactorImpl) -> None:
        """Uncommitted data in tx_A is not visible to tx_B."""
        tx_a = _tx()
        tx_b = _tx()
        transactor.begin(tx_a)
        transactor.begin(tx_b)

        _insert(transactor, tx_a, [(1, "uncommitted")])

        # tx_b should not see tx_a's uncommitted row
        table = _scan(transactor, tx_b, columns=["id"])
        assert table.num_rows == 0

        transactor.commit(tx_a)
        transactor.commit(tx_b)


# ============================================================================
# DDL
# ============================================================================


class TestDDL:
    """DDL execution."""

    def test_execute_ddl(self, transactor: TransactorImpl) -> None:
        """DDL creates a table that can be scanned."""
        transactor.execute_ddl("CREATE TABLE other (val INTEGER)")

        tx = _tx()
        transactor.begin(tx)
        stream = transactor.scan(tx_id=tx, table_name="other", columns=["val"])
        transactor.commit(tx)

        assert stream.output_schema.names == ["val"]


# ============================================================================
# Error handling
# ============================================================================


class TestErrorHandling:
    """Operations with invalid tx_id raise ValueError."""

    def test_insert_wrong_tx_raises(self, transactor: TransactorImpl) -> None:
        """Insert with unknown tx_id raises."""
        with pytest.raises(ValueError, match="No active transaction"):
            transactor.insert(tx_id=_tx(), table_name="data")

    def test_scan_wrong_tx_raises(self, transactor: TransactorImpl) -> None:
        """Scan with unknown tx_id raises."""
        with pytest.raises(ValueError, match="No active transaction"):
            transactor.scan(tx_id=_tx(), table_name="data", columns=["id"])

    def test_delete_wrong_tx_raises(self, transactor: TransactorImpl) -> None:
        """Delete with unknown tx_id raises."""
        with pytest.raises(ValueError, match="No active transaction"):
            transactor.delete(tx_id=_tx(), table_name="data")

    def test_update_wrong_tx_raises(self, transactor: TransactorImpl) -> None:
        """Update with unknown tx_id raises."""
        with pytest.raises(ValueError, match="No active transaction"):
            transactor.update(tx_id=_tx(), table_name="data")

    def test_begin_duplicate_tx_raises(self, transactor: TransactorImpl) -> None:
        """Beginning a transaction with an existing tx_id raises."""
        tx = _tx()
        transactor.begin(tx)
        with pytest.raises(ValueError, match="Transaction already active"):
            transactor.begin(tx)
        transactor.rollback(tx)

    def test_insert_constraint_violation(self, transactor: TransactorImpl) -> None:
        """Insert that violates a constraint propagates the DuckDB error."""
        # Create a table with a unique constraint
        transactor.execute_ddl("CREATE TABLE unique_test (id BIGINT UNIQUE, name VARCHAR)")
        tx = _tx()
        transactor.begin(tx)
        _insert_into(transactor, tx, "unique_test", [(1, "first")])
        with pytest.raises(Exception, match="Constraint"):
            _insert_into(transactor, tx, "unique_test", [(1, "duplicate")])
        transactor.rollback(tx)
