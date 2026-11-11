# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The ``txfix`` fixture worker: writable catalog tables and transaction hooks.

Serves the C++ extension's cross-SDK writable-table and transaction-hook
fixtures (Query-farm/vgi ``docs/writable_tx_fixtures.md``,
``test/sql/integration/writable_tx/*.test``, ``VGI_WRITABLE_TX_WORKER``).

Built only from the SDK's public API: a declarative catalog whose ``items``
table declares insert, update and delete functions, write result modes and a
``qty`` default, ATTACH options (one
secret, one typed ``str | None``) read by every function and hook through
the framework's ``attach_options`` accessor (the secret travels encrypted
inside ``attach_opaque_data``; the catalog never handles it), a per-attach
``supports_transactions`` (``attach_supports_transactions``), the catalog's
``catalog_transaction_begin/commit/rollback`` hooks, and the write-result
helpers (``row_id_field``, ``write_options``, ``write_result_schema``,
``write_result_batch``)::

    ATTACH 'txfix' AS t   (TYPE vgi, LOCATION '...', token 'tok-123');
    ATTACH 'txfix' AS ttx (TYPE vgi, LOCATION '...', token 'tok-123', transactional true);

``items`` has an insert, an update and a delete function, all table-only and
all with a finalize stage (``has_finalize``) that marks the call's ``writes()``
row finalized. On a transactional attach INSERT, UPDATE and DELETE stage their
changes under the plaintext transaction id the function sees; scans on that
attach read their own writes; the commit hook applies them (or refuses a
negative qty) and the rollback hook discards them. Scans read a snapshot when
they start. State lives in one SQLite file under ``$VGI_TXFIX_STATE_DIR``
(default ``<tmpdir>/vgi-txfix``), partitioned by the ``token`` option, because
the extension may route the statements of one transaction to different worker
processes.

Registered as the ``vgi-fixture-writable-tx-worker`` entry point (stdio,
``--unix`` for the launcher, ``--http [--port N]``).
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.arguments import Arg, TableInput
from vgi.catalog import (
    AttachOpaqueData,
    Catalog,
    ReadOnlyCatalogInterface,
    Schema,
    Table,
    TransactionOpaqueData,
    attach_options,
    constraint_record,
)
from vgi.catalog.attach_option import AttachOption
from vgi.catalog.constraints import ConstraintInfo
from vgi.invocation import BindResponse, GlobalInitResponse
from vgi.metadata import FunctionStability
from vgi.table_function import BindParams, InitParams, ProcessParams, TableFunctionGenerator, init_single_worker
from vgi.table_in_out_function import TableInOutGenerator
from vgi.worker import Worker
from vgi.write_results import row_id_field, write_options, write_result_batch, write_result_schema

__all__ = ["TxfixCatalog", "TxfixWorker", "main"]

CATALOG_NAME = "txfix"


# ---------------------------------------------------------------------------
# ATTACH options
# ---------------------------------------------------------------------------


class TxfixAttachOptions:
    """Options accepted by ``ATTACH 'txfix' (TYPE vgi, ...)``."""

    transactional: Annotated[
        bool, AttachOption(desc="Stage UPDATEs in DuckDB transactions (supports_transactions for this attach)")
    ] = False
    token: Annotated[str | None, AttachOption(desc="A credential every function must receive", secret=True)] = None


@dataclass(frozen=True, slots=True)
class _Attach:
    token: str | None
    transactional: bool

    @property
    def part(self) -> str:
        """The state partition of this attach: its token (NULL is a partition of its own)."""
        return "null:" if self.token is None else f"token:{self.token}"


def _attach_of(attach_opaque_data: bytes | None) -> _Attach:
    """The attach's options, via the framework accessor (secret ``token`` included)."""
    if attach_opaque_data is None:
        raise ValueError("txfix: attach_opaque_data missing")
    options = attach_options(attach_opaque_data)
    token = options.get("token")
    return _Attach(token=None if token is None else str(token), transactional=options.get("transactional") is True)


def _tx_of(transaction_opaque_data: bytes | None) -> str | None:
    """The plaintext transaction id as lowercase hex, or None outside a transaction."""
    return bytes(transaction_opaque_data).hex() if transaction_opaque_data else None


# ---------------------------------------------------------------------------
# State: one SQLite file shared by every worker process
# ---------------------------------------------------------------------------

_INITIAL_ITEMS = [(1, "a", 10), (2, "b", 20), (3, "c", 30)]
_INITIAL_LABELS = [(1, "x"), (2, "y")]

_DDL = """
CREATE TABLE IF NOT EXISTS parts (part TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS items (part TEXT NOT NULL, id INTEGER NOT NULL, name TEXT, qty INTEGER,
    PRIMARY KEY (part, id));
CREATE TABLE IF NOT EXISTS labels (part TEXT NOT NULL, id INTEGER NOT NULL, label TEXT, PRIMARY KEY (part, id));
CREATE TABLE IF NOT EXISTS staged (part TEXT NOT NULL, tx TEXT NOT NULL, id INTEGER NOT NULL, name TEXT,
    qty INTEGER, deleted INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (part, tx, id));
CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT, part TEXT NOT NULL, kind TEXT NOT NULL,
    tx TEXT, ok INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS writes (seq INTEGER PRIMARY KEY AUTOINCREMENT, part TEXT NOT NULL, op TEXT NOT NULL,
    tx TEXT, rows INTEGER NOT NULL DEFAULT 0, finalized INTEGER NOT NULL DEFAULT 0);
"""

#: Bumped whenever ``_DDL`` changes shape, so an old state file is never reused.
_STATE_FILE = "txfix-v2.db"


@contextmanager
def _state() -> Iterator[sqlite3.Connection]:
    """One write transaction on the state file (BEGIN IMMEDIATE is the cross-process lock)."""
    directory = os.environ.get("VGI_TXFIX_STATE_DIR") or os.path.join(tempfile.gettempdir(), "vgi-txfix")
    os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(os.path.join(directory, _STATE_FILE), timeout=30, isolation_level=None)
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(_DDL)
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
    finally:
        conn.close()


def _seed(conn: sqlite3.Connection, part: str, *, force: bool) -> None:
    fresh = conn.execute("INSERT OR IGNORE INTO parts (part) VALUES (?)", (part,)).rowcount == 1
    if not fresh and not force:
        return
    for table in ("items", "labels", "staged", "events", "writes"):
        conn.execute(f"DELETE FROM {table} WHERE part = ?", (part,))  # noqa: S608 - fixed table names
    conn.executemany(
        "INSERT INTO items (part, id, name, qty) VALUES (?, ?, ?, ?)", [(part, *r) for r in _INITIAL_ITEMS]
    )
    conn.executemany("INSERT INTO labels (part, id, label) VALUES (?, ?, ?)", [(part, *r) for r in _INITIAL_LABELS])


Row = dict[str, Any]


def _visible_items(conn: sqlite3.Connection, part: str, tx: str | None) -> dict[int, Row]:
    """The rows ``part`` sees: committed, with transaction ``tx``'s staged rows on top."""
    _seed(conn, part, force=False)
    rows = {
        r[0]: {"id": r[0], "name": r[1], "qty": r[2]}
        for r in conn.execute("SELECT id, name, qty FROM items WHERE part = ?", (part,))
    }
    if tx is not None:
        for r in conn.execute("SELECT id, name, qty, deleted FROM staged WHERE part = ? AND tx = ?", (part, tx)):
            if r[3]:
                rows.pop(r[0], None)
            else:
                rows[r[0]] = {"id": r[0], "name": r[1], "qty": r[2]}
    return dict(sorted(rows.items()))


def _put(conn: sqlite3.Connection, part: str, tx: str | None, row: Row) -> None:
    """Insert or overwrite one row: committed on ``t``, staged under ``tx`` on ``ttx``."""
    if tx is None:
        conn.execute(
            "INSERT OR REPLACE INTO items (part, id, name, qty) VALUES (?, ?, ?, ?)",
            (part, row["id"], row["name"], row["qty"]),
        )
    else:
        conn.execute(
            "INSERT OR REPLACE INTO staged (part, tx, id, name, qty, deleted) VALUES (?, ?, ?, ?, ?, 0)",
            (part, tx, row["id"], row["name"], row["qty"]),
        )


def _remove(conn: sqlite3.Connection, part: str, tx: str | None, row_id: int) -> None:
    """Delete one row: committed on ``t``; on ``ttx`` a staged tombstone (which also drops a staged insert)."""
    if tx is None:
        conn.execute("DELETE FROM items WHERE part = ? AND id = ?", (part, row_id))
    else:
        conn.execute(
            "INSERT OR REPLACE INTO staged (part, tx, id, name, qty, deleted) VALUES (?, ?, ?, NULL, NULL, 1)",
            (part, tx, row_id),
        )


def _log_event(conn: sqlite3.Connection, part: str, kind: str, tx: str | None, *, ok: bool) -> None:
    conn.execute("INSERT INTO events (part, kind, tx, ok) VALUES (?, ?, ?, ?)", (part, kind, tx, int(ok)))


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------


def _schema(*fields: pa.Field[Any]) -> pa.Schema:
    return pa.schema(list(fields))


ITEMS = _schema(
    pa.field("id", pa.int32(), nullable=False),
    pa.field("name", pa.string(), nullable=False),
    pa.field("qty", pa.int64()),
)
LABELS = _schema(pa.field("id", pa.int32(), nullable=False), pa.field("label", pa.string()))
_UPDATABLE = frozenset({"name", "qty"})


@dataclass(kw_only=True)
class _Once(ArrowSerializableDataclass):
    done: bool = False


@init_single_worker
class _OneShot(TableFunctionGenerator[None, _Once]):
    """A no-argument table function that emits one batch, computed at process time."""

    OUTPUT: ClassVar[pa.Schema]

    @classmethod
    def rows(cls, params: ProcessParams[None]) -> dict[str, list[Any]]:
        """Columns of the one batch."""
        raise NotImplementedError

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """Declare the fixed output schema."""
        del params
        return BindResponse(output_schema=cls.OUTPUT)

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> _Once:
        """Nothing emitted yet."""
        del params
        return _Once()

    @classmethod
    def process(cls, params: ProcessParams[None], state: _Once, out: OutputCollector) -> None:
        """Emit the batch once, reading attach and transaction data now."""
        if state.done:
            out.finish()
            return
        state.done = True
        out.emit(pa.RecordBatch.from_pydict(cls.rows(params), schema=cls.OUTPUT))
        out.finish()


@init_single_worker
class ItemsScan(TableFunctionGenerator[None, _Once]):
    """Scan main.items (staged changes on top inside a transaction), with its row id."""

    class Meta:
        """Function metadata."""

        name = "items_scan"
        description = "The rows of main.items (staged changes included inside a transaction), with rowid"
        stability = FunctionStability.VOLATILE
        # DuckDB reads `rowid` as a virtual column, which needs projection pushdown.
        projection_pushdown = True

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """User columns plus the row id."""
        del params
        return BindResponse(output_schema=pa.schema([*ITEMS, row_id_field()]))

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> _Once:
        """Nothing emitted yet."""
        del params
        return _Once()

    @classmethod
    def process(cls, params: ProcessParams[None], state: _Once, out: OutputCollector) -> None:
        """Emit the visible rows, only the columns DuckDB asked for."""
        if state.done:
            out.finish()
            return
        state.done = True
        attach = _attach_of(params.attach_opaque_data)
        tx = _tx_of(params.transaction_opaque_data) if attach.transactional else None
        # Snapshot: every row is read here, once, when the scan starts, so an
        # INSERT INTO items SELECT ... FROM items (or a MERGE) never sees the
        # rows its own write calls add while this scan runs.
        with _state() as conn:
            rows = list(_visible_items(conn, attach.part, tx).values())
        # Positional: DuckDB may request a column twice.
        arrays = [
            pa.array([r["id"] if f.name == "rowid" else r[f.name] for r in rows], type=f.type)
            for f in params.output_schema
        ]
        out.emit(pa.RecordBatch.from_arrays(arrays, schema=params.output_schema))
        out.finish()


class LabelsScan(_OneShot):
    """Scan main.labels."""

    OUTPUT = LABELS

    class Meta:
        """Function metadata."""

        name = "labels_scan"
        description = "The rows of main.labels"
        stability = FunctionStability.VOLATILE

    @classmethod
    def rows(cls, params: ProcessParams[None]) -> dict[str, list[Any]]:
        """The partition's labels."""
        part = _attach_of(params.attach_opaque_data).part
        with _state() as conn:
            _seed(conn, part, force=False)
            rows = conn.execute("SELECT id, label FROM labels WHERE part = ? ORDER BY id", (part,)).fetchall()
        return {"id": [r[0] for r in rows], "label": [r[1] for r in rows]}


def _same(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return str(a) == str(b)


#: Execution-scoped storage slot holding the call's ``writes`` seq (on_init -> process -> finalize).
_CALL_NS = b"txfix.write_call"
_CALL_KEY = b"seq"


class _ItemsWrite(TableInOutGenerator[None, None]):
    """One write call on main.items: recorded in ``writes()`` at init, finalized at finalize.

    The call's ``writes`` row is created when the call starts (``on_init``, once
    per call, before any batch: a MERGE action no row reaches still gets one).
    Each batch adds the rows it reports, in the same SQLite transaction that
    applies them; ``finalize`` (``has_finalize``) emits nothing and marks the
    row finalized. The row's ``seq`` travels from init to process and finalize
    in the call's execution-scoped storage, so it survives the call moving
    between processes.
    """

    OP: ClassVar[str]

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """The result shape DuckDB asked for (count or rows)."""
        return BindResponse(output_schema=write_result_schema(write_options(params).result_mode, ITEMS))

    @classmethod
    def on_init(cls, params: InitParams[None]) -> GlobalInitResponse:
        """Record the call (not yet finalized); one worker, so batches arrive in order."""
        attach = _attach_of(params.attach_opaque_data)
        tx = _tx_of(params.transaction_opaque_data) if attach.transactional else None
        with _state() as conn:
            _seed(conn, attach.part, force=False)
            seq = conn.execute(
                "INSERT INTO writes (part, op, tx, rows, finalized) VALUES (?, ?, ?, 0, 0)",
                (attach.part, cls.OP, tx),
            ).lastrowid
        params.storage.state_put(_CALL_NS, _CALL_KEY, str(seq).encode())
        return GlobalInitResponse(max_workers=1)

    @classmethod
    def _call_seq(cls, params: ProcessParams[None]) -> int:
        raw = params.storage.state_get(_CALL_NS, _CALL_KEY)
        if raw is None:
            raise ValueError(f"txfix: {cls.Meta.name} has no recorded write call")  # type: ignore[attr-defined]
        return int(raw)

    @classmethod
    def apply(
        cls, conn: sqlite3.Connection, part: str, tx: str | None, columns: dict[str, list[Any]], n: int
    ) -> tuple[list[Row | None], list[Row | None]]:
        """Validate every row of the batch, then apply or stage it; return (before, after) images."""
        raise NotImplementedError

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        """Validate the whole batch, apply (or stage) it, count it, and emit the write result."""
        del state
        attach = _attach_of(params.attach_opaque_data)
        tx = _tx_of(params.transaction_opaque_data) if attach.transactional else None
        seq = cls._call_seq(params)
        with _state() as conn:
            before, after = cls.apply(conn, attach.part, tx, batch.to_pydict(), batch.num_rows)
            conn.execute("UPDATE writes SET rows = rows + ? WHERE seq = ?", (len(after), seq))
        out.emit(write_result_batch(write_options(params).result_mode, ITEMS, before, after))

    @classmethod
    def finalize(cls, params: ProcessParams[None]) -> list[pa.RecordBatch]:
        """Mark the call finalized; the per-batch results already carried every row."""
        seq = cls._call_seq(params)
        with _state() as conn:
            conn.execute("UPDATE writes SET finalized = 1 WHERE seq = ?", (seq,))
        return []


class ItemsInsert(_ItemsWrite):
    """Backs INSERT main.items: inserts (or, in a transaction, stages) full rows."""

    OP = "insert"

    class Meta:
        """Function metadata."""

        name = "items_insert"
        description = "Backs INSERT main.items: inserts (or, in a transaction, stages) full rows"
        stability = FunctionStability.VOLATILE
        has_finalize = True

    @classmethod
    def apply(
        cls, conn: sqlite3.Connection, part: str, tx: str | None, columns: dict[str, list[Any]], n: int
    ) -> tuple[list[Row | None], list[Row | None]]:
        """Check id/name/duplicates/qty for every row, then write them all."""
        current = _visible_items(conn, part, tx)
        rows: list[Row] = []
        seen: set[int] = set()
        for i in range(n):
            row = {name: columns[name][i] if name in columns else None for name in ("id", "name", "qty")}
            if row["id"] is None:
                raise ValueError("txfix: id required")
            if row["name"] is None:
                raise ValueError("txfix: name required")
            if row["id"] in current or row["id"] in seen:
                raise ValueError(f"txfix: duplicate id {row['id']}")
            if tx is None and row["qty"] is not None and row["qty"] < 0:
                raise ValueError("txfix: qty must be >= 0")
            seen.add(row["id"])
            rows.append(row)
        for row in rows:
            _put(conn, part, tx, row)
        return [None] * len(rows), list(rows)


class ItemsDelete(_ItemsWrite):
    """Backs DELETE main.items: rowid = id; a missing or repeated rowid is not counted."""

    OP = "delete"

    class Meta:
        """Function metadata."""

        name = "items_delete"
        description = "Backs DELETE main.items: deletes (or, in a transaction, stages the deletion of) rows by rowid"
        stability = FunctionStability.VOLATILE
        has_finalize = True

    @classmethod
    def apply(
        cls, conn: sqlite3.Connection, part: str, tx: str | None, columns: dict[str, list[Any]], n: int
    ) -> tuple[list[Row | None], list[Row | None]]:
        """Delete each visible rowid once; return the deleted rows' old images."""
        current = _visible_items(conn, part, tx)
        deleted: list[Row] = []
        for rowid in columns["rowid"][:n]:
            old = current.pop(rowid, None) if rowid is not None else None
            if old is None:
                continue  # no visible row, or already deleted by this call: a no-op, not counted
            deleted.append(old)
        for old in deleted:
            _remove(conn, part, tx, old["id"])
        return list(deleted), [None] * len(deleted)


class ItemsUpdate(_ItemsWrite):
    """Backs UPDATE main.items: applies (or, in a transaction, stages) the new values."""

    OP = "update"

    class Meta:
        """Function metadata."""

        name = "items_update"
        description = "Backs UPDATE main.items: applies (or, in a transaction, stages) the new values"
        stability = FunctionStability.VOLATILE
        has_finalize = True

    @classmethod
    def apply(
        cls, conn: sqlite3.Connection, part: str, tx: str | None, columns: dict[str, list[Any]], n: int
    ) -> tuple[list[Row | None], list[Row | None]]:
        """Validate every row, then apply or stage them all."""
        before: list[Row] = []
        after: list[Row] = []
        current = _visible_items(conn, part, tx)
        for i in range(n):
            rowid = columns["rowid"][i]
            old = current.get(rowid)
            if old is None:
                raise ValueError(f"txfix: no row with rowid {rowid}")
            new = dict(old)
            for name, values in columns.items():
                if name == "rowid" or _same(values[i], old[name]):
                    continue
                if name == "id":
                    raise ValueError("txfix: id is not updatable")
                if name not in _UPDATABLE:
                    raise ValueError(f"txfix: {name} is not a column of items")
                if name == "name" and values[i] is None:
                    raise ValueError("txfix: name must not be NULL")
                new[name] = values[i]
            if tx is None and new["qty"] is not None and new["qty"] < 0:
                raise ValueError("txfix: qty must be >= 0")
            before.append(old)
            after.append(new)
            current[rowid] = new
        for row in after:
            _put(conn, part, tx, row)
        return list(before), list(after)


class Reset(_OneShot):
    """Restore the partition's initial rows; clear its staged changes, events and write calls."""

    OUTPUT = pa.schema([pa.field("ok", pa.bool_())])

    class Meta:
        """Function metadata."""

        name = "reset"
        description = "Restore this token's initial rows; clear its staged changes, events and write calls"
        stability = FunctionStability.VOLATILE

    @classmethod
    def rows(cls, params: ProcessParams[None]) -> dict[str, list[Any]]:
        """Reseed and report success."""
        part = _attach_of(params.attach_opaque_data).part
        with _state() as conn:
            _seed(conn, part, force=True)
        return {"ok": [True]}


class WhoAmI(_OneShot):
    """The attach's options as functions see them."""

    OUTPUT = _schema(
        pa.field("token", pa.string()),
        pa.field("transactional", pa.bool_()),
        pa.field("token_in_attach_data", pa.bool_()),
    )

    class Meta:
        """Function metadata."""

        name = "whoami"
        description = "The attach's options as functions see them"
        stability = FunctionStability.VOLATILE

    @classmethod
    def rows(cls, params: ProcessParams[None]) -> dict[str, list[Any]]:
        """Token, transactional flag, and whether the token's bytes appear in the attach bytes."""
        attach = _attach_of(params.attach_opaque_data)
        raw = bytes(params.attach_opaque_data or b"")
        leaked = attach.token is not None and attach.token.encode() in raw
        return {"token": [attach.token], "transactional": [attach.transactional], "token_in_attach_data": [leaked]}


class CurrentTx(_OneShot):
    """The plaintext transaction id this call received (lowercase hex)."""

    OUTPUT = _schema(pa.field("in_tx", pa.bool_()), pa.field("tx_id", pa.string()))

    class Meta:
        """Function metadata."""

        name = "current_tx"
        description = "The plaintext transaction id this call received (lowercase hex)"
        stability = FunctionStability.VOLATILE

    @classmethod
    def rows(cls, params: ProcessParams[None]) -> dict[str, list[Any]]:
        """Whether a transaction id arrived, and which."""
        tx = _tx_of(params.transaction_opaque_data)
        return {"in_tx": [tx is not None], "tx_id": [tx]}


class Events(_OneShot):
    """This token's transaction-hook event log."""

    OUTPUT = _schema(
        pa.field("seq", pa.int64()),
        pa.field("kind", pa.string()),
        pa.field("tx_id", pa.string()),
        pa.field("ok", pa.bool_()),
    )

    class Meta:
        """Function metadata."""

        name = "events"
        description = "This token's transaction-hook event log"
        stability = FunctionStability.VOLATILE

    @classmethod
    def rows(cls, params: ProcessParams[None]) -> dict[str, list[Any]]:
        """The partition's events in order."""
        part = _attach_of(params.attach_opaque_data).part
        with _state() as conn:
            rows = conn.execute("SELECT seq, kind, tx, ok FROM events WHERE part = ? ORDER BY seq", (part,)).fetchall()
        return {
            "seq": [r[0] for r in rows],
            "kind": [r[1] for r in rows],
            "tx_id": [r[2] for r in rows],
            "ok": [r[3] == 1 for r in rows],
        }


class Writes(_OneShot):
    """One row per write call on main.items (``finalized`` once its finalize stage ran)."""

    OUTPUT = _schema(
        pa.field("seq", pa.int64()),
        pa.field("op", pa.string()),
        pa.field("tx_id", pa.string()),
        pa.field("rows", pa.int64()),
        pa.field("finalized", pa.bool_()),
    )

    class Meta:
        """Function metadata."""

        name = "writes"
        description = "One row per write call on main.items (finalized once its finalize stage ran)"
        stability = FunctionStability.VOLATILE

    @classmethod
    def rows(cls, params: ProcessParams[None]) -> dict[str, list[Any]]:
        """The partition's write calls in order."""
        part = _attach_of(params.attach_opaque_data).part
        with _state() as conn:
            rows = conn.execute(
                "SELECT seq, op, tx, rows, finalized FROM writes WHERE part = ? ORDER BY seq", (part,)
            ).fetchall()
        return {
            "seq": [r[0] for r in rows],
            "op": [r[1] for r in rows],
            "tx_id": [r[2] for r in rows],
            "rows": [r[3] for r in rows],
            "finalized": [r[4] == 1 for r in rows],
        }


@dataclass(slots=True, frozen=True, kw_only=True)
class _TableArg:
    """tag_tx(TABLE): one table argument."""

    data: Annotated[TableInput, Arg(0, doc="Input rows")]


class TagTx(TableInOutGenerator[_TableArg, None]):
    """Pass every input row through, adding the transaction id seen while processing it."""

    class Meta:
        """Function metadata."""

        name = "tag_tx"
        description = "Pass every input row through, adding the transaction id seen while processing it"
        stability = FunctionStability.VOLATILE

    @classmethod
    def on_bind(cls, params: BindParams[_TableArg]) -> BindResponse:
        """Input columns plus ``tx_id``."""
        input_schema = params.bind_call.input_schema
        fields = list(input_schema) if input_schema is not None else []
        return BindResponse(output_schema=pa.schema([*fields, pa.field("tx_id", pa.string())]))

    @classmethod
    def process(
        cls, params: ProcessParams[_TableArg], state: None, batch: pa.RecordBatch, out: OutputCollector
    ) -> None:
        """Append the process-time transaction id to every row."""
        del state
        tx = _tx_of(params.transaction_opaque_data)
        tagged = batch.append_column(pa.field("tx_id", pa.string()), pa.array([tx] * batch.num_rows, pa.string()))
        out.emit(tagged.select(params.output_schema.names) if tagged.schema != params.output_schema else tagged)


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------


def _constraints(table: str, not_null: list[str], key: str) -> tuple[ConstraintInfo, ...]:
    def record(constraint_id: str, kind: str, column: str) -> ConstraintInfo:
        return constraint_record(
            ["main"], table, constraint_id, kind, [[column]], enforcement="ENFORCED", validation="ALL_ROWS"
        )

    return (
        *(record(f"{col}_not_null", "NOT_NULL", col) for col in not_null),
        record("pk", "PRIMARY_KEY", key),
    )


CATALOG = Catalog(
    name=CATALOG_NAME,
    schemas=[
        Schema(
            path=["main"],
            # Protocol 3.3 fixture contract: items_scan and the three write
            # functions are listed automatically as table_only (DuckDB still sees
            # items_scan's projection pushdown, which rowid needs, and the write
            # functions' has_finalize); labels_scan is exposed as a SQL function.
            functions=[Reset, WhoAmI, CurrentTx, Events, Writes, TagTx],
            tables=[
                Table(
                    name="items",
                    function=ItemsScan,
                    insert_function=ItemsInsert,
                    update_function=ItemsUpdate,
                    delete_function=ItemsDelete,
                    write_result_modes={"insert": "changes", "update": "changes", "delete": "changes"},
                    constraints=_constraints("items", ["id", "name"], "id"),
                    # Public column-default API: served as vgi.column.default.v1 metadata,
                    # so DuckDB fills an omitted qty before items_insert sees the row.
                    defaults={"qty": 0},
                ),
                Table(
                    name="labels",
                    function=LabelsScan,
                    expose_scan=True,
                    constraints=_constraints("labels", ["id"], "id"),
                ),
            ],
        )
    ],
)


class TxfixCatalog(ReadOnlyCatalogInterface):
    """The descriptor catalog plus ATTACH options and transaction hooks."""

    catalog = CATALOG
    catalog_name = CATALOG_NAME

    def attach_supports_transactions(
        self, *, name: str, options: dict[str, Any], attach_opaque_data: AttachOpaqueData
    ) -> bool:
        """Only ``transactional true`` asks DuckDB for the hooks below."""
        del name, attach_opaque_data
        return {key.lower(): value for key, value in options.items()}.get("transactional") is True

    def catalog_transaction_begin(self, *, attach_opaque_data: AttachOpaqueData) -> TransactionOpaqueData | None:
        """Mint a 16-byte transaction id and log ``begin``."""
        tx = os.urandom(16)
        part = _attach_of(attach_opaque_data).part
        with _state() as conn:
            _seed(conn, part, force=False)
            _log_event(conn, part, "begin", tx.hex(), ok=True)
        return TransactionOpaqueData(tx)

    def catalog_transaction_commit(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData
    ) -> None:
        """Apply the staged rows, or refuse a negative qty (discarding them)."""
        part = _attach_of(attach_opaque_data).part
        tx = bytes(transaction_opaque_data).hex()
        refusal: str | None = None
        with _state() as conn:
            rows = conn.execute(
                "SELECT id, name, qty, deleted FROM staged WHERE part = ? AND tx = ? ORDER BY id", (part, tx)
            ).fetchall()
            conn.execute("DELETE FROM staged WHERE part = ? AND tx = ?", (part, tx))
            # Inserted or updated rows (not tombstones) with a negative qty refuse the commit.
            bad = next((r for r in rows if not r[3] and r[2] is not None and r[2] < 0), None)
            if bad is not None:
                _log_event(conn, part, "commit", tx, ok=False)
                refusal = f"txfix: commit refused: qty {bad[2]} for id {bad[0]}"
            else:
                for row_id, name, qty, deleted in rows:
                    if deleted:
                        _remove(conn, part, None, row_id)
                    else:
                        _put(conn, part, None, {"id": row_id, "name": name, "qty": qty})
                _log_event(conn, part, "commit", tx, ok=True)
        if refusal is not None:
            raise ValueError(refusal)

    def catalog_transaction_rollback(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData
    ) -> None:
        """Discard the staged rows and log ``rollback``."""
        part = _attach_of(attach_opaque_data).part
        tx = bytes(transaction_opaque_data).hex()
        with _state() as conn:
            conn.execute("DELETE FROM staged WHERE part = ? AND tx = ?", (part, tx))
            _log_event(conn, part, "rollback", tx, ok=True)


class TxfixWorker(Worker):
    """Serves the ``txfix`` catalog."""

    AttachOptions = TxfixAttachOptions
    catalog_interface = TxfixCatalog
    catalog_name = CATALOG_NAME
    catalog = CATALOG


def main() -> None:
    """Run the worker (stdio, ``--unix`` or ``--http``)."""
    TxfixWorker.main()


if __name__ == "__main__":
    main()
