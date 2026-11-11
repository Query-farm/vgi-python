# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The ``txfix`` fixture worker: writable catalog tables and transaction hooks.

Serves the C++ extension's cross-SDK writable-table and transaction-hook
fixtures (Query-farm/vgi ``docs/writable_tx_fixtures.md``,
``test/sql/integration/writable_tx/*.test``, ``VGI_WRITABLE_TX_WORKER``).

Built only from the SDK's public API: a declarative catalog whose ``items``
table declares an update function and write result modes, ATTACH options (one
secret, one typed ``str | None``) read by every function and hook through
the framework's ``attach_options`` accessor (the secret travels encrypted
inside ``attach_opaque_data``; the catalog never handles it), a per-attach
``supports_transactions`` (``attach_supports_transactions``), the catalog's
``catalog_transaction_begin/commit/rollback`` hooks, and the write-result
helpers (``row_id_field``, ``write_options``, ``write_result_schema``,
``write_result_batch``)::

    ATTACH 'txfix' AS t   (TYPE vgi, LOCATION '...', token 'tok-123');
    ATTACH 'txfix' AS ttx (TYPE vgi, LOCATION '...', token 'tok-123', transactional true);

On a transactional attach UPDATE stages its changes under the plaintext
transaction id the function sees; scans on that attach read their own writes;
the commit hook applies them (or refuses a negative qty) and the rollback hook
discards them. State lives in one SQLite file under ``$VGI_TXFIX_STATE_DIR``
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
from vgi.invocation import BindResponse
from vgi.metadata import FunctionStability
from vgi.table_function import BindParams, ProcessParams, TableFunctionGenerator, init_single_worker
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
    qty INTEGER, PRIMARY KEY (part, tx, id));
CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT, part TEXT NOT NULL, kind TEXT NOT NULL,
    tx TEXT, ok INTEGER NOT NULL);
"""


@contextmanager
def _state() -> Iterator[sqlite3.Connection]:
    """One write transaction on the state file (BEGIN IMMEDIATE is the cross-process lock)."""
    directory = os.environ.get("VGI_TXFIX_STATE_DIR") or os.path.join(tempfile.gettempdir(), "vgi-txfix")
    os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(os.path.join(directory, "txfix.db"), timeout=30, isolation_level=None)
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
    for table in ("items", "labels", "staged", "events"):
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
        for r in conn.execute("SELECT id, name, qty FROM staged WHERE part = ? AND tx = ?", (part, tx)):
            rows[r[0]] = {"id": r[0], "name": r[1], "qty": r[2]}
    return dict(sorted(rows.items()))


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


class ItemsUpdate(TableInOutGenerator[None, None]):
    """Backs UPDATE main.items: applies (or, in a transaction, stages) the new values."""

    class Meta:
        """Function metadata."""

        name = "items_update"
        description = "Backs UPDATE main.items: applies (or, in a transaction, stages) the new values"
        stability = FunctionStability.VOLATILE

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """The result shape DuckDB asked for (count or rows)."""
        return BindResponse(output_schema=write_result_schema(write_options(params).result_mode, ITEMS))

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        """Validate every row, then apply or stage them all and emit the write result."""
        del state
        attach = _attach_of(params.attach_opaque_data)
        tx = _tx_of(params.transaction_opaque_data) if attach.transactional else None
        columns = batch.to_pydict()
        before: list[Row] = []
        after: list[Row] = []
        with _state() as conn:
            current = _visible_items(conn, attach.part, tx)
            for i in range(batch.num_rows):
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
                if tx is not None:
                    conn.execute(
                        "INSERT OR REPLACE INTO staged (part, tx, id, name, qty) VALUES (?, ?, ?, ?, ?)",
                        (attach.part, tx, row["id"], row["name"], row["qty"]),
                    )
                else:
                    conn.execute(
                        "UPDATE items SET name = ?, qty = ? WHERE part = ? AND id = ?",
                        (row["name"], row["qty"], attach.part, row["id"]),
                    )
        out.emit(write_result_batch(write_options(params).result_mode, ITEMS, before, after))


class Reset(_OneShot):
    """Restore the partition's initial rows; clear its staged changes and events."""

    OUTPUT = pa.schema([pa.field("ok", pa.bool_())])

    class Meta:
        """Function metadata."""

        name = "reset"
        description = "Restore this token's initial rows; clear its staged changes and events"
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
            # Protocol 3.3 fixture contract: items_scan and items_update are listed
            # automatically as table_only (DuckDB still sees items_scan's projection
            # pushdown, which rowid needs); labels_scan is exposed as a SQL function.
            functions=[Reset, WhoAmI, CurrentTx, Events, TagTx],
            tables=[
                Table(
                    name="items",
                    function=ItemsScan,
                    update_function=ItemsUpdate,
                    write_result_modes={"update": "changes"},
                    constraints=_constraints("items", ["id", "name"], "id"),
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
                "SELECT id, name, qty FROM staged WHERE part = ? AND tx = ? ORDER BY id", (part, tx)
            ).fetchall()
            conn.execute("DELETE FROM staged WHERE part = ? AND tx = ?", (part, tx))
            bad = next((r for r in rows if r[2] is not None and r[2] < 0), None)
            if bad is not None:
                _log_event(conn, part, "commit", tx, ok=False)
                refusal = f"txfix: commit refused: qty {bad[2]} for id {bad[0]}"
            else:
                conn.executemany(
                    "UPDATE items SET name = ?, qty = ? WHERE part = ? AND id = ?",
                    [(r[1], r[2], part, r[0]) for r in rows],
                )
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
