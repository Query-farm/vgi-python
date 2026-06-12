# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""VGI worker that accumulates table rows, keyed by name, in framework storage.

Functions
---------
- ``accumulate(name, <rows>, ttl, max_row_size, result)`` — append rows to a
  named collection and optionally return its contents.
- ``accumulate_read(name)`` — read a collection's contents without modifying it.
- ``accumulate_clear(name)`` — drop a collection; returns rows removed.

Each ``accumulate`` call stamps the input rows with a single call-time
``_timestamp`` and appends them. The ``result`` option controls what it returns:
``'all'`` (the whole collection, default), ``'new'`` (only the rows added by
this call), or ``'none'`` (nothing — a cheap append). The input schema is
validated against whatever schema was first accumulated under that name. Two
optional named parameters bound the collection: ``ttl`` (an INTERVAL — rows
older than ``call_time - ttl`` are evicted) and ``max_row_size`` (a row cap;
oldest dropped first).

Storage, scoping & performance
------------------------------
Data is persisted through the VGI framework's ``FunctionStorage`` (the worker's
``cls.storage``), so the backend is pluggable via ``VGI_WORKER_SHARED_STORAGE``:
a file-backed SQLite (default; persistent across restarts), in-memory, Azure
SQL, or Cloudflare Durable Objects (the last two are durable across machines).

Each collection is scoped to a random *attach id* minted once per ``ATTACH``
(carried back on every call via the catalog's attach-opaque-data), so two
independent ATTACH sessions never share a collection. Within that scope a
collection's rows live as append-only *segments* keyed by ingest time under a
per-collection namespace, so an append is O(batch) and needs no lock: each op is
a single atomic storage statement. A TTL evicts in one ranged delete of the
time-ordered key range (whole expired segments, exactly the expired rows, since
a segment carries a single call timestamp). ``max_row_size`` keeps an atomic
int64 row counter and, when the cap is exceeded, drops the oldest segments
(trimming only the one straddling segment) — no whole-collection repack.

Usage
-----
Hosted inside the consolidated ``vgi-fixture-worker`` (and the
``vgi-fixture-http`` server) via MetaWorker — attach by catalog name:

    ATTACH 'accumulate' AS accumulate (TYPE vgi, LOCATION '${VGI_TEST_WORKER}');
    SELECT * FROM accumulate.main.accumulate('events', (SELECT * FROM my_rows));
    SELECT * FROM accumulate.main.accumulate('events', (VALUES (1)) t(x), result := 'new');
    SELECT * FROM accumulate.main.accumulate_read('events');
    SELECT * FROM accumulate.main.accumulate_clear('events');

Exercised end-to-end by ``test/sql/integration/accumulate/*.test`` in the C++
repo and mirrored by ``tests/conformance/test_accumulate.py``.
"""

from __future__ import annotations

import dataclasses
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Annotated, Any

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi import Worker
from vgi.arguments import Arg, TableInput
from vgi.catalog import Catalog, ReadOnlyCatalogInterface, Schema
from vgi.catalog.catalog_interface import AttachOpaqueData, CatalogAttachResult, CatalogInfo
from vgi.function_storage import BoundStorage, FunctionStorage
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_buffering_function import TableBufferingFunction, TableBufferingParams
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)

if TYPE_CHECKING:
    from vgi_rpc.rpc import CallContext

DATA_VERSION = "2.0.0"
IMPLEMENTATION_VERSION = "vgi-fixture"

# Name of the column appended to every output row holding the per-call ingest
# time. Plain (tz-naive) microsecond timestamp so it surfaces as DuckDB
# TIMESTAMP rather than TIMESTAMP WITH TIME ZONE. Underscore-prefixed so it is
# unlikely to collide with a user's own column named ``timestamp``.
TIMESTAMP_COLUMN = "_timestamp"
TIMESTAMP_TYPE = pa.timestamp("us")

# Target rows per emitted/staged batch (output is streamed in chunks of this
# size) and per stored/repacked segment.
OUT_BATCH_ROWS = 65536

# Execution-scoped BoundStorage namespaces (transient per query) for the
# buffering operator's Sink->Combine->Source handoff and for accumulate_read.
_NS_IN = b"in"  # staged input batches (Sink -> Combine)
_NS_OUT = b"out"  # staged result rows  (Combine -> Source/finalize)
_NS_READ = b"read"  # staged snapshot for accumulate_read

# Persistent (attach-scoped) namespaces. A collection's segments live under a
# per-collection namespace keyed by ingest time, so the whole collection wipes
# with one namespace delete and a TTL cutoff is one ranged delete. The schema
# lives under a shared meta namespace; the row count under a per-collection
# int64 counter (the separate function_counter table) keyed by collection name
# in that same namespace.
_SEG_NS_PREFIX = b"seg:"
_META_NS = b"meta"

_EPOCH = datetime(1970, 1, 1)

# Width of the big-endian ingest-time prefix on each segment key, so segment
# keys sort by time (memcmp == numeric for fixed-width unsigned big-endian).
_TS_KEY_BYTES = 8


# ---------------------------------------------------------------------------
# Time / schema helpers
# ---------------------------------------------------------------------------


def _now_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _to_us(dt: datetime) -> int:
    return (dt - _EPOCH) // timedelta(microseconds=1)


def _interval_to_timedelta(interval: Any) -> timedelta:
    """Convert a DuckDB INTERVAL (pa.MonthDayNano) to a timedelta.

    Calendar months have no fixed length, so each month is approximated as 30
    days: ``INTERVAL '1 month'`` evicts rows older than 30 days, not older than
    one calendar month. Use ``INTERVAL '30 days'`` / ``'24 hours'`` etc. when an
    exact span matters.
    """
    months = getattr(interval, "months", 0) or 0
    days = getattr(interval, "days", 0) or 0
    nanoseconds = getattr(interval, "nanoseconds", 0) or 0
    return timedelta(days=months * 30 + days, microseconds=nanoseconds // 1000)


def _output_schema(input_schema: pa.Schema) -> pa.Schema:
    return pa.schema(list(input_schema) + [pa.field(TIMESTAMP_COLUMN, TIMESTAMP_TYPE)])


def _input_schema_of(output_schema: pa.Schema) -> pa.Schema:
    return pa.schema([f for f in output_schema if f.name != TIMESTAMP_COLUMN])


def _schemas_match(expected: pa.Schema, actual: pa.Schema) -> bool:
    return expected.equals(actual, check_metadata=False)


# Upper bound on a collection name's UTF-8 byte length. The name becomes the
# suffix of a storage namespace key (``seg:<name>``), so it is bounded to keep
# keys small; the limit is generous enough for any real-world name.
_MAX_NAME_BYTES = 255


def _validate_name(name: str) -> None:
    """Reject empty/blank or oversized collection names at bind time."""
    if not name or not name.strip():
        raise ValueError("collection name must be a non-empty string")
    if len(name.encode()) > _MAX_NAME_BYTES:
        raise ValueError(f"collection name must be at most {_MAX_NAME_BYTES} bytes")


# ---------------------------------------------------------------------------
# Arrow IPC (de)serialization
# ---------------------------------------------------------------------------


def _table_to_ipc(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _table_from_ipc(blob: bytes) -> pa.Table:
    with pa.ipc.open_stream(pa.py_buffer(blob)) as reader:
        return reader.read_all()


def _batch_to_ipc(batch: pa.RecordBatch) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _batch_from_ipc(value: bytes) -> pa.RecordBatch:
    return pa.ipc.open_stream(value).read_next_batch()


def _schema_to_ipc(schema: pa.Schema) -> bytes:
    return _table_to_ipc(schema.empty_table())


def _schema_from_ipc(blob: bytes) -> pa.Schema:
    return _table_from_ipc(blob).schema


def _stage_table(storage: BoundStorage, ns: bytes, table: pa.Table) -> None:
    """Stage an in-memory table into an execution-scoped log in bounded batches."""
    for batch in table.to_batches(max_chunksize=OUT_BATCH_ROWS):
        storage.state_append(ns, b"", _batch_to_ipc(batch))


# ---------------------------------------------------------------------------
# Persistent, attach-scoped collection store (over FunctionStorage)
# ---------------------------------------------------------------------------
#
# `ps` below is a BoundStorage bound to the ATTACH scope (stable across queries),
# distinct from the per-query `params.storage`.


def _store(storage: FunctionStorage, attach_opaque_data: bytes | None) -> BoundStorage:
    """Build a BoundStorage scoped to the ATTACH session (persists across queries).

    Constructed without ``attach_plaintext``, so under shard-routing backends
    (``VGI_SQLITE_SHARD=1``, cloudflare-do) the data lands on the default
    shard — irrelevant for the plain sqlite backends the test suites use.
    """
    return BoundStorage(storage, attach_opaque_data if attach_opaque_data else b"default")


def _seg_ns(name: bytes) -> bytes:
    return _SEG_NS_PREFIX + name


def _seg_key(call_ts_us: int) -> bytes:
    """Segment key: big-endian ingest time + uuid, so keys sort by time."""
    return call_ts_us.to_bytes(_TS_KEY_BYTES, "big") + uuid.uuid4().bytes


def _get_schema(ps: BoundStorage, name: bytes) -> pa.Schema | None:
    blob = ps.state_get(_META_NS, name)
    return _schema_from_ipc(blob) if blob is not None else None


def _put_schema(ps: BoundStorage, name: bytes, output_schema: pa.Schema) -> None:
    ps.state_put(_META_NS, name, _schema_to_ipc(output_schema))


def _get_count(ps: BoundStorage, name: bytes) -> int:
    """Return a collection's current row count (the per-collection int64 counter)."""
    return ps.counter_get(_META_NS, name)


def _append_segment(ps: BoundStorage, name: bytes, table: pa.Table, call_ts_us: int) -> None:
    """Append one time-keyed segment (O(batch)) and bump the row counter."""
    ps.state_put(_seg_ns(name), _seg_key(call_ts_us), _table_to_ipc(table))
    ps.counter_add(_META_NS, name, table.num_rows)


def _read_collection(ps: BoundStorage, name: bytes, output_schema: pa.Schema) -> pa.Table:
    # Segments are time-keyed, so the scan returns them oldest-first.
    parts = [_table_from_ipc(value) for _key, value in ps.state_scan(_seg_ns(name))]
    return pa.concat_tables(parts) if parts else output_schema.empty_table()


def _evict_ttl(ps: BoundStorage, name: bytes, cutoff_us: int) -> None:
    """Drop segments whose ingest time is before ``cutoff_us`` (one ranged delete).

    A segment carries a single call timestamp, so the time-keyed range
    ``[.., cutoff)`` is exactly the expired rows. We sum their rows first (the
    expired set is small and about to be deleted) to keep the counter exact.
    """
    if cutoff_us <= 0:
        return  # nothing predates the epoch
    end = cutoff_us.to_bytes(_TS_KEY_BYTES, "big")
    removed = sum(_table_from_ipc(value).num_rows for _key, value in ps.state_scan(_seg_ns(name), end=end))
    if removed:
        ps.state_delete(_seg_ns(name), end=end)
        ps.counter_add(_META_NS, name, -removed)


def _evict_max_rows(ps: BoundStorage, name: bytes, total: int, max_row_size: int) -> None:
    """Drop the oldest rows until at most ``max_row_size`` remain.

    Walks segments oldest-first, deleting whole segments and trimming only the
    one segment that straddles the cap — never a whole-collection rewrite.
    """
    overflow = total - max_row_size
    removed = 0
    delete_keys: list[bytes] = []
    trim: tuple[bytes, pa.Table] | None = None
    for key, value in ps.state_scan(_seg_ns(name)):  # oldest-first
        seg = _table_from_ipc(value)
        if removed + seg.num_rows <= overflow:
            removed += seg.num_rows
            delete_keys.append(key)
            if removed == overflow:
                break
        else:
            # Boundary segment: keep its newest rows, drop the oldest.
            trim = (key, seg.slice(overflow - removed))
            removed = overflow
            break
    if delete_keys:
        ps.state_delete(_seg_ns(name), delete_keys)
    if trim is not None:
        trim_key, trim_table = trim
        ps.state_put(_seg_ns(name), trim_key, _table_to_ipc(trim_table))
    if removed:
        ps.counter_add(_META_NS, name, -removed)


def _clear_collection(ps: BoundStorage, name: bytes) -> int:
    """Drop a collection (segments + schema + counter); return rows removed."""
    total = _get_count(ps, name)
    ps.state_delete(_seg_ns(name), None)
    ps.state_delete(_META_NS, [name])
    ps.counter_delete(_META_NS, name)
    return total


# ---------------------------------------------------------------------------
# accumulate(name, <rows>, ttl, max_row_size, result)
# ---------------------------------------------------------------------------

_RESULT_CHOICES = ("all", "new", "none")


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class AccumulateArgs:
    """Arguments for the ``accumulate`` table function."""

    name: Annotated[str, Arg(0, doc="Name of the collection to accumulate into")]
    data: Annotated[TableInput, Arg(1, doc="Rows to accumulate (any table expression)")]
    ttl: Annotated[
        object | None,
        Arg(
            "ttl",
            default=None,
            arrow_type=pa.month_day_nano_interval(),
            doc="Evict rows older than this INTERVAL before returning (months are treated as 30 days)",
        ),
    ] = None
    max_row_size: Annotated[
        int,
        Arg(
            "max_row_size",
            default=0,
            ge=0,
            doc="Maximum rows retained per name; oldest dropped first (0 = unlimited)",
        ),
    ] = 0
    result: Annotated[
        str,
        Arg(
            "result",
            default="all",
            choices=_RESULT_CHOICES,
            doc="What to return: 'all' accumulated rows (default), only the 'new' rows, or 'none'",
        ),
    ] = "all"


@dataclasses.dataclass
class AccumulateDrainState(ArrowSerializableDataclass):
    """Cursor over the staged output log, advanced one batch per finalize tick."""

    after_id: int = -1


class AccumulateFunction(TableBufferingFunction[AccumulateArgs, AccumulateDrainState]):
    """Append input rows to a named collection; optionally return the collection.

    A buffering (Sink -> Combine -> Source) operator: the input is staged across
    the parallel sink, ``combine`` runs once to stamp the rows with a single
    timestamp, append them to the persistent collection, apply ttl/max_row_size,
    and stage the rows to return, and the source streams them back.
    """

    class Meta:
        """Function metadata."""

        name = "accumulate"
        description = "Append rows to a named collection; return all/new/no rows with a _timestamp column"
        categories = ["stateful", "utility"]
        tags = {"category": "stateful", "type": "accumulator"}
        examples = [
            FunctionExample(
                sql="SELECT * FROM accumulate('events', (VALUES (1), (2)) t(x))",
                description="Accumulate two rows under 'events' and return the full collection",
            ),
            FunctionExample(
                sql="SELECT * FROM accumulate('events', (VALUES (3)) t(x), result := 'new')",
                description="Append a row and return only the newly-added rows",
            ),
            FunctionExample(
                sql="SELECT * FROM accumulate('events', (VALUES (4)) t(x), result := 'none')",
                description="Append a row and return nothing (cheap, fire-and-forget)",
            ),
            FunctionExample(
                sql=(
                    "SELECT * FROM accumulate('events', (VALUES (5)) t(x), "
                    "ttl := INTERVAL '1 hour', max_row_size := 1000)"
                ),
                description="Append with a 1-hour TTL and a 1000-row cap",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[AccumulateArgs]) -> BindResponse:
        """Validate the input schema against the named collection and add timestamp."""
        _validate_name(params.args.name)
        input_schema = params.bind_call.input_schema
        if input_schema is None:
            raise ValueError("accumulate requires a table input")
        if TIMESTAMP_COLUMN in input_schema.names:
            raise ValueError(
                f"input may not contain a reserved '{TIMESTAMP_COLUMN}' column; "
                "accumulate adds this column to its output"
            )

        ps = _store(cls.storage, params.attach_opaque_data)
        name = params.args.name.encode()
        out_schema = _output_schema(input_schema)
        # Lock-free schema pin: read the pinned schema, write it if absent, or
        # reject a mismatch. The only race is two *simultaneous first* appends
        # of *incompatible* schemas to a brand-new name (pathological); the
        # worst case is a confusing validation error, never data corruption.
        existing = _get_schema(ps, name)
        if existing is None:
            _put_schema(ps, name, out_schema)
        elif not _schemas_match(_input_schema_of(existing), input_schema):
            raise ValueError(
                f"input schema for accumulate('{params.args.name}', ...) does not match the "
                f"schema already accumulated under that name.\n"
                f"  accumulated: {_input_schema_of(existing)}\n"
                f"  received:    {input_schema}"
            )
        return BindResponse(output_schema=out_schema)

    # ---- Sink: stage each input batch (parallel across DuckDB threads) ----
    @classmethod
    def process(cls, batch: pa.RecordBatch, params: TableBufferingParams[AccumulateArgs]) -> bytes:
        """Stage one input batch into the execution-scoped log."""
        params.storage.state_append(_NS_IN, b"", _batch_to_ipc(batch))
        return params.execution_id

    # ---- Combine: append, evict, and stage the requested result ----
    @classmethod
    def combine(cls, state_ids: list[bytes], params: TableBufferingParams[AccumulateArgs]) -> list[bytes]:
        """Append staged input to the collection, apply eviction, stage the result."""
        ps = _store(cls.storage, params.attach_opaque_data)
        name = params.args.name.encode()
        ttl = params.args.ttl
        max_row_size = params.args.max_row_size
        result_mode = params.args.result
        output_schema = params.output_schema
        input_schema = _input_schema_of(output_schema)

        # Reassemble this call's input from the execution-scoped staging log.
        staged = params.storage.state_log_scan(_NS_IN, b"", after_id=-1, limit=None)
        input_batches = [_batch_from_ipc(value) for _id, value in staged]
        new_input = (
            pa.Table.from_batches(input_batches, schema=input_schema) if input_batches else input_schema.empty_table()
        )

        call_ts = _now_naive()
        call_ts_us = _to_us(call_ts)
        if new_input.num_rows:
            ts_col = pa.array([call_ts] * new_input.num_rows, type=TIMESTAMP_TYPE)
            new_table = new_input.append_column(pa.field(TIMESTAMP_COLUMN, TIMESTAMP_TYPE), ts_col)
        else:
            new_table = output_schema.empty_table()

        # No lock: each step below is a single atomic storage op. Append is
        # O(batch); a TTL is one ranged delete; max_row_size drops whole oldest
        # segments plus at most one trimmed boundary segment.
        if new_table.num_rows:
            _append_segment(ps, name, new_table, call_ts_us)

        if ttl is not None:
            _evict_ttl(ps, name, _to_us(call_ts - _interval_to_timedelta(ttl)))

        if max_row_size:
            total = _get_count(ps, name)
            if total > max_row_size:
                _evict_max_rows(ps, name, total, max_row_size)

        if result_mode == "all":
            to_emit: pa.Table | None = _read_collection(ps, name, output_schema)
        elif result_mode == "new":
            to_emit = new_table  # the rows this call added (pre-eviction)
        else:  # "none"
            to_emit = None

        if to_emit is not None and to_emit.num_rows:
            _stage_table(params.storage, _NS_OUT, to_emit)

        return [params.execution_id]

    # ---- Source: drain the staged result, one batch per tick ----
    @classmethod
    def initial_finalize_state(
        cls, finalize_state_id: bytes, params: TableBufferingParams[AccumulateArgs]
    ) -> AccumulateDrainState:
        """Start the drain cursor at the beginning of the staged output log."""
        return AccumulateDrainState(after_id=-1)

    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[AccumulateArgs],
        finalize_state_id: bytes,
        state: AccumulateDrainState,
        out: OutputCollector,
    ) -> None:
        """Emit the next staged output batch, or finish when the log is drained."""
        rows = params.storage.state_log_scan(_NS_OUT, b"", after_id=state.after_id, limit=1)
        if not rows:
            out.finish()
            return
        log_id, value = rows[0]
        out.emit(_batch_from_ipc(value))
        state.after_id = log_id


# ---------------------------------------------------------------------------
# accumulate_read(name) — read a collection without modifying it
# ---------------------------------------------------------------------------


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class AccumulateReadArgs:
    """Arguments for the ``accumulate_read`` table function."""

    name: Annotated[str, Arg(0, doc="Name of the collection to read")]


@dataclasses.dataclass
class AccumulateReadState(ArrowSerializableDataclass):
    """Whether the snapshot has been staged, plus the drain cursor."""

    staged: bool = False
    after_id: int = -1


@init_single_worker
class AccumulateReadFunction(TableFunctionGenerator[AccumulateReadArgs, AccumulateReadState]):
    """Return a collection's accumulated rows without modifying it.

    Emits the same columns ``accumulate`` returns (input columns + ``_timestamp``).
    Reading a name that doesn't exist in this session raises. Row order is not
    guaranteed; ``ORDER BY _timestamp`` for a stable ordering.
    """

    class Meta:
        """Function metadata."""

        name = "accumulate_read"
        description = "Read an accumulated collection's rows without modifying it"
        categories = ["stateful", "utility"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM accumulate_read('events')",
                description="Return all rows accumulated under 'events'",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[AccumulateReadArgs]) -> BindResponse:
        """Resolve the collection's pinned schema; raise if the name is unknown."""
        _validate_name(params.args.name)
        ps = _store(cls.storage, params.attach_opaque_data)
        schema = _get_schema(ps, params.args.name.encode())
        if schema is None:
            raise ValueError(f"no accumulation named '{params.args.name}' in this session")
        return BindResponse(output_schema=schema)

    @classmethod
    def initial_state(cls, params: ProcessParams[AccumulateReadArgs]) -> AccumulateReadState:
        """Start unstaged with the drain cursor at the beginning."""
        return AccumulateReadState(staged=False, after_id=-1)

    @classmethod
    def process(
        cls,
        params: ProcessParams[AccumulateReadArgs],
        state: AccumulateReadState,
        out: OutputCollector,
    ) -> None:
        """Snapshot the collection into bounded batches (first tick), then drain one per tick."""
        if not state.staged:
            ps = _store(cls.storage, params.attach_opaque_data)
            table = _read_collection(ps, params.args.name.encode(), params.output_schema)
            _stage_table(params.storage, _NS_READ, table)
            state.staged = True

        rows = params.storage.state_log_scan(_NS_READ, b"", after_id=state.after_id, limit=1)
        if not rows:
            out.finish()
            return
        log_id, value = rows[0]
        out.emit(_batch_from_ipc(value))
        state.after_id = log_id


# ---------------------------------------------------------------------------
# accumulate_clear(name)
# ---------------------------------------------------------------------------

_CLEAR_FIELDS: list[pa.Field[Any]] = [pa.field("name", pa.string()), pa.field("rows_cleared", pa.int64())]
CLEAR_SCHEMA = pa.schema(_CLEAR_FIELDS)


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class AccumulateClearArgs:
    """Arguments for the ``accumulate_clear`` table function."""

    name: Annotated[str, Arg(0, doc="Name of the collection to clear")]


@dataclasses.dataclass
class AccumulateClearState(ArrowSerializableDataclass):
    """Whether the single result row has been emitted yet."""

    done: bool = False


@init_single_worker
class AccumulateClearFunction(TableFunctionGenerator[AccumulateClearArgs, AccumulateClearState]):
    """Remove an accumulated collection by name (scoped to the ATTACH session).

    Drops the entire collection (rows + pinned schema), so the name is free to be
    re-accumulated with any schema afterward. Emits a single row
    ``(name, rows_cleared)``.
    """

    class Meta:
        """Function metadata."""

        name = "accumulate_clear"
        description = "Remove an accumulated collection by name; returns rows cleared"
        categories = ["stateful", "utility"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM accumulate_clear('events')",
                description="Clear the 'events' collection, returning how many rows were removed",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[AccumulateClearArgs]) -> BindResponse:
        """Validate the name; the output schema is fixed."""
        _validate_name(params.args.name)
        return BindResponse(output_schema=CLEAR_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[AccumulateClearArgs]) -> AccumulateClearState:
        """Start with the result row not yet emitted."""
        return AccumulateClearState(done=False)

    @classmethod
    def process(
        cls,
        params: ProcessParams[AccumulateClearArgs],
        state: AccumulateClearState,
        out: OutputCollector,
    ) -> None:
        """Clear the collection (first tick) and emit the single result row."""
        if state.done:
            out.finish()
            return

        ps = _store(cls.storage, params.attach_opaque_data)
        name = params.args.name
        rows_cleared = _clear_collection(ps, name.encode())

        out.emit(
            pa.RecordBatch.from_arrays(
                [pa.array([name], pa.string()), pa.array([rows_cleared], pa.int64())],
                schema=CLEAR_SCHEMA,
            )
        )
        state.done = True


# ---------------------------------------------------------------------------
# Catalog & worker
# ---------------------------------------------------------------------------

_ACCUMULATE_CATALOG = Catalog(
    name="accumulate",
    default_schema="main",
    schemas=[
        Schema(
            name="main",
            comment="Row accumulation keyed by name, persisted via FunctionStorage and scoped per ATTACH",
            functions=[
                AccumulateFunction,
                AccumulateReadFunction,
                AccumulateClearFunction,
            ],
        ),
    ],
)


class AccumulateCatalog(ReadOnlyCatalogInterface):
    """Catalog that mints a random per-ATTACH id and advertises versions.

    The random ``attach_opaque_data`` is carried back on every call and used as
    the storage scope, isolating each ATTACH session's accumulations.
    """

    catalog = _ACCUMULATE_CATALOG
    catalog_name = _ACCUMULATE_CATALOG.name

    def catalogs(self) -> list[CatalogInfo]:
        """Advertise the catalog with its data/implementation versions."""
        return [
            CatalogInfo(
                name=self._effective_catalog_name,
                implementation_version=IMPLEMENTATION_VERSION,
                data_version_spec=DATA_VERSION,
                attach_option_specs=[spec.serialize() for spec in self.attach_option_specs],
            )
        ]

    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None,
        implementation_version: str | None,
        ctx: CallContext | None = None,
    ) -> CatalogAttachResult:
        """Attach, minting a random per-ATTACH storage scope id."""
        result = super().catalog_attach(
            name=name,
            options=options,
            data_version_spec=data_version_spec,
            implementation_version=implementation_version,
            ctx=ctx,
        )
        return dataclasses.replace(
            result,
            # Random id, unique per ATTACH; the client persists it and resends it
            # on every call, so it also survives a worker restart.
            attach_opaque_data=AttachOpaqueData(uuid.uuid4().bytes),
            attach_opaque_data_required=True,
            resolved_data_version=DATA_VERSION,
            resolved_implementation_version=IMPLEMENTATION_VERSION,
        )


class AccumulateWorker(Worker):
    """Worker process hosting the accumulate functions."""

    catalog = _ACCUMULATE_CATALOG
    catalog_interface = AccumulateCatalog
