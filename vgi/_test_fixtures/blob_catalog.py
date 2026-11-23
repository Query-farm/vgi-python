# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The ``blob_test`` catalog: functions sized by their arguments, for externalization tests.

WARNING: EXAMPLE/TEST FUNCTIONS ONLY. They let the DuckDB extension's
externalization lane move as many bytes as it likes through each function
kind and check them on the SQL side:

* RPC results over ``VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES`` come back as
  blob-vendor pointers, and requests over ``VGI_RPC_MAX_REQUEST_BYTES`` go up
  through vendor upload URLs (HTTP, with ``VGI_BLOB_VENDOR_URL`` set);
* storage values over ``VGI_STORAGE_EXTERNALIZE_BYTES`` go to the vendor
  (``VGI_STORAGE_EXTERNALIZE=1`` over SQLite, or the cloudflare-do backend).

A separate catalog (``ATTACH 'blob_test' (TYPE vgi, ...)``) so the example
catalog's function counts, which other SDKs mirror, do not change.

=========================  ==========================  ========================================
Function                   Kind                        What it moves
=========================  ==========================  ========================================
``blob_repeat(s, n)``      scalar                      large output: ``s`` repeated ``n`` times
``blob_len(s)``            scalar                      large input: returns ``strlen(s)`` (bytes)
``blob_rows(rows, width)`` table                       large output: ``rows`` x ``width`` chars
``blob_param_echo(p)``     table                       large bind parameter: length + sha256
``blob_buffer_echo(t)``    table-buffering             stores each input batch raw, replays it
``blob_tio(t)``            table-in-out                stores each batch in state, reads back
``blob_store_values(t)``   table-in-out                stores each value of column 0 exactly
``blob_param_batches``     table                       large bind parameter over many turns
``blob_param_streams``     table (one split / stream)  large bind parameter, parallel streams
``blob_param_scalar``      scalar (constant param)     large constant parameter of a scalar
``blob_param_table``       catalog table               a scan binding ``blob_param_echo``
=========================  ==========================  ========================================

The four ``blob_param_*`` fixtures back the extension's protocol 3.4
argument-reference tests (``test/sql/external_storage/arguments_ref*.test``):
``blob_param_batches(payload, batches)`` emits ``batches`` one-row batches,
one per stream turn; ``blob_param_streams(payload, streams)`` plans ``streams``
splits that each emit one row; ``blob_param_scalar(i, payload)`` returns
``sha256(payload)`` for each non-NULL ``i`` with ``payload`` a constant
parameter; ``blob_param_table`` binds ``repeat('t', 65536)``, reports
``STABLE_WITHIN_QUERY`` and is not inline-bound, so every scan binds over RPC.

``blob_buffer_echo`` and ``blob_tio`` pass every input column through and add
``payload_len`` (UTF-8 byte length) and ``payload_sha256`` (lowercase hex) of
the first column cast to text, matching DuckDB's ``strlen(x)`` and
``sha256(x)``. ``blob_rows`` row ``i`` is ``chr(97 + i % 26)`` repeated
``width`` times, i.e. ``repeat(chr(97 + i % 26), width)``.

``blob_store_values`` stores the UTF-8 bytes of each non-NULL value of the
first column as one storage value of exactly that size, reads them back and
reports ``payload_len`` / ``payload_sha256`` of the bytes *read back*, so a
SQL test controls the stored value size exactly (e.g. one byte either side of
``VGI_STORAGE_EXTERNALIZE_BYTES``).

The vendor's counters are read from the fake vendor itself
(``GET $VGI_BLOB_VENDOR_URL/__stats__``, vgi-rpc's ``vgi-rpc-fake-vendor
--test-routes``), not through this catalog. The SDK-neutral contract for this
catalog is the vgi extension's ``docs/testing/blob-test-catalog.md``; keep the
two in step.
"""

from __future__ import annotations

import hashlib
import struct
import uuid
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
import pyarrow.compute as pc
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.arguments import Arg, Arguments, ConstParam, Param, Returns, TableInput
from vgi.catalog import Catalog, ReadOnlyCatalogInterface, Schema, Table
from vgi.invocation import BindRepeatability, BindResponse
from vgi.metadata import FunctionExample
from vgi.protocol import PlanResponse, ScanSplit
from vgi.scalar_function import ScalarFunction
from vgi.schema_utils import schema
from vgi.table_buffering_function import TableBufferingFunction, TableBufferingParams
from vgi.table_function import (
    BindParams,
    InitParams,
    ProcessParams,
    TableCardinality,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)
from vgi.table_in_out_function import TableInOutFunction
from vgi.worker import Worker

__all__ = ["CATALOG_NAME", "BlobTestWorker"]

CATALOG_NAME = "blob_test"
_ROWS_PER_BATCH = 2048


def _ipc(batch: pa.RecordBatch) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _with_checksums(batch: pa.RecordBatch, output_schema: pa.Schema) -> pa.RecordBatch:
    """Append ``payload_len`` / ``payload_sha256`` of column 0 (as text) to *batch*."""
    text = batch.column(0) if pa.types.is_string(batch.column(0).type) else pc.cast(batch.column(0), pa.string())
    lens: list[int | None] = []
    digests: list[str | None] = []
    for value in text.to_pylist():
        if value is None:
            lens.append(None)
            digests.append(None)
        else:
            raw = value.encode()
            lens.append(len(raw))
            digests.append(hashlib.sha256(raw).hexdigest())
    columns = [*batch.columns, pa.array(lens, pa.int64()), pa.array(digests, pa.string())]
    return pa.RecordBatch.from_arrays(columns, schema=output_schema)


def _checksum_schema(input_schema: pa.Schema) -> pa.Schema:
    return pa.schema([*input_schema, pa.field("payload_len", pa.int64()), pa.field("payload_sha256", pa.string())])


# ---------------------------------------------------------------------------
# Scalars
# ---------------------------------------------------------------------------


class BlobRepeatFunction(ScalarFunction):
    """``blob_repeat(s, n)``: ``s`` repeated ``n`` times (large outputs)."""

    class Meta:
        """Function metadata."""

        name = "blob_repeat"
        description = "Repeat a string n times (externalization test: large scalar output)"
        examples = [FunctionExample(sql="SELECT blob_repeat('ab', 3)", description="Returns 'ababab'")]

    @classmethod
    def compute(
        cls,
        s: Annotated[pa.StringArray, Param(doc="String to repeat")],
        n: Annotated[pa.Int64Array, Param(doc="Number of copies")],
    ) -> Annotated[pa.LargeStringArray, Returns()]:
        """Repeat each ``s`` ``n`` times; NULL in either gives NULL."""
        out = [
            None if a is None or b is None else a * max(b, 0) for a, b in zip(s.to_pylist(), n.to_pylist(), strict=True)
        ]
        return pa.array(out, type=pa.large_string())


class BlobLenFunction(ScalarFunction):
    """``blob_len(s)``: UTF-8 byte length of ``s`` (large inputs)."""

    class Meta:
        """Function metadata."""

        name = "blob_len"
        description = "UTF-8 byte length of a string (externalization test: large scalar input)"
        examples = [FunctionExample(sql="SELECT blob_len('abc')", description="Returns 3")]

    @classmethod
    def compute(
        cls, s: Annotated[pa.StringArray, Param(doc="String to measure")]
    ) -> Annotated[pa.Int64Array, Returns()]:
        """Return ``strlen(s)`` for each row."""
        result: pa.Int64Array = pc.cast(pc.binary_length(s), pa.int64())  # type: ignore[assignment]
        return result


# ---------------------------------------------------------------------------
# Table functions
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class _BlobRowsArgs:
    rows: Annotated[int, Arg(0, doc="Number of rows", ge=0)]
    width: Annotated[int, Arg(1, doc="Characters per row", ge=0)]


@dataclass(kw_only=True)
class _BlobRowsState(ArrowSerializableDataclass):
    next_row: int = 0


@init_single_worker
@bind_fixed_schema
class BlobRowsFunction(TableFunctionGenerator[_BlobRowsArgs, _BlobRowsState]):
    """``blob_rows(rows, width)``: row ``i`` is ``repeat(chr(97 + i % 26), width)``.

    Attributes:
        FIXED_SCHEMA: ``(i BIGINT, payload VARCHAR)``.
    """

    class Meta:
        """Function metadata."""

        name = "blob_rows"
        description = "Rows of a given width (externalization test: large table output)"
        examples = [FunctionExample(sql="SELECT * FROM blob_rows(3, 4)", description="aaaa, bbbb, cccc")]

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(i=pa.int64(), payload=pa.string())

    @classmethod
    def cardinality(cls, params: BindParams[_BlobRowsArgs]) -> TableCardinality:
        """Exactly ``rows`` rows."""
        return TableCardinality(estimate=params.args.rows, max=params.args.rows)

    @classmethod
    def initial_state(cls, params: ProcessParams[_BlobRowsArgs]) -> _BlobRowsState:
        """Start at row 0."""
        return _BlobRowsState()

    @classmethod
    def process(cls, params: ProcessParams[_BlobRowsArgs], state: _BlobRowsState, out: OutputCollector) -> None:
        """Emit up to 2,048 rows per tick."""
        start, total = state.next_row, params.args.rows
        if start >= total:
            out.finish()
            return
        stop = min(start + _ROWS_PER_BATCH, total)
        width = params.args.width
        out.emit(
            pa.RecordBatch.from_pydict(
                {"i": list(range(start, stop)), "payload": [chr(97 + i % 26) * width for i in range(start, stop)]},
                schema=params.output_schema,
            )
        )
        state.next_row = stop


@dataclass(slots=True, frozen=True)
class _BlobParamArgs:
    payload: Annotated[str, Arg(0, doc="Parameter to measure")]


@dataclass(kw_only=True)
class _EmitOnceState(ArrowSerializableDataclass):
    emitted: bool = False


@init_single_worker
@bind_fixed_schema
class BlobParamEchoFunction(TableFunctionGenerator[_BlobParamArgs, _EmitOnceState]):
    """``blob_param_echo(payload)``: one row with the parameter's byte length and sha256.

    Attributes:
        FIXED_SCHEMA: ``(payload_len BIGINT, payload_sha256 VARCHAR)``.
    """

    class Meta:
        """Function metadata."""

        name = "blob_param_echo"
        description = "Length and SHA-256 of a parameter (externalization test: large bind parameter)"
        examples = [FunctionExample(sql="SELECT * FROM blob_param_echo('abc')", description="3, sha256('abc')")]

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(payload_len=pa.int64(), payload_sha256=pa.string())

    @classmethod
    def cardinality(cls, params: BindParams[_BlobParamArgs]) -> TableCardinality:
        """One row."""
        return TableCardinality(estimate=1, max=1)

    @classmethod
    def initial_state(cls, params: ProcessParams[_BlobParamArgs]) -> _EmitOnceState:
        """Not yet emitted."""
        return _EmitOnceState()

    @classmethod
    def process(cls, params: ProcessParams[_BlobParamArgs], state: _EmitOnceState, out: OutputCollector) -> None:
        """Emit the one row, then finish."""
        if state.emitted:
            out.finish()
            return
        raw = params.args.payload.encode()
        out.emit(
            pa.RecordBatch.from_pydict(
                {"payload_len": [len(raw)], "payload_sha256": [hashlib.sha256(raw).hexdigest()]},
                schema=params.output_schema,
            )
        )
        state.emitted = True


def _digest(payload: str) -> tuple[int, str]:
    raw = payload.encode()
    return len(raw), hashlib.sha256(raw).hexdigest()


@dataclass(slots=True, frozen=True)
class _BlobParamBatchesArgs:
    payload: Annotated[str, Arg(0, doc="Parameter to measure")]
    batches: Annotated[int, Arg(1, doc="Number of one-row batches", ge=0)]


@dataclass(kw_only=True)
class _BatchCursor(ArrowSerializableDataclass):
    next_batch: int = 0


@init_single_worker
@bind_fixed_schema
class BlobParamBatchesFunction(TableFunctionGenerator[_BlobParamBatchesArgs, _BatchCursor]):
    """``blob_param_batches(payload, batches)``: ``batches`` one-row batches, one per stream turn.

    Row ``i`` is ``(i, strlen(payload), sha256(payload))``, so a 60-batch call is
    at least 60 continuations, each carrying the stream's ``init`` state.

    Attributes:
        FIXED_SCHEMA: ``(i BIGINT, payload_len BIGINT, payload_sha256 VARCHAR)``.
    """

    class Meta:
        """Function metadata."""

        name = "blob_param_batches"
        description = "Length and SHA-256 of a parameter, once per batch (argument references over many turns)"
        examples = [FunctionExample(sql="SELECT * FROM blob_param_batches('abc', 3)", description="3 rows, i = 0..2")]

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(i=pa.int64(), payload_len=pa.int64(), payload_sha256=pa.string())

    @classmethod
    def cardinality(cls, params: BindParams[_BlobParamBatchesArgs]) -> TableCardinality:
        """Exactly ``batches`` rows."""
        return TableCardinality(estimate=params.args.batches, max=params.args.batches)

    @classmethod
    def initial_state(cls, params: ProcessParams[_BlobParamBatchesArgs]) -> _BatchCursor:
        """Start at batch 0."""
        return _BatchCursor()

    @classmethod
    def process(cls, params: ProcessParams[_BlobParamBatchesArgs], state: _BatchCursor, out: OutputCollector) -> None:
        """Emit one one-row batch per tick."""
        if state.next_batch >= params.args.batches:
            out.finish()
            return
        length, digest = _digest(params.args.payload)
        out.emit(
            pa.RecordBatch.from_pydict(
                {"i": [state.next_batch], "payload_len": [length], "payload_sha256": [digest]},
                schema=params.output_schema,
            )
        )
        state.next_batch += 1


@dataclass(slots=True, frozen=True)
class _BlobParamStreamsArgs:
    payload: Annotated[str, Arg(0, doc="Parameter to measure")]
    streams: Annotated[int, Arg(1, doc="Number of parallel streams (one split each)", ge=1)]


@dataclass(kw_only=True)
class _StreamCursor(ArrowSerializableDataclass):
    streams: list[int]
    next_index: int = 0


@bind_fixed_schema
class BlobParamStreamsFunction(TableFunctionGenerator[_BlobParamStreamsArgs, _StreamCursor]):
    """``blob_param_streams(payload, streams)``: one bind planned as ``streams`` splits.

    Each split emits ``(stream index, strlen(payload), sha256(payload))``. Every
    split's init embeds the same bind call, so with an argument reference the
    worker process fetches the argument once (vgi-rpc's object cache).

    Attributes:
        FIXED_SCHEMA: ``(stream BIGINT, payload_len BIGINT, payload_sha256 VARCHAR)``.
    """

    class Meta:
        """Function metadata."""

        name = "blob_param_streams"
        description = "Length and SHA-256 of a parameter from parallel streams (argument references)"
        supports_splits = True
        split_token_ttl_seconds = None
        examples = [
            FunctionExample(sql="SELECT * FROM blob_param_streams('abc', 4)", description="4 rows, stream = 0..3")
        ]

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(stream=pa.int64(), payload_len=pa.int64(), payload_sha256=pa.string())

    @classmethod
    def cardinality(cls, params: BindParams[_BlobParamStreamsArgs]) -> TableCardinality:
        """One row per stream."""
        return TableCardinality(estimate=params.args.streams, max=params.args.streams)

    @classmethod
    def on_plan(cls, params: BindParams[_BlobParamStreamsArgs], request: Any) -> PlanResponse:
        """One split per stream; the payload is the stream index."""
        count = params.args.streams
        return PlanResponse(
            splits=[ScanSplit(payload=struct.pack("<q", i), estimated_rows=1, rows_exact=True) for i in range(count)],
            estimated_total_splits=count,
            estimated_total_rows=count,
        )

    @classmethod
    def on_split(cls, params: InitParams[_BlobParamStreamsArgs], request: Any, payloads: list[bytes]) -> None:
        """Splits are redeemed from ``params.split_payloads`` in ``initial_state``."""
        return None

    @classmethod
    def initial_state(cls, params: ProcessParams[_BlobParamStreamsArgs]) -> _StreamCursor:
        """The stream indices this reader claimed."""
        if getattr(params.init_call, "split_tokens", None) is None:
            raise RuntimeError("blob_param_streams is split-only; vgi_split_scans is probably off")
        return _StreamCursor(streams=[struct.unpack("<q", p)[0] for p in (params.split_payloads or [])])

    @classmethod
    def process(cls, params: ProcessParams[_BlobParamStreamsArgs], state: _StreamCursor, out: OutputCollector) -> None:
        """Emit one row per claimed stream, then finish."""
        if state.next_index >= len(state.streams):
            out.finish()
            return
        length, digest = _digest(params.args.payload)
        out.emit(
            pa.RecordBatch.from_pydict(
                {"stream": [state.streams[state.next_index]], "payload_len": [length], "payload_sha256": [digest]},
                schema=params.output_schema,
            )
        )
        state.next_index += 1


class BlobParamScalarFunction(ScalarFunction):
    """``blob_param_scalar(i, payload)``: ``sha256(payload)`` per non-NULL ``i``.

    ``payload`` is a constant parameter, so it rides the scalar's bind (and
    with it an argument reference), not its input batches.
    """

    class Meta:
        """Function metadata."""

        name = "blob_param_scalar"
        description = "SHA-256 of a constant parameter for every row (argument references for scalars)"
        examples = [FunctionExample(sql="SELECT blob_param_scalar(1, 'abc')", description="sha256('abc')")]

    @classmethod
    def compute(
        cls,
        i: Annotated[pa.Int64Array, Param(doc="Row value; NULL gives NULL")],
        payload: Annotated[str, ConstParam("Constant parameter to hash")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """``sha256(payload)`` for each row whose ``i`` is not NULL."""
        digest = hashlib.sha256(payload.encode()).hexdigest() if payload is not None else None
        return pa.array([None if v is None else digest for v in i.to_pylist()], type=pa.string())


#: The fixed argument ``blob_param_table`` binds: ``repeat('t', 65536)``.
BLOB_PARAM_TABLE_ARGUMENT = "t" * 65536


@init_single_worker
class BlobParamTableScan(TableFunctionGenerator[_BlobParamArgs, _EmitOnceState]):
    """The scan behind the ``blob_param_table`` catalog table: ``blob_param_echo`` with a fixed argument.

    It reports ``STABLE_WITHIN_QUERY`` and is never inline-bound, so every scan
    binds over RPC and a serialized-plan rebind must reproduce the same bind
    bytes (the extension's per-query argument-reference memo).

    Attributes:
        FIXED_SCHEMA: ``(payload_len BIGINT, payload_sha256 VARCHAR)``.
    """

    class Meta:
        """Function metadata."""

        name = "blob_param_table_scan"
        description = "Length and SHA-256 of blob_param_table's fixed parameter (STABLE_WITHIN_QUERY)"

    FIXED_SCHEMA: ClassVar[pa.Schema] = BlobParamEchoFunction.FIXED_SCHEMA

    @classmethod
    def on_bind(cls, params: BindParams[_BlobParamArgs]) -> BindResponse:
        """The echo schema, promising equivalent scans within one query."""
        return BindResponse(output_schema=cls.FIXED_SCHEMA, repeatability=BindRepeatability.STABLE_WITHIN_QUERY)

    @classmethod
    def cardinality(cls, params: BindParams[_BlobParamArgs]) -> TableCardinality:
        """One row."""
        return TableCardinality(estimate=1, max=1)

    @classmethod
    def initial_state(cls, params: ProcessParams[_BlobParamArgs]) -> _EmitOnceState:
        """Not yet emitted."""
        return _EmitOnceState()

    @classmethod
    def process(cls, params: ProcessParams[_BlobParamArgs], state: _EmitOnceState, out: OutputCollector) -> None:
        """Emit the one row, then finish."""
        BlobParamEchoFunction.process(params, state, out)


# ---------------------------------------------------------------------------
# Table-buffering and table-in-out
# ---------------------------------------------------------------------------


@dataclass(kw_only=True)
class _TableArgs:
    data: Annotated[TableInput, Arg(0, doc="Input table")]


@dataclass
class _LogCursor(ArrowSerializableDataclass):
    after_id: int = -1


_BUFFER_NS = b"blob_buffer"


class BlobBufferEchoFunction(TableBufferingFunction[_TableArgs, _LogCursor]):
    """``blob_buffer_echo(t)``: stores each input batch raw (``state_append``), replays it with checksums."""

    class Meta:
        """Function metadata."""

        name = "blob_buffer_echo"
        description = "Buffer input batches in storage and replay them (externalization test: storage values)"
        examples = [
            FunctionExample(
                sql="SELECT * FROM blob_buffer_echo((SELECT repeat('x', 10) AS s))",
                description="s, payload_len, payload_sha256",
            )
        ]

    @classmethod
    def on_bind(cls, params: BindParams[_TableArgs]) -> BindResponse:
        """Input columns plus ``payload_len`` and ``payload_sha256``."""
        assert params.bind_call.input_schema is not None
        return BindResponse(output_schema=_checksum_schema(params.bind_call.input_schema))

    @classmethod
    def process(cls, batch: pa.RecordBatch, params: TableBufferingParams[_TableArgs]) -> bytes:
        """Append the raw batch to the execution's log."""
        params.storage.state_append(_BUFFER_NS, b"", _ipc(batch))
        return params.execution_id

    @classmethod
    def combine(cls, state_ids: list[bytes], params: TableBufferingParams[_TableArgs]) -> list[bytes]:
        """One replay stream per execution."""
        return [params.execution_id]

    @classmethod
    def initial_finalize_state(cls, finalize_state_id: bytes, params: TableBufferingParams[_TableArgs]) -> _LogCursor:
        """Start before the first log entry."""
        return _LogCursor()

    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[_TableArgs],
        finalize_state_id: bytes,
        state: _LogCursor,
        out: OutputCollector,
    ) -> None:
        """Emit one stored batch per tick, with checksums; finish at the end of the log."""
        rows = params.storage.state_log_scan(_BUFFER_NS, b"", after_id=state.after_id, limit=1)
        if not rows:
            out.finish()
            return
        log_id, value = rows[0]
        out.emit(_with_checksums(pa.ipc.open_stream(value).read_next_batch(), params.output_schema))
        state.after_id = log_id


_TIO_NS = b"blob_tio"


class BlobTioFunction(TableInOutFunction[_TableArgs]):
    """``blob_tio(t)``: stores each batch in execution state, reads it back, emits it with checksums."""

    class Meta:
        """Function metadata."""

        name = "blob_tio"
        description = "Round-trip each batch through storage (externalization test: table-in-out state)"
        examples = [
            FunctionExample(
                sql="SELECT * FROM blob_tio((SELECT repeat('x', 10) AS s))",
                description="s, payload_len, payload_sha256",
            )
        ]

    @classmethod
    def on_bind(cls, params: BindParams[_TableArgs]) -> BindResponse:
        """Input columns plus ``payload_len`` and ``payload_sha256``."""
        assert params.bind_call.input_schema is not None
        return BindResponse(output_schema=_checksum_schema(params.bind_call.input_schema))

    @classmethod
    def transform(cls, batch: pa.RecordBatch, params: ProcessParams[_TableArgs], state: Any) -> list[pa.RecordBatch]:
        """Put the batch in storage under a fresh key, read it back, emit it with checksums."""
        storage = params.storage
        assert storage is not None
        key = uuid.uuid4().bytes
        storage.state_put(_TIO_NS, key, _ipc(batch))
        stored = storage.state_get(_TIO_NS, key)
        if stored is None:
            raise RuntimeError("blob_tio: the batch just stored is missing")
        storage.state_delete(_TIO_NS, [key])
        return [_with_checksums(pa.ipc.open_stream(stored).read_next_batch(), params.output_schema)]


_VALUES_NS = b"blob_values"


def _text_values(column: pa.Array[Any]) -> list[bytes | None]:
    text = column if pa.types.is_string(column.type) else pc.cast(column, pa.string())
    return [None if v is None else v.encode() for v in text.to_pylist()]


class BlobStoreValuesFunction(TableInOutFunction[_TableArgs]):
    """``blob_store_values(t)``: stores each value of column 0 as one storage value, reads them back.

    Output: input columns 2..n passed through, then ``payload_len`` and
    ``payload_sha256`` of each stored value *as read back* (NULL for a NULL
    input, which stores nothing).
    """

    class Meta:
        """Function metadata."""

        name = "blob_store_values"
        description = "Store each value of the first column exactly and read it back (externalization test)"
        examples = [
            FunctionExample(
                sql="SELECT * FROM blob_store_values((SELECT repeat('x', 10) AS s, 1 AS id))",
                description="id, payload_len, payload_sha256",
            )
        ]

    @classmethod
    def on_bind(cls, params: BindParams[_TableArgs]) -> BindResponse:
        """Input columns after the first, plus ``payload_len`` and ``payload_sha256``."""
        input_schema = params.bind_call.input_schema
        assert input_schema is not None
        rest = [input_schema.field(i) for i in range(1, len(input_schema))]
        return BindResponse(output_schema=_checksum_schema(pa.schema(rest)))

    @classmethod
    def transform(cls, batch: pa.RecordBatch, params: ProcessParams[_TableArgs], state: Any) -> list[pa.RecordBatch]:
        """Put every non-NULL value under its own key, read all back, emit their length and SHA-256."""
        storage = params.storage
        assert storage is not None
        values = _text_values(batch.column(0))
        prefix = uuid.uuid4().bytes
        keys = [prefix + i.to_bytes(4, "big") for i in range(len(values))]
        items = [(k, v) for k, v in zip(keys, values, strict=True) if v is not None]
        if items:
            storage.state_put_many(_VALUES_NS, items)
            stored = storage.state_get_many(_VALUES_NS, [k for k, _v in items])
            storage.state_delete(_VALUES_NS, [k for k, _v in items])
        else:
            stored = []
        back: dict[bytes, bytes | None] = {k: got for (k, _v), got in zip(items, stored, strict=True)}
        lens: list[int | None] = []
        digests: list[str | None] = []
        for key, value in zip(keys, values, strict=True):
            if value is None:
                lens.append(None)
                digests.append(None)
                continue
            got = back[key]
            if got is None:
                raise RuntimeError("blob_store_values: a value just stored is missing")
            lens.append(len(got))
            digests.append(hashlib.sha256(got).hexdigest())
        columns = [batch.column(i) for i in range(1, batch.num_columns)]
        columns += [pa.array(lens, pa.int64()), pa.array(digests, pa.string())]
        return [pa.RecordBatch.from_arrays(columns, schema=params.output_schema)]


# ---------------------------------------------------------------------------
# Catalog and worker
# ---------------------------------------------------------------------------

_CATALOG = Catalog(
    name=CATALOG_NAME,
    default_schema="main",
    comment="Functions sized by their arguments, for blob externalization tests",
    schemas=[
        Schema(
            path=["main"],
            comment="Externalization test functions",
            functions=[
                BlobRepeatFunction,
                BlobLenFunction,
                BlobRowsFunction,
                BlobParamEchoFunction,
                BlobBufferEchoFunction,
                BlobTioFunction,
                BlobStoreValuesFunction,
                BlobParamBatchesFunction,
                BlobParamStreamsFunction,
                BlobParamScalarFunction,
            ],
            tables=[
                Table(
                    name="blob_param_table",
                    function=BlobParamTableScan,
                    arguments=Arguments(positional=(pa.scalar(BLOB_PARAM_TABLE_ARGUMENT),)),
                    comment="blob_param_echo(repeat('t', 65536)), STABLE_WITHIN_QUERY, bound over RPC on every scan",
                )
            ],
        )
    ],
)


class BlobTestCatalog(ReadOnlyCatalogInterface):
    """Catalog interface for ``blob_test``."""

    catalog = _CATALOG
    catalog_name = CATALOG_NAME


class BlobTestWorker(Worker):
    """Serves the ``blob_test`` catalog."""

    catalog_interface = BlobTestCatalog
    catalog_name = CATALOG_NAME
    catalog = _CATALOG
