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
``vgi_test_blob_stats()``  table                       the fake vendor's counters
=========================  ==========================  ========================================

``blob_buffer_echo`` and ``blob_tio`` pass every input column through and add
``payload_len`` (UTF-8 byte length) and ``payload_sha256`` (lowercase hex) of
the first column cast to text, matching DuckDB's ``strlen(x)`` and
``sha256(x)``. ``blob_rows`` row ``i`` is ``chr(97 + i % 26)`` repeated
``width`` times, i.e. ``repeat(chr(97 + i % 26), width)``.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
import pyarrow.compute as pc
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.arguments import Arg, Param, Returns, TableInput
from vgi.catalog import Catalog, ReadOnlyCatalogInterface, Schema
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction
from vgi.schema_utils import schema
from vgi.table_buffering_function import TableBufferingFunction, TableBufferingParams
from vgi.table_function import (
    BindParams,
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
_STATS_KEYS = (
    "puts",
    "gets",
    "deletes",
    "delete_scopes",
    "upload_urls",
    "storage_puts",
    "storage_gets",
    "objects",
    "bytes",
    "bytes_uploaded",
    "bytes_downloaded",
)
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


@dataclass(slots=True, frozen=True)
class _NoArgs:
    pass


def blob_stats_url() -> str:
    """The fake vendor's stats URL: ``VGI_TEST_BLOB_STATS_URL`` or ``$VGI_BLOB_VENDOR_URL/__stats__``.

    Returns:
        The URL.

    Raises:
        ValueError: If neither is set.
    """
    explicit = os.environ.get("VGI_TEST_BLOB_STATS_URL", "").strip()
    if explicit:
        return explicit
    base = os.environ.get("VGI_BLOB_VENDOR_URL", "").strip()
    if not base:
        raise ValueError("vgi_test_blob_stats() needs VGI_BLOB_VENDOR_URL (the fake vendor) or VGI_TEST_BLOB_STATS_URL")
    return base.rstrip("/") + "/__stats__"


@init_single_worker
@bind_fixed_schema
class BlobStatsFunction(TableFunctionGenerator[_NoArgs, _EmitOnceState]):
    """``vgi_test_blob_stats()``: one row of the fake blob vendor's counters.

    Reads ``GET /__stats__`` from `vgi._test_fixtures.fake_blob_vendor`; see
    its ``stats`` for what each column counts. Counters are cumulative for
    the fake's lifetime, so tests compare before/after values.

    Attributes:
        FIXED_SCHEMA: One BIGINT column per counter.
    """

    class Meta:
        """Function metadata."""

        name = "vgi_test_blob_stats"
        description = "Counters of the fake blob vendor (externalization tests)"
        examples = [FunctionExample(sql="SELECT puts, bytes FROM vgi_test_blob_stats()", description="Vendor counters")]

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([pa.field(k, pa.int64()) for k in _STATS_KEYS])

    @classmethod
    def cardinality(cls, params: BindParams[_NoArgs]) -> TableCardinality:
        """One row."""
        return TableCardinality(estimate=1, max=1)

    @classmethod
    def initial_state(cls, params: ProcessParams[_NoArgs]) -> _EmitOnceState:
        """Not yet emitted."""
        return _EmitOnceState()

    @classmethod
    def process(cls, params: ProcessParams[_NoArgs], state: _EmitOnceState, out: OutputCollector) -> None:
        """Fetch the counters and emit them as one row."""
        if state.emitted:
            out.finish()
            return
        import httpx2

        resp = httpx2.get(blob_stats_url(), timeout=10.0)
        resp.raise_for_status()
        counters: dict[str, Any] = json.loads(resp.content)
        row = {k: [int(counters.get(k, 0))] for k in params.output_schema.names}
        out.emit(pa.RecordBatch.from_pydict(row, schema=params.output_schema))
        state.emitted = True


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
                BlobStatsFunction,
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
