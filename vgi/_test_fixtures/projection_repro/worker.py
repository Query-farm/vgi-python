"""Projection-pushdown reproducer worker.

Two table functions, both declaring ``projection_pushdown = True`` and a
12-column ``FIXED_SCHEMA`` (mirrors ``vgi-kafka``'s ``kafka_consume``):

  * ``proj_repro_strict`` — emits batches built strictly from
    ``params.output_schema`` (the projected subset). This is what
    ``projected_data`` does and what every projection-aware function is
    supposed to do.
  * ``proj_repro_full_schema`` — emits batches built against the
    declared ``FIXED_SCHEMA`` (all 12 columns), even when projection is
    in effect. Mirrors what a worker would do if it didn't observe
    ``params.output_schema``.

Plus a catalog interface that exposes both as virtual tables under
``main`` schema, so the same functions can be exercised by end-to-end
SQL ``SELECT`` against ``projection_repro.main.<name>`` (catalog-routed
scan).

The reproducer test calls each function:
  - directly via ``Client.table_function`` with explicit
    ``projection_ids``;
  - through the catalog-routed scan path (DuckDB → C++ extension →
    ``table_scan_function_get`` → bind → init with planner-derived
    projection_ids).

Mismatches between ``params.output_schema`` and the OutputCollector's
configured schema (which the framework's ``emit`` uses for the cast)
will surface as ``ValueError: Target schema's field names are not
matching the record batch's field names``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi_rpc.rpc import OutputCollector

from vgi import Worker
from vgi.arguments import Arg
from vgi.catalog import Catalog, Schema
from vgi.catalog.catalog_interface import (
    AttachOpaqueData,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    TableInfo,
    TransactionOpaqueData,
)
from vgi.function import Function
from vgi.invocation import GlobalInitResponse
from vgi.table_function import (
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

CATALOG_NAME = "projection_repro"


# A 12-column schema mirroring kafka_consume's CONSUME_SCHEMA in shape:
# string topic, primitives, BLOBs, list-of-struct headers, etc. Real-world
# projection_pushdown candidates often have wide schemas like this.
_WIDE_FIELDS: list[pa.Field[Any]] = [
    pa.field("topic", pa.string(), nullable=False),
    pa.field("partition", pa.int32(), nullable=False),
    pa.field("offset", pa.int64(), nullable=False),
    pa.field("timestamp", pa.timestamp("ms", tz="UTC"), nullable=True),
    pa.field("timestamp_type", pa.string(), nullable=True),
    pa.field("key", pa.binary(), nullable=True),
    pa.field("key_string", pa.string(), nullable=True),
    pa.field("key_schema_id", pa.int32(), nullable=True),
    pa.field("value", pa.binary(), nullable=True),
    pa.field("value_string", pa.string(), nullable=True),
    pa.field("value_schema_id", pa.int32(), nullable=True),
    pa.field(
        "headers",
        pa.list_(pa.struct([pa.field("k", pa.string()), pa.field("v", pa.binary())])),
        nullable=False,
    ),
]
WIDE_SCHEMA: pa.Schema = pa.schema(_WIDE_FIELDS)


@dataclass(slots=True, frozen=True)
class _Args:
    n: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]


def _build_row_dict(i: int) -> dict[str, object]:
    """One row's worth of values for every column in WIDE_SCHEMA."""
    return {
        "topic": "demo_topic",
        "partition": int(i % 4),
        "offset": int(i),
        "timestamp": None,
        "timestamp_type": None,
        "key": f"k{i}".encode(),
        "key_string": f"k{i}",
        "key_schema_id": None,
        "value": f"v{i}".encode(),
        "value_string": f"v{i}",
        "value_schema_id": None,
        "headers": [],
    }


@init_single_worker
@bind_fixed_schema
class ProjReproStrict(TableFunctionGenerator[_Args, None]):
    """Builds batch from ``params.output_schema`` only.

    Mirrors how ``projected_data`` does it — the canonical projection-aware
    pattern. Emits a batch shaped exactly like what DuckDB asked for.
    """

    FunctionArguments = _Args

    class Meta:
        name = "proj_repro_strict"
        description = "projection-pushdown reproducer (strict params.output_schema)"
        projection_pushdown = True

    FIXED_SCHEMA: ClassVar[pa.Schema] = WIDE_SCHEMA

    @classmethod
    def process(
        cls,
        params: ProcessParams[_Args],
        state: None,
        out: OutputCollector,
    ) -> None:
        n = params.args.n
        out_schema: pa.Schema = params.output_schema
        wanted = list(out_schema.names)
        if not wanted:
            # Empty projection (count(*) shape) — the output schema has
            # zero columns. ``pa.RecordBatch.from_pylist`` with an empty
            # schema can't infer row count from empty dicts, so use the
            # canonical pyarrow idiom for an N-row 0-column batch:
            # build a 1-column placeholder array of the right length and
            # then ``select([])`` it down to zero columns. This preserves
            # the row count, which is what DuckDB's count(*) needs.
            out.emit(pa.RecordBatch.from_arrays([pa.nulls(n)], names=[""]).select([]))
        else:
            rows: list[dict[str, object]] = []
            for i in range(n):
                full = _build_row_dict(i)
                rows.append({name: full[name] for name in wanted})
            out.emit(pa.RecordBatch.from_pylist(rows, schema=out_schema))
        out.finish()


@init_single_worker
@bind_fixed_schema
class ProjReproFullSchema(TableFunctionGenerator[_Args, None]):
    """Builds batch from FIXED_SCHEMA (all 12 columns) regardless of projection.

    A naive worker that forgets to observe ``params.output_schema``. We
    expect the framework to either:

    * accept the over-wide batch and project it down to ``output_schema``
      on its side (the lenient interpretation), or
    * raise a clear error like "expected projected schema, got full".

    Whichever the framework does, it should be deterministic and not the
    confusing "different schema" cast error.
    """

    FunctionArguments = _Args

    class Meta:
        name = "proj_repro_full_schema"
        description = "projection-pushdown reproducer (emits full FIXED_SCHEMA)"
        projection_pushdown = True

    FIXED_SCHEMA: ClassVar[pa.Schema] = WIDE_SCHEMA

    @classmethod
    def process(
        cls,
        params: ProcessParams[_Args],
        state: None,
        out: OutputCollector,
    ) -> None:
        n = params.args.n
        rows = [_build_row_dict(i) for i in range(n)]
        out.emit(pa.RecordBatch.from_pylist(rows, schema=cls.FIXED_SCHEMA))
        out.finish()


# ---------------------------------------------------------------------------
# Catalog — exposes both functions as virtual tables under main schema, so
# they can be invoked via catalog-routed scan path
# (table_scan_function_get → bound function with projection_ids from
# DuckDB's planner).
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _ChunkedState:
    emitted: int = 0


@init_single_worker
@bind_fixed_schema
class ProjReproChunked(TableFunctionGenerator[_Args, _ChunkedState]):
    """Multi-tick variant — emits one small batch per ``process()`` call.

    Mirrors ``kafka_consume``'s shard-queue pattern where each ``process()``
    tick emits one batch and returns, letting the framework reschedule.
    Multi-tick output is where we observed the projection bug in
    vgi-kafka: ``count(*) WHERE value_schema_id IS NOT NULL`` returned
    a non-zero count even though the worker emitted ``None`` for every
    row's ``value_schema_id``.
    """

    FunctionArguments = _Args

    class Meta:
        name = "proj_repro_chunked"
        description = "projection-pushdown reproducer (multi-tick, full FIXED_SCHEMA)"
        projection_pushdown = True

    FIXED_SCHEMA: ClassVar[pa.Schema] = WIDE_SCHEMA

    @classmethod
    def initial_state(cls, params: Any) -> _ChunkedState:
        return _ChunkedState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_Args],
        state: _ChunkedState,
        out: OutputCollector,
    ) -> None:
        n = params.args.n
        chunk = 2  # tiny — exercise multi-batch shape like kafka shard ticks
        if state.emitted >= n:
            out.finish()
            return
        end = min(state.emitted + chunk, n)
        rows = [_build_row_dict(i) for i in range(state.emitted, end)]
        out.emit(pa.RecordBatch.from_pylist(rows, schema=cls.FIXED_SCHEMA))
        state.emitted = end
        if state.emitted >= n:
            out.finish()


@bind_fixed_schema
class ProjReproMultiWorker(TableFunctionGenerator[_Args, _ChunkedState]):
    """Multi-worker, multi-tick variant.

    Mirrors ``kafka_consume`` with 4 partitions: ``on_init`` requests
    ``max_workers=4`` and each worker emits chunks of 2 rows per
    ``process()`` tick. Together with full-FIXED_SCHEMA emission and
    projection_pushdown, this exercises the same code path that
    misbehaved in vgi-kafka where ``count(*) WHERE value_schema_id IS
    NOT NULL`` returned 4 instead of 0 on a topic where every emitted
    row had ``value_schema_id=None``.
    """

    FunctionArguments = _Args

    class Meta:
        name = "proj_repro_multi_worker"
        description = "projection-pushdown reproducer (4 workers, multi-tick, full FIXED_SCHEMA)"
        projection_pushdown = True

    FIXED_SCHEMA: ClassVar[pa.Schema] = WIDE_SCHEMA

    @classmethod
    def on_init(cls, params: Any) -> GlobalInitResponse:
        return GlobalInitResponse(max_workers=4)

    @classmethod
    def initial_state(cls, params: Any) -> _ChunkedState:
        return _ChunkedState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_Args],
        state: _ChunkedState,
        out: OutputCollector,
    ) -> None:
        n = params.args.n
        chunk = 2
        if state.emitted >= n:
            out.finish()
            return
        end = min(state.emitted + chunk, n)
        rows = [_build_row_dict(i) for i in range(state.emitted, end)]
        out.emit(pa.RecordBatch.from_pylist(rows, schema=cls.FIXED_SCHEMA))
        state.emitted = end
        if state.emitted >= n:
            out.finish()


_FUNCTIONS: list[type[Function]] = [
    ProjReproStrict,
    ProjReproFullSchema,
    ProjReproChunked,
    ProjReproMultiWorker,
]


_CATALOG = Catalog(
    name=CATALOG_NAME,
    default_schema="main",
    schemas=[
        Schema(
            name="main",
            comment="projection-pushdown reproducer catalog",
            functions=list(_FUNCTIONS),
            tables=[],
        ),
    ],
)


def _serialize_schema(s: pa.Schema) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, s):
        pass
    return sink.getvalue().to_pybytes()


_TABLE_NAMES = {
    "strict_table": "proj_repro_strict",
    "full_table": "proj_repro_full_schema",
}


class ProjReproCatalog(ReadOnlyCatalogInterface):
    """Exposes virtual tables backed by the two reproducer functions."""

    catalog = _CATALOG
    catalog_name = CATALOG_NAME

    def _info(self, table_name: str) -> TableInfo:
        return TableInfo(
            comment=f"reproducer table -> {_TABLE_NAMES[table_name]}",
            tags={},
            name=table_name,
            schema_name="main",
            columns=SerializedSchema(_serialize_schema(WIDE_SCHEMA)),
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

    def schemas(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None
    ) -> list[SchemaInfo]:
        # Override the declarative ``Schema(tables=[])``-derived
        # ``estimated_object_count[table] = 0`` with the real population.
        # Without this, the C++ client treats the static zero as a hard
        # guarantee and skips ``catalog_schema_contents_tables``, hiding
        # every table this catalog publishes via the override below.
        infos = super().schemas(attach_opaque_data=attach_opaque_data, transaction_opaque_data=transaction_opaque_data)
        for i, info in enumerate(infos):
            if info.name == "main":
                infos[i] = SchemaInfo(
                    attach_opaque_data=info.attach_opaque_data,
                    name=info.name,
                    comment=info.comment,
                    tags=info.tags,
                    estimated_object_count={
                        **(info.estimated_object_count or {}),
                        "table": len(_TABLE_NAMES),
                    },
                )
        return infos

    def schema_contents(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        name: str,
        type: Any,
    ) -> Any:
        if name.lower() == "main" and type == SchemaObjectType.TABLE:
            return [self._info(table_name) for table_name in _TABLE_NAMES]
        return super().schema_contents(
            attach_opaque_data=attach_opaque_data, transaction_opaque_data=transaction_opaque_data, name=name, type=type
        )

    def table_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        if schema_name.lower() != "main":
            return None
        if name in _TABLE_NAMES:
            return self._info(name)
        return None

    def table_scan_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_name: str,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        fn = _TABLE_NAMES.get(name)
        if fn is None:
            raise ValueError(f"unknown reproducer table: {name}")
        return ScanFunctionResult(
            function_name=fn,
            # The reproducer functions take a single ``n`` argument — pass
            # 100 by default so any SELECT against the virtual table
            # actually has rows. (Real workloads would derive this from
            # filter pushdown or other state; we just need a constant.)
            positional_arguments=[pa.scalar(100, type=pa.int64())],
            named_arguments={},
            required_extensions=[],
        )


class ProjReproWorker(Worker):
    catalog_interface = ProjReproCatalog
    catalog_name = CATALOG_NAME
    catalog = _CATALOG
    functions = list(_FUNCTIONS)
