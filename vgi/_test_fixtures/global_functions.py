# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Probe functions for global (``system.main``) registration.

WARNING: EXAMPLE/TEST FUNCTIONS ONLY — see ``table_in_out.py`` for the full
rationale. These exist purely so a client can be observed publishing a worker's
functions into its *global* function namespace, and are deliberately separate
from every other fixture:

* **They are additive for other language implementations.** The example catalog
  is a cross-language contract — Go, TypeScript, and Java workers mirror it. If
  global registration reused existing fixtures (``double``, ``ten_thousand``,
  ``vgi_sum``, ``echo_buffering``), every implementation would have to make the
  same semantic change to functions it already ships. New functions cost each
  implementation only what it chooses to add, when it chooses to add it.
* **They document their own purpose.** Nothing else depends on them, so
  changing one cannot break an unrelated test, and a reader at the definition
  site can see what they are for.

One per :class:`~vgi.metadata.CatalogFunctionType`, so the client's registration
path is exercised for every function kind:

============================  ===================  ==============================
Class                         Registered name      Published as (``vgi_example``)
============================  ===================  ==============================
``GlobalScalarFunction``      ``global_scalar``    ``vgi_example_global_scalar``
``GlobalTableFunction``       ``global_table``     ``vgi_example_global_table``
``GlobalAggFunction``         ``global_agg``       ``vgi_example_global_agg``
``GlobalBufferedFunction``    ``global_buffered``  ``vgi_example_global_buffered``
============================  ===================  ==============================

Each returns a value tagged with its own name so a test can assert that the
globally-published name reached the function it was supposed to, rather than
some same-named function belonging to another catalog.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType
from vgi_rpc.rpc import OutputCollector

from vgi.aggregate_function import AggregateFunction
from vgi.arguments import Arg, Param, Returns, TableInput
from vgi.invocation import BindResponse
from vgi.metadata import DistinctDependence, FunctionExample, NullHandling, OrderDependence
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

__all__ = [
    "GlobalAggFunction",
    "GlobalBufferedFunction",
    "GlobalScalarFunction",
    "GlobalTableFunction",
]


class GlobalScalarFunction(ScalarFunction):
    """Scalar probe — labels each input so the caller can prove which impl ran.

    SQL: ``SELECT vgi_example_global_scalar(7)`` -> ``'global_scalar:7'``
    """

    class Meta:
        """Metadata for GlobalScalarFunction."""

        name = "global_scalar"
        description = "Global-registration probe (scalar)"
        categories = ["test", "global"]
        examples = [
            FunctionExample(
                sql="SELECT vgi_example_global_scalar(7)",
                description="Scalar probe published into system.main",
            ),
        ]

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.Int64Array, Param(doc="Value to label")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Return ``global_scalar:<value>`` for each row."""
        return pa.array(
            [None if v.as_py() is None else f"global_scalar:{v.as_py()}" for v in value],
            type=pa.string(),
        )


@dataclass(kw_only=True)
class GlobalTableArguments:
    """Arguments for GlobalTableFunction (none — it is a fixed generator)."""


@dataclass(kw_only=True)
class GlobalTableState(ArrowSerializableDataclass):
    """Cursor for GlobalTableFunction."""

    emitted: bool = False


@init_single_worker
@bind_fixed_schema
class GlobalTableFunction(TableFunctionGenerator[GlobalTableArguments, GlobalTableState]):
    """Table probe — three labelled rows, no arguments.

    SQL: ``SELECT * FROM vgi_example_global_table()``

    Attributes:
        FIXED_SCHEMA: The fixed Arrow output schema this function always produces.

    """

    class Meta:
        """Metadata for GlobalTableFunction."""

        name = "global_table"
        description = "Global-registration probe (table)"
        categories = ["test", "global"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM vgi_example_global_table()",
                description="Table probe published into system.main",
            ),
        ]

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64(), label=pa.string())

    @classmethod
    def cardinality(cls, params: BindParams[GlobalTableArguments]) -> TableCardinality:
        """Exactly three rows, always."""
        return TableCardinality(estimate=3, max=3)

    @classmethod
    def initial_state(cls, params: ProcessParams[GlobalTableArguments]) -> GlobalTableState:
        """Create initial state."""
        return GlobalTableState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[GlobalTableArguments],
        state: GlobalTableState,
        out: OutputCollector,
    ) -> None:
        """Emit the three probe rows once, then finish."""
        if state.emitted:
            out.finish()
            return
        out.emit(
            pa.RecordBatch.from_pydict(
                {"n": [0, 1, 2], "label": [f"global_table:{i}" for i in range(3)]},
                schema=params.output_schema,
            )
        )
        state.emitted = True


@dataclass(kw_only=True)
class GlobalAggState(ArrowSerializableDataclass):
    """Running total for GlobalAggFunction."""

    total: Annotated[int, ArrowType(pa.int64())] = 0


class GlobalAggFunction(AggregateFunction[GlobalAggState]):
    """Aggregate probe — sums int64 input.

    SQL: ``SELECT vgi_example_global_agg(v) FROM t``
    """

    class Meta:
        """Metadata for GlobalAggFunction."""

        name = "global_agg"
        description = "Global-registration probe (aggregate)"
        categories = ["test", "global"]
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> GlobalAggState:
        """Create initial per-group state."""
        return GlobalAggState()

    @classmethod
    def update(
        cls,
        states: dict[int, GlobalAggState],
        group_ids: pa.Int64Array,
        value: Annotated[pa.Int64Array, Param(doc="Column to sum")],
    ) -> None:
        """Accumulate each group's values."""
        table = pa.table({"gid": group_ids, "value": value})
        grouped = table.group_by("gid").aggregate([("value", "sum")])
        for i in range(grouped.num_rows):
            gid: int = grouped.column("gid")[i].as_py()
            val = grouped.column("value_sum")[i].as_py()
            if val is not None:
                states[gid] = GlobalAggState(total=states[gid].total + val)

    @classmethod
    def combine(cls, source: GlobalAggState, target: GlobalAggState, params: ProcessParams[None]) -> GlobalAggState:
        """Merge two partial states."""
        return GlobalAggState(total=source.total + target.total)

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, GlobalAggState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns(pa.int64())]:
        """Emit one total per group; a group with no state yields NULL."""
        results = [s.total if (s := states[gid.as_py()]) is not None else None for gid in group_ids]
        return pa.record_batch({"result": pa.array(results, type=pa.int64())})


@dataclass(kw_only=True)
class GlobalBufferedArguments:
    """Arguments for GlobalBufferedFunction."""

    data: Annotated[TableInput, Arg(0, doc="Input table")]


@dataclass
class GlobalBufferedState(ArrowSerializableDataclass):
    """Cursor over the buffered state_log; ``after_id`` starts before-first."""

    after_id: int = -1


class GlobalBufferedFunction(TableBufferingFunction[GlobalBufferedArguments, GlobalBufferedState]):
    """Table-buffering probe — buffers all input, replays it on finalize.

    SQL: ``SELECT * FROM vgi_example_global_buffered((SELECT * FROM t))``
    """

    class Meta:
        """Metadata for GlobalBufferedFunction."""

        name = "global_buffered"
        description = "Global-registration probe (table-buffering)"
        categories = ["test", "global"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM vgi_example_global_buffered((SELECT 1 AS x))",
                description="Buffering probe published into system.main",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[GlobalBufferedArguments]) -> BindResponse:
        """Output schema = input schema (passthrough)."""
        assert params.bind_call.input_schema is not None
        return BindResponse(output_schema=params.bind_call.input_schema)

    @classmethod
    def process(
        cls,
        batch: pa.RecordBatch,
        params: TableBufferingParams[GlobalBufferedArguments],
    ) -> bytes:
        """Append the batch to the shared state_log; return execution_id."""
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        params.storage.state_append(b"global_buf", b"", sink.getvalue().to_pybytes())
        return params.execution_id

    @classmethod
    def combine(
        cls,
        state_ids: list[bytes],
        params: TableBufferingParams[GlobalBufferedArguments],
    ) -> list[bytes]:
        """Every state_id is the execution_id; collapse to one stream."""
        return [params.execution_id]

    @classmethod
    def initial_finalize_state(
        cls,
        finalize_state_id: bytes,
        params: TableBufferingParams[GlobalBufferedArguments],
    ) -> GlobalBufferedState:
        """Start the drain cursor before the first log entry."""
        return GlobalBufferedState(after_id=-1)

    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[GlobalBufferedArguments],
        finalize_state_id: bytes,
        state: GlobalBufferedState,
        out: OutputCollector,
    ) -> None:
        """Emit one buffered batch per tick; finish at end-of-log."""
        rows = params.storage.state_log_scan(b"global_buf", b"", after_id=state.after_id, limit=1)
        if not rows:
            out.finish()
            return
        log_id, value = rows[0]
        out.emit(pa.ipc.open_stream(value).read_next_batch())
        state.after_id = log_id
