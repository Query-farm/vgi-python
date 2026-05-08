"""Multi-worker partitioned sequence fixtures, one per ``OrderPreservation`` mode.

These three fixtures are clones of :class:`PartitionedSequenceFunction` (see
``sequence.py``); the only difference is ``Meta.preserves_order``. They exist
so SQL integration tests can verify that DuckDB's planner honors each mode
end-to-end:

* ``partitioned_preserves_order``     — ``PRESERVES_ORDER``     → DuckDB ``INSERTION_ORDER``
* ``partitioned_no_order_guarantee``  — ``NO_ORDER_GUARANTEE``  → DuckDB ``NO_ORDER``
* ``partitioned_fixed_order``         — ``FIXED_ORDER``         → DuckDB ``FIXED_ORDER``

DuckDB serializes the pipeline (single worker) for ``FIXED_ORDER`` and
parallelizes for the other two. Tests grep ``conn=`` from ``duckdb_logs()``
to count distinct workers — the same pattern used by
``partitioned_sequence.test``.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Annotated, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import _cardinality_from_count
from vgi.arguments import Arg
from vgi.invocation import GlobalInitResponse
from vgi.metadata import FunctionExample, OrderPreservation
from vgi.schema_utils import schema
from vgi.table_function import (
    InitParams,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
)


@dataclass(slots=True, frozen=True)
class _OrderModeArgs:
    """Arguments for the order-preservation-mode fixtures."""

    count: Annotated[int, Arg(0, doc="Total number of integers to generate", ge=0)]


@dataclass(kw_only=True)
class _OrderModeState(ArrowSerializableDataclass):
    """Mutable per-worker state for the order-preservation-mode fixtures."""

    current_start: int | None = None
    current_end: int | None = None
    current_idx: int = 0


class _BasePartitionedOrderMode(
    TableFunctionGenerator[_OrderModeArgs, _OrderModeState]
):
    """Shared multi-worker work-queue logic. Subclasses pin ``Meta``.

    The chunk/batch sizing matches ``PartitionedSequenceFunction``: 1k chunks,
    1k-row output batches. The primary worker enqueues all chunks during
    ``on_init``; every worker (including the primary) pulls chunks atomically
    via ``params.storage.queue_pop``.
    """

    CHUNK_SIZE: ClassVar[int] = 1000
    BATCH_SIZE: ClassVar[int] = 1000

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())

    @classmethod
    def on_init(cls, params: InitParams[_OrderModeArgs]) -> GlobalInitResponse:
        work_items: list[bytes] = []
        for start_idx in range(0, params.args.count, cls.CHUNK_SIZE):
            end_idx = min(start_idx + cls.CHUNK_SIZE, params.args.count)
            work_items.append(struct.pack(">QQ", start_idx, end_idx))
        params.storage.queue_push(work_items)
        return GlobalInitResponse()

    @classmethod
    def initial_state(cls, params: ProcessParams[_OrderModeArgs]) -> _OrderModeState:
        return _OrderModeState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_OrderModeArgs],
        state: _OrderModeState,
        out: OutputCollector,
    ) -> None:
        if state.current_start is None or state.current_idx >= (state.current_end or 0):
            work_data = params.storage.queue_pop()
            if work_data is None:
                out.finish()
                return
            state.current_start, state.current_end = struct.unpack(">QQ", work_data)
            assert state.current_start is not None
            state.current_idx = state.current_start

        batch_end_idx = min(state.current_idx + cls.BATCH_SIZE, state.current_end or 0)
        values = list(range(state.current_idx, batch_end_idx))
        out.emit(pa.RecordBatch.from_pydict({"n": values}, schema=params.output_schema))
        state.current_idx = batch_end_idx


@bind_fixed_schema
@_cardinality_from_count
class PartitionedPreservesOrderFunction(_BasePartitionedOrderMode):
    """Multi-worker partitioned sequence — ``PRESERVES_ORDER``."""

    class Meta:
        name = "partitioned_preserves_order"
        description = (
            "Multi-worker partitioned sequence; preserves_order=PRESERVES_ORDER "
            "(maps to DuckDB INSERTION_ORDER)."
        )
        categories = ["generator", "utility"]
        preserves_order = OrderPreservation.PRESERVES_ORDER
        examples = [
            FunctionExample(
                sql="SELECT * FROM partitioned_preserves_order(100)",
                description="Generate 0-99 in parallel; preserves_order=PRESERVES_ORDER",
            ),
        ]


@bind_fixed_schema
@_cardinality_from_count
class PartitionedNoOrderGuaranteeFunction(_BasePartitionedOrderMode):
    """Multi-worker partitioned sequence — ``NO_ORDER_GUARANTEE``."""

    class Meta:
        name = "partitioned_no_order_guarantee"
        description = (
            "Multi-worker partitioned sequence; preserves_order=NO_ORDER_GUARANTEE "
            "(maps to DuckDB NO_ORDER)."
        )
        categories = ["generator", "utility"]
        preserves_order = OrderPreservation.NO_ORDER_GUARANTEE
        examples = [
            FunctionExample(
                sql="SELECT * FROM partitioned_no_order_guarantee(100)",
                description="Generate 0-99 in parallel; preserves_order=NO_ORDER_GUARANTEE",
            ),
        ]


@bind_fixed_schema
@_cardinality_from_count
class PartitionedFixedOrderFunction(_BasePartitionedOrderMode):
    """Multi-worker partitioned sequence — ``FIXED_ORDER`` (DuckDB serializes)."""

    class Meta:
        name = "partitioned_fixed_order"
        description = (
            "Multi-worker partitioned sequence; preserves_order=FIXED_ORDER "
            "(DuckDB serializes the pipeline so a single worker produces all rows)."
        )
        categories = ["generator", "utility"]
        preserves_order = OrderPreservation.FIXED_ORDER
        examples = [
            FunctionExample(
                sql="SELECT * FROM partitioned_fixed_order(100)",
                description="Generate 0-99; FIXED_ORDER forces single-worker execution",
            ),
        ]
