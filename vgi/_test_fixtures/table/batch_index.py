# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Partitioned-queue fixtures that opt in to ``supports_batch_index``.

These exist so SQL integration tests can verify the batch_index feature:

* ``partitioned_batch_index(count)`` — single-column ``n int64`` output;
  parallel scan with FIXED_ORDER preservation. Each queue item is tagged
  with a stable partition_id; the worker emits Arrow batches tagged with
  that id via ``out.emit(batch, batch_index=partition_id)``. The DuckDB
  extension reads the tag from each batch's KeyValueMetadata, threads it
  through ``TableFunction::get_partition_data``, and ordered sinks
  (``PhysicalBatchCollector``, ``PhysicalBatchInsert``,
  ``PhysicalBatchCopyToFile``, ``PhysicalLimit``) reassemble output in
  partition_id order. The FIXED_ORDER ``MaxThreads=1`` clamp is dropped
  for opted-in functions.

* ``partitioned_batch_index_marked(count, chunk_size)`` — two-column
  ``(partition_id int64, seq int64)`` output. Lets tests directly
  observe partition boundaries in the output stream (e.g. "no row with
  partition_id=N appears after a row with partition_id=N+1"). Projection
  pushdown is disabled so the ``partition_id`` column survives even
  ``SELECT seq FROM …`` queries.

The worker uses the existing in-process ``state`` to carry per-worker
cursor information across ``process()`` calls — same approach as
``_BasePartitionedOrderMode`` in ``order_modes.py``. HTTP transport's
existing STATE_KEY mechanism (in vgi_rpc.http) round-trips this state
across requests; nothing new is added for HTTP resumption.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Annotated, ClassVar, cast

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import _cardinality_from_count
from vgi.arguments import Arg
from vgi.invocation import GlobalInitResponse
from vgi.metadata import FunctionExample, OrderPreservation
from vgi.protocol import VgiOutputCollector
from vgi.schema_utils import schema
from vgi.table_function import (
    InitParams,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
)

# Queue-item encoding: (partition_id, start, end) packed as three uint64s.
# Decoded by ``process()`` on the worker; partition_id is what the worker
# emits to DuckDB via the batch_index= kwarg.
_ITEM_FMT = ">QQQ"
_ITEM_SIZE = struct.calcsize(_ITEM_FMT)


# =============================================================================
# Single-column variant: partitioned_batch_index(count)
# =============================================================================


@dataclass(slots=True, frozen=True)
class _BatchIndexArgs:
    """Arguments for ``partitioned_batch_index``."""

    count: Annotated[int, Arg(0, doc="Total number of integers to generate", ge=0)]


@dataclass(kw_only=True)
class _BatchIndexState(ArrowSerializableDataclass):
    """Per-worker cursor state.

    ``partition_id`` is the queue-push order of the current work item; emitted
    on every Arrow batch via the batch_index= kwarg. ``current_idx`` advances
    through ``[current_start, current_end)`` as the worker produces batches.
    All three reset to None at the moment a partition is exhausted; the next
    ``process()`` call pops a fresh item.
    """

    partition_id: int | None = None
    current_start: int | None = None
    current_end: int | None = None
    current_idx: int = 0


@bind_fixed_schema
@_cardinality_from_count
class PartitionedBatchIndexFunction(TableFunctionGenerator[_BatchIndexArgs, _BatchIndexState]):
    """Parallel-scan sequence with batch_index ordering.

    The primary worker enqueues N work items at on_init, each encoding
    ``(partition_id, start, end)``. Any worker pulls the next item via
    ``queue_pop``; emits a stream of Arrow batches tagged with
    partition_id; advances to the next item when exhausted. DuckDB's
    ordered sinks reassemble output in partition_id order — final output
    matches a single-threaded scan, but the source itself fans out across
    threads.
    """

    CHUNK_SIZE: ClassVar[int] = 1000
    BATCH_SIZE: ClassVar[int] = 1000

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())

    class Meta:
        name = "partitioned_batch_index"
        description = (
            "Multi-worker partitioned sequence with per-batch batch_index "
            "tagging; parallel scan + ordered sink reassembly."
        )
        categories = ["generator", "utility"]
        preserves_order = OrderPreservation.FIXED_ORDER
        supports_batch_index = True
        examples = [
            FunctionExample(
                sql="SELECT * FROM partitioned_batch_index(100)",
                description=(
                    "Generate 0..99 in parallel; DuckDB sinks reassemble output in partition_id (insertion) order."
                ),
            ),
        ]

    @classmethod
    def on_init(cls, params: InitParams[_BatchIndexArgs]) -> GlobalInitResponse:
        work_items: list[bytes] = []
        for partition_id, start_idx in enumerate(range(0, params.args.count, cls.CHUNK_SIZE)):
            end_idx = min(start_idx + cls.CHUNK_SIZE, params.args.count)
            work_items.append(struct.pack(_ITEM_FMT, partition_id, start_idx, end_idx))
        params.storage.queue_push(work_items)
        return GlobalInitResponse()

    @classmethod
    def initial_state(cls, params: ProcessParams[_BatchIndexArgs]) -> _BatchIndexState:
        return _BatchIndexState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_BatchIndexArgs],
        state: _BatchIndexState,
        out: OutputCollector,
    ) -> None:
        if state.partition_id is None or state.current_idx >= (state.current_end or 0):
            work_data = params.storage.queue_pop()
            if work_data is None:
                out.finish()
                return
            partition_id, start, end = struct.unpack(_ITEM_FMT, work_data)
            state.partition_id = partition_id
            state.current_start = start
            state.current_end = end
            state.current_idx = start

        batch_end_idx = min(state.current_idx + cls.BATCH_SIZE, state.current_end or 0)
        values = list(range(state.current_idx, batch_end_idx))
        cast(VgiOutputCollector, out).emit(
            pa.RecordBatch.from_pydict({"n": values}, schema=params.output_schema),
            batch_index=state.partition_id,
        )
        state.current_idx = batch_end_idx


# =============================================================================
# Two-column variant: partitioned_batch_index_marked(count, chunk_size)
# =============================================================================


@dataclass(slots=True, frozen=True)
class _BatchIndexMarkedArgs:
    """Arguments for ``partitioned_batch_index_marked``."""

    count: Annotated[int, Arg(0, doc="Total number of rows to generate", ge=0)]
    chunk_size: Annotated[int, Arg("chunk_size", default=1000, doc="Rows per partition", ge=1)]


@dataclass(kw_only=True)
class _BatchIndexMarkedState(ArrowSerializableDataclass):
    partition_id: int | None = None
    current_start: int | None = None
    current_end: int | None = None
    current_idx: int = 0


@bind_fixed_schema
@_cardinality_from_count
class PartitionedBatchIndexMarkedFunction(TableFunctionGenerator[_BatchIndexMarkedArgs, _BatchIndexMarkedState]):
    """Two-column batch_index fixture for direct ordering observation.

    Output rows are ``(partition_id, seq)`` where ``partition_id`` is the
    queue-push order (matches the emitted batch_index) and ``seq`` counts
    up within each partition starting at 0. Tests assert that no row with
    a higher partition_id appears before a row with a lower one — proving
    that DuckDB's sink-side reassembly worked.

    Projection pushdown is OFF on this fixture so ``SELECT seq FROM …``
    still gets the partition_id column emitted by the worker; the C++
    extension's projection then drops it after the ordering metadata has
    been observed.
    """

    BATCH_SIZE: ClassVar[int] = 256

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(
        partition_id=pa.int64(),
        seq=pa.int64(),
    )

    class Meta:
        name = "partitioned_batch_index_marked"
        description = (
            "Two-column batch_index demo: rows are (partition_id, seq). Tests "
            "assert that DuckDB's ordered sinks reassemble output in "
            "partition_id order under parallel execution."
        )
        categories = ["generator", "utility", "testing"]
        preserves_order = OrderPreservation.FIXED_ORDER
        supports_batch_index = True
        projection_pushdown = False
        examples = [
            FunctionExample(
                sql="SELECT * FROM partitioned_batch_index_marked(100, chunk_size := 25) LIMIT 5",
                description="First 5 rows of partition 0",
            ),
        ]

    @classmethod
    def on_init(cls, params: InitParams[_BatchIndexMarkedArgs]) -> GlobalInitResponse:
        work_items: list[bytes] = []
        chunk_size = params.args.chunk_size
        for partition_id, start_idx in enumerate(range(0, params.args.count, chunk_size)):
            end_idx = min(start_idx + chunk_size, params.args.count)
            work_items.append(struct.pack(_ITEM_FMT, partition_id, start_idx, end_idx))
        params.storage.queue_push(work_items)
        return GlobalInitResponse()

    @classmethod
    def initial_state(cls, params: ProcessParams[_BatchIndexMarkedArgs]) -> _BatchIndexMarkedState:
        return _BatchIndexMarkedState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_BatchIndexMarkedArgs],
        state: _BatchIndexMarkedState,
        out: OutputCollector,
    ) -> None:
        if state.partition_id is None or state.current_idx >= (state.current_end or 0):
            work_data = params.storage.queue_pop()
            if work_data is None:
                out.finish()
                return
            partition_id, start, end = struct.unpack(_ITEM_FMT, work_data)
            state.partition_id = partition_id
            state.current_start = start
            state.current_end = end
            state.current_idx = start

        batch_end_idx = min(state.current_idx + cls.BATCH_SIZE, state.current_end or 0)
        rows = batch_end_idx - state.current_idx
        partition_ids = [state.partition_id] * rows
        seqs = list(range(state.current_idx - (state.current_start or 0), batch_end_idx - (state.current_start or 0)))
        cast(VgiOutputCollector, out).emit(
            pa.RecordBatch.from_pydict(
                {"partition_id": partition_ids, "seq": seqs},
                schema=params.output_schema,
            ),
            batch_index=state.partition_id,
        )
        state.current_idx = batch_end_idx
