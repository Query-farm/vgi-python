"""Streaming-partitioned aggregate fixtures.

Exercise the ``streaming_partitioned`` opt-in: ``streaming_open`` /
``streaming_chunk`` / ``streaming_close``. These are routed through the
VGI DuckDB extension's custom streaming operator, which pipes input
chunks straight to the worker without materialising the partition on
the DuckDB side. State is bounded by partitions × per-partition state,
not by row count — the structural answer to "running aggregate over
unbounded ordered input."

These fixtures are reference implementations for the protocol. Real
production aggregates (e.g. ``portfolio_agg``) follow the same shape
but with domain-specific state and I/O optimisations (Decimal128 buffer
tricks, etc.).

When the optimizer rule rejects a query (non-cumulative frame, EXCLUDE
clause, DISTINCT/FILTER, etc.) DuckDB falls back to the standard
windowed path — so all three of these classes also implement
update/combine/finalize for plain GROUP BY usage and (optionally) the
windowed callbacks. The streaming methods are additive.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass

from vgi._test_fixtures.aggregate._common import SumState
from vgi.aggregate_function import AggregateFunction
from vgi.arguments import Param, Returns
from vgi.metadata import DistinctDependence, NullHandling, OrderDependence
from vgi.table_function import ProcessParams


class StreamingSumFunction(AggregateFunction[SumState]):
    """Streaming-partitioned running sum.

    Cumulative across each `(PARTITION BY key)` group, in `ORDER BY` order.
    For every input row, emits the running sum of the value column at that
    row's position in its partition.

    Also wired for ``GROUP BY`` via ``update`` / ``combine`` / ``finalize``,
    so the same function works in both shapes::

        -- streaming-partitioned (one running value per fill row):
        SELECT k, v, vgi_streaming_sum(v) OVER (PARTITION BY k ORDER BY ts)
        FROM trades;

        -- group-by (one final value per partition):
        SELECT k, vgi_streaming_sum(v) FROM trades GROUP BY k;

    State persistence: the per-partition dict lives in worker memory in an
    in-process LRU and is also persisted to ``FunctionStorage`` after each
    chunk so a follow-up chunk landing on a different worker pool entry
    can rehydrate. No special handling required from this class — the
    framework does it.
    """

    class Meta:
        name = "vgi_streaming_sum"
        description = (
            "Running sum across PARTITION BY keys via the streaming-partitioned "
            "protocol. Each input row emits the cumulative sum at its position."
        )
        null_handling = NullHandling.DEFAULT
        order_dependent = OrderDependence.NOT_ORDER_DEPENDENT
        distinct_dependent = DistinctDependence.NOT_DISTINCT_DEPENDENT
        supports_window = False
        # Opt into the streaming-partitioned operator. The optimizer rule
        # will route eligible OVER queries through it; ineligible shapes
        # (sliding frames, EXCLUDE, DISTINCT, FILTER) fall back to the
        # standard windowed path automatically.
        streaming_partitioned = True

    # ------------------------------------------------------------------
    # GROUP BY path — required for plain aggregation queries.
    # ------------------------------------------------------------------

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> SumState:
        return SumState()

    @classmethod
    def update(
        cls,
        states: dict[int, SumState],
        group_ids: pa.Int64Array,
        value: Annotated[pa.Int64Array, Param(doc="Column to sum")],
    ) -> None:
        table = pa.table({"gid": group_ids, "value": value})
        grouped = table.group_by("gid").aggregate([("value", "sum")])
        for i in range(grouped.num_rows):
            gid: int = grouped.column("gid")[i].as_py()
            v = grouped.column("value_sum")[i].as_py()
            if v is not None:
                states[gid] = SumState(total=states[gid].total + v)

    @classmethod
    def combine(
        cls, source: SumState, target: SumState, params: ProcessParams[None]
    ) -> SumState:
        return SumState(total=source.total + target.total)

    @classmethod
    def finalize(
        cls,
        group_ids: pa.Int64Array,
        states: dict[int, SumState],
        params: ProcessParams[None],
    ) -> Annotated[pa.RecordBatch, Returns(pa.int64())]:
        results = [
            (s.total if (s := states.get(gid.as_py())) is not None else None)
            for gid in group_ids
        ]
        return pa.record_batch({"result": pa.array(results, type=pa.int64())})

    # ------------------------------------------------------------------
    # Streaming-partitioned path.
    # ------------------------------------------------------------------
    #
    # Three callbacks: open / chunk / close. The framework handles session
    # lifecycle (allocates execution_id, persists to FunctionStorage,
    # rehydrates across pool workers); user code only owns the in-memory
    # state object.

    @classmethod
    def streaming_open(cls, params: ProcessParams[None]) -> dict[str, Any]:
        # Session state. Free shape — anything picklable. The framework
        # passes this object back to streaming_chunk and streaming_close
        # unchanged. For multi-partition aggregates, hold a per-partition
        # dict here; for single-partition, just hold the running scalar.
        return {
            # partition_key_tuple -> running int sum
            "partition_states": {},
        }

    @classmethod
    def streaming_chunk(
        cls,
        chunk: pa.RecordBatch,
        streaming_state: dict[str, Any],
        partition_key_count: int,
        order_key_count: int,
        params: ProcessParams[None],
    ) -> pa.Array:
        # Column layout from the operator:
        #   [partition_key_cols..., order_key_cols..., value_cols...]
        # We don't actually need the order keys at runtime here — the
        # input arrives in (partition, order) order already, so cumulative
        # state is naturally correct.
        n = chunk.num_rows
        value_idx = partition_key_count + order_key_count

        if partition_key_count > 0:
            pk_columns = [chunk.column(i).to_pylist() for i in range(partition_key_count)]
        else:
            pk_columns = []
        values = chunk.column(value_idx).to_pylist()

        partition_states: dict[Any, int] = streaming_state["partition_states"]

        # Returns one cumulative-sum int per input row. NULL value rows
        # leave state unchanged but still emit the current sum (matches
        # the GROUP BY path's NullHandling.DEFAULT semantics).
        out: list[int] = [0] * n
        for i in range(n):
            if partition_key_count == 0:
                key: Any = ()
            elif partition_key_count == 1:
                key = pk_columns[0][i]
            else:
                key = tuple(col[i] for col in pk_columns)

            running = partition_states.get(key, 0)
            v = values[i]
            if v is not None:
                running += v
                partition_states[key] = running
            out[i] = running

        return pa.array(out, type=pa.int64())

    @classmethod
    def streaming_close(
        cls,
        streaming_state: dict[str, Any],
        params: ProcessParams[None],
    ) -> None:
        # Cleanup hook. For this fixture there's nothing to release;
        # state is plain Python objects that GC collects when the
        # session is dropped from the framework's cache. Real
        # implementations might release file handles, close DB
        # connections, or flush logs here.
        return None
