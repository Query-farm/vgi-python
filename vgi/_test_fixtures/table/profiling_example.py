"""Example function exercising the dynamic_to_string callback.

``ProfilingDemoFunction`` demonstrates the recommended persistence
pattern for diagnostics that should surface under ``EXPLAIN ANALYZE``:

1. ``process()`` keeps per-stream counters in user state (rows,
   batches, start time), and after every tick writes a serialized
   snapshot via ``params.storage.put(bytes)``.
2. ``dynamic_to_string()`` constructs a ``BoundStorage`` for the
   given ``execution_id``, calls ``collect()`` to gather every
   worker's last snapshot, and sums them.

``BoundStorage`` defaults to the sqlite-backed shared storage (see
CLAUDE.md → ``VGI_WORKER_SHARED_STORAGE``), so the pattern works across
worker processes — both subprocess transport and HTTP transport with
``max_workers > 1``. No in-memory class state is involved.
"""

from __future__ import annotations

import struct
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, ClassVar

import numpy as np
import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import CountBatchArgs
from vgi.arguments import Arg
from vgi.function_storage import BoundStorage
from vgi.metadata import FunctionExample
from vgi.schema_utils import schema
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableCardinality,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)


@dataclass(frozen=True)
class ProfilingDemoArgs(CountBatchArgs):
    """Arguments for ProfilingDemoFunction."""

    increment: Annotated[int, Arg("increment", default=1, doc="Step between values", ge=1)]


@dataclass(kw_only=True)
class ProfilingState(ArrowSerializableDataclass):
    """Per-stream counters."""

    remaining: int
    current_index: int = 0
    rows_emitted: int = 0
    batches_emitted: int = 0
    started_at_ns: int = 0


# Serialized snapshot wire format: three little-endian uint64s
# (rows, batches, elapsed_us). Compact; survives multi-worker collect().
_SNAPSHOT = struct.Struct("<QQQ")


def _pack_snapshot(rows: int, batches: int, elapsed_us: int) -> bytes:
    return _SNAPSHOT.pack(rows, batches, elapsed_us)


def _unpack_snapshot(data: bytes) -> tuple[int, int, int]:
    return _SNAPSHOT.unpack(data)


@init_single_worker
@bind_fixed_schema
class ProfilingDemoFunction(TableFunctionGenerator[ProfilingDemoArgs, ProfilingState]):
    """Sequence generator that publishes per-execution metrics under EXPLAIN ANALYZE.

    Output is identical to ``sequence(count, batch_size, increment)``.
    Additionally tracks ``rows_produced``, ``batches_emitted``, and
    ``elapsed_ms`` and surfaces them via ``dynamic_to_string``.
    """

    FunctionArguments = ProfilingDemoArgs

    class Meta:
        """Metadata for ProfilingDemoFunction."""

        name = "profiling_demo"
        description = "Sequence generator publishing diagnostics under EXPLAIN ANALYZE"
        categories = ["generator", "utility"]
        examples = [
            FunctionExample(
                sql="EXPLAIN ANALYZE SELECT count(*) FROM profiling_demo(500)",
                description="Run with diagnostics surfaced as Extra Info",
            ),
        ]

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())
    NUMPY_DTYPE: ClassVar[type[np.generic]] = np.int64

    @classmethod
    def cardinality(cls, params: BindParams[ProfilingDemoArgs]) -> TableCardinality:
        count = params.args.count
        return TableCardinality(estimate=count, max=count)

    @classmethod
    def initial_state(cls, params: ProcessParams[ProfilingDemoArgs]) -> ProfilingState:
        return ProfilingState(
            remaining=params.args.count,
            started_at_ns=time.monotonic_ns(),
        )

    @classmethod
    def process(
        cls,
        params: ProcessParams[ProfilingDemoArgs],
        state: ProfilingState,
        out: OutputCollector,
    ) -> None:
        if state.remaining <= 0:
            # Final write so dynamic_to_string sees the totals even after
            # the stream finishes. One row per OS pid via state_put under
            # namespace b"profile" — dynamic_to_string drains them all.
            elapsed_us = (time.monotonic_ns() - state.started_at_ns) // 1000
            import os as _os
            params.storage.state_put(
                b"profile",
                BoundStorage.pack_int_key(_os.getpid()),
                _pack_snapshot(state.rows_emitted, state.batches_emitted, elapsed_us),
            )
            out.finish()
            return
        batch_size = params.args.batch_size
        size = min(state.remaining, batch_size)
        increment = params.args.increment
        values = np.arange(
            state.current_index * increment,
            (state.current_index + size) * increment,
            increment,
            dtype=cls.NUMPY_DTYPE,
        )
        out.emit(pa.RecordBatch.from_arrays([pa.array(values)], schema=params.output_schema))
        state.current_index += size
        state.remaining -= size
        state.rows_emitted += size
        state.batches_emitted += 1

        # Per-tick snapshot — overwrites this worker's slot. The dispatcher's
        # state_drain on dynamic_to_string sums one snapshot per worker pid.
        elapsed_us = (time.monotonic_ns() - state.started_at_ns) // 1000
        import os as _os
        params.storage.state_put(
            b"profile",
            BoundStorage.pack_int_key(_os.getpid()),
            _pack_snapshot(state.rows_emitted, state.batches_emitted, elapsed_us),
        )

    @classmethod
    def dynamic_to_string(
        cls,
        params: BindParams[ProfilingDemoArgs],
        execution_id: bytes,
    ) -> Mapping[str, str]:
        # BindParams doesn't carry a BoundStorage (no execution_id at bind
        # time). Construct one with the execution_id we received.
        storage = BoundStorage(cls.storage, execution_id, request=params.bind_call, auth=params.auth_context)
        try:
            # state_drain returns (key, value) pairs; we only want the values.
            snapshots = [v for _, v in storage.state_drain(b"profile")]
        except Exception:
            return {}
        if not snapshots:
            return {}
        rows = 0
        batches = 0
        elapsed_us = 0
        for blob in snapshots:
            r, b, e = _unpack_snapshot(blob)
            rows += r
            batches += b
            elapsed_us = max(elapsed_us, e)
        return {
            "rows_produced": str(rows),
            "batches_emitted": str(batches),
            "elapsed_ms": f"{elapsed_us / 1000.0:.2f}",
        }
