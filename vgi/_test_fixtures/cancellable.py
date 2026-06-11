# Copyright 2025, 2026 Query Farm LLC - https://query.farm

# ruff: noqa: D102, D106
"""Instrumented fixture functions for the `on_cancel` contract.

Two example functions registered in the example worker so that
integration tests (notably the destructor-side cancel path in the VGI
C++ extension) can observe whether the cancel signal actually reached
the Python worker.

Both functions are test fixtures — not production-useful — so they
live here next to the other example-worker scaffolding rather than
under a generic location.

AVAILABLE FUNCTIONS
-------------------
SlowCancellableFunction — source-only table function that produces
    one row per batch with a configurable per-batch sleep. When the
    C++ extension tears down the stream early (LIMIT, Ctrl-C, etc.)
    its ``on_cancel`` appends a line to a caller-supplied file path.
SlowCancellableInOutFunction — table-in-out variant, used to
    exercise the ``VgiTableInOutLocalState`` destructor path once PR 2
    wires it.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Annotated, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.arguments import Arg, TableInput
from vgi.invocation import BindResponse
from vgi.schema_utils import schema
from vgi.table_buffering_function import (
    TableBufferingFunction,
    TableBufferingParams,
)
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)
from vgi.table_in_out_function import TableInOutGenerator

__all__ = [
    "SlowCancellableBufferingFunction",
    "SlowCancellableFunction",
    "SlowCancellableInOutFunction",
]


def _append_cancel_probe(path: str, **fields: int) -> None:
    # Opened O_APPEND so concurrent writers (HTTP-pool case) serialise
    # naturally at the OS level.
    parts = [f"pid={os.getpid()}"] + [f"{k}={v}" for k, v in fields.items()]
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(" ".join(parts) + "\n")


@dataclass(slots=True, frozen=True, kw_only=True)
class SlowCancellableArgs:
    """Arguments for :class:`SlowCancellableFunction`."""

    probe_path: Annotated[str, Arg(0, doc="Path to append to when on_cancel fires")]
    sleep_ms: Annotated[int, Arg("sleep_ms", default=50, doc="Sleep per batch (ms)", ge=0)] = 50
    count: Annotated[
        int,
        Arg("count", default=1_000_000, doc="Total rows to produce (caps the source)", ge=0),
    ] = 1_000_000


@dataclass(kw_only=True)
class SlowCancellableState(ArrowSerializableDataclass):
    """Counter of rows already emitted; survives HTTP state round-trips."""

    emitted: int = 0


@init_single_worker
@bind_fixed_schema
class SlowCancellableFunction(TableFunctionGenerator[SlowCancellableArgs, SlowCancellableState]):
    """Slow producer that records every ``on_cancel`` invocation to a file.

    SQL::

        SELECT * FROM slow_cancellable('/tmp/probe.txt', sleep_ms := 100) LIMIT 2;

    Each tick produces one row ``(n INTEGER)`` after a short sleep.
    When the stream is torn down early, ``on_cancel`` appends a single
    line to ``probe_path``. The line includes the PID so multi-worker
    pools can be disambiguated in tests:

        pid=12345 emitted=2
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema({"n": pa.int64()})

    class Meta:
        name = "slow_cancellable"
        description = "Slow producer with an on_cancel file-writing probe (test fixture)"
        categories = ["test"]

    @classmethod
    def initial_state(cls, params: ProcessParams[SlowCancellableArgs]) -> SlowCancellableState:
        return SlowCancellableState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[SlowCancellableArgs],
        state: SlowCancellableState,
        out: OutputCollector,
    ) -> None:
        if state.emitted >= params.args.count:
            out.finish()
            return
        if params.args.sleep_ms > 0:
            time.sleep(params.args.sleep_ms / 1000.0)
        batch = pa.RecordBatch.from_pydict({"n": [state.emitted]}, schema=params.output_schema)
        state.emitted += 1
        out.emit(batch)

    @classmethod
    def on_cancel(
        cls,
        params: ProcessParams[SlowCancellableArgs],
        state: SlowCancellableState,
    ) -> None:
        _append_cancel_probe(params.args.probe_path, emitted=state.emitted)


@dataclass(slots=True, frozen=True, kw_only=True)
class SlowCancellableInOutArgs:
    """Arguments for :class:`SlowCancellableInOutFunction`."""

    probe_path: Annotated[str, Arg(0, doc="Path to append to when on_cancel fires")]
    data: Annotated[TableInput, Arg(1, doc="Input table")]
    sleep_ms: Annotated[int, Arg("sleep_ms", default=50, doc="Sleep per batch (ms)", ge=0)] = 50


@dataclass(kw_only=True)
class SlowCancellableInOutState(ArrowSerializableDataclass):
    """Counter of batches seen; survives HTTP state round-trips."""

    processed: int = 0


class SlowCancellableInOutFunction(TableInOutGenerator[SlowCancellableInOutArgs, SlowCancellableInOutState]):
    """Slow table-in-out variant of :class:`SlowCancellableFunction`."""

    class Meta:
        name = "slow_cancellable_inout"
        description = "Slow table-in-out with on_cancel probe (test fixture)"
        categories = ["test"]

    @classmethod
    def on_bind(cls, params: BindParams[SlowCancellableInOutArgs]) -> BindResponse:
        assert params.bind_call.input_schema is not None
        return BindResponse(output_schema=params.bind_call.input_schema)

    @classmethod
    def initial_state(cls, params: ProcessParams[SlowCancellableInOutArgs]) -> SlowCancellableInOutState:
        return SlowCancellableInOutState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[SlowCancellableInOutArgs],
        state: SlowCancellableInOutState,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        if params.args.sleep_ms > 0:
            time.sleep(params.args.sleep_ms / 1000.0)
        state.processed += 1
        out.emit(batch)

    @classmethod
    def on_cancel(
        cls,
        params: ProcessParams[SlowCancellableInOutArgs],
        state: SlowCancellableInOutState | None,
    ) -> None:
        processed = state.processed if state is not None else 0
        _append_cancel_probe(params.args.probe_path, processed=processed)


# ============================================================================
# TableBufferingFunction variant — exercises the on_cancel wiring on
# ``TableBufferingFinalizeState`` (the Sink+Source path). Mirrors the
# streaming fixtures above; the only structural difference is that
# emission lives in ``finalize()`` rather than ``process()``, so cancel
# fires through the producer-mode stream-cancel path on the Source side.
# ============================================================================


@dataclass(slots=True, frozen=True, kw_only=True)
class SlowCancellableBufferingArgs:
    """Arguments for :class:`SlowCancellableBufferingFunction`."""

    probe_path: Annotated[str, Arg(0, doc="Path to append to when on_cancel fires")]
    # TableBufferingFunction must accept a TABLE input — the operator's
    # Sink phase wraps the input pipeline. We ignore the rows themselves
    # (the test is purely about Source-side cancel), but DuckDB's binder
    # requires the function to declare TableInput for subquery args.
    data: Annotated[TableInput, Arg(1, doc="Input table (rows ignored)")]
    count: Annotated[
        int,
        Arg("count", default=1_000, doc="Total rows to emit during finalize", ge=1),
    ] = 1_000
    sleep_ms: Annotated[
        int,
        Arg("sleep_ms", default=10, doc="Sleep per emitted row (ms)", ge=0),
    ] = 10


@dataclass(kw_only=True)
class SlowCancellableBufferingState(ArrowSerializableDataclass):
    """Per-tick state — counter survives wire round-trips for HTTP rehydration."""

    emitted: int = 0
    # We snapshot probe_path and total count here so on_cancel doesn't need
    # to chase them off ``params.args`` (which is fine on subprocess but
    # forces an extra cold-load round-trip on HTTP rehydration).
    probe_path: str = ""
    total: int = 0


class SlowCancellableBufferingFunction(
    TableBufferingFunction[SlowCancellableBufferingArgs, SlowCancellableBufferingState],
):
    """Slow buffered producer that records ``on_cancel`` to a file.

    Sink absorbs all input (we don't actually use the input data — this
    fixture is purely about exercising the Source-side cancel path).
    ``finalize()`` then emits ``count`` rows with a per-row sleep so a
    LIMIT 1 query reliably triggers cancel before EOS. ``on_cancel``
    appends ``pid=<n> emitted=<m>`` to the probe path so the integration
    test can assert that the cancel hook actually fired.

    SQL::

        SELECT n FROM slow_cancellable_buffering('/tmp/probe.txt',
                                                 (SELECT 1 AS x),
                                                 sleep_ms := 20,
                                                 count := 1000)
        LIMIT 1;
    """

    class Meta:
        name = "slow_cancellable_buffering"
        description = "Slow buffered table function with an on_cancel file probe (test fixture)"
        categories = ["test"]

    @classmethod
    def on_bind(
        cls,
        params: BindParams[SlowCancellableBufferingArgs],
    ) -> BindResponse:
        # Emit a single-column INT64 output regardless of input schema;
        # the input is ignored (Sink absorbs but doesn't accumulate).
        return BindResponse(output_schema=schema({"n": pa.int64()}))

    @classmethod
    def process(
        cls,
        batch: pa.RecordBatch,  # noqa: ARG003 — sink absorbs but ignores
        params: TableBufferingParams[SlowCancellableBufferingArgs],
    ) -> bytes:
        # We don't store anything; just return the execution_id so
        # combine() sees a stable bucket. Cancel testing is a Source-side
        # concern, not a Sink-side one.
        return params.execution_id

    @classmethod
    def combine(
        cls,
        state_ids: list[bytes],  # noqa: ARG003
        params: TableBufferingParams[SlowCancellableBufferingArgs],
    ) -> list[bytes]:
        return [params.execution_id]

    @classmethod
    def initial_finalize_state(
        cls,
        finalize_state_id: bytes,  # noqa: ARG003
        params: TableBufferingParams[SlowCancellableBufferingArgs],
    ) -> SlowCancellableBufferingState:
        return SlowCancellableBufferingState(
            probe_path=params.args.probe_path,
            total=params.args.count,
        )

    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[SlowCancellableBufferingArgs],
        finalize_state_id: bytes,  # noqa: ARG002 — unused; producer-mode tick
        state: SlowCancellableBufferingState,
        out: OutputCollector,
    ) -> None:
        if state.emitted >= state.total:
            out.finish()
            return
        if params.args.sleep_ms > 0:
            time.sleep(params.args.sleep_ms / 1000.0)
        batch = pa.RecordBatch.from_pydict(
            {"n": [state.emitted]},
            schema=params.output_schema,
        )
        state.emitted += 1
        out.emit(batch)

    @classmethod
    def on_cancel(
        cls,
        params: TableBufferingParams[SlowCancellableBufferingArgs],  # noqa: ARG003
        finalize_state_id: bytes,  # noqa: ARG003
        state: SlowCancellableBufferingState | None,
    ) -> None:
        # ``state`` is None when cancel fires before initial_finalize_state
        # ran. In that case there's nothing to attribute (we never reached
        # the user's setup), but we still write a probe line so the test
        # can distinguish "cancel never fired" from "cancel fired pre-init".
        if state is None:
            _append_cancel_probe("/dev/null", emitted=-1)
            return
        _append_cancel_probe(state.probe_path, emitted=state.emitted)
