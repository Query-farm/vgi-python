"""Framework for implementing table sink+source functions.

``TableBufferingFunction`` is the worker-side base for functions that must
see *every* input row before producing any output (e.g. buffer-then-emit,
global aggregations, sort-then-emit). Routed through the C++
``PhysicalVgiTableBuffering`` Sink+Source operator.

Three callbacks, mirroring the operator's three phases:

  * ``process(batch, params) -> bytes`` — ingest one batch, return an opaque
    state_id naming where the worker stored it.
  * ``combine(state_ids, params) -> list[bytes]`` — once per query, on the
    coordinator worker; group/merge/sort the per-batch state_ids and
    return finalize_state_ids for the Source phase.
  * ``finalize(params, finalize_state_id, state, out)`` — producer-mode
    streaming RPC mirroring ``TableFunctionGenerator.process``: one tick
    per call, emit one batch via ``out.emit(batch)`` (or ``out.finish()``
    for EOS), state persists between ticks via wire-serialization.

State_ids are opaque ``bytes``. The worker picks the granularity (per-batch,
per-thread, custom partitioning); the framework just round-trips them.

INVARIANT: any state the worker stores in ``process()`` that ``finalize()``
will need MUST live in cross-process storage scoped by
``params.execution_id`` (``BoundStorage`` is the canonical choice). The
Source phase may route a given ``finalize_state_id`` to a worker process
that did NOT run the corresponding ``process()`` calls.
"""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, TypeVar, get_args, get_origin

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.invocation import (
    BindResponse,
)
from vgi.table_function import (
    _ON_CANCEL_CAVEATS,
    BindParams,
    ProcessParams,
    TableFunctionBase,
)

if TYPE_CHECKING:
    pass

__all__ = [
    "TableBufferingFunction",
    "TableBufferingParams",
]


@dataclass(slots=True, frozen=True, kw_only=True)
class TableBufferingParams[TArgs](ProcessParams[TArgs]):
    """Params for ``TableBufferingFunction`` callbacks.

    Adds identity fields that the buffered API needs to scope worker-owned
    storage and coordinate cross-process state. Other function shapes
    (``TableFunctionGenerator``, ``TableInOutGenerator``, aggregates) keep
    using the plain ``ProcessParams`` they always have.

    Attributes:
        execution_id: Stable across coordinator + secondary workers for one
            DuckDB query execution. Key worker-owned storage by this.
        attach_id: Catalog attach identity; pin attach-time config lookups
            by this.
        transaction_id: Hex-encoded VGI transaction id when running inside
            a DuckDB transaction, ``None`` otherwise.
        function_name: Convenience accessor — same as
            ``init_call.function_name``.
        worker_path: Subprocess path / ``unix://`` / ``launch:`` argv. For
            diagnostics.

    """

    execution_id: bytes
    attach_id: bytes
    transaction_id: bytes | None
    function_name: str
    worker_path: str | None = None


class TableBufferingFunction[TArgs, TFinalizeState = None](TableFunctionBase[TArgs]):
    """Base class for table sink+source functions.

    Subclass to declare a function that must see every input row before
    producing output. The C++ ``PhysicalVgiTableBuffering`` operator
    routes calls through three phases:

      1. **Sink** — ``process(batch, params) -> state_id`` is called per
         input batch (parallel across DuckDB threads unless
         ``Meta.sink_order_dependent`` is set).
      2. **Combine** — ``combine(state_ids, params) -> finalize_state_ids``
         is called once on the coordinator worker after every ``process()``
         completes.
      3. **Source** — ``finalize(params, fid, state, out)`` is called per
         tick by the framework, emitting one batch per call (parallel
         across ``finalize_state_ids`` unless ``Meta.source_order_dependent``).

    Cross-process invariant: any state the worker writes during
    ``process()`` that ``finalize()`` will read MUST live in cross-process
    storage scoped by ``params.execution_id`` — ``BoundStorage`` is the
    canonical choice. The Source phase routes a given ``finalize_state_id``
    to whatever worker process the C++ scheduler picks; it is NOT
    guaranteed to be the same process that ran ``process()``.

    Type parameters:
        TArgs: User-facing function arguments dataclass.
        TFinalizeState: Wire-serializable state carried between
            ``finalize()`` ticks. Must subclass ``ArrowSerializableDataclass``
            when set to anything other than ``None``.
    """

    # Resolved at class-definition time by ``__init_subclass__`` from the
    # ``TFinalizeState`` generic parameter (position 1 in the parameterized
    # base). ``None`` means "no per-tick state" (the user passed ``None`` as
    # ``TFinalizeState`` or didn't parameterize). Inherits through subclassing,
    # so ``class Foo(BufferInputFunction): ...`` reuses the parent's resolution
    # without re-walking ``__orig_bases__``.
    _finalize_state_class: ClassVar[type[ArrowSerializableDataclass] | None] = None

    class Meta:
        """Per-class metadata for TableBufferingFunction."""

        name: ClassVar[str]
        # Output schema declared via Meta.return_schema or via on_bind().
        # Sink-side ordering: forces ParallelSink=false in the C++ operator.
        sink_order_dependent: ClassVar[bool] = False
        # Source-side ordering: forces serial output in finalize_queue order.
        source_order_dependent: ClassVar[bool] = False
        # Threads DuckDB's per-chunk batch_index into every process() call.
        # Mutually exclusive with sink_order_dependent (validated below).
        requires_input_batch_index: ClassVar[bool] = False

    def __init_subclass__(cls) -> None:  # noqa: D105 — internal hook
        super().__init_subclass__()

        # Resolve ``TFinalizeState`` from this class's directly-declared
        # ``__orig_bases__`` (i.e. the ``TableBufferingFunction[Args, State]``
        # specialization). When the subclass doesn't re-parameterize the base
        # (e.g. ``class Foo(BufferInputFunction): ...``) we leave the parent's
        # resolution alone via normal class-attribute inheritance.
        # ``TableFunctionBase.__init_subclass__`` (called via super() above)
        # has already validated that ``state_type`` is None or an
        # ``ArrowSerializableDataclass`` subclass; we just record it.
        for base in cls.__dict__.get("__orig_bases__", ()):
            origin = get_origin(base)
            if origin is None or not (
                isinstance(origin, type) and issubclass(origin, TableBufferingFunction)
            ):
                continue
            type_args = get_args(base)
            if len(type_args) < 2:
                continue
            state_type = type_args[1]
            if state_type is None or state_type is type(None):
                cls._finalize_state_class = None
            elif isinstance(state_type, TypeVar):
                # Generic-through (e.g. ``class Foo[X](TableBufferingFunction[Args, X]): ...``).
                # Concrete subclasses below this one will resolve it.
                pass
            else:
                cls._finalize_state_class = state_type
            break

        meta = getattr(cls, "Meta", None)
        if meta is None:
            return
        sink_order = bool(getattr(meta, "sink_order_dependent", False))
        requires_batch_index = bool(getattr(meta, "requires_input_batch_index", False))
        if sink_order and requires_batch_index:
            raise TypeError(
                f"{cls.__name__}.Meta: sink_order_dependent and "
                f"requires_input_batch_index are mutually exclusive — "
                f"single-thread sink already orders input, batch_index is "
                f"only meaningful under parallel ingest."
            )

    @classmethod
    def on_bind(
        cls,
        params: BindParams[TArgs],
    ) -> BindResponse:
        """Pass-through default — output schema is the input schema.

        Override to validate arguments, compute a dynamic output type, or
        request secrets via ``SecretsAccessor``. See
        ``TableFunctionBase.on_bind`` for the broader contract.
        """
        assert params.bind_call.input_schema is not None
        return BindResponse(output_schema=params.bind_call.input_schema)

    # bind / on_init / global_init are defined on TableFunctionBase.

    # ------------------------------------------------------------------
    # Sink phase
    # ------------------------------------------------------------------

    @classmethod
    @abstractmethod
    def process(
        cls,
        batch: pa.RecordBatch,
        params: TableBufferingParams[TArgs],
    ) -> bytes:
        """Ingest one input batch and return an opaque ``state_id``.

        The worker chooses both *where* to store the batch (BoundStorage,
        external files, in-memory cross-process structures, etc.) and the
        *granularity* of state_ids (per-batch, per-thread, custom
        partitioning). The framework collects all returned state_ids and
        passes them to ``combine()`` on the coordinator worker.

        Common pattern for "one bucket per execution" is to return
        ``params.execution_id``; ``combine()`` then collapses the list of
        identical state_ids to a single finalize stream.

        Cross-process invariant: any state the worker stores here that
        ``finalize()`` will need MUST live in cross-process storage scoped
        by ``params.execution_id``. The Source phase may route the
        corresponding finalize_state_id to a different worker process.

        Args:
            batch: One input batch from DuckDB. Schema matches the
                function's declared ``input_schema``.
            params: Process-time params, including identity fields
                (``execution_id``, ``attach_id``, ``transaction_id``,
                ``function_name``) and ``params.batch_index`` when
                ``Meta.requires_input_batch_index=True``.

        Returns:
            Opaque state_id naming where the batch was stored.

        """

    # ------------------------------------------------------------------
    # Combine phase
    # ------------------------------------------------------------------

    @classmethod
    @abstractmethod
    def combine(
        cls,
        state_ids: list[bytes],
        params: TableBufferingParams[TArgs],
    ) -> list[bytes]:
        """Group / merge / sort state_ids; return finalize_state_ids.

        Called once on the coordinator worker after every ``process()``
        completes. State_ids are opaque bytes — the framework does not
        inspect, dedup, or transform them. ``combine`` returns the exact
        list of finalize_state_ids the Source phase will iterate; one
        finalize stream per returned id.

        Typical patterns:

          * **Single-bucket execution** — process() returns ``params.execution_id``
            for every call; combine() returns ``[params.execution_id]`` so
            one finalize stream drains the single accumulator.
          * **Per-shard fan-out** — process() returns a per-shard
            identifier; combine() returns the list of unique shard ids
            for parallel finalize.
          * **Global sort under ``Meta.sink_order_dependent``** — process()
            returns per-batch ids; combine() reads each, sorts globally,
            returns ``[sentinel]`` so a single ordered finalize stream
            emits the merged result.

        Args:
            state_ids: Every state_id returned from every ``process()``
                call across every DuckDB thread, in arbitrary order.
                Duplicates from multiple Sink threads using the same
                state_id are NOT dedup'd by the framework.
            params: Process-time params (same identity fields as
                ``process()``).

        Returns:
            finalize_state_ids — keys the Source phase will iterate.

        """

    # ------------------------------------------------------------------
    # Source phase — mirrors TableFunctionGenerator.process producer-mode
    # ------------------------------------------------------------------

    @classmethod
    def initial_finalize_state(
        cls,
        finalize_state_id: bytes,
        params: TableBufferingParams[TArgs],
    ) -> TFinalizeState | None:
        """Build the initial wire-serializable state for a finalize stream.

        Called once per finalize_state_id at stream init time. The
        returned state is passed to the first ``finalize()`` tick; the
        framework serializes it between ticks so the stream survives
        worker process boundaries (HTTP transport).

        Default returns ``None`` (suitable when ``TFinalizeState = None``).
        Override and declare a concrete ``TFinalizeState`` subclass of
        ``ArrowSerializableDataclass`` to carry cursor / progress state
        between ticks.
        """
        return None

    @classmethod
    @abstractmethod
    def finalize(
        cls,
        params: TableBufferingParams[TArgs],
        finalize_state_id: bytes,
        state: TFinalizeState,
        out: OutputCollector,
    ) -> None:
        """Produce one batch's worth of output for ``finalize_state_id``.

        Called repeatedly by the framework (one call per tick). Each call
        should either:

          * ``out.emit(batch)`` to produce one output batch and mutate
            ``state`` in place — ``state`` is wire-serialized after the
            call so the next tick (possibly on a different worker)
            resumes from the updated value.
          * ``out.finish()`` to signal EOS for this ``finalize_state_id``.

        Mirrors ``TableFunctionGenerator.process`` exactly — the only
        difference is the parameterization by ``finalize_state_id``
        instead of free function arguments.
        """

    @classmethod
    def on_cancel(
        cls,
        params: TableBufferingParams[TArgs],
        finalize_state_id: bytes,
        state: TFinalizeState,
    ) -> None:
        """No-op default; runtime docstring set below via __func__.__doc__."""

    on_cancel.__func__.__doc__ = (  # type: ignore[attr-defined]
        f"""Release resources when a finalize stream is cancelled before EOS.

        Fired when DuckDB tears down a scan early (LIMIT clause, user
        break, exception unwind). Override to release expensive resources
        held in ``state`` (DB connections, large buffers, etc.).

{_ON_CANCEL_CAVEATS}
        """
    )
