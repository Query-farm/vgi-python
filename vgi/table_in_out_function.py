# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Framework for implementing streaming table-in-table-out functions.

[`TableInOutGenerator`][] processes input batches via a per-batch callback.
Each call to `process()` emits one output batch via `out.emit()`.

[`TableInOutFunction`][] provides a simpler callback API (transform/finish)
with automatic state serialization for distributed processing.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, final, get_args, get_origin

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector
from vgi_rpc.utils import empty_batch

from vgi.function_storage import BoundStorage, FrameworkNS
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
    "TableInOutGenerator",
    "TableInOutFunction",
    "TableInOutFunctionStateNoOp",
    "RowTransformFunction",
    "pack_int_cursor",
    "unpack_int_cursor",
]


# --- Cursor helpers for cursor-based finalize streams -----------------------
#
# The framework's BufferedFinalizeState carries an opaque ``cursor: bytes``
# wire-state field. The canonical encoding is the int64 of the last
# state_log id consumed; these helpers make that intent explicit at
# call sites without coupling user code to struct layout.


def pack_int_cursor(value: int) -> bytes:
    """Encode a signed int64 cursor (e.g., last log_id consumed).

    Args:
        value: The signed integer cursor to encode.

    Returns:
        The cursor as 8 little-endian bytes.

    """
    return value.to_bytes(8, "little", signed=True)


def unpack_int_cursor(cursor: bytes, default: int = -1) -> int:
    """Decode a packed int64 cursor; ``b""`` returns ``default``.

    Use ``default=-1`` (before-first sentinel) to start at the beginning
    of a state_log when no prior cursor exists.

    Args:
        cursor: The packed int64 cursor bytes (``b""`` for none).
        default: Value returned when ``cursor`` is empty.

    Returns:
        The decoded integer cursor, or ``default`` when empty.

    """
    if not cursor:
        return default
    return int.from_bytes(cursor, "little", signed=True)


class TableInOutGenerator[TArgs, TState = None](TableFunctionBase[TArgs]):
    """Base class for streaming table functions that transform Arrow RecordBatches.

    Each call to `process()` should emit exactly one output batch via `out.emit()`.
    Use `TState` to persist state between `process()` calls.

    For functions that need a finalize phase (e.g., aggregation), override
    `finalize()` to return the final output batches.

    Attributes:
        state_class: Concrete ``ArrowSerializableDataclass`` type subclasses set
            to opt into framework-managed state; None (the default) means
            process()/finalize() get ``state=None`` and the framework skips its
            round-trip.
        on_cancel.__func__.__doc__: Docstring assigned at class-definition time
            to the ``on_cancel`` cancellation hook (see ``on_cancel``).

    """

    # Subclasses opt into framework-managed state by setting this to a
    # concrete ArrowSerializableDataclass type. Default None means
    # process()/finalize() get state=None and the framework skips its
    # round-trip. TableInOutFunction's __init_subclass__ infers this from
    # the TState type parameter when a subclass declares one. Constrained
    # to ArrowSerializableDataclass so the framework can call
    # serialize_to_bytes / deserialize_from_bytes on instances without
    # further type narrowing at the call site.
    state_class: type[ArrowSerializableDataclass] | None = None

    @classmethod
    def has_finalize_override(cls) -> bool:
        """Whether this class's ``finalize``/``finish`` represents real work.

        Returns True iff either:

        - The class's ``Meta`` declares ``has_finalize`` as ``True`` or ``False``
          (explicit override — the declared value wins, even if it disagrees
          with the auto-detection).
        - Auto-detection finds a user subclass (one that is itself a
          `[`TableInOutGenerator`][]` subclass) strictly above the VGI bases in
          the MRO defining a callable ``finish`` or ``finalize`` attribute.

        The framework uses this to decide whether to advertise a finalize
        callback to DuckDB; DuckDB rejects LATERAL with correlated input on
        table functions that register ``in_out_function_final``.

        Returns:
            True if a real finalize/finish override is present.

        """
        # Explicit Meta override.
        meta = getattr(cls, "Meta", None)
        explicit = getattr(meta, "has_finalize", None) if meta is not None else None
        if explicit is not None:
            return bool(explicit)

        # Auto-detect.
        bases: set[type] = {TableInOutGenerator, TableInOutFunction}
        for klass in cls.__mro__:
            if klass in bases:
                return False
            # Only count overrides defined on an actual TableInOut subclass, so
            # an unrelated mixin with an identically-named attribute can't
            # trigger a false positive.
            if not (isinstance(klass, type) and issubclass(klass, TableInOutGenerator)):
                continue
            for attr_name in ("finish", "finalize"):
                raw = klass.__dict__.get(attr_name)
                if raw is None:
                    continue
                if isinstance(raw, (classmethod, staticmethod)):
                    raw = raw.__func__
                if callable(raw):
                    return True
        return False

    @classmethod
    def on_bind(
        cls,
        params: BindParams[TArgs],
    ) -> BindResponse:
        """Pass-through default — output schema is the input schema.

        Override to compute a dynamic output type or validate arguments.
        See ``TableFunctionBase.on_bind`` for the broader contract.

        Args:
            params: Bind parameters including arguments and the bind request.

        Returns:
            A BindResponse whose output schema equals the input schema.

        """
        assert params.bind_call.input_schema is not None
        return BindResponse(output_schema=params.bind_call.input_schema)

    # bind / on_init / global_init are defined on TableFunctionBase.

    @classmethod
    def initial_state(cls, params: ProcessParams[TArgs]) -> TState | None:
        """Create initial processing state. Override when `TState` is used.

        Called once during init to create the state object that will be
        passed to `process()` on each input batch.

        Args:
            params: Process parameters including arguments and schemas.

        Returns:
            Initial state, or None if no state is needed.

        """
        return None

    @classmethod
    def process(
        cls,
        params: ProcessParams[TArgs],
        state: TState,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        """Process one input batch.

        Called once per input batch during the INPUT phase. Must call
        `out.emit(batch)` exactly once to produce output.

        Use `out.client_log(level, message)` for in-band logging.

        Args:
            params: Process parameters including arguments and schemas.
            state: Mutable state persisted between calls. None if TState not used.
            batch: The input RecordBatch to process.
            out: `OutputCollector` for emitting output and logging.

        """
        out.emit(batch)

    @classmethod
    def finalize(cls, params: ProcessParams[TArgs]) -> list[pa.RecordBatch]:
        """Finalize processing and produce any remaining output.

        Called after all input batches have been processed during the
        FINALIZE phase. Override to emit buffered or aggregated results.

        Args:
            params: Process parameters including arguments and schemas.

        Returns:
            List of output RecordBatches, or empty list if no finalization needed.

        """
        return []

    @classmethod
    def on_cancel(cls, params: ProcessParams[TArgs], state: TState | None) -> None:  # noqa: D102
        pass

    on_cancel.__func__.__doc__ = (  # type: ignore[attr-defined]
        f"""Release resources when the stream is cancelled before natural end.

        The VGI C++ extension fires this hook when a DuckDB query tears
        down a VGI table-in-out scan early (LIMIT clause upstream, user
        break, Ctrl-C, exception unwind). Override to release expensive
        per-stream resources the function was holding in ``state``
        (database cursors, LLM streaming sessions, file handles, GPU
        buffers).

{_ON_CANCEL_CAVEATS}

        Args:
            params: Process parameters (same as ``process()`` received).
            state: The current user state; ``None`` when state is unused.
        """
    )


@dataclass(slots=True, frozen=True, kw_only=True)
class TableInOutFunctionStateNoOp(ArrowSerializableDataclass):
    """No-op state class for [`TableInOutFunction`][] when no state is needed."""


class TableInOutFunction[
    TArgs,
    TState: ArrowSerializableDataclass = TableInOutFunctionStateNoOp,
](TableInOutGenerator[TArgs, TState]):
    """Simplified base class using transform/finish callbacks.

    This class provides a simpler API for common use cases where you don't need
    to work directly with `OutputCollector`. Instead of implementing `process()`
    directly, you override `transform()` and optionally `finish()` as regular methods.

    `TState` is optional. If not provided, state management is disabled and
    `transform()` will always receive state=None. When `TState` is an
    `ArrowSerializableDataclass`, state is automatically saved to storage
    after each `transform()` call for distributed processing.

    Attributes:
        state_class: The ``TState`` dataclass type, inferred automatically from
            the generic type parameters; None disables state management.

    """

    state_class: type[TState] | None = None

    def __init_subclass__(cls, **kwargs: object) -> None:
        """Automatically infer the state_class from the generic type parameters.

        Args:
            **kwargs: Subclass keyword arguments forwarded to ``super()``.

        """
        super().__init_subclass__(**kwargs)

        # Iterate over the original bases to find the generic parameters
        orig_bases = getattr(cls, "__orig_bases__", ())
        for base in orig_bases:
            origin = get_origin(base)
            if origin is None:
                continue  # not a generic base
            args = get_args(base)
            if len(args) >= 2:
                # Assign the second type parameter to state_class
                cls.state_class = args[1]
                break

    @classmethod
    def transform(
        cls,
        batch: pa.RecordBatch,
        params: ProcessParams[TArgs],
        state: TState | None,
    ) -> pa.RecordBatch | list[pa.RecordBatch]:
        """Transform a single input batch.

        Override this method to implement your transformation logic. This is called
        once for each input batch.

        Args:
            batch: Input RecordBatch to transform.
            params: [`ProcessParams`][] containing arguments, schemas, and settings.
            state: Mutable state that should be updated and will be serialized as needed.

        Returns:
            Either:
            - A single pa.RecordBatch: The transformed output
            - A list of pa.RecordBatch: Multiple outputs (will be concatenated)

        """
        return batch

    @classmethod
    def finish(
        cls,
        params: ProcessParams[TArgs],
        states: list[TState],
    ) -> list[pa.RecordBatch]:
        """Return final batches after all input is processed.

        Override this method to emit results after all input batches have been
        processed. This is useful for aggregations, sorting, or any operation
        that needs to see all data before producing output.

        Args:
            params: The process parameters — function args, settings, secrets.
            states: The accumulated per-partition states from ``transform()``.

        Returns:
            List of pa.RecordBatch to emit as final output.
            Return an empty list if no finalization output is needed.

        """
        return []

    @classmethod
    def initial_state(
        cls,
        params: ProcessParams[TArgs],
    ) -> TState | None:
        """Create the initial state for processing.

        Override this method to initialize the state object before processing
        begins.

        Args:
            params: [`ProcessParams`][] containing arguments, schemas, and settings.

        Returns:
            An instance of `TState` representing the initial state.

        """
        return None

    @final
    @classmethod
    def process(
        cls,
        params: ProcessParams[TArgs],
        state: TState,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        """Process input batches by calling `transform()`. Do not override.

        This method implements the exchange protocol by calling your `transform()`
        method for each input batch. State is automatically saved to storage
        after each call for distributed processing.

        Args:
            params: Process parameters including arguments and schemas.
            state: Mutable state persisted between calls. None if TState unused.
            batch: The input RecordBatch to process.
            out: `OutputCollector` for emitting output and logging.

        """
        result = cls.transform(batch, params, state)

        # Save state for distributed processing (upsert semantics)
        if state is not None:
            params.storage.state_put(
                FrameworkNS.TIO_STATE, BoundStorage.pack_int_key(os.getpid()), state.serialize_to_bytes()
            )

        # Handle single batch or list of batches — exchange must emit exactly one
        if isinstance(result, list):
            if not result:
                out.emit(empty_batch(params.output_schema))
            elif len(result) == 1:
                out.emit(result[0])
            else:
                combined = pa.Table.from_batches(result).combine_chunks()
                out.emit(combined.to_batches()[0])
        else:
            out.emit(result)

    @final
    @classmethod
    def finalize(cls, params: ProcessParams[TArgs]) -> list[pa.RecordBatch]:
        """Emit final batches by calling `finish()`. Do not override.

        This method collects serialized states from all workers, deserializes
        them, and passes them to your `finish()` method.

        Args:
            params: Process parameters including arguments and schemas.

        Returns:
            List of output RecordBatches produced by ``finish()``.

        """
        if cls.state_class is not None and cls.state_class is not TableInOutFunctionStateNoOp:
            states = [
                cls.state_class.deserialize_from_bytes(v) for _k, v in params.storage.state_drain(FrameworkNS.TIO_STATE)
            ]
        else:
            states = []

        return cls.finish(params, states)


class RowTransformFunction[TArgs](TableInOutGenerator[TArgs, None]):
    r"""Blended ("UNNEST-style") table-in-out: positional args ARE per-row input columns.

    A ``RowTransformFunction`` collapses the classic either/or between a standard
    table function (literal args only) and a table-in-out function (an explicit
    ``TABLE`` subquery arg). Its **positional** ``Arg``\s declare its per-row input
    columns — real typed args, NO synthetic ``TABLE`` placeholder — so ONE
    registration serves every call shape::

        f(52, 13)                       -- literal   -> one input row
        FROM t, f(t.x, t.y)             -- columns    -> streaming input
        SELECT ... FROM t, LATERAL f(t.x, t.y)

    **Contract.**

    * Positional args are the input columns; they are read from ``batch`` in
      ``process()`` (by declared name for fixed args, positionally for varargs —
      use :meth:`input_columns`). They are NOT surfaced on ``params.args``.
    * Named (``str``-position) args stay bind-time scalars on ``params.args``.
    * Map-shaped, per-row: implement ``process()`` to emit output via
      ``out.emit()``. 1->1, 1->N, 1->0 all work. There is **no finalize** — a
      ``finalize()``/``finish()`` override is rejected at ``resolve_metadata``
      (DuckDB forbids ``FinalExecute`` under correlated LATERAL, one of the call
      shapes blended must serve). Accumulating functions use a classic
      ``TableInput`` table-in-out or a ``TableBufferingFunction``.
    * A positional ``const`` arg is rejected (in the column form DuckDB sweeps a
      constant into the input subquery; in the literal form it is
      indistinguishable from an input column). Use a named arg for optional
      config, or classic ``TableInput`` mode for a required constant.

    Subclassing ``RowTransformFunction`` (not a ``Meta`` flag) IS the blended
    signal — a per-arg or Meta flag could be forgotten on one of N same-named
    overloads; inheritance cannot. ``function_type`` stays ``TABLE``; the resolver
    sets ``ResolvedMetadata.input_from_args=True`` (see
    ``metadata._detect_input_from_args``).
    """

    @staticmethod
    def input_columns(batch: pa.RecordBatch) -> list[pa.Array]:  # type: ignore[type-arg]
        """This row-batch's input columns, positionally.

        For a **varargs** blended function the runtime column names are not known
        at declaration time, so read them positionally with this helper. For
        fixed-arity blended functions read by declared name (``batch.column(name)``).
        """
        return list(batch.columns)
