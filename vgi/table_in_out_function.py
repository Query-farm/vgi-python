"""Framework for implementing streaming table-in-table-out functions.

TableInOutGenerator processes input batches via a per-batch callback.
Each call to process() emits one output batch via out.emit().

TableInOutFunction provides a simpler callback API (transform/finish)
with automatic state serialization for distributed processing.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, final, get_args, get_origin

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import AuthContext, CallContext, OutputCollector
from vgi_rpc.utils import empty_batch

from vgi.function_storage import BoundStorage
from vgi.invocation import (
    BindResponse,
    GlobalInitResponse,
)
from vgi.table_function import (
    _ON_CANCEL_CAVEATS,
    BindParams,
    InitParams,
    ProcessParams,
    SecretsAccessor,
    TableFunctionBase,
    _batch_to_scalar_dict,
    _effective_projection_ids,
    project_schema,
)

if TYPE_CHECKING:
    from vgi.protocol import BindRequest, InitRequest

__all__ = [
    "TableInOutGenerator",
    "TableInOutFunction",
    "TableInOutFunctionStateNoOp",
]


class TableInOutGenerator[TArgs, TState = None](TableFunctionBase[TArgs]):
    """Base class for streaming table functions that transform Arrow RecordBatches.

    Each call to process() should emit exactly one output batch via out.emit().
    Use TState to persist state between process() calls.

    For functions that need a finalize phase (e.g., aggregation), override
    finalize() to return the final output batches.

    """

    @classmethod
    def has_finalize_override(cls) -> bool:
        """Whether this class's ``finalize``/``finish`` represents real work.

        Returns True iff either:

        - The class's ``Meta`` declares ``has_finalize`` as ``True`` or ``False``
          (explicit override — the declared value wins, even if it disagrees
          with the auto-detection).
        - Auto-detection finds a user subclass (one that is itself a
          ``TableInOutGenerator`` subclass) strictly above the VGI bases in
          the MRO defining a callable ``finish`` or ``finalize`` attribute.

        The framework uses this to decide whether to advertise a finalize
        callback to DuckDB; DuckDB rejects LATERAL with correlated input on
        table functions that register ``in_out_function_final``.
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
        """Produce the output schema and perform other bind time logic.

        Override to perform custom bind-time logic such as validating
        arguments or computing a dynamic output type.

        Subclasses may declare keyword-only parameters annotated with
        ``Setting()`` or ``Secret()`` to receive values automatically.

        Args:
            params: Bind parameters including arguments and schema.

        Returns:
            BindResponse with output_schema and optional opaque_data.

        """
        assert params.bind_call.input_schema is not None
        return BindResponse(output_schema=params.bind_call.input_schema)

    @final
    @classmethod
    def bind(
        cls,
        input: BindRequest,
        *,
        ctx: CallContext | None = None,
    ) -> BindResponse:
        """Bind protocol entry point. Do not override; use on_bind() instead.

        Validates type bounds, constructs BindParameters, calls on_bind(),
        and wraps the result for transmission to global_init. If on_bind()
        triggers dynamic secret lookups, returns a secret scope request.

        """
        auth = ctx.auth if ctx is not None else AuthContext.anonymous()
        params = cls._make_bind_params(input, auth_context=auth)

        if input.input_schema is not None:
            cls._validate_arg_type_bounds(cls.FunctionArguments, params.args, input.input_schema)

        result = cls.on_bind(params, **cls._extract_bind_kwargs(input))

        # Check if on_bind() registered pending secret lookups
        if params.secrets.needs_resolution:
            return BindResponse.secret_scope_request(params.secrets.pending_lookups)

        return result

    @classmethod
    def on_init(
        cls,
        params: InitParams[TArgs],
    ) -> GlobalInitResponse:
        """Initialize the function during the init API call.

        Override to perform one-time setup that should happen after bind
        but before processing batches.

        Args:
            params: Init parameters including arguments, schemas, and opaque data from
                bind.

        Returns:
            GlobalInitResponse

        """
        return GlobalInitResponse()

    @final
    @classmethod
    def global_init(cls, input: InitRequest, *, ctx: CallContext | None = None) -> GlobalInitResponse:
        """Global init protocol entry point. Do not override; use on_init() instead.

        Deserializes the wrapped bind data, calls on_init(), and
        wraps the result for transmission to process().

        """
        execution_id = uuid.uuid4().bytes
        auth = ctx.auth if ctx is not None else AuthContext.anonymous()
        params = InitParams[TArgs](
            args=cls._parse_arguments(cls.FunctionArguments, input.bind_call.arguments),
            init_call=input,
            output_schema=project_schema(_effective_projection_ids(cls, input.projection_ids), input.output_schema),
            settings=_batch_to_scalar_dict(input.bind_call.settings),
            secrets=SecretsAccessor(input.bind_call.secrets).to_dict(),
            execution_id=execution_id,
            storage=BoundStorage(cls.storage, execution_id, request=input, auth=auth),
            auth_context=auth,
        )

        result = cls.on_init(params)

        return GlobalInitResponse(
            max_workers=result.max_workers,
            execution_id=execution_id,
            opaque_data=result.opaque_data,
        )

    @classmethod
    def initial_state(cls, params: ProcessParams[TArgs]) -> TState | None:
        """Create initial processing state. Override when TState is used.

        Called once during init to create the state object that will be
        passed to process() on each input batch.

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
        out.emit(batch) exactly once to produce output.

        Use out.client_log(level, message) for in-band logging.

        Args:
            params: Process parameters including arguments and schemas.
            state: Mutable state persisted between calls. None if TState not used.
            batch: The input RecordBatch to process.
            out: OutputCollector for emitting output and logging.

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
    """No-op state class for TableInOutFunction when no state is needed."""


class TableInOutFunction[
    TArgs,
    TState: ArrowSerializableDataclass = TableInOutFunctionStateNoOp,
](TableInOutGenerator[TArgs, TState]):
    """Simplified base class using transform/finish callbacks.

    This class provides a simpler API for common use cases where you don't need
    to work directly with OutputCollector. Instead of implementing process()
    directly, you override transform() and optionally finish() as regular methods.

    TState is optional. If not provided, state management is disabled and
    transform() will always receive state=None. When TState is an
    ArrowSerializableDataclass, state is automatically saved to storage
    after each transform() call for distributed processing.

    """

    state_class: type[TState] | None = None

    def __init_subclass__(cls, **kwargs: object) -> None:
        """Automatically infer the state_class from the generic type parameters."""
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
            params: ProcessParams containing arguments, schemas, and settings.
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
            params: ProcessParams containing arguments, schemas, and settings.

        Returns:
            An instance of TState representing the initial state.

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
        """Process input batches by calling transform(). Do not override.

        This method implements the exchange protocol by calling your transform()
        method for each input batch. State is automatically saved to storage
        after each call for distributed processing.

        """
        result = cls.transform(batch, params, state)

        # Save state for distributed processing (upsert semantics)
        if state is not None:
            params.storage.state_put(
                b"sd", BoundStorage.pack_int_key(os.getpid()), state.serialize_to_bytes()
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
        """Emit final batches by calling finish(). Do not override.

        This method collects serialized states from all workers, deserializes
        them, and passes them to your finish() method.

        """
        if cls.state_class is not None and cls.state_class is not TableInOutFunctionStateNoOp:
            states = [
                cls.state_class.deserialize_from_bytes(v)
                for _k, v in params.storage.state_drain(b"sd")
            ]
        else:
            states = []

        return cls.finish(params, states)
