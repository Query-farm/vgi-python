"""Framework for implementing aggregate functions.

AggregateFunction provides a batch-oriented API for DuckDB aggregate functions
(e.g., ``SELECT my_agg(col) FROM t GROUP BY category``). The C++ side manages
trivial per-group state (just an int64 group_id), while Python holds the real
accumulation state in FunctionStorage.

Three phases:
- UPDATE: accumulate input rows into per-group state
- COMBINE: merge states from parallel workers
- FINALIZE: produce one result per group
"""

from __future__ import annotations

import contextlib
import inspect
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Final, TypeVar, final, get_args, get_origin

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import AuthContext

import vgi.function
from vgi.arguments import Arguments
from vgi.invocation import (
    BindResponse,
)
from vgi.schema_utils import schema
from vgi.table_function import (
    ProcessParams,
    SecretsAccessor,
)

__all__ = [
    "AggregateBindParams",
    "AggregateFunction",
    "GROUP_COLUMN_NAME",
    "WindowPartition",
]


@dataclass(slots=True, frozen=True, kw_only=True)
class AggregateBindParams:
    """Parameters passed to AggregateFunction.on_bind()."""

    args: Arguments | None
    input_schema: pa.Schema | None
    settings: dict[str, Any]
    secrets: SecretsAccessor
    auth_context: AuthContext = AuthContext.anonymous()


@dataclass(slots=True, frozen=True)
class WindowPartition:
    """Full partition data passed to a windowed aggregate callback.

    Constructed by the worker from the ``aggregate_window_init`` RPC payload
    and re-hydrated on every ``aggregate_window`` call via storage.

    Attributes:
        inputs: The partition's input RecordBatch (all input columns, all rows).
        row_count: Total number of rows in the partition.
        filter_mask: Boolean mask from an optional ``FILTER (WHERE ...)`` clause.
            Length equals ``row_count``.
        frame_stats: ``((begin_delta, end_delta), (begin_delta, end_delta))`` —
            DuckDB's per-partition frame statistics for planning.
        all_valid: Per-input-column validity flag (True if no nulls in column).

    """

    inputs: pa.RecordBatch
    row_count: int
    filter_mask: pa.BooleanArray
    frame_stats: tuple[tuple[int, int], tuple[int, int]]
    all_valid: list[bool]

    def filter(self, start: int, end: int) -> pa.RecordBatch:
        """Slice the partition inputs for rows ``[start, end)``."""
        return self.inputs.slice(start, end - start)


GROUP_COLUMN_NAME: Final[str] = "__vgi_group_id"
"""Reserved column name prepended by C++ to UPDATE exchange batches."""

TState = TypeVar("TState", bound=ArrowSerializableDataclass)


class AggregateFunction[TState: ArrowSerializableDataclass](vgi.function.Function):
    """Base class for aggregate functions.

    Aggregate functions accumulate input rows into per-group state during
    UPDATE, merge parallel worker states during COMBINE, and produce one
    result row per group during FINALIZE.

    Input columns are declared via ``Param`` annotations on ``update()``,
    and the output type via ``Returns`` annotation — the same pattern as
    ``ScalarFunction.compute()``.

    Type Parameters:
        TState: ``ArrowSerializableDataclass`` for per-group accumulation state.

    Example::

        class SumFunction(AggregateFunction[SumState]):
            class Meta:
                name = "vgi_sum"

            @classmethod
            def initial_state(cls, params):
                return SumState()

            @classmethod
            def update(
                cls,
                states: dict[int, SumState],
                group_ids: pa.Int64Array,
                value: Annotated[pa.Int64Array, Param(doc="Column to sum")],
            ) -> None:
                ...

            @classmethod
            def combine(cls, source, target, params):
                return SumState(total=source.total + target.total)

            @classmethod
            def finalize(
                cls,
                group_ids: pa.Int64Array,
                states: dict[int, SumState],
                params: ProcessParams,
            ) -> Annotated[pa.RecordBatch, Returns(pa.int64())]:
                ...

    """

    state_class: type[TState] | None = None
    _compute_params: dict[str, Any] = {}  # noqa: RUF012
    _const_params: dict[str, Any] = {}  # noqa: RUF012
    _setting_params: dict[str, str] = {}  # noqa: RUF012
    _secret_params: dict[str, Any] = {}  # noqa: RUF012
    _const_param_phases: dict[str, str] = {}  # noqa: RUF012
    _returns_output_type: pa.DataType | None = None

    def __init_subclass__(cls, **kwargs: object) -> None:
        """Extract state_class, Param annotations, and Returns type."""
        super().__init_subclass__(**kwargs)

        from typing import cast, get_type_hints

        from vgi.arguments import ARRAY_CLASS_TO_DATATYPE, Arg, ConstParam, Param, Returns
        from vgi.scalar_function import _const_param_to_arg, _param_to_arg

        # Skip abstract classes
        if inspect.isabstract(cls):
            return

        # Extract TState from generic type parameters
        orig_bases = getattr(cls, "__orig_bases__", ())
        for base in orig_bases:
            origin = get_origin(base)
            if origin is None:
                continue
            if not (isinstance(origin, type) and issubclass(origin, AggregateFunction)):
                continue
            type_args = get_args(base)
            if type_args:
                state_type = type_args[0]
                if not isinstance(state_type, TypeVar):
                    cls.state_class = state_type
            break

        # Parse Param and ConstParam annotations from update() method.
        # Single interleaved loop to get correct overall_position values.
        update_method = getattr(cls, "update", None)
        if update_method is None:
            return

        hints: dict[str, Any] = {}
        with contextlib.suppress(Exception):
            hints = get_type_hints(update_method, include_extras=True)

        compute_params: dict[str, Arg[Any]] = {}
        const_params: dict[str, Arg[Any]] = {}
        const_param_phases: dict[str, str] = {}
        overall_position = 0
        column_index = 0
        const_index = 0

        sig = inspect.signature(update_method)
        skip_params = {"self", "cls", "states", "group_ids", "params"}

        for name in sig.parameters:
            if name in skip_params:
                continue

            hint = hints.get(name)
            if hint is None:
                continue

            if hasattr(hint, "__metadata__"):
                for meta in hint.__metadata__:
                    if isinstance(meta, Param):
                        hint_args = get_args(hint)
                        base_type = hint_args[0] if hint_args else pa.Array
                        arg = _param_to_arg(meta, base_type, overall_position)
                        arg._name = name
                        arg._resolution_index = column_index
                        compute_params[name] = arg
                        overall_position += 1
                        column_index += 1
                        break
                    if isinstance(meta, ConstParam):
                        hint_args = get_args(hint)
                        base_type = cast(type, hint_args[0] if hint_args else Any)
                        arg = _const_param_to_arg(meta, base_type, overall_position)
                        arg._name = name
                        arg._resolution_index = const_index
                        const_params[name] = arg
                        const_param_phases[name] = getattr(meta, "phase", "all")
                        overall_position += 1
                        const_index += 1
                        break

        cls._compute_params = compute_params
        cls._const_params = const_params
        cls._const_param_phases = const_param_phases

        # Parse Returns annotation from finalize() return type
        finalize_method = getattr(cls, "finalize", None)
        returns_output_type: pa.DataType | None = None
        if finalize_method is not None:
            finalize_hints: dict[str, Any] = {}
            with contextlib.suppress(Exception):
                finalize_hints = get_type_hints(finalize_method, include_extras=True)
            return_hint = finalize_hints.get("return")
            if return_hint is not None and hasattr(return_hint, "__metadata__"):
                for meta in return_hint.__metadata__:
                    if isinstance(meta, Returns):
                        if meta.arrow_type is not None:
                            returns_output_type = meta.arrow_type
                        else:
                            ret_args = get_args(return_hint)
                            if ret_args and ret_args[0] in ARRAY_CLASS_TO_DATATYPE:
                                returns_output_type = ARRAY_CLASS_TO_DATATYPE[ret_args[0]]
                        break

        cls._returns_output_type = returns_output_type

        # Parse on_bind() signature for Setting/Secret annotations
        from vgi.table_function import _extract_setting_secret_params

        on_bind_method = getattr(cls, "on_bind", None)
        if on_bind_method is not None and "on_bind" in cls.__dict__:
            cls._setting_params, cls._secret_params = _extract_setting_secret_params(on_bind_method)
        else:
            cls._setting_params = getattr(cls, "_setting_params", {})
            cls._secret_params = getattr(cls, "_secret_params", {})

    @classmethod
    def on_bind(cls, params: AggregateBindParams, **kwargs: Any) -> BindResponse:
        """Override to provide output schema and optional bind-time logic.

        Must return a ``BindResponse`` with an ``output_schema`` containing
        exactly one field (the aggregate result column).
        """
        # Default: use Returns annotation if available
        if cls._returns_output_type is not None:
            return BindResponse(output_schema=schema(result=cls._returns_output_type))
        raise NotImplementedError(
            f"{cls.__name__} must either implement on_bind() or annotate finalize() with Returns(arrow_type=...)"
        )

    @final
    @classmethod
    def catalog_output_schema(cls) -> pa.Schema:
        """Return output schema for catalog introspection."""
        if cls._returns_output_type is not None:
            return schema(result=cls._returns_output_type)
        # Dynamic type (Returns() with no arrow_type) — mark as "any" for C++
        field = pa.field("result", pa.null(), metadata={b"vgi:any": b"true"})
        return pa.schema([field])

    @classmethod
    @abstractmethod
    def initial_state(cls, params: ProcessParams[Any]) -> TState:
        """Create the initial state for a new group.

        Called when a group_id is first encountered during UPDATE.
        Must return a valid ``TState`` instance representing the identity
        element (e.g., 0 for SUM, empty list for LISTAGG).
        """
        ...

    @classmethod
    @abstractmethod
    def update(cls, *args: Any, **kwargs: Any) -> None:
        """Accumulate input rows into per-group state.

        Declare input columns as ``Param``-annotated parameters::

            @classmethod
            def update(
                cls,
                states: dict[int, MyState],
                group_ids: pa.Int64Array,
                value: Annotated[pa.Int64Array, Param(doc="Column to sum")],
            ) -> None:
                ...

        The ``states`` dict is pre-populated with ``initial_state()`` for
        all new group_ids. ``group_ids`` is parallel to each column array.

        """
        ...

    @classmethod
    @abstractmethod
    def combine(
        cls,
        source: TState,
        target: TState,
        params: ProcessParams[Any],
    ) -> TState:
        """Merge two partial states from parallel workers.

        Returns the merged ``TState``. Framework replaces target and removes source.

        """
        ...

    @classmethod
    @abstractmethod
    def finalize(cls, *args: Any, **kwargs: Any) -> Any:
        """Produce results for the requested group_ids.

        Annotate the return type with ``Returns``::

            @classmethod
            def finalize(
                cls,
                group_ids: pa.Int64Array,
                states: dict[int, MyState],
                params: ProcessParams,
            ) -> Annotated[pa.RecordBatch, Returns(pa.int64())]:
                ...

        Must return a RecordBatch with one row per ``group_id``.

        """
        ...

    @classmethod
    def ensure_state(
        cls,
        states: dict[int, TState],
        group_id: int,
        params: ProcessParams[Any],
    ) -> TState:
        """Get or create state for a group_id.

        The framework pre-populates the states dict before calling ``update()``
        and ``finalize()``, so this helper should not normally be needed.
        Provided for defensive coding.

        Returns:
            The state for the given group_id.

        """
        if group_id not in states:
            states[group_id] = cls.initial_state(params)
        return states[group_id]

    # ------------------------------------------------------------------
    # Optional windowed-aggregate callbacks
    # ------------------------------------------------------------------
    # Enable by setting ``Meta.supports_window = True`` and overriding
    # ``window()`` (and optionally ``window_init()``).
    #
    # The C++ extension ships the full partition once per ``OVER`` partition
    # via ``aggregate_window_init``; the worker serialises it to
    # ``FunctionStorage`` keyed by ``(execution_id, partition_id)``. Each
    # subsequent ``aggregate_window`` RPC carries just ``(rid, subframes)``
    # and re-hydrates the partition from storage before calling ``window()``.
    # See ``plan`` for the per-call flushing rationale (DuckDB's window
    # callback has no per-Evaluate finalize hook).

    @classmethod
    def window_init(
        cls,
        partition: WindowPartition,
        params: ProcessParams[Any],
    ) -> Any:
        """Derive optional per-partition state from the raw partition.

        Called once per partition before any ``window()`` call. Return any
        ``ArrowSerializableDataclass`` (so it can round-trip through storage),
        or ``None`` if no derived state is required. The return value is
        passed back to ``window()`` as ``window_state``.

        Default implementation returns ``None``.
        """
        return None

    @classmethod
    def window(
        cls,
        rid: int,
        subframes: list[tuple[int, int]],
        partition: WindowPartition,
        window_state: Any,
        params: ProcessParams[Any],
    ) -> Any:
        """Compute the aggregate value for one output row.

        Args:
            rid: Partition-local row index being filled.
            subframes: Frame ranges ``[(begin, end), ...]`` — 1 for the default
                frame, 3 when ``EXCLUDE`` produces multiple subframes.
            partition: The cached partition data.
            window_state: Value returned by ``window_init()`` (may be ``None``).
            params: Shared ``ProcessParams``.

        Returns:
            A Python scalar or Arrow-compatible value; the worker wraps it
            into an IPC batch matching the function's output schema.

        """
        raise NotImplementedError(f"{cls.__name__}: Meta.supports_window=True requires overriding window()")
