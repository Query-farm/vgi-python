"""Base classes for table functions with cardinality hints and callback-based processing.

TableFunctionGenerator produces output batches via a per-tick callback. Each call
to process() either emits a batch via out.emit() or signals completion via out.finish().
"""

from __future__ import annotations

import uuid
from abc import abstractmethod
from dataclasses import dataclass, is_dataclass
from enum import Enum, auto
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    TypeVar,
    final,
    get_args,
    get_origin,
    get_type_hints,
)

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import AuthContext, CallContext, OutputCollector

import vgi.function
from vgi.arguments import Arg, Arguments, Secret, SecretLookupEntry, TableInput, _extract_setting_secret_params
from vgi.function_storage import BoundStorage
from vgi.invocation import (
    BaseInitResponse,
    BindResponse,
    GlobalInitResponse,
)

if TYPE_CHECKING:
    from vgi.catalog.catalog_interface import ColumnStatistics
    from vgi.protocol import BindRequest, InitRequest
    from vgi.table_filter_pushdown import PushdownFilters

_ON_CANCEL_CAVEATS = """\
        **Best-effort only.** This hook does not fire in every
        cancellation path — process kills, network partitions, and
        some error-on-error unwinds skip it. Never rely on
        ``on_cancel`` for correctness-critical cleanup; treat it as a
        resource-release optimization.

        Under HTTP pooling with ``max_workers > 1``, ``on_cancel`` may
        fire on a different worker process than the one that produced
        batches for this stream. Process-local resources held in a
        specific worker's memory cannot be reliably released from
        another worker's ``on_cancel``; prefer shared infrastructure
        whose handle is re-derivable from the serialized state."""

__all__ = [
    "TableCardinality",
    "BindParams",
    "InitParams",
    "ProcessParams",
    "SecretsAccessor",
    "TableFunctionBase",
    "TableFunctionGenerator",
    "TableInOutFunctionInitPhase",
    "init_single_worker",
    "bind_fixed_schema",
    "_struct_scalar_to_dict",
    "_extract_setting_secret_params",
]


@dataclass(frozen=True, slots=True)
class TableCardinality(ArrowSerializableDataclass):
    """Cardinality hints for query optimization.

    Provides optional row count estimates that can help query planners make
    better decisions about join ordering, memory allocation, and parallelization.

    Attributes:
        estimate: Estimated number of output rows, or None if unknown.
        max: Maximum possible output rows, or None if unbounded.

    """

    estimate: int | None
    max: int | None


def _batch_to_scalar_dict(batch: pa.RecordBatch | None) -> dict[str, pa.Scalar[Any]]:
    """Extract a single-row RecordBatch into a dict of column-name to scalar value."""
    if batch is None:
        return {}
    return {name: batch.column(i)[0] for i, name in enumerate(batch.schema.names)}


def _struct_scalar_to_dict(scalar: pa.StructScalar) -> dict[str, pa.Scalar[Any]]:
    """Expand a struct scalar into a dict of field name to scalar."""
    return {key: scalar[key] for key in scalar}


class SecretsAccessor:
    """Unified access to secrets — pre-resolved and dynamically requested.

    Pre-resolved secrets (from Secret() annotations with static scope/name, or
    unscoped lookups) are available immediately. Dynamic lookups (computed scope
    from function arguments) register pending requests — the framework
    automatically triggers a two-phase bind retry to resolve them.
    """

    __slots__ = ("_unscoped", "_scoped", "_is_retry", "_pending_lookups")

    def __init__(self, secrets_batch: pa.RecordBatch | None, *, is_retry: bool = False) -> None:
        """Initialize from a secrets RecordBatch."""
        self._is_retry = is_retry
        self._pending_lookups: list[SecretLookupEntry] = []

        # Parse unscoped secrets (columns named by secret_type)
        self._unscoped: dict[str, dict[str, pa.Scalar[Any]]] = {}
        # Parse scoped secrets (columns named "secret_N" with field metadata)
        self._scoped: list[tuple[dict[str, str], dict[str, pa.Scalar[Any]] | None]] = []

        if secrets_batch is not None:
            for i, name in enumerate(secrets_batch.schema.names):
                col_field = secrets_batch.schema.field(i)
                scalar = secrets_batch.column(i)[0]

                if name.startswith("secret_"):
                    # Scoped secret with metadata on the Arrow field
                    raw_meta = col_field.metadata or {}
                    entry_meta = {
                        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
                        for k, v in raw_meta.items()
                    }
                    if scalar.is_valid:
                        self._scoped.append((entry_meta, _struct_scalar_to_dict(scalar)))
                    else:
                        self._scoped.append((entry_meta, None))
                else:
                    # Unscoped secret (column name = secret_type)
                    if scalar.is_valid:
                        self._unscoped[name] = _struct_scalar_to_dict(scalar)

    def get(
        self,
        secret_type: str,
        *,
        name: str | None = None,
        scope: str | None = None,
        required: bool = False,
    ) -> dict[str, pa.Scalar[Any]] | None:
        """Get a secret by type, with optional name and/or scope.

        Args:
            secret_type: The secret type (e.g., "vgi_example", "s3").
            name: Optional secret name for name-based lookup.
            scope: Optional scope for scoped lookup (longest-prefix match).
            required: If True, raises ValueError when the secret is genuinely
                not found (after resolution).

        Returns:
            dict of string keys to Arrow scalars, or None if not found.

        """
        # Simple unscoped lookup (no dynamic scope/name)
        if not scope and not name:
            result = self._unscoped.get(secret_type)
            if result is not None:
                return result
            if self._is_retry:
                # Retry but still not found — genuinely missing
                if required:
                    raise ValueError(f"Required secret '{secret_type}' not found")
                return None
            # First call, not found — register pending lookup for two-phase bind
            self._pending_lookups.append(SecretLookupEntry(secret_type=secret_type))
            return None

        # Check resolved scoped secrets (from retry)
        if self._is_retry:
            result = self._find_scoped(secret_type, name, scope)
            if required and result is None:
                raise ValueError(f"Required secret '{secret_type}' not found (scope={scope!r}, name={name!r})")
            return result

        # First call, dynamic scope/name — register pending lookup
        self._pending_lookups.append(SecretLookupEntry(secret_type=secret_type, scope=scope, secret_name=name))
        return None

    @property
    def all_resolved(self) -> bool:
        """True if all requested secrets have been resolved (no pending lookups).

        Use this to distinguish 'not yet resolved' from 'genuinely not found'
        when not using required=True on get().
        """
        return len(self._pending_lookups) == 0

    @property
    def needs_resolution(self) -> bool:
        """True if there are pending lookups that need resolution."""
        return len(self._pending_lookups) > 0

    @property
    def pending_lookups(self) -> list[SecretLookupEntry]:
        """Return the list of pending secret lookups."""
        return list(self._pending_lookups)

    def to_dict(self) -> dict[str, dict[str, pa.Scalar[Any]]]:
        """Return all resolved secrets as a flat dict keyed by secret_type.

        Combines unscoped entries (column name = secret_type) with scoped
        entries (``secret_N`` columns, keyed by ``secret_type`` from Arrow
        field metadata).  Null/unresolved entries are omitted.
        """
        result = dict(self._unscoped)
        for meta, secret_dict in self._scoped:
            if secret_dict is not None:
                key = meta.get("secret_type", "")
                if key:
                    result[key] = secret_dict
        return result

    def _find_scoped(
        self,
        secret_type: str,
        name: str | None,
        scope: str | None,
    ) -> dict[str, pa.Scalar[Any]] | None:
        """Find a resolved scoped secret matching the given criteria."""
        for meta, secret_dict in self._scoped:
            if meta.get("secret_type") != secret_type:
                continue
            if scope is not None and meta.get("scope") != scope:
                continue
            if name is not None and meta.get("secret_name") != name:
                continue
            return secret_dict
        return None


def project_schema(projection_ids: list[int] | None, schema: pa.Schema) -> pa.Schema:
    """Create the projected schema if projection_ids are supplied."""
    if projection_ids is not None:
        return pa.schema([schema.field(proj_id) for proj_id in projection_ids])
    return schema


def _effective_projection_ids(func_cls: Any, projection_ids: list[int] | None) -> list[int] | None:
    """Return projection_ids only if the function supports projection pushdown."""
    if projection_ids is not None and func_cls.get_metadata().projection_pushdown:
        return projection_ids
    return None


class TableInOutFunctionInitPhase(Enum):
    """Indicate the phase of the init call for TableInOutFunction.

    There are two phases input and finalize.
    """

    INPUT = auto()
    FINALIZE = auto()


class OrderByDirection(Enum):
    """ORDER BY direction pushed down from DuckDB's RowGroupPruner optimizer."""

    ASC = auto()
    DESC = auto()


class OrderByNullOrder(Enum):
    """NULL ordering pushed down from DuckDB's RowGroupPruner optimizer."""

    NULLS_FIRST = auto()
    NULLS_LAST = auto()


@dataclass(slots=True, frozen=True, kw_only=True)
class BindParams[TArgs]:
    """Parameters passed to on_bind()."""

    args: TArgs
    bind_call: BindRequest
    # Convenient access to settings and secrets, extracted from the bind_call.
    settings: dict[str, pa.Scalar[Any]]
    secrets: SecretsAccessor
    auth_context: AuthContext = AuthContext.anonymous()


@dataclass(slots=True, frozen=True, kw_only=True)
class InitParams[TArgs]:
    """Parameters passed to on_init()."""

    args: TArgs
    init_call: InitRequest

    execution_id: bytes

    # This is the projected schema based on projection_ids,
    # which is what the function should produce.
    output_schema: pa.Schema

    # Convenient access to settings and secrets as dicts, extracted from the bind_call.
    settings: dict[str, pa.Scalar[Any]]
    secrets: dict[str, dict[str, pa.Scalar[Any]]]

    storage: BoundStorage
    auth_context: AuthContext = AuthContext.anonymous()


@dataclass(slots=True, frozen=True, kw_only=True)
class ProcessParams[TArgs]:
    """Parameters passed to process() and finalize()."""

    args: TArgs
    init_call: InitRequest | None  # None for aggregate functions
    init_response: BaseInitResponse | None  # None for aggregate functions

    # This is the projected schema based on projection_ids,
    # which is what the function should produce.
    output_schema: pa.Schema

    # Convenient access to settings and secrets as dicts, extracted from the bind_call.
    settings: dict[str, pa.Scalar[Any]]
    secrets: dict[str, dict[str, pa.Scalar[Any]]]

    storage: BoundStorage
    auth_context: AuthContext = AuthContext.anonymous()

    # Current pushdown filters (updated dynamically from tick metadata for Top-N queries).
    # None if no filters have been received. Updated before each process() call.
    current_pushdown_filters: Any = None  # PushdownFilters | None


class TableFunctionBase[TArgs](vgi.function.Function):
    """Base class for table functions with cardinality and schema validation.

    Extends Function with:
    - Cardinality hints for query optimization
    - Projection pushdown support

    This class is not meant to be used directly. Subclass either:
    - TableFunctionGenerator: For simple generators that produce output
    - TableInOutGenerator: For functions that transform input batches

    See Also:
        TableFunctionGenerator: Simple generator base class
        TableInOutGenerator: Full streaming with input batches

    """

    FunctionArguments: ClassVar[type]
    _setting_params: ClassVar[dict[str, str]]
    _secret_params: ClassVar[dict[str, Secret]]

    def __init_subclass__(cls) -> None:
        """Validate FunctionArguments, auto-extracting from generic parameter if needed."""
        super().__init_subclass__()

        # Auto-extract FunctionArguments from generic type parameter if not explicitly set.
        # e.g., class MyFunc(TableFunctionGenerator[MyArgs]) -> cls.FunctionArguments = MyArgs
        if not hasattr(cls, "FunctionArguments"):
            for base in cls.__dict__.get("__orig_bases__", ()):
                origin = get_origin(base)
                if origin is not None and issubclass(origin, TableFunctionBase):
                    type_args = get_args(base)
                    if type_args and not isinstance(type_args[0], TypeVar):
                        if type_args[0] is type(None):
                            # None means no arguments — create empty dataclass
                            from dataclasses import make_dataclass

                            cls.FunctionArguments = make_dataclass(f"_{cls.__name__}Args", [])
                        else:
                            cls.FunctionArguments = type_args[0]

                        # Validate TState (second type parameter) is serializable
                        if len(type_args) >= 2:
                            state_type = type_args[1]
                            if (
                                state_type is not None
                                and state_type is not type(None)
                                and not isinstance(state_type, TypeVar)
                                and isinstance(state_type, type)
                                and not issubclass(state_type, ArrowSerializableDataclass)
                            ):
                                raise TypeError(
                                    f"{cls.__name__}: TState type {state_type.__name__} must extend "
                                    f"ArrowSerializableDataclass for HTTP state serialization. "
                                    f"Use @dataclass(kw_only=True) and inherit from ArrowSerializableDataclass."
                                )
                        break

        # Skip validation for abstract base classes
        is_abstract = any(getattr(getattr(cls, name, None), "__isabstractmethod__", False) for name in dir(cls))
        if is_abstract:
            cls._setting_params = {}
            cls._secret_params = {}
            return

        # Skip intermediate base classes that still have unresolved type parameters
        if not hasattr(cls, "FunctionArguments"):
            has_unresolved = False
            for base in cls.__dict__.get("__orig_bases__", ()):
                type_args = get_args(base)
                if type_args and isinstance(type_args[0], TypeVar):
                    has_unresolved = True
                    break
            if has_unresolved:
                cls._setting_params = {}
                cls._secret_params = {}
                return

        if not hasattr(cls, "FunctionArguments"):
            # Provide a default empty FunctionArguments for classes that use
            # class-level Arg descriptors (e.g., TableInOutFunction subclasses
            # without type parameters). This preserves backward compatibility.
            from dataclasses import make_dataclass

            cls.FunctionArguments = make_dataclass(f"_{cls.__name__}Args", [])
        else:
            args_class = cls.FunctionArguments

            # Validate FunctionArguments is a dataclass
            if not is_dataclass(args_class):
                raise TypeError(
                    f"{cls.__name__}.FunctionArguments must be a dataclass. "
                    f"Add @dataclass decorator to {args_class.__name__}"
                )

            # Validate all fields are Annotated with Arg
            hints = get_type_hints(args_class, include_extras=True)
            for field_name, hint in hints.items():
                if get_origin(hint) is not Annotated:
                    raise TypeError(
                        f"{cls.__name__}.FunctionArguments.{field_name} must use Annotated[T, Arg(...)], got {hint}"
                    )

                # Check that Arg is in the metadata
                metadata = get_args(hint)[1:]
                has_arg = any(isinstance(meta, Arg) for meta in metadata)
                if not has_arg:
                    raise TypeError(
                        f"{cls.__name__}.FunctionArguments.{field_name} must have Arg(...) in Annotated metadata"
                    )

        # Parse on_bind() signature for Setting/Secret annotations
        on_bind_method = getattr(cls, "on_bind", None)
        if on_bind_method is not None and "on_bind" in cls.__dict__:
            cls._setting_params, cls._secret_params = _extract_setting_secret_params(on_bind_method)
        else:
            cls._setting_params = getattr(cls, "_setting_params", {})
            cls._secret_params = getattr(cls, "_secret_params", {})

    @final
    @staticmethod
    def _parse_arguments(args_class: type[TArgs], arguments: Arguments) -> TArgs:
        """Convert Arguments to typed FunctionArguments instance."""
        hints = get_type_hints(args_class, include_extras=True)
        kwargs: dict[str, Any] = {}

        for attr_name, hint in hints.items():
            if get_origin(hint) is not Annotated:
                continue
            # Check if this is a TableInput parameter (sentinel, no real data)
            base_type = get_args(hint)[0]
            if base_type is TableInput:
                kwargs[attr_name] = TableInput()
                continue
            for meta in get_args(hint)[1:]:
                if isinstance(meta, Arg):
                    if meta.varargs:
                        # Varargs: collect remaining positional args as raw pa.Scalar objects
                        assert isinstance(meta.position, int)
                        kwargs[attr_name] = tuple(arguments.positional[meta.position :])
                    else:
                        kwargs[attr_name] = arguments.get(meta.position, default=meta.default)
                    break

        return args_class(**kwargs)

    @final
    @staticmethod
    def _validate_arg_type_bounds(
        args_class: type,
        args: Any,
        input_schema: pa.Schema,
    ) -> None:
        """Validate type bounds for Arg parameters against the input schema.

        Walks the FunctionArguments type hints to find Arg instances with
        type_bound set. For each, gets the resolved column name from the
        args dataclass and validates the column's Arrow type against the bound.

        Args:
            args_class: The FunctionArguments class with Annotated type hints.
            args: The resolved FunctionArguments dataclass instance.
            input_schema: The input schema to validate column types against.

        """
        hints = get_type_hints(args_class, include_extras=True)
        for attr_name, hint in hints.items():
            if get_origin(hint) is not Annotated:
                continue
            for meta in get_args(hint)[1:]:
                if isinstance(meta, Arg) and meta.type_bound is not None:
                    value = getattr(args, attr_name)
                    if isinstance(value, tuple):
                        for col_name in value:
                            if isinstance(col_name, str):
                                meta.validate_type_bound(input_schema.field(col_name).type)
                    elif isinstance(value, str):
                        meta.validate_type_bound(input_schema.field(value).type)
                    break

    @classmethod
    def _extract_bind_kwargs(cls, input: BindRequest) -> dict[str, Any]:
        """Extract Setting/Secret kwargs from a BindRequest for on_bind().

        Returns dict of keyword arguments matching Setting/Secret annotations
        on the on_bind() method.
        """
        kwargs: dict[str, Any] = {}

        # Setting params: extract pa.Scalar from settings RecordBatch
        if input.settings is not None and cls._setting_params:
            settings_schema = input.settings.schema
            for name, setting_key in cls._setting_params.items():
                col_idx = settings_schema.get_field_index(setting_key)
                kwargs[name] = input.settings.column(col_idx)[0] if col_idx >= 0 else None

        # Secret params: extract dict[str, pa.Scalar] from secrets RecordBatch
        if input.secrets is not None and cls._secret_params:
            secrets_schema = input.secrets.schema
            for name, secret in cls._secret_params.items():
                col_idx = secrets_schema.get_field_index(secret.secret_type)
                kwargs[name] = _struct_scalar_to_dict(input.secrets.column(col_idx)[0]) if col_idx >= 0 else None

        return kwargs

    @final
    @classmethod
    def _make_bind_params(
        cls,
        input: BindRequest,
        *,
        auth_context: AuthContext | None = None,
    ) -> BindParams[TArgs]:
        """Construct BindParams from a BindRequest.

        Shared by bind() and table_function_cardinality() to avoid
        duplicating BindParams construction logic.
        """
        return BindParams[TArgs](
            args=cls._parse_arguments(cls.FunctionArguments, input.arguments),
            bind_call=input,
            settings=_batch_to_scalar_dict(input.settings),
            secrets=SecretsAccessor(input.secrets, is_retry=input.resolved_secrets_provided),
            auth_context=auth_context if auth_context is not None else AuthContext.anonymous(),
        )

    @classmethod
    def cardinality(cls, params: BindParams[TArgs]) -> TableCardinality:
        """Return the cardinality for the output.

        Override to provide row count estimates that help query planners
        make better decisions about join ordering and memory allocation.

        Returns:
            TableCardinality with estimate and/or max, or None if unknown.

        """
        return TableCardinality(estimate=None, max=None)

    @classmethod
    def statistics(cls, params: BindParams[TArgs]) -> list[ColumnStatistics] | None:
        """Return per-output-column statistics for this invocation.

        Override to provide min/max/distinct/null stats so DuckDB's optimizer can
        do filter elimination (e.g. prune a scan entirely when the filter is out
        of range), improve join ordering, and fold always-true/always-false
        predicates at plan time.

        ``params`` is the same ``BindParams[TArgs]`` used by ``cardinality`` and
        ``initial_state``, so stats can be derived directly from user-supplied
        arguments.

        Returns:
            A list of ColumnStatistics (one entry per column for which stats
            are known — columns not listed get unknown stats), or None when no
            stats are available (same effect as today: optimizer receives no
            column stats).

        """
        return None

    @staticmethod
    def pushdown_filters(
        pushdown_filters: pa.RecordBatch,
        join_keys: list[pa.RecordBatch] | None = None,
    ) -> PushdownFilters | None:
        """Get deserialized pushdown filters, or None if not present.

        Use this property to access the filter AST for:
        - Custom filter handling (push to SQL, APIs, etc.)
        - Extracting column bounds for partition pruning
        - Checking column constants for optimized lookups

        For automatic filtering, set auto_apply_filters=True in Meta.

        Args:
            pushdown_filters: Arrow RecordBatch containing serialized filters.
            join_keys: Optional list of single-column Arrow RecordBatches,
                one per IN filter column. Available via
                ``get_join_keys_batch()`` / ``get_join_keys_batches()``
                on the returned ``PushdownFilters``.

        Returns:
            PushdownFilters container with parsed filter AST, or None.

        """
        if pushdown_filters is None:
            return None
        from vgi.table_filter_pushdown import deserialize_filters

        return deserialize_filters(pushdown_filters, join_keys=join_keys)

    @classmethod
    def _should_auto_apply_filters(cls) -> bool:
        """Check if auto_apply_filters is enabled in Meta."""
        meta = getattr(cls, "Meta", None)
        return bool(getattr(meta, "auto_apply_filters", False))

    @staticmethod
    def _apply_pushdown_filter(batch: pa.RecordBatch, pushdown_filters: PushdownFilters | None) -> pa.RecordBatch:
        """Apply pushdown filters to a batch if present.

        Args:
            batch: RecordBatch to filter
            pushdown_filters: The PushdownFilters to apply or None.

        Returns:
            Filtered batch, or original if no filters or batch is None/empty.

        """
        if batch.num_rows == 0:
            return batch
        if pushdown_filters:
            result = pushdown_filters.apply(batch)
            return result
        return batch


class TableFunctionGenerator[TArgs, TState = None](TableFunctionBase[TArgs]):
    """Callback-based table function that produces output batches.

    Each call to process() should either:
    - Emit a batch via out.emit(batch)
    - Signal completion via out.finish()

    Use TState to persist state between process() calls.

    For functions that transform input batches, use TableInOutGenerator.

    """

    @classmethod
    @abstractmethod
    def on_bind(
        cls,
        params: BindParams[TArgs],
    ) -> BindResponse:
        """Produce the output schema and perform other bind time logic.

        Override to perform custom bind-time logic such as validating
        arguments or computing a dynamic output type.

        Subclasses may declare keyword-only parameters annotated with
        ``Setting()`` or ``Secret()`` to receive values automatically::

            @classmethod
            def on_bind(cls, params, *, my_setting: Annotated[pa.Scalar, Setting()] = None):
                ...

        Args:
            params: Bind parameters including arguments and schema.

        Returns:
            BindResponse with output_schema and optional opaque_data.

        """

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
        triggers dynamic secret lookups via SecretsAccessor, returns a
        secret scope request to trigger two-phase bind.

        Note: unlike ScalarFunction.bind(), we do NOT auto-request secrets
        before on_bind(). Table functions handle secrets via on_bind()
        kwargs (Secret() annotations) and SecretsAccessor.get() calls,
        which may use dynamic scopes computed from function arguments.

        """
        auth = ctx.auth if ctx is not None else AuthContext.anonymous()
        params = cls._make_bind_params(input, auth_context=auth)
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
            storage=BoundStorage(cls.storage, execution_id),
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
        passed to process() on each tick.

        Args:
            params: Process parameters including arguments and schemas.

        Returns:
            Initial state, or None if no state is needed.

        """
        return None

    @classmethod
    @abstractmethod
    def process(
        cls,
        params: ProcessParams[TArgs],
        state: TState,
        out: OutputCollector,
    ) -> None:
        """Produce output for one tick.

        Called repeatedly by the framework. Each call should either:
        - Call out.emit(batch) to produce one output batch
        - Call out.finish() to signal that generation is complete

        Use out.client_log(level, message) for in-band logging.

        Args:
            params: Process parameters including arguments and schemas.
            state: Mutable state persisted between calls. None if TState not used.
            out: OutputCollector for emitting batches, logging, and signaling finish.

        """

    @classmethod
    def on_cancel(cls, params: ProcessParams[TArgs], state: TState) -> None:  # noqa: D102
        pass

    on_cancel.__func__.__doc__ = (  # type: ignore[attr-defined]
        f"""Release resources when the stream is cancelled before natural end.

        The VGI C++ extension fires this hook when a DuckDB query tears
        down a VGI scan early (LIMIT clause, user break, Ctrl-C,
        exception unwind). Override to release expensive per-stream
        resources the function was holding in ``state`` (database
        cursors, LLM streaming sessions, file handles, GPU buffers).

{_ON_CANCEL_CAVEATS}

        The stream has already been torn down by the time this fires;
        no further batches may be emitted.

        Args:
            params: Process parameters (same as ``process()`` received).
            state: The current user state, possibly deserialized from a
                state-token on a different worker than the one that
                originally built it.
        """
    )


def init_single_worker[T: TableFunctionGenerator[Any, Any]](cls: type[T]) -> type[T]:
    """Class decorator to set max_workers=1 for a TableFunctionGenerator subclass."""
    if "on_init" not in cls.__dict__:

        def on_init_impl(cls_: type[T], params: Any) -> GlobalInitResponse:
            return GlobalInitResponse(max_workers=1)

        cls.on_init = classmethod(on_init_impl)  # type: ignore[assignment]

        # Clear 'on_init' from __abstractmethods__ — the metaclass set it
        # before decorators ran, so we must update it manually.
        if hasattr(cls, "__abstractmethods__") and "on_init" in cls.__abstractmethods__:
            cls.__abstractmethods__ = cls.__abstractmethods__ - {"on_init"}

    return cls


def bind_fixed_schema[T: TableFunctionGenerator[Any, Any]](cls: type[T]) -> type[T]:
    """Class decorator to return FIXED_SCHEMA from on_bind for a TableFunctionGenerator subclass."""
    if "on_bind" not in cls.__dict__:  # only inject if subclass hasn't overridden
        if not hasattr(cls, "FIXED_SCHEMA"):
            raise ValueError(f"Class {cls.__name__} must define FIXED_SCHEMA to use @bind_fixed_schema")

        def on_bind_impl(cls_: type[T], params: Any) -> BindResponse:
            value = getattr(cls_, "FIXED_SCHEMA", None)

            if value is None or not isinstance(value, pa.Schema):
                raise TypeError(f"Class {cls_.__name__}.FIXED_SCHEMA must be a pyarrow.Schema")
            return BindResponse(output_schema=value)

        # assign as classmethod
        cls.on_bind = classmethod(on_bind_impl)  # type: ignore[assignment]

        # Clear 'on_bind' from __abstractmethods__ — the metaclass set it
        # before decorators ran, so we must update it manually.
        if hasattr(cls, "__abstractmethods__") and "on_bind" in cls.__abstractmethods__:
            cls.__abstractmethods__ = cls.__abstractmethods__ - {"on_bind"}

    return cls
