# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Base classes for table functions with cardinality hints and callback-based processing.

[`TableFunctionGenerator`][] produces output batches via a per-tick callback. Each call
to `process()` either emits a batch via `out.emit()` or signals completion via `out.finish()`.
"""

from __future__ import annotations

import uuid
from abc import abstractmethod
from collections.abc import Mapping
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
from vgi.arguments import (
    Arg,
    Arguments,
    Secret,
    SecretLookupEntry,
    TableInput,
    _accepts_none,
    _extract_setting_secret_params,
    _scalar_to_py,
)
from vgi.function_storage import BoundStorage, TransactionBoundStorage, attach_catalog_bytes
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
    """Extract a single-row RecordBatch into a dict of column-name to scalar value.

    Args:
        batch: A single-row RecordBatch, or None.

    Returns:
        Mapping of column name to its scalar value (empty when batch is None).

    """
    if batch is None:
        return {}
    return {name: batch.column(i)[0] for i, name in enumerate(batch.schema.names)}


def _struct_scalar_to_dict(scalar: pa.StructScalar) -> dict[str, pa.Scalar[Any]]:
    """Expand a struct scalar into a dict of field name to scalar.

    Args:
        scalar: The struct scalar to expand.

    Returns:
        Mapping of field name to its scalar value.

    """
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
        """Initialize from a secrets RecordBatch.

        Args:
            secrets_batch: Single-row RecordBatch of resolved secrets, or None.
            is_retry: True when invoked on a two-phase bind retry, so unresolved
                dynamic lookups are treated as genuinely missing.

        """
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

    def to_dict(self) -> ResolvedSecrets:
        """Return all resolved secrets keyed by secret name.

        Resolved secrets are keyed by their unique DuckDB secret name, so several
        secrets of the same type (e.g. one per S3 bucket) coexist. Each carries a
        ``type`` field (the DuckDB secret type) and a ``scope`` field
        (newline-joined scope prefixes). Scoped ``secret_N`` columns (keyed by
        ``secret_type`` from Arrow field metadata) are merged in. Null/unresolved
        entries are omitted.

        Returns:
            A :class:`ResolvedSecrets` (a dict keyed by secret name) with
            type- and scope-aware selection helpers.

        """
        result = dict(self._unscoped)
        for meta, secret_dict in self._scoped:
            if secret_dict is not None:
                key = meta.get("secret_name") or meta.get("secret_type", "")
                if key:
                    result[key] = secret_dict
        return ResolvedSecrets(result)

    def _find_scoped(
        self,
        secret_type: str,
        name: str | None,
        scope: str | None,
    ) -> dict[str, pa.Scalar[Any]] | None:
        """Find a resolved scoped secret matching the given criteria.

        Args:
            secret_type: The secret type to match.
            name: Optional secret name to match.
            scope: Optional scope to match.

        Returns:
            The matching secret's key/scalar dict, or None if not found.

        """
        for meta, secret_dict in self._scoped:
            if meta.get("secret_type") != secret_type:
                continue
            if scope is not None and meta.get("scope") != scope:
                continue
            if name is not None and meta.get("secret_name") != name:
                continue
            return secret_dict
        return None


def _secret_scalar_str(v: Any) -> str:
    """Render a resolved-secret field (a pyarrow Scalar or plain value) to str."""
    if v is None:
        return ""
    py = v.as_py() if hasattr(v, "as_py") else v
    return "" if py is None else str(py)


class ResolvedSecrets(dict[str, dict[str, Any]]):
    """Resolved secrets keyed by secret name, with type- and scope-aware lookup.

    A plain ``dict`` (so ``secrets[name]`` and ``secrets.get(name)`` still work)
    plus selectors that read each secret's connector-serialized ``type`` and
    ``scope`` fields. Mirrors ``vgi::Secrets`` in the Rust SDK.
    """

    def secret_type(self, name: str) -> str | None:
        """The DuckDB secret type of the named secret (its ``type`` field)."""
        fields = self.get(name)
        if not fields or "type" not in fields:
            return None
        return _secret_scalar_str(fields["type"])

    def of_type(self, secret_type: str) -> list[dict[str, Any]]:
        """Every resolved secret whose ``type`` field matches ``secret_type``."""
        return [f for f in self.values() if _secret_scalar_str(f.get("type")) == secret_type]

    def for_scope(self, path: str) -> dict[str, Any] | None:
        """The secret whose ``scope`` is the longest prefix of ``path``.

        The connector serializes each secret's scope as a newline-joined list of
        prefixes; a secret with no (or empty) scope matches as a last-resort
        fallback. Returns ``None`` only when there are no candidate secrets.
        """
        return self._select_for_scope(path, None)

    def for_scope_of_type(self, path: str, secret_type: str) -> dict[str, Any] | None:
        """Like :meth:`for_scope` but only over secrets of ``secret_type``."""
        return self._select_for_scope(path, secret_type)

    def field_for(self, path: str, field: str) -> Any | None:
        """A field of the best scope-matching secret for ``path``."""
        fields = self.for_scope(path)
        return None if fields is None else fields.get(field)

    def _select_for_scope(self, path: str, secret_type: str | None) -> dict[str, Any] | None:
        best: dict[str, Any] | None = None
        best_len = -1
        fallback: dict[str, Any] | None = None
        for fields in self.values():
            if secret_type is not None and _secret_scalar_str(fields.get("type")) != secret_type:
                continue
            scope = _secret_scalar_str(fields.get("scope"))
            if not scope:
                if fallback is None:
                    fallback = fields
                continue
            for prefix in scope.split("\n"):
                if prefix and path.startswith(prefix) and len(prefix) > best_len:
                    best_len = len(prefix)
                    best = fields
        return best if best is not None else fallback


def project_schema(projection_ids: list[int] | None, schema: pa.Schema) -> pa.Schema:
    """Create the projected schema if projection_ids are supplied.

    Args:
        projection_ids: Column indices to project, or None for all columns.
        schema: The full output schema to project from.

    Returns:
        The projected schema, or the original schema when projection_ids is None.

    """
    if projection_ids is not None:
        return pa.schema([schema.field(proj_id) for proj_id in projection_ids])
    return schema


def _effective_projection_ids(func_cls: Any, projection_ids: list[int] | None) -> list[int] | None:
    """Return projection_ids only if the function supports projection pushdown.

    Args:
        func_cls: The function class whose metadata is inspected.
        projection_ids: The requested projection column indices, or None.

    Returns:
        The projection_ids if the function supports projection pushdown, else None.

    """
    if projection_ids is not None and func_cls.get_metadata().projection_pushdown:
        return projection_ids
    return None


class TableInOutFunctionInitPhase(Enum):
    """Init-call phase for table functions.

    ``INPUT`` / ``FINALIZE`` drive the streaming [`TableInOutGenerator`][] path.
    ``TABLE_BUFFERING`` is the Sink+Source init phase for
    ``TableBufferingFunction`` — after init, traffic moves to
    ``table_buffering_process`` / ``_combine`` (unary) and
    ``TABLE_BUFFERING_FINALIZE`` opens a producer-mode finalize stream
    per finalize_state_id.

    Attributes:
        INPUT: Streaming input phase for the table-in-out generator path.
        FINALIZE: End-of-input finalize phase for the streaming path.
        TABLE_BUFFERING: Sink+Source init phase for ``TableBufferingFunction``.
        TABLE_BUFFERING_FINALIZE: Producer-mode finalize stream phase, opened
            per finalize_state_id.

    """

    INPUT = auto()
    FINALIZE = auto()
    TABLE_BUFFERING = auto()
    TABLE_BUFFERING_FINALIZE = auto()


class OrderByDirection(Enum):
    """ORDER BY direction pushed down from DuckDB's RowGroupPruner optimizer.

    Attributes:
        ASC: Ascending order.
        DESC: Descending order.

    """

    ASC = auto()
    DESC = auto()


class OrderByNullOrder(Enum):
    """NULL ordering pushed down from DuckDB's RowGroupPruner optimizer.

    Attributes:
        NULLS_FIRST: Nulls sort before non-null values.
        NULLS_LAST: Nulls sort after non-null values.

    """

    NULLS_FIRST = auto()
    NULLS_LAST = auto()


@dataclass(slots=True, frozen=True, kw_only=True)
class BindParams[TArgs]:
    """Parameters passed to `on_bind()`.

    Attributes:
        args: The parsed function arguments.
        bind_call: The underlying bind request from the client.
        settings: DuckDB settings extracted from the bind_call, keyed by name.
        secrets: Accessor for pre-resolved and dynamically-requested secrets.
        transaction_storage: Transaction-scoped storage view that lets
            ``cardinality()`` / ``statistics()`` cache expensive lookups (e.g.
            Kafka watermarks) in the same store ``on_init`` reads/writes for
            snapshot isolation. None when ``bind_call.transaction_opaque_data``
            is unset.
        storage: Execution-scoped storage view, populated only on call paths
            that carry a ``global_execution_id`` (currently ``dynamic_to_string``).
            None for bind/cardinality/statistics (they predate execution).
        auth_context: Authentication context for the caller.
        attach_opaque_data: The catalog's attach bytes, unwrapped by the
            framework (shard-UUID prefix stripped). None without an ATTACH.

    """

    args: TArgs
    bind_call: BindRequest
    settings: dict[str, pa.Scalar[Any]]
    secrets: SecretsAccessor
    transaction_storage: TransactionBoundStorage | None = None
    storage: BoundStorage | None = None
    auth_context: AuthContext = AuthContext.anonymous()
    attach_opaque_data: bytes | None = None

    @property
    def at_unit(self) -> str | None:
        """The AT (TIMESTAMP|VERSION) unit for this scan, or None without an AT clause.

        NOTE: for inline-bound (function-backed) tables on_bind runs once
        at attach with no AT, so this is None here — read AT at init/process via
        ``ProcessParams.at_value``. See ``BindRequest.at_unit``.
        """
        return self.bind_call.at_unit

    @property
    def at_value(self) -> str | None:
        """The AT (TIMESTAMP|VERSION) value for this scan, or None. See ``at_unit``."""
        return self.bind_call.at_value


@dataclass(slots=True, frozen=True, kw_only=True)
class InitParams[TArgs]:
    """Parameters passed to `on_init()`.

    Attributes:
        args: The parsed function arguments.
        init_call: The underlying init request from the client.
        execution_id: Unique identifier for this execution.
        output_schema: The projected output schema (based on projection_ids)
            that the function should produce.
        settings: DuckDB settings extracted from the bind_call, keyed by name.
        secrets: Resolved secrets as dicts keyed by secret_type.
        storage: Execution-scoped storage view for this init.
        auth_context: Authentication context for the caller.
        attach_opaque_data: The catalog's attach bytes, unwrapped by the
            framework (uuid prefix stripped). None without an ATTACH.

    """

    args: TArgs
    init_call: InitRequest

    execution_id: bytes

    output_schema: pa.Schema

    settings: dict[str, pa.Scalar[Any]]
    secrets: ResolvedSecrets

    storage: BoundStorage
    auth_context: AuthContext = AuthContext.anonymous()
    attach_opaque_data: bytes | None = None

    @property
    def at_unit(self) -> str | None:
        """AT (TIMESTAMP|VERSION) unit for this scan, or None.

        Carried on the per-scan bind embedded in the init request.
        See ``BindRequest.at_unit``.
        """
        return self.init_call.bind_call.at_unit

    @property
    def at_value(self) -> str | None:
        """AT (TIMESTAMP|VERSION) value for this scan, or None. See ``at_unit``."""
        return self.init_call.bind_call.at_value


@dataclass(slots=True, frozen=True, kw_only=True)
class ProcessParams[TArgs]:
    """Parameters passed to `process()` and `finalize()`.

    Attributes:
        args: The parsed function arguments.
        init_call: The init request, or None for aggregate functions.
        init_response: The init response, or None for aggregate functions.
        output_schema: The projected output schema (based on projection_ids)
            that the function should produce.
        settings: DuckDB settings extracted from the bind_call, keyed by name.
        secrets: Resolved secrets as dicts keyed by secret_type.
        storage: Execution-scoped storage view.
        auth_context: Authentication context for the caller.
        current_pushdown_filters: Current pushdown filters
            (``PushdownFilters | None``), updated dynamically from tick metadata
            (e.g. for Top-N queries) before each process() call. None if no
            filters have been received.
        batch_index: Globally-unique monotonic batch index for this process()
            call. Populated only for ``TableBufferingFunction`` subclasses with
            ``Meta.requires_input_batch_index=True``, letting workers reconstruct
            source order under parallel ingest. None for every other call path.
        attach_opaque_data: The catalog's attach bytes, unwrapped by the
            framework (uuid prefix stripped). None without an ATTACH.
        if_none_match: Conditional-revalidation validator (client's stored
            ETag). Set when the client holds a stale-but-revalidatable cached
            result and asks the worker to confirm freshness cheaply; a worker
            that advertised ``revalidatable`` compares it and, if unchanged,
            emits a 0-row ``CacheControl(not_modified=True)`` batch. None
            otherwise.
        if_modified_since: Conditional-revalidation validator (client's stored
            Last-Modified). Companion to ``if_none_match``. None otherwise.

    """

    args: TArgs
    init_call: InitRequest | None
    init_response: BaseInitResponse | None

    output_schema: pa.Schema

    settings: dict[str, pa.Scalar[Any]]
    secrets: ResolvedSecrets

    storage: BoundStorage
    auth_context: AuthContext = AuthContext.anonymous()

    current_pushdown_filters: Any = None

    batch_index: int | None = None

    attach_opaque_data: bytes | None = None

    # Conditional-revalidation validators (client cache -> worker). Set when the
    # client holds a stale-but-revalidatable cached result and asks the worker to
    # confirm freshness cheaply. A worker that advertised ``revalidatable`` reads
    # these; if its data is unchanged it emits a 0-row ``CacheControl(not_modified
    # =True, ...)`` batch instead of re-streaming. Both None on a normal call.
    if_none_match: str | None = None
    if_modified_since: str | None = None

    @property
    def at_unit(self) -> str | None:
        """AT (TIMESTAMP|VERSION) unit for this scan, or None.

        Carried on the per-scan bind embedded in the init request; None for
        aggregate functions (no init_call). See ``BindRequest.at_unit``.
        """
        return self.init_call.bind_call.at_unit if self.init_call is not None else None

    @property
    def at_value(self) -> str | None:
        """AT (TIMESTAMP|VERSION) value for this scan, or None. See ``at_unit``."""
        return self.init_call.bind_call.at_value if self.init_call is not None else None

    @property
    def substream_id(self) -> bytes | None:
        """Stable client-minted id for this streaming table-in-out substream.

        Present (identical across init / every process() / finalize) when the
        client fanned this function out across per-substream workers; use it to
        key per-substream accumulated state in shared storage so a finalize()
        that lands on a different HTTP backend than the process() calls still
        finds it. ``None`` for the serial path, aggregate functions (no
        ``init_call``), or an old client that did not supply one. See
        ``InitRequest.substream_id``.
        """
        return self.init_call.substream_id if self.init_call is not None else None


class TableFunctionBase[TArgs](vgi.function.Function):
    """Base class for table functions with cardinality and schema validation.

    Extends `Function` with:
    - Cardinality hints for query optimization
    - Projection pushdown support

    This class is not meant to be used directly. Subclass either:
    - [`TableFunctionGenerator`][]: For simple generators that produce output
    - [`TableInOutGenerator`][]: For functions that transform input batches

    See Also:
        `TableFunctionGenerator`: Simple generator base class
        `TableInOutGenerator`: Full streaming with input batches

    Attributes:
        FunctionArguments: The dataclass type describing this function's
            arguments, auto-extracted from the generic parameter if not set.

    """

    FunctionArguments: ClassVar[type]
    _setting_params: ClassVar[dict[str, str]]
    _secret_params: ClassVar[dict[str, Secret]]

    def __init_subclass__(cls) -> None:
        """Validate FunctionArguments, auto-extracting from generic parameter if needed."""
        super().__init_subclass__()

        # Validate TState (second generic type parameter) is serializable.
        #
        # This runs unconditionally — independently of the FunctionArguments
        # auto-extraction below. The check used to be nested inside the
        # ``not hasattr(cls, "FunctionArguments")`` branch, so any class that
        # set ``FunctionArguments`` explicitly in its body silently skipped
        # TState validation. That let non-serializable state slip through: it
        # appears to work on subprocess transport (the worker process is
        # long-lived, so the live state object survives between ``process()``
        # ticks) but breaks on HTTP, where each tick is an independent request
        # and state must round-trip through the stream-state token.
        for base in cls.__dict__.get("__orig_bases__", ()):
            origin = get_origin(base)
            if origin is not None and issubclass(origin, TableFunctionBase):
                type_args = get_args(base)
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
    def _parse_arguments(args_class: type[TArgs], arguments: Arguments, *, blended: bool = False) -> TArgs:
        """Convert Arguments to typed FunctionArguments instance.

        Args:
            args_class: The FunctionArguments dataclass type to build.
            arguments: The positional/named arguments to convert.
            blended: When True (a ``RowTransformFunction``), the POSITIONAL Args
                are the per-row input columns — they are NOT on the wire
                (``arguments.positional`` is empty in the column/LATERAL form),
                so skip them here (the worker reads them from ``batch``). Only
                named (``str``-position) Args are parsed. Positional access on the
                resulting dataclass field raises via the Arg guard.

        Returns:
            An instance of ``args_class`` populated from ``arguments``.

        """
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
                    # Blended: skip positional/varargs Args — they are the input
                    # columns (read from batch), absent from the wire args. Set the
                    # field to None so construction succeeds; the worker reads the
                    # value from ``batch``, never from ``params.args.<name>``.
                    if blended and isinstance(meta.position, int):
                        kwargs[attr_name] = None
                        break
                    if meta.varargs:
                        # Varargs: collect remaining positional args as raw pa.Scalar
                        # objects (e.g. constant_columns reads .type / pa.repeat off
                        # them). Union-typed varargs are the exception: decode each
                        # scalar to a TaggedUnion so the active member discriminator
                        # is preserved — matching how non-vararg union args resolve
                        # via Arguments.get()/_scalar_to_py(). Keyed on the declared
                        # arrow_type so the raw-scalar contract is untouched otherwise.
                        assert isinstance(meta.position, int)
                        varargs_scalars = arguments.positional[meta.position :]
                        if meta.arrow_type is not None and pa.types.is_union(meta.arrow_type):
                            kwargs[attr_name] = tuple(
                                _scalar_to_py(s) if s is not None else None for s in varargs_scalars
                            )
                        else:
                            kwargs[attr_name] = tuple(varargs_scalars)
                    else:
                        value = arguments.get(meta.position, default=meta.default)
                        # Reject SQL NULL for non-Optional Args. Without this,
                        # None silently propagated through validation and
                        # crashed deep in the user's process()/update() with
                        # an opaque Python ``TypeError`` (e.g. ``'<=' not
                        # supported between instances of NoneType and int``)
                        # that surfaced in the C++ extension as a worker
                        # exception with no hint at the cause.
                        if value is None and not _accepts_none(base_type):
                            raise meta._reject_none()
                        # Run Arg constraint validation (ge/le/gt/lt/choices/pattern).
                        # Skip for None — accepted via Optional[T].
                        if value is not None:
                            meta._validate(value)
                        kwargs[attr_name] = value
                    break

        return args_class(**kwargs)

    @classmethod
    def _is_blended(cls) -> bool:
        """Return True iff this is a blended ``RowTransformFunction``.

        Positional args are the per-row input columns. Lazy import to avoid
        a circular dependency.
        """
        try:
            from vgi.table_in_out_function import RowTransformFunction
        except ImportError:  # pragma: no cover
            return False
        return issubclass(cls, RowTransformFunction)

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

        Args:
            input: The bind request carrying settings and secrets batches.

        Returns:
            Keyword arguments matching the Setting/Secret annotations.

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
        execution_id: bytes | None = None,
        attach_plaintext: bytes | None = None,
    ) -> BindParams[TArgs]:
        """Construct BindParams from a BindRequest.

        Shared by bind() and table_function_cardinality() to avoid
        duplicating BindParams construction logic. ``execution_id`` is
        only populated on call paths that have one (currently just
        ``dynamic_to_string``); when provided, ``BindParams.storage`` is
        a `[`BoundStorage`][]` view keyed by it.

        Args:
            input: The bind request to build parameters from.
            auth_context: Authentication context for the caller, if any.
            execution_id: Execution id used to key execution-scoped storage,
                or None on call paths without one.
            attach_plaintext: Full framework attach plaintext (uuid || catalog
                bytes), or None without an ATTACH.

        Returns:
            The constructed BindParams.

        """
        txn_id = input.transaction_opaque_data
        # ``attach_plaintext`` is the full framework plaintext (``uuid(16) ||
        # catalog_bytes``) the worker unwrapped. Storage shards on its UUID;
        # bodies see only the catalog bytes via ``attach_opaque_data``.
        return BindParams[TArgs](
            args=cls._parse_arguments(cls.FunctionArguments, input.arguments, blended=cls._is_blended()),
            bind_call=input,
            settings=_batch_to_scalar_dict(input.settings),
            secrets=SecretsAccessor(input.secrets, is_retry=input.resolved_secrets_provided),
            transaction_storage=TransactionBoundStorage(
                cls.storage,
                txn_id,
                request=input,
                attach_plaintext=attach_plaintext,
            )
            if txn_id
            else None,
            storage=BoundStorage(
                cls.storage,
                execution_id,
                request=input,
                attach_plaintext=attach_plaintext,
            )
            if execution_id
            else None,
            auth_context=auth_context if auth_context is not None else AuthContext.anonymous(),
            attach_opaque_data=attach_catalog_bytes(attach_plaintext),
        )

    # ------------------------------------------------------------------
    # Bind / global_init — shared framework hooks for every table function.
    #
    # Subclasses define ``on_bind`` (and optionally ``on_init``) for the
    # user-facing behavior; the framework's wire entry points ``bind`` and
    # ``global_init`` are ``@final`` and live here so we have a single
    # source of truth across TableFunctionGenerator / TableInOutGenerator /
    # TableBufferingFunction.
    # ------------------------------------------------------------------

    @classmethod
    @abstractmethod
    def on_bind(
        cls,
        params: BindParams[TArgs],
    ) -> BindResponse:
        """Produce the output schema and perform other bind-time logic.

        Subclasses must override. Common patterns:

          * Pass through: ``return BindResponse(output_schema=params.bind_call.input_schema)``
          * Custom shape: build a ``pa.Schema`` from ``params.args`` and return it.
          * Dynamic secrets: declare ``*, my_secret: Annotated[..., Secret()] = None``
            or call ``params.secrets.get(...)``; the framework will issue a
            secret-scope retry automatically.

        Args:
            params: Bind parameters including arguments and schema.

        Returns:
            [`BindResponse`][] with output_schema and optional opaque_data.

        """

    @final
    @classmethod
    def bind(
        cls,
        input: BindRequest,
        *,
        ctx: CallContext | None = None,
        attach_plaintext: bytes | None = None,
    ) -> BindResponse:
        """Bind protocol entry point. Do not override; use ``on_bind()``.

        Validates type bounds when an input schema is present (table-input
        functions), constructs BindParameters, calls ``on_bind()``, and
        wraps the result for transmission to global_init. If ``on_bind()``
        triggered dynamic secret lookups via [`SecretsAccessor`][], returns a
        secret-scope request to trigger two-phase bind.

        Note: we do NOT auto-request secrets before ``on_bind()``. Table
        functions handle secrets via ``on_bind`` kwargs (``Secret()``
        annotations) and ``SecretsAccessor.get()`` calls, which may use
        dynamic scopes computed from function arguments.

        Args:
            input: The bind request from the client.
            ctx: Call context carrying the caller's auth, if any.
            attach_plaintext: Full framework attach plaintext, or None.

        Returns:
            The BindResponse from ``on_bind()``, or a secret-scope request when
            dynamic secret lookups need a two-phase bind.

        """
        auth = ctx.auth if ctx is not None else AuthContext.anonymous()
        params = cls._make_bind_params(input, auth_context=auth, attach_plaintext=attach_plaintext)

        if input.input_schema is not None:
            cls._validate_arg_type_bounds(cls.FunctionArguments, params.args, input.input_schema)

        result = cls.on_bind(params, **cls._extract_bind_kwargs(input))

        if params.secrets.needs_resolution:
            return BindResponse.secret_scope_request(params.secrets.pending_lookups)

        return result

    @classmethod
    def on_init(
        cls,
        params: InitParams[TArgs],
    ) -> GlobalInitResponse:
        """One-time setup after bind, before processing batches.

        Override to perform per-execution setup (open external resources,
        allocate caches, etc.). Default is a no-op.

        Args:
            params: Init parameters including arguments, schema, and storage.

        Returns:
            A GlobalInitResponse (default empty).

        """
        return GlobalInitResponse()

    @final
    @classmethod
    def global_init(
        cls,
        input: InitRequest,
        *,
        ctx: CallContext | None = None,
        attach_plaintext: bytes | None = None,
    ) -> GlobalInitResponse:
        """Global init protocol entry point. Do not override; use ``on_init()``.

        Args:
            input: The init request from the client.
            ctx: Call context carrying the caller's auth, if any.
            attach_plaintext: Full framework attach plaintext, or None.

        Returns:
            The GlobalInitResponse with worker count and execution id.

        """
        execution_id = uuid.uuid4().bytes
        auth = ctx.auth if ctx is not None else AuthContext.anonymous()
        params = InitParams[TArgs](
            args=cls._parse_arguments(cls.FunctionArguments, input.bind_call.arguments, blended=cls._is_blended()),
            init_call=input,
            output_schema=project_schema(
                _effective_projection_ids(cls, input.projection_ids),
                input.output_schema,
            ),
            settings=_batch_to_scalar_dict(input.bind_call.settings),
            secrets=SecretsAccessor(input.bind_call.secrets).to_dict(),
            execution_id=execution_id,
            # ``attach_plaintext`` is the full framework plaintext (uuid||catalog
            # bytes); storage shards on its UUID, the body sees the catalog bytes.
            storage=BoundStorage(cls.storage, execution_id, request=input, attach_plaintext=attach_plaintext),
            auth_context=auth,
            attach_opaque_data=attach_catalog_bytes(attach_plaintext),
        )

        result = cls.on_init(params)

        return GlobalInitResponse(
            max_workers=result.max_workers,
            execution_id=execution_id,
            opaque_data=result.opaque_data,
        )

    @classmethod
    def cardinality(cls, params: BindParams[TArgs]) -> TableCardinality:
        """Return the cardinality for the output.

        Override to provide row count estimates that help query planners
        make better decisions about join ordering and memory allocation.

        Args:
            params: Bind parameters — function args, settings, and secrets.

        Returns:
            [`TableCardinality`][] with estimate and/or max, or None if unknown.

        """
        return TableCardinality(estimate=None, max=None)

    @classmethod
    def dynamic_to_string(
        cls,
        params: BindParams[TArgs],
        execution_id: bytes,
    ) -> Mapping[str, str]:
        """Return diagnostics rendered as Extra Info under EXPLAIN ANALYZE.

        Fired once per parallel scan thread at end-of-stream. The function
        class is responsible for persisting whatever diagnostics it cares
        about during ``process()`` (shared storage, external service,
        in-memory class state for single-worker setups) and retrieving
        them by ``execution_id`` here.

        DuckDB merges the per-thread maps with last-write-wins semantics,
        so the *last* thread to finish — by which time every thread has
        persisted — supplies the visible final view.

        Best-effort: must not raise. The dispatcher catches exceptions
        and returns an empty map so EXPLAIN ANALYZE never breaks the
        query.

        Args:
            params: Same `[`BindParams`][]` ``cardinality`` and ``statistics``
                receive — function args, settings, secrets.
            execution_id: ``VgiTableFunctionGlobalState::global_execution_id``,
                stable for the duration of the query.

        Returns:
            Ordered key/value pairs. Insertion order is preserved on the
            wire and re-emitted into the C++ profiler's
            ``InsertionOrderPreservingMap``. The C++ wrapper appends
            intrinsic keys (`[`Worker`][]`, ``Function``, ``Rows Read``,
            ``Threads``) after this map; user keys override on conflict.

        """
        return {}

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

        Args:
            params: Bind parameters — function args, settings, and secrets.

        Returns:
            A list of `ColumnStatistics` (one entry per column for which stats
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
                on the returned `[`PushdownFilters`][]`.

        Returns:
            `PushdownFilters` container with parsed filter AST, or None.

        """
        if pushdown_filters is None:
            return None
        from vgi.table_filter_pushdown import deserialize_filters

        return deserialize_filters(pushdown_filters, join_keys=join_keys)

    @classmethod
    def _should_auto_apply_filters(cls) -> bool:
        """Check if auto_apply_filters is enabled in Meta.

        Returns:
            True if ``Meta.auto_apply_filters`` is set.

        """
        meta = getattr(cls, "Meta", None)
        return bool(getattr(meta, "auto_apply_filters", False))

    @classmethod
    def _supports_batch_index(cls) -> bool:
        """Return True if Meta.supports_batch_index is set.

        Drives the ``batch_index=`` kwarg validation on ``out.emit()`` in the
        table-producer harness (see vgi.protocol._TrackingOutputCollector).

        Returns:
            True if ``Meta.supports_batch_index`` is set.

        """
        meta = getattr(cls, "Meta", None)
        return bool(getattr(meta, "supports_batch_index", False))

    @classmethod
    def _partition_kind(cls) -> Any:
        """Return Meta.partition_kind, defaulting to ``NOT_PARTITIONED``.

        Drives the ``partition_values=`` kwarg validation on ``out.emit()``
        in the table-producer harness. Imported lazily so the base class
        doesn't pull in ``vgi.metadata`` at module load time.

        Returns:
            The configured ``Meta.partition_kind``, or ``NOT_PARTITIONED``.

        """
        from vgi.metadata import PartitionKind

        meta = getattr(cls, "Meta", None)
        return getattr(meta, "partition_kind", PartitionKind.NOT_PARTITIONED)

    @staticmethod
    def _apply_pushdown_filter(batch: pa.RecordBatch, pushdown_filters: PushdownFilters | None) -> pa.RecordBatch:
        """Apply pushdown filters to a batch if present.

        Args:
            batch: RecordBatch to filter
            pushdown_filters: The [`PushdownFilters`][] to apply or None.

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

    Each call to `process()` should either:
    - Emit a batch via `out.emit(batch)`
    - Signal completion via `out.finish()`

    Use `TState` to persist state between `process()` calls.

    For functions that transform input batches, use [`TableInOutGenerator`][].

    Attributes:
        on_cancel.__func__.__doc__: Docstring assigned at class-definition time
            to the ``on_cancel`` cancellation hook (see ``on_cancel``).

    """

    # bind / on_bind / on_init / global_init are defined on TableFunctionBase.
    # TableFunctionGenerator subclasses must override the abstract on_bind
    # to declare an output schema (TFG has no input schema to default to).

    @classmethod
    def initial_state(cls, params: ProcessParams[TArgs]) -> TState | None:
        """Create initial processing state. Override when `TState` is used.

        Called once during init to create the state object that will be
        passed to `process()` on each tick.

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
        - Call `out.emit(batch)` to produce one output batch
        - Call `out.finish()` to signal that generation is complete

        Use `out.client_log(level, message)` for in-band logging.

        Args:
            params: Process parameters including arguments and schemas.
            state: Mutable state persisted between calls. None if TState not used.
            out: `OutputCollector` for emitting batches, logging, and signaling finish.

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
    """Class decorator to set max_workers=1 for a [`TableFunctionGenerator`][] subclass.

    Args:
        cls: The TableFunctionGenerator subclass to decorate.

    Returns:
        The same class, with an ``on_init`` returning ``max_workers=1`` injected
        if it did not already define one.

    """
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
    """Class decorator to return FIXED_SCHEMA from on_bind for a [`TableFunctionGenerator`][] subclass.

    Sets ``cls._inline_bind_safe = True`` *only when* the decorator actually
    installs its own ``on_bind``. The catalog framework reads this marker to
    decide whether `Table(inline_bind=True)` is allowed — the contract is "the
    decorator's bind is in control, output is exactly ``cls.FIXED_SCHEMA``,
    no kwargs inspected." If the class already defined its own ``on_bind``,
    the decorator silently leaves it alone and we *must not* set the marker;
    otherwise the framework would inline a bind it doesn't actually control.

    Subclasses inherit the marker via Python attribute lookup. A subclass
    that overrides ``on_bind`` adds it to its own ``__dict__``; the catalog
    framework's eligibility check is
    ``getattr(cls, "_inline_bind_safe", False) and "on_bind" not in cls.__dict__``,
    which correctly excludes such subclasses.

    Args:
        cls: The TableFunctionGenerator subclass to decorate.

    Returns:
        The same class, with an ``on_bind`` returning ``cls.FIXED_SCHEMA``
        injected if it did not already define one.

    """
    if "on_bind" not in cls.__dict__:  # only inject if subclass hasn't overridden
        if not hasattr(cls, "FIXED_SCHEMA"):
            raise ValueError(f"Class {cls.__name__} must define FIXED_SCHEMA to use @bind_fixed_schema")

        def on_bind_impl(cls_: type[T], params: Any) -> BindResponse:
            value = getattr(cls_, "FIXED_SCHEMA", None)

            if value is None or not isinstance(value, pa.Schema):
                raise TypeError(f"Class {cls_.__name__}.FIXED_SCHEMA must be a pyarrow.Schema")
            return BindResponse(output_schema=value)

        # Mark the function itself so we can later distinguish "decorator
        # installed this on_bind" from "user overrode on_bind" — useful for
        # downstream callers (e.g. catalog inline-bind) that need to confirm
        # the bind logic in effect is the decorator's, not a subclass override.
        on_bind_impl._is_bind_fixed_schema = True  # type: ignore[attr-defined]

        # assign as classmethod
        cls.on_bind = classmethod(on_bind_impl)  # type: ignore[assignment]

        # Clear 'on_bind' from __abstractmethods__ — the metaclass set it
        # before decorators ran, so we must update it manually.
        if hasattr(cls, "__abstractmethods__") and "on_bind" in cls.__abstractmethods__:
            cls.__abstractmethods__ = cls.__abstractmethods__ - {"on_bind"}

        # Mark the class as inline-bind-safe *only when* we actually installed
        # the on_bind. If the class had a pre-existing custom on_bind, we left
        # it alone and have no claim about its purity — the marker stays unset.
        cls._inline_bind_safe = True  # type: ignore[attr-defined]

    return cls
