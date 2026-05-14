"""VGI protocol definition for vgi_rpc server integration.

Defines the VgiProtocol, consolidated request types (BindRequest, InitRequest),
catalog request/response types, and StreamState implementations for each function type.

VgiProtocol Methods
-------------------
- **bind()**: Schema resolution and argument validation (unary)
- **init()**: Worker initialization, returns a Stream for data processing
- **catalog_*()**: ~35 typed catalog interface methods (unary)

StreamState Implementations
---------------------------
- **ScalarExchangeState**: Calls ScalarFunctionGenerator.process() per batch
- **TableProducerState**: Calls TableFunctionGenerator.process() per tick
- **TableInOutExchangeState**: Calls TableInOutGenerator.process() per input
- **TableInOutFinalizeState**: Emits pre-computed finalize batches as producer

"""

from __future__ import annotations

import base64
import dataclasses
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Annotated, Any, Protocol, get_args, get_origin

import pyarrow as pa
import pyarrow.compute as pc
from vgi_rpc import ArrowSerializableDataclass, ArrowType, Transient
from vgi_rpc.rpc import (
    AnnotatedBatch,
    CallContext,
    ExchangeState,
    OutputCollector,
    ProducerState,
    Stream,
)

from vgi.arguments import Arguments
from vgi.catalog.catalog_interface import (
    CatalogAttachResult,
    CatalogInfo,
    FunctionInfo,
    IndexConstraintType,
    IndexInfo,
    MacroInfo,
    MacroType,
    OnConflict,
    PartitionKind,
    SchemaInfo,
    SchemaObjectType,
    TableInfo,
    ViewInfo,
)
from vgi.function_storage import BoundStorage
from vgi.invocation import BindResponse, FunctionType, GlobalInitResponse
from vgi.otel import VgiTracer, _batch_bytes, _timed_exchange, get_noop_tracer
from vgi.scalar_function import ScalarFunctionGenerator
from vgi.table_function import (
    OrderByDirection,
    OrderByNullOrder,
    ProcessParams,
    SecretsAccessor,
    TableCardinality,
    TableFunctionBase,
    TableFunctionGenerator,
    TableInOutFunctionInitPhase,
    _batch_to_scalar_dict,
    _effective_projection_ids,
    project_schema,
)
from vgi.table_in_out_function import TableInOutGenerator

__all__ = [
    "BindRequest",
    "CatalogAttachRequest",
    "CatalogCreateRequest",
    "CatalogsResponse",
    "IndexCreateRequest",
    "IndexesResponse",
    "MacroCreateRequest",
    "MacrosResponse",
    "TableCreateRequest",
    "CatalogVersionResponse",
    "FunctionsResponse",
    "InitRequest",
    "ProcessState",
    "ScalarExchangeState",
    "SchemasResponse",
    "TableFunctionDynamicToStringRequest",
    "TableFunctionDynamicToStringResponse",
    "TableInOutExchangeState",
    "TableInOutFinalizeState",
    "TableProducerState",
    "TablesResponse",
    "TransactionBeginResponse",
    "VgiProtocol",
    "ViewsResponse",
]


# ---------------------------------------------------------------------------
# Request types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class BindRequest(ArrowSerializableDataclass):
    """Consolidated bind request for all function types.

    For table functions (no input schema), ``input_schema`` is ``None``.
    For scalar and table-in-out functions, ``input_schema`` is set.

    """

    function_name: str
    arguments: Annotated[Arguments, ArrowType(pa.binary())]
    function_type: FunctionType
    input_schema: Annotated[pa.Schema | None, ArrowType(pa.binary())] = None
    settings: Annotated[pa.RecordBatch | None, ArrowType(pa.binary())] = None
    secrets: Annotated[pa.RecordBatch | None, ArrowType(pa.binary())] = None
    attach_opaque_data: bytes | None = None
    transaction_opaque_data: bytes | None = None
    resolved_secrets_provided: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class InitRequest(ArrowSerializableDataclass):
    """Consolidated init request for all function types.

    For secondary init requests, ``execution_id`` and ``init_opaque_data``
    are set; use :attr:`is_secondary` to distinguish.

    """

    # Core (always present)
    bind_call: BindRequest
    output_schema: Annotated[pa.Schema, ArrowType(pa.binary())]
    # Wire-facing — bytes the framework produced from the typed
    # ``BindResult.opaque_data``. Consumers reconstruct via
    # ``MyConcreteDataclass.deserialize_from_bytes(raw)``. See
    # ``BindResponse.opaque_data`` in vgi/invocation.py for the full
    # contract rationale (typed producer / bytes wire / explicit
    # consumer; abstract-base reconstruction can't be done in Python
    # without a class registry).
    bind_opaque_data: Annotated[bytes | None, ArrowType(pa.binary())] = None

    # Table function extras (None for scalar)
    projection_ids: list[int] | None = None
    pushdown_filters: Annotated[pa.RecordBatch | None, ArrowType(pa.large_binary())] = None
    join_keys: Annotated[list[pa.RecordBatch] | None, ArrowType(pa.list_(pa.large_binary()))] = None

    # Table-in-out extras
    phase: TableInOutFunctionInitPhase | None = None

    # Secondary init (None = global init, set = secondary)
    execution_id: bytes | None = None
    # Same contract as ``bind_opaque_data`` above.
    init_opaque_data: Annotated[bytes | None, ArrowType(pa.binary())] = None

    # Order pushdown hint from DuckDB's RowGroupPruner optimizer (all None when no hint)
    order_by_column_name: str | None = None
    order_by_direction: OrderByDirection | None = None
    order_by_null_order: OrderByNullOrder | None = None
    order_by_limit: int | None = None

    # TABLESAMPLE pushdown hint from DuckDB's SamplingPushdown optimizer (all None when no hint)
    tablesample_percentage: float | None = None
    tablesample_seed: int | None = None

    @property
    def is_secondary(self) -> bool:
        """True if this is a secondary init request."""
        return self.execution_id is not None


@dataclass(frozen=True, slots=True, kw_only=True)
class TableFunctionCardinalityRequest(ArrowSerializableDataclass):
    """Consolidated request for table function cardinality."""

    bind_call: BindRequest
    # Same contract as InitRequest.bind_opaque_data above.
    bind_opaque_data: Annotated[bytes | None, ArrowType(pa.binary())] = None


@dataclass(frozen=True, slots=True, kw_only=True)
class TableFunctionStatisticsRequest(ArrowSerializableDataclass):
    """Consolidated request for table function per-column statistics.

    Mirrors TableFunctionCardinalityRequest: the worker receives a full
    copy of the original BindRequest (including parsed Arguments), so it
    can derive per-column stats from the user-supplied args.
    """

    bind_call: BindRequest
    # Same contract as InitRequest.bind_opaque_data above.
    bind_opaque_data: Annotated[bytes | None, ArrowType(pa.binary())] = None


@dataclass(frozen=True, slots=True, kw_only=True)
class TableFunctionDynamicToStringRequest(ArrowSerializableDataclass):
    """Post-execution profile-info request, fired once per scan thread.

    Carries ``global_execution_id`` so the function class can retrieve
    whatever diagnostics it persisted during ``process()`` (shared
    storage, external service, in-memory class state for single-worker
    setups, etc.). VGI does not serialize per-thread ``_user_state``
    across the boundary — the user owns persistence.
    """

    bind_call: BindRequest
    # Same contract as InitRequest.bind_opaque_data above.
    bind_opaque_data: Annotated[bytes | None, ArrowType(pa.binary())] = None
    global_execution_id: bytes


@dataclass(frozen=True, slots=True, kw_only=True)
class TableFunctionDynamicToStringResponse(ArrowSerializableDataclass):
    """Ordered key/value pairs surfaced as Extra Info under EXPLAIN ANALYZE.

    Parallel ``keys``/``values`` lists keep insertion order explicit on
    the wire. The C++ side reassembles them into an
    ``InsertionOrderPreservingMap<string>``.
    """

    keys: Annotated[list[str], ArrowType(pa.list_(pa.string()))]
    values: Annotated[list[str], ArrowType(pa.list_(pa.string()))]


# ---------------------------------------------------------------------------
# Catalog request types (for methods with complex parameters)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogAttachRequest(ArrowSerializableDataclass):
    """Request for catalog_attach. Uses RecordBatch for mixed-type options.

    ``data_version_spec`` and ``implementation_version`` carry semver
    strings the user supplied at ATTACH time (concrete or range). ``None``
    = unconstrained. The worker is responsible for interpreting and
    validating them.
    """

    name: str
    options: Annotated[pa.RecordBatch | None, ArrowType(pa.binary())] = None
    data_version_spec: str | None
    implementation_version: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogCreateRequest(ArrowSerializableDataclass):
    """Request for catalog_create. Uses RecordBatch for mixed-type options."""

    name: str
    on_conflict: OnConflict
    options: Annotated[pa.RecordBatch | None, ArrowType(pa.binary())] = None


@dataclass(frozen=True, slots=True, kw_only=True)
class TableCreateRequest(ArrowSerializableDataclass):
    """Request for catalog_table_create with complex constraint types."""

    attach_opaque_data: bytes
    schema_name: str
    name: str
    columns: bytes  # SerializedSchema
    on_conflict: OnConflict
    not_null_constraints: Annotated[list[int], ArrowType(pa.list_(pa.int32()))] = field(default_factory=list)
    unique_constraints: Annotated[list[list[int]], ArrowType(pa.list_(pa.list_(pa.int32())))] = field(
        default_factory=list
    )
    check_constraints: list[str] = field(default_factory=list)
    primary_key_constraints: Annotated[list[list[int]], ArrowType(pa.list_(pa.list_(pa.int32())))] = field(
        default_factory=list
    )
    foreign_key_constraints: Annotated[list[bytes], ArrowType(pa.list_(pa.binary()))] = field(default_factory=list)
    transaction_opaque_data: bytes | None = None


# ---------------------------------------------------------------------------
# Catalog response types
# ---------------------------------------------------------------------------


# ``CatalogsResponse`` is generated below via ``_catalog_items_response`` once
# that factory is defined — it wraps a list of CatalogInfo records serialized
# as bytes, matching the pattern used for other list[Info] responses.


@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogVersionResponse(ArrowSerializableDataclass):
    """Response wrapping int for catalog_version()."""

    version: int


@dataclass(frozen=True, slots=True, kw_only=True)
class TransactionBeginResponse(ArrowSerializableDataclass):
    """Response wrapping optional TransactionOpaqueData for catalog_transaction_begin()."""

    transaction_opaque_data: bytes | None = None


def _catalog_items_response(item_type: type) -> type:
    """Generate a catalog items response class for the given ArrowSerializableDataclass type.

    Each generated class wraps a list of IPC-serialized items with helpers:
    - from_infos(items) / from_optional(item) — serialize into response
    - to_infos() / to_optional() — deserialize from response

    The item_type must have serialize_to_bytes() and deserialize_from_bytes() methods
    (i.e., be an ArrowSerializableDataclass).
    """
    type_name = item_type.__name__

    @dataclass(frozen=True, slots=True, kw_only=True)
    class _Response(ArrowSerializableDataclass):
        items: Annotated[list[bytes], ArrowType(pa.list_(pa.binary()))]

        @staticmethod
        def from_infos(infos: list) -> _Response:  # type: ignore[type-arg]
            return _Response(items=[info.serialize_to_bytes() for info in infos])

        @staticmethod
        def from_optional(info: object | None) -> _Response:
            if info is None:
                return _Response(items=[])
            return _Response(items=[info.serialize_to_bytes()])  # type: ignore[attr-defined]

        def to_infos(self) -> list:  # type: ignore[type-arg]
            return [item_type.deserialize_from_bytes(b) for b in self.items]  # type: ignore[attr-defined]

        def to_optional(self) -> object | None:
            if not self.items:
                return None
            return item_type.deserialize_from_bytes(self.items[0])  # type: ignore[attr-defined,no-any-return]

    # Give the class a meaningful name for vgi_rpc introspection and repr
    # "TableInfo" -> "TablesResponse", "IndexInfo" -> "IndexesResponse"
    stem = type_name.removesuffix("Info")
    plural = f"{stem}es" if stem.endswith(("x", "s", "sh", "ch")) else f"{stem}s"
    class_name = f"{plural}Response"
    _Response.__name__ = class_name
    _Response.__qualname__ = class_name
    _Response.__doc__ = f"Response wrapping list of {type_name}."

    return _Response


if TYPE_CHECKING:
    from typing import Self

    # Provide mypy with explicit class shapes for the dynamically generated responses.
    class _CatalogItemsResponseStub(ArrowSerializableDataclass):
        items: list[bytes]

        @classmethod
        def from_infos(cls, infos: list[Any]) -> Self: ...

        @classmethod
        def from_optional(cls, info: object | None) -> Self: ...

        def to_infos(self) -> list[Any]: ...

        def to_optional(self) -> Any: ...

    class CatalogsResponse(_CatalogItemsResponseStub):
        """Response wrapping list of CatalogInfo."""

    class SchemasResponse(_CatalogItemsResponseStub):
        """Response wrapping list of SchemaInfo."""

    class TablesResponse(_CatalogItemsResponseStub):
        """Response wrapping list of TableInfo."""

    class ViewsResponse(_CatalogItemsResponseStub):
        """Response wrapping list of ViewInfo."""

    class FunctionsResponse(_CatalogItemsResponseStub):
        """Response wrapping list of FunctionInfo."""

    class MacrosResponse(_CatalogItemsResponseStub):
        """Response wrapping list of MacroInfo."""
else:
    CatalogsResponse = _catalog_items_response(CatalogInfo)
    SchemasResponse = _catalog_items_response(SchemaInfo)
    TablesResponse = _catalog_items_response(TableInfo)
    ViewsResponse = _catalog_items_response(ViewInfo)
    FunctionsResponse = _catalog_items_response(FunctionInfo)
    MacrosResponse = _catalog_items_response(MacroInfo)


@dataclass(frozen=True, slots=True, kw_only=True)
class MacroCreateRequest(ArrowSerializableDataclass):
    """Request for catalog_macro_create with RecordBatch for parameter defaults."""

    attach_opaque_data: bytes
    schema_name: str
    name: str
    macro_type: MacroType
    parameters: list[str]
    definition: str
    on_conflict: OnConflict
    parameter_default_values: Annotated[pa.RecordBatch | None, ArrowType(pa.binary())] = None
    transaction_opaque_data: bytes | None = None


if TYPE_CHECKING:

    class IndexesResponse(_CatalogItemsResponseStub):  # noqa: E302
        """Response wrapping list of IndexInfo."""
else:
    IndexesResponse = _catalog_items_response(IndexInfo)


@dataclass(frozen=True, slots=True, kw_only=True)
class IndexCreateRequest(ArrowSerializableDataclass):
    """Request for catalog_index_create."""

    attach_opaque_data: bytes
    schema_name: str
    name: str
    table_name: str
    index_type: str = ""
    constraint_type: IndexConstraintType = IndexConstraintType.NONE
    expressions: list[str] = field(default_factory=list)
    on_conflict: OnConflict = OnConflict.ERROR
    options: dict[str, str] = field(default_factory=dict)
    transaction_opaque_data: bytes | None = None


# ---------------------------------------------------------------------------
# StreamState implementations
# ---------------------------------------------------------------------------


@dataclass
class ScalarExchangeState(ExchangeState):
    """Exchange state for scalar function streams.

    Calls ``ScalarFunctionGenerator.process()`` per batch. Each ``exchange()``
    call sends one input batch and receives one output batch.

    ``_init_call`` and ``_init_response`` are serialized into the state token
    so they survive HTTP round-trips.  ``_func_cls`` is transient and restored
    via ``rehydrate()``.

    """

    _init_call: Annotated[InitRequest, ArrowType(pa.binary())] = field(default=None, repr=False)  # type: ignore[assignment]
    _init_response: Annotated[GlobalInitResponse, ArrowType(pa.binary())] = field(default=None, repr=False)  # type: ignore[assignment]
    _func_cls: Annotated[type[ScalarFunctionGenerator], Transient()] = field(default=None, repr=False)  # type: ignore[assignment]
    _vgi_tracer: Annotated[VgiTracer, Transient()] = field(default_factory=get_noop_tracer, repr=False)

    def rehydrate(self, implementation: object) -> None:
        """Restore ``_func_cls`` from the worker's function registry."""
        from vgi.worker import Worker

        worker: Worker = implementation  # type: ignore[assignment]
        self._func_cls = worker._resolve_function(self._init_call.bind_call)  # type: ignore[assignment]
        self._vgi_tracer = worker._vgi_tracer

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Process one input batch through the scalar function."""
        cls = self._func_cls
        batch = input.batch

        # Workaround: over HTTP, 0-column batches lose their row count because
        # Arrow IPC RecordBatch messages with no arrays default to length 0.
        # When a scalar function has no column inputs (e.g. "SELECT func()"),
        # the caller expects 1 output row but sends num_rows=0. Add a dummy
        # column so PyArrow preserves the row count, then strip it before
        # validation.
        inject_row = batch.num_columns == 0 and batch.num_rows == 0
        if inject_row:
            batch = pa.record_batch({"__row": pa.array([True])})

        timer = _timed_exchange(
            self._vgi_tracer,
            "vgi.execute.scalar",
            self._init_call.bind_call.function_name,
            self._init_call.bind_call.function_type.value,
            self._init_response.execution_id,
        )
        with timer:
            output = cls.process(
                batch=batch,
                init_call=self._init_call,
                init_response=self._init_response,
                storage=BoundStorage(
                    cls.storage, self._init_response.execution_id, request=self._init_call, auth=ctx.auth
                ),
                auth_context=ctx.auth,
            )
            if inject_row:
                cls._validate_row_count(output, batch)
            else:
                cls._validate_row_count(output, input.batch)
            timer.record(
                input_rows=input.batch.num_rows,
                output_rows=output.num_rows,
                input_bytes=_batch_bytes(input.batch),
                output_bytes=_batch_bytes(output),
            )
        out.emit(output)


_log = logging.getLogger(__name__)


def _resolve_state_type(func_cls: type) -> type[ArrowSerializableDataclass] | None:
    """Extract the TState type parameter from a TableFunctionGenerator or TableInOutGenerator.

    Walks the MRO looking for ``TableFunctionGenerator[TArgs, TState]`` or
    ``TableInOutGenerator[TArgs, TState]`` and returns ``TState`` if it is a
    concrete ``ArrowSerializableDataclass`` subclass.

    Raises TypeError if the state type is a concrete class that does not
    extend ArrowSerializableDataclass — this catches the problem early
    rather than silently falling back to initial_state() on each HTTP exchange.
    """
    for klass in func_cls.__mro__:
        for base in getattr(klass, "__orig_bases__", ()):
            origin = get_origin(base)
            if origin is None:
                continue
            if issubclass(origin, (TableFunctionGenerator, TableInOutGenerator)):
                args = get_args(base)
                if len(args) >= 2:
                    state_type = args[1]
                    if isinstance(state_type, type) and issubclass(state_type, ArrowSerializableDataclass):
                        return state_type
                    if (
                        isinstance(state_type, type)
                        and state_type is not type(None)
                        and not issubclass(state_type, ArrowSerializableDataclass)
                    ):
                        raise TypeError(
                            f"{func_cls.__name__}: TState type {state_type.__name__} must extend "
                            f"ArrowSerializableDataclass for HTTP state serialization."
                        )
    return None


def _partition_fields_from_schema(bind_schema: pa.Schema) -> list[pa.Field[Any]]:
    """Walk a bind schema and return fields annotated as partition columns.

    Recognises the ``vgi.partition_column = b"true"`` field metadata
    set by :func:`vgi.schema_utils.partition_field`. Used by the
    table-producer harness to precompute the list of partition fields
    once at wrapper construction, so per-emit validation only does an
    O(P) walk where P is the partition column count.
    """
    from vgi.schema_utils import VGI_PARTITION_COLUMN_KEY

    result: list[pa.Field[Any]] = []
    for f in bind_schema:
        md = f.metadata
        if md is not None and md.get(VGI_PARTITION_COLUMN_KEY) == b"true":
            result.append(f)
    return result


def _resolve_partition_min_max(
    field: pa.Field[Any],
    partition_kind: PartitionKind,
    batch: pa.RecordBatch,
    explicit: dict[str, tuple[pa.Scalar[Any], pa.Scalar[Any]]] | None,
) -> tuple[pa.Scalar[Any], pa.Scalar[Any]]:
    """Resolve ``(min, max)`` for one partition column.

    Two paths:
    * Explicit: ``explicit[field.name]`` is a ``(pa.Scalar, pa.Scalar)``
      tuple with both elements typed to ``field.type``.
    * Auto-extract: read the column from the batch, derive
      ``(min, max)``. For SINGLE_VALUE, also validate single distinct
      non-null value.
    """
    if explicit is not None and field.name in explicit:
        pair = explicit[field.name]
        if not isinstance(pair, tuple) or len(pair) != 2:
            raise RuntimeError(f"partition_values[{field.name!r}] must be (min, max) tuple; got {pair!r}")
        min_s, max_s = pair
        if not isinstance(min_s, pa.Scalar) or not isinstance(max_s, pa.Scalar):
            raise RuntimeError(
                f"partition_values[{field.name!r}] elements must be pa.Scalar; "
                f"got ({type(min_s).__name__}, {type(max_s).__name__})"
            )
        if min_s.type != field.type:
            raise RuntimeError(
                f"partition_values[{field.name!r}] min type mismatch: declared {field.type}, got {min_s.type}"
            )
        if max_s.type != field.type:
            raise RuntimeError(
                f"partition_values[{field.name!r}] max type mismatch: declared {field.type}, got {max_s.type}"
            )
        return min_s, max_s

    # Auto-extract path.
    try:
        column = batch.column(field.name)
    except KeyError as exc:
        raise RuntimeError(
            f"column {field.name!r} is partition-annotated but absent from emitted batch; "
            f"pass partition_values={{{field.name!r}: (pa.scalar(...), pa.scalar(...))}}"
        ) from exc

    if partition_kind == PartitionKind.SINGLE_VALUE_PARTITIONS:
        # Count distinct non-null values; SINGLE_VALUE requires <= 1.
        # All-NULL columns are accepted: DuckDB routes NULL as its own
        # partition (Value::NotDistinctFrom(NULL, NULL) is true).
        non_null = pc.drop_null(column)
        if len(non_null) > 0:
            unique = pc.unique(non_null)
            if len(unique) > 1:
                raise RuntimeError(
                    f"column {field.name!r} has {len(unique)} distinct values; "
                    f"partition_kind=SINGLE_VALUE_PARTITIONS requires 1"
                )

    # ``pa.compute.min_max`` returns a scalar struct with min/max fields.
    # For all-null columns it returns null/null of the column's type,
    # which is exactly what we want.
    mm_struct = pc.min_max(column)
    return mm_struct["min"], mm_struct["max"]


def _build_partition_values_batch(
    partition_fields: list[pa.Field[Any]],
    resolved: dict[str, tuple[pa.Scalar[Any], pa.Scalar[Any]]],
) -> pa.RecordBatch:
    """Build the 2-row ``(min, max)`` RecordBatch from resolved scalars."""
    arrays: list[pa.Array[Any]] = []
    fields: list[pa.Field[Any]] = []
    for pf in partition_fields:
        min_s, max_s = resolved[pf.name]
        # pa.array([scalar, scalar]) infers the same type as the scalars;
        # the resolve step already validated those match field.type, so a
        # direct cast is a no-op except for any storage-layout normalisation.
        arr = pa.array([min_s, max_s], type=pf.type)
        arrays.append(arr)
        fields.append(pf)
    return pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields))


def _serialize_partition_values_batch(batch: pa.RecordBatch) -> str:
    """Serialize via Arrow IPC stream + base64.

    Matches the ``vgi_rpc.stream_state#b64`` convention used elsewhere.
    """
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return base64.b64encode(sink.getvalue().to_pybytes()).decode("ascii")


def _merge_partition_values(
    *,
    partition_fields: list[pa.Field[Any]],
    partition_kind: PartitionKind,
    batch: pa.RecordBatch,
    partition_values: dict[str, tuple[pa.Scalar[Any], pa.Scalar[Any]]] | None,
    metadata: dict[str, str] | None,
) -> dict[str, str] | None:
    """Validate the partition_values kwarg and fold it into the emit metadata.

    Folds the resulting Arrow IPC bytes into the emit metadata dict under
    ``vgi_partition_values#b64``.

    Contract:

    * If ``partition_fields`` is empty (function did not annotate any
      partition column), then ``partition_values`` MUST be None —
      catches "I forgot to mark fields" bugs that would otherwise
      silently drop the kwarg.
    * If ``partition_fields`` is non-empty AND ``batch.num_rows == 0``:
      no metadata is emitted (empty-batch exemption — the C++ extension
      skips its requirement check on 0-row batches).
    * Otherwise: for each partition field, resolve ``(min, max)`` via
      :func:`_resolve_partition_min_max`. Build a 2-row IPC batch,
      serialize, base64-encode, set
      ``metadata["vgi_partition_values#b64"]``.
    """
    if not partition_fields:
        if partition_values is not None:
            raise RuntimeError(
                "out.emit(partition_values=...) requires partition-annotated fields "
                "in the bind schema. Use vgi.schema_utils.partition_field() to mark "
                "the column(s) and set Meta.partition_kind to a non-default value."
            )
        return metadata

    if batch.num_rows == 0:
        # Empty batches are exempt from partition-values; the C++ side
        # skips its requirement check for 0-row batches. Leave metadata
        # untouched so callers don't pay base64+IPC overhead for nothing.
        return metadata

    resolved: dict[str, tuple[pa.Scalar[Any], pa.Scalar[Any]]] = {}
    for pf in partition_fields:
        resolved[pf.name] = _resolve_partition_min_max(
            pf,
            partition_kind,
            batch,
            partition_values,
        )

    values_batch = _build_partition_values_batch(partition_fields, resolved)
    b64 = _serialize_partition_values_batch(values_batch)

    merged: dict[str, str] = dict(metadata) if metadata else {}
    merged["vgi_partition_values#b64"] = b64
    return merged


def _merge_batch_index(
    *,
    supports_batch_index: bool,
    batch_index: int | None,
    metadata: dict[str, str] | None,
) -> dict[str, str] | None:
    """Validate the batch_index kwarg and fold it into the emit metadata dict.

    Contract:
      * If ``supports_batch_index`` is True, ``batch_index`` MUST be supplied.
        Forgetting the kwarg on an opted-in function is a programming error
        that would otherwise produce a data batch with no
        ``vgi_batch_index`` metadata — the C++ extension would raise an
        IOException at scan time; raising here gives the worker author a
        clearer line number.
      * If ``supports_batch_index`` is False, ``batch_index`` MUST NOT be
        supplied — catches "I forgot to set the Meta flag" bugs.
      * The merged value is a decimal-string of the int (matches the wire
        convention used by ``vgi_filter_version`` / ``vgi_join_keys_version``
        elsewhere in the codebase).
    """
    if supports_batch_index:
        if batch_index is None:
            raise RuntimeError("out.emit() requires batch_index= on a function with Meta.supports_batch_index = True")
    else:
        if batch_index is not None:
            raise RuntimeError("out.emit(batch_index=...) requires Meta.supports_batch_index = True")
    if batch_index is None:
        return metadata
    merged: dict[str, str] = dict(metadata) if metadata else {}
    merged["vgi_batch_index"] = str(batch_index)
    return merged


class VgiOutputCollector(Protocol):
    """Structural type for the ``out`` handed to a table function's body.

    VGI's emit-path wrappers (:class:`_TrackingOutputCollector`,
    :class:`_FilteringOutputCollector`) extend vgi-rpc's
    ``OutputCollector.emit`` with ``batch_index=`` and ``partition_values=``
    kwargs. Function bodies that opt into those features ``cast`` the
    framework-supplied ``out`` to this protocol before calling ``emit``:
    the base ``OutputCollector`` type cannot carry the wider signature
    without breaking ``process()`` override compatibility across every
    fixture.
    """

    def emit(
        self,
        batch: pa.RecordBatch,
        batch_index: int | None = None,
        partition_values: dict[str, tuple[pa.Scalar[Any], pa.Scalar[Any]]] | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None: ...

    def finish(self) -> None: ...

    def client_log(self, level: Any, message: str, **extra: str) -> None: ...


class _FilteringOutputCollector:
    """Wrapper that applies pushdown filters to emitted data batches.

    Intercepts emit() calls and applies the pushdown filter before
    delegating to the real OutputCollector. Threads ``batch_index=`` and
    ``metadata=`` kwargs through unchanged — validation lives on the
    innermost wrapper (``_TrackingOutputCollector``) so it happens exactly
    once regardless of which wrappers are stacked.
    """

    __slots__ = ("_inner", "_func_cls", "_filters")

    def __init__(self, inner: _TrackingOutputCollector, func_cls: type[TableFunctionBase[Any]], filters: Any) -> None:
        self._inner = inner
        self._func_cls = func_cls
        self._filters = filters

    def emit(
        self,
        batch: pa.RecordBatch,
        batch_index: int | None = None,
        partition_values: dict[str, tuple[pa.Scalar[Any], pa.Scalar[Any]]] | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None:
        filtered = self._func_cls._apply_pushdown_filter(batch, self._filters)
        self._inner.emit(
            filtered,
            batch_index=batch_index,
            partition_values=partition_values,
            metadata=metadata,
        )

    def emit_pydict(self, data: dict[str, Any], schema: pa.Schema | None = None) -> None:
        batch = pa.RecordBatch.from_pydict(data, schema=schema or self._inner.output_schema)
        self.emit(batch)

    def finish(self) -> None:
        self._inner.finish()

    @property
    def finished(self) -> bool:
        return self._inner.finished

    def emit_client_log_message(self, msg: Any) -> None:
        self._inner.emit_client_log_message(msg)

    def client_log(self, level: Any, message: str, **extra: str) -> None:
        self._inner.client_log(level, message, **extra)

    def propagate(self) -> None:
        """No-op: state already propagated to inner collector."""

    @property
    def output_schema(self) -> pa.Schema:
        return self._inner.output_schema


class _TrackingOutputCollector:
    """Wrapper that tracks total rows and bytes emitted, delegating all else.

    Also the validation point for the ``batch_index=`` and
    ``partition_values=`` kwargs on ``out.emit()`` (see
    :func:`_merge_batch_index` and :func:`_merge_partition_values`). This
    wrapper is always the innermost wrapper in the table-function emit
    path, so validating here happens exactly once per emit regardless of
    whether :class:`_FilteringOutputCollector` is also in the stack.
    """

    __slots__ = (
        "_inner",
        "_supports_batch_index",
        "_partition_fields",
        "_partition_kind",
        "total_rows",
        "total_bytes",
    )

    def __init__(
        self,
        inner: OutputCollector,
        supports_batch_index: bool = False,
        partition_fields: list[pa.Field[Any]] | None = None,
        partition_kind: PartitionKind = PartitionKind.NOT_PARTITIONED,
    ) -> None:
        self._inner = inner
        self._supports_batch_index = supports_batch_index
        # Pre-computed list of partition-annotated fields from the bind
        # schema; empty when the function did not opt in to PartitionColumns.
        self._partition_fields = partition_fields or []
        self._partition_kind = partition_kind
        self.total_rows = 0
        self.total_bytes = 0

    def emit(
        self,
        batch: pa.RecordBatch,
        batch_index: int | None = None,
        partition_values: dict[str, tuple[pa.Scalar[Any], pa.Scalar[Any]]] | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None:
        merged_metadata = _merge_batch_index(
            supports_batch_index=self._supports_batch_index,
            batch_index=batch_index,
            metadata=metadata,
        )
        merged_metadata = _merge_partition_values(
            partition_fields=self._partition_fields,
            partition_kind=self._partition_kind,
            batch=batch,
            partition_values=partition_values,
            metadata=merged_metadata,
        )
        self.total_rows += batch.num_rows
        self.total_bytes += _batch_bytes(batch)
        if merged_metadata is None:
            self._inner.emit(batch)
        else:
            self._inner.emit(batch, metadata=merged_metadata)

    @property
    def finished(self) -> bool:
        return self._inner.finished

    @property
    def output_schema(self) -> pa.Schema:
        return self._inner.output_schema

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


@dataclass
class TableProducerState(ProducerState):
    """Producer state for table function streams.

    Calls ``TableFunctionGenerator.process()`` per tick. Each ``produce()``
    call delegates to the function's process method which uses ``out`` directly.

    When ``auto_apply_filters`` is enabled on the function class, pushdown
    filters from the init request are automatically applied to each output
    batch after ``process()`` produces it.

    ``_init_call`` and ``_init_response`` are serialized into the state token
    so they survive HTTP round-trips.  Transient fields are restored via
    ``rehydrate()``.

    ``_user_state`` is serialized when it is an ``ArrowSerializableDataclass``,
    allowing iteration state to survive HTTP round-trips.  When the state type
    is not serializable, it falls back to ``initial_state()`` on rehydration.

    """

    _init_call: Annotated[InitRequest, ArrowType(pa.binary())] = field(default=None, repr=False)  # type: ignore[assignment]
    _init_response: Annotated[GlobalInitResponse, ArrowType(pa.binary())] = field(default=None, repr=False)  # type: ignore[assignment]
    _user_state_bytes: bytes | None = field(default=None, repr=False)
    _func_cls: Annotated[type[TableFunctionGenerator[Any]], Transient()] = field(default=None, repr=False)  # type: ignore[assignment]
    _params: Annotated[ProcessParams[Any], Transient()] = field(default=None, repr=False)  # type: ignore[arg-type]
    _user_state: Annotated[Any, Transient()] = field(default=None, repr=False)
    _pushdown_filters: Annotated[Any, Transient()] = field(default=None, repr=False)  # PushdownFilters | None
    _auto_apply: Annotated[bool, Transient()] = field(default=False, repr=False)
    _vgi_tracer: Annotated[VgiTracer, Transient()] = field(default_factory=get_noop_tracer, repr=False)

    def __post_init__(self) -> None:
        """Resolve pushdown filters if auto_apply_filters is enabled."""
        if self._func_cls is not None and self._func_cls._should_auto_apply_filters():
            self._auto_apply = True
            init_call = self._params.init_call if self._params is not None else None
            if init_call is not None and init_call.pushdown_filters is not None:
                self._pushdown_filters = self._func_cls.pushdown_filters(
                    init_call.pushdown_filters,
                    join_keys=init_call.join_keys,
                )

    def _to_row_dict(self) -> dict[str, object]:
        """Serialize _user_state into _user_state_bytes before standard serialization."""
        if self._user_state is not None and isinstance(self._user_state, ArrowSerializableDataclass):
            self._user_state_bytes = self._user_state.serialize_to_bytes()
        return super()._to_row_dict()

    def rehydrate(self, implementation: object) -> None:
        """Restore transient fields from serialized init data."""
        from vgi.worker import Worker

        worker: Worker = implementation  # type: ignore[assignment]
        func_cls = worker._resolve_function(self._init_call.bind_call)
        assert issubclass(func_cls, TableFunctionGenerator)
        self._func_cls = func_cls
        self._vgi_tracer = worker._vgi_tracer
        proj_ids = _effective_projection_ids(func_cls, self._init_call.projection_ids)
        output_schema = project_schema(proj_ids, self._init_call.output_schema)
        self._params = ProcessParams(
            args=func_cls._parse_arguments(func_cls.FunctionArguments, self._init_call.bind_call.arguments),
            init_call=self._init_call,
            init_response=self._init_response,
            output_schema=output_schema,
            settings=_batch_to_scalar_dict(self._init_call.bind_call.settings),
            secrets=SecretsAccessor(self._init_call.bind_call.secrets).to_dict(),
            storage=BoundStorage(
                func_cls.storage, self._init_response.execution_id, request=self._init_call, auth=None
            ),
        )
        # Restore _user_state from serialized bytes if available
        if self._user_state_bytes is not None:
            state_type = _resolve_state_type(func_cls)
            if state_type is not None:
                self._user_state = state_type.deserialize_from_bytes(self._user_state_bytes)
                _log.debug("Restored user state from token: %s", type(self._user_state).__name__)
            else:
                _log.debug("State type not serializable, falling back to initial_state()")
                self._user_state = func_cls.initial_state(self._params)
        else:
            self._user_state = func_cls.initial_state(self._params)
        # Re-derive pushdown filters (triggers same logic as __post_init__)
        if func_cls._should_auto_apply_filters():
            self._auto_apply = True
            if self._init_call.pushdown_filters is not None:
                self._pushdown_filters = func_cls.pushdown_filters(
                    self._init_call.pushdown_filters,
                    join_keys=self._init_call.join_keys,
                )

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Process tick batch — check for dynamic filter updates, then produce."""
        if input.custom_metadata is not None:
            encoded = input.custom_metadata.get(b"vgi_pushdown_filters")
            if encoded is not None:
                self._update_filters_from_metadata(encoded)
        self.produce(out, ctx)

    def _update_filters_from_metadata(self, encoded_filters: bytes) -> None:
        """Decode and apply dynamic filter update from tick metadata."""
        import base64

        from vgi.table_filter_pushdown import deserialize_filters

        try:
            filter_bytes = base64.b64decode(encoded_filters)
            table = pa.ipc.open_stream(filter_bytes).read_all()
            if table.num_rows > 0:
                filter_batch = table.to_batches()[0]
                new_filters = deserialize_filters(filter_batch)
                self._pushdown_filters = new_filters
        except Exception:
            _log.warning("Failed to deserialize dynamic filter from tick metadata", exc_info=True)

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce the next output batch from the table function."""
        params = dataclasses.replace(
            self._params,
            auth_context=ctx.auth,
            current_pushdown_filters=self._pushdown_filters,
        )
        timer = _timed_exchange(
            self._vgi_tracer,
            "vgi.execute.table",
            self._init_call.bind_call.function_name,
            self._init_call.bind_call.function_type.value,
            self._init_response.execution_id,
        )
        with timer:
            tracking_out = _TrackingOutputCollector(
                out,
                supports_batch_index=self._func_cls._supports_batch_index(),
                partition_fields=_partition_fields_from_schema(self._init_call.output_schema),
                partition_kind=self._func_cls._partition_kind(),
            )
            if self._auto_apply and self._pushdown_filters is not None:
                filtered_out = _FilteringOutputCollector(tracking_out, self._func_cls, self._pushdown_filters)
                self._func_cls.process(params, self._user_state, filtered_out)  # type: ignore[arg-type]
                filtered_out.propagate()
            else:
                self._func_cls.process(params, self._user_state, tracking_out)  # type: ignore[arg-type]
            timer.record(
                output_rows=tracking_out.total_rows,
                output_bytes=tracking_out.total_bytes,
            )

    def on_cancel(self, ctx: CallContext) -> None:
        """Forward cancel signal to the user function's classmethod."""
        if self._func_cls is None or self._params is None:
            return
        params = dataclasses.replace(self._params, auth_context=ctx.auth)
        try:
            self._func_cls.on_cancel(params, self._user_state)
        except Exception:
            _log.debug("on_cancel hook raised", exc_info=True)


@dataclass
class TableInOutExchangeState(ExchangeState):
    """Exchange state for table-in-out function streams (INPUT phase).

    Calls ``TableInOutGenerator.process()`` per input batch. Each
    ``exchange()`` call sends one input batch and receives one output batch.

    When ``auto_apply_filters`` is enabled, pushdown filters from the init
    request are automatically applied to each output batch.

    ``_init_call`` and ``_init_response`` are serialized into the state token
    so they survive HTTP round-trips.  Transient fields are restored via
    ``rehydrate()``.

    ``_user_state`` is serialized when it is an ``ArrowSerializableDataclass``,
    allowing iteration state to survive HTTP round-trips.

    """

    _init_call: Annotated[InitRequest, ArrowType(pa.binary())] = field(default=None, repr=False)  # type: ignore[assignment]
    _init_response: Annotated[GlobalInitResponse, ArrowType(pa.binary())] = field(default=None, repr=False)  # type: ignore[assignment]
    _user_state_bytes: bytes | None = field(default=None, repr=False)
    _func_cls: Annotated[type[TableInOutGenerator[Any]], Transient()] = field(default=None, repr=False)  # type: ignore[assignment]
    _params: Annotated[ProcessParams[Any], Transient()] = field(default=None, repr=False)  # type: ignore[arg-type]
    _user_state: Annotated[Any, Transient()] = field(default=None, repr=False)
    _pushdown_filters: Annotated[Any, Transient()] = field(default=None, repr=False)  # PushdownFilters | None
    _auto_apply: Annotated[bool, Transient()] = field(default=False, repr=False)
    _vgi_tracer: Annotated[VgiTracer, Transient()] = field(default_factory=get_noop_tracer, repr=False)

    def __post_init__(self) -> None:
        """Resolve pushdown filters if auto_apply_filters is enabled."""
        if self._func_cls is not None and self._func_cls._should_auto_apply_filters():
            self._auto_apply = True
            init_call = self._params.init_call if self._params is not None else None
            if init_call is not None and init_call.pushdown_filters is not None:
                self._pushdown_filters = self._func_cls.pushdown_filters(
                    init_call.pushdown_filters,
                    join_keys=init_call.join_keys,
                )

    def _to_row_dict(self) -> dict[str, object]:
        """Serialize _user_state into _user_state_bytes before standard serialization."""
        if self._user_state is not None and isinstance(self._user_state, ArrowSerializableDataclass):
            self._user_state_bytes = self._user_state.serialize_to_bytes()
        return super()._to_row_dict()

    def rehydrate(self, implementation: object) -> None:
        """Restore transient fields from serialized init data."""
        from vgi.worker import Worker

        worker: Worker = implementation  # type: ignore[assignment]
        func_cls = worker._resolve_function(self._init_call.bind_call)
        assert issubclass(func_cls, TableInOutGenerator)
        self._func_cls = func_cls
        self._vgi_tracer = worker._vgi_tracer
        proj_ids = _effective_projection_ids(func_cls, self._init_call.projection_ids)
        output_schema = project_schema(proj_ids, self._init_call.output_schema)
        self._params = ProcessParams(
            args=func_cls._parse_arguments(func_cls.FunctionArguments, self._init_call.bind_call.arguments),
            init_call=self._init_call,
            init_response=self._init_response,
            output_schema=output_schema,
            settings=_batch_to_scalar_dict(self._init_call.bind_call.settings),
            secrets=SecretsAccessor(self._init_call.bind_call.secrets).to_dict(),
            storage=BoundStorage(
                func_cls.storage, self._init_response.execution_id, request=self._init_call, auth=None
            ),
        )
        # Restore _user_state from serialized bytes if available
        if self._user_state_bytes is not None:
            state_type = _resolve_state_type(func_cls)
            if state_type is not None:
                self._user_state = state_type.deserialize_from_bytes(self._user_state_bytes)
            else:
                self._user_state = func_cls.initial_state(self._params)
        else:
            self._user_state = func_cls.initial_state(self._params)
        if func_cls._should_auto_apply_filters():
            self._auto_apply = True
            if self._init_call.pushdown_filters is not None:
                self._pushdown_filters = func_cls.pushdown_filters(
                    self._init_call.pushdown_filters,
                    join_keys=self._init_call.join_keys,
                )

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Process one input batch through the table-in-out function."""
        params = dataclasses.replace(self._params, auth_context=ctx.auth)
        timer = _timed_exchange(
            self._vgi_tracer,
            "vgi.execute.table_in_out",
            self._init_call.bind_call.function_name,
            self._init_call.bind_call.function_type.value,
            self._init_response.execution_id,
        )
        with timer:
            tracking_out = _TrackingOutputCollector(
                out,
                supports_batch_index=self._func_cls._supports_batch_index(),
                partition_fields=_partition_fields_from_schema(self._init_call.output_schema),
                partition_kind=self._func_cls._partition_kind(),
            )
            if self._auto_apply and self._pushdown_filters is not None:
                filtered_out = _FilteringOutputCollector(tracking_out, self._func_cls, self._pushdown_filters)
                self._func_cls.process(params, self._user_state, input.batch, filtered_out)  # type: ignore[arg-type]
                filtered_out.propagate()
            else:
                self._func_cls.process(params, self._user_state, input.batch, tracking_out)  # type: ignore[arg-type]
            timer.record(
                input_rows=input.batch.num_rows,
                output_rows=tracking_out.total_rows,
                input_bytes=_batch_bytes(input.batch),
                output_bytes=tracking_out.total_bytes,
            )

    def on_cancel(self, ctx: CallContext) -> None:
        """Forward cancel signal to the user function's classmethod."""
        if self._func_cls is None or self._params is None:
            return
        params = dataclasses.replace(self._params, auth_context=ctx.auth)
        try:
            self._func_cls.on_cancel(params, self._user_state)
        except Exception:
            _log.debug("on_cancel hook raised", exc_info=True)


@dataclass
class TableInOutFinalizeState(ProducerState):
    """Producer state for table-in-out function finalize streams.

    Emits pre-computed finalize batches one per ``produce()`` call.
    Calls ``out.finish()`` when all batches have been emitted.

    """

    _batches: Annotated[list[pa.RecordBatch], Transient()] = field(default_factory=list, repr=False)
    _index: Annotated[int, Transient()] = field(default=0, repr=False)

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Emit the next finalize batch, or finish if done."""
        if self._index >= len(self._batches):
            out.finish()
            return
        out.emit(self._batches[self._index])
        self._index += 1


# Type alias for the union of all stream state variants produced by init().
# vgi-rpc resolves this union using a method-local numeric tag in HTTP state
# tokens, so state recovery does not depend on Python class names.
ProcessState = ScalarExchangeState | TableProducerState | TableInOutExchangeState | TableInOutFinalizeState


# ---------------------------------------------------------------------------
# Aggregate Function RPC Types (all unary request/response)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateBindRequest(ArrowSerializableDataclass):
    """Request for aggregate_bind — resolve output schema."""

    function_name: str
    arguments: Annotated[Arguments, ArrowType(pa.binary())]
    input_schema: Annotated[pa.Schema | None, ArrowType(pa.binary())] = None
    settings: Annotated[pa.RecordBatch | None, ArrowType(pa.binary())] = None
    secrets: Annotated[pa.RecordBatch | None, ArrowType(pa.binary())] = None
    attach_opaque_data: bytes | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateBindResponse(ArrowSerializableDataclass):
    """Response from aggregate_bind."""

    output_schema: Annotated[pa.Schema, ArrowType(pa.binary())]
    execution_id: bytes


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateUpdateRequest(ArrowSerializableDataclass):
    """Request for aggregate_update — accumulate rows into per-group state."""

    function_name: str
    execution_id: bytes
    input_batch: bytes  # Full IPC stream bytes (schema + data + EOS)
    attach_opaque_data: bytes | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateUpdateResponse(ArrowSerializableDataclass):
    """Response from aggregate_update — empty ack."""

    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateCombineRequest(ArrowSerializableDataclass):
    """Request for aggregate_combine — merge source states into targets."""

    function_name: str
    execution_id: bytes
    merge_batch: bytes  # Full IPC stream bytes
    attach_opaque_data: bytes | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateCombineResponse(ArrowSerializableDataclass):
    """Response from aggregate_combine — empty ack."""

    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateFinalizeRequest(ArrowSerializableDataclass):
    """Request for aggregate_finalize — produce results for group_ids."""

    function_name: str
    execution_id: bytes
    group_ids_batch: bytes  # Full IPC stream bytes
    output_schema: Annotated[pa.Schema, ArrowType(pa.binary())]
    attach_opaque_data: bytes | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateFinalizeResponse(ArrowSerializableDataclass):
    """Response from aggregate_finalize — result batch as IPC stream bytes."""

    result_batch: bytes  # Full IPC stream bytes


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateDestructorRequest(ArrowSerializableDataclass):
    """Request for aggregate_destructor — best-effort state cleanup."""

    function_name: str
    execution_id: bytes
    group_ids_batch: bytes  # Full IPC stream bytes
    attach_opaque_data: bytes | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateDestructorResponse(ArrowSerializableDataclass):
    """Response from aggregate_destructor — empty ack."""

    pass


# ---------------------------------------------------------------------------
# Buffered Table Function RPC Types
# ---------------------------------------------------------------------------
# Sink+Source PhysicalOperator path. Bind and init reuse the existing bind +
# PerformInit(phase="BUFFERED_TABLE") RPCs; per-thread secondary workers
# attach to the coordinator's execution_id via FunctionConnectionParams.
# After bind+init, traffic uses the three RPCs below.


@dataclass(frozen=True, slots=True, kw_only=True)
class BufferedTableProcessRequest(ArrowSerializableDataclass):
    """Request for buffered_table_process — sink one batch into state_id.

    The C++ side serializes Sink chunks for this thread's worker. The worker
    loads state for ``(execution_id, state_id)``, calls
    ``cls.process(params, state, batch, NoOpCollector())``, persists. process
    must not emit non-empty batches — the source phase owns output.
    """

    function_name: str
    execution_id: bytes
    state_id: int  # int64; assigned by C++ atomic counter per Sink thread
    input_batch: bytes  # Full IPC stream bytes
    attach_opaque_data: bytes | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class BufferedTableProcessResponse(ArrowSerializableDataclass):
    """Response from buffered_table_process — empty ack.

    Note: in-band ``OutputCollector.client_log`` calls inside the user's
    process() route to the worker's local logger only (stderr) — they do
    not currently surface in DuckDB's ``duckdb_logs()`` view. To forward
    logs through, a future revision will carry log messages on the response
    schema. v1 known limitation.
    """

    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class BufferedTableCombineRequest(ArrowSerializableDataclass):
    """Request for buffered_table_combine — once-per-query end-of-input.

    Carries every state_id observed across all Sink threads. The worker
    runs ``cls.combine(state_ids, params)`` and returns the
    ``finalize_state_ids`` partition keys that the source phase will iterate.
    Workers may coordinate with peers here (e.g. via shared BoundStorage).
    """

    function_name: str
    execution_id: bytes
    state_ids: Annotated[list[int], ArrowType(pa.list_(pa.int64()))]
    attach_opaque_data: bytes | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class BufferedTableCombineResponse(ArrowSerializableDataclass):
    """Response from buffered_table_combine — opaque partition keys."""

    finalize_state_ids: Annotated[list[int], ArrowType(pa.list_(pa.int64()))]


@dataclass(frozen=True, slots=True, kw_only=True)
class BufferedTableFinalizeRequest(ArrowSerializableDataclass):
    """Request for buffered_table_finalize — pull one batch for a partition.

    The C++ source phase calls this repeatedly with the same
    ``finalize_state_id`` while the response's ``has_more`` is True; when
    has_more is False, the partition is exhausted and the source phase moves
    to the next ``finalize_state_id`` from the queue.
    """

    function_name: str
    execution_id: bytes
    finalize_state_id: int
    attach_opaque_data: bytes | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class BufferedTableFinalizeResponse(ArrowSerializableDataclass):
    """Response from buffered_table_finalize — one IPC batch + has_more."""

    output_batch: bytes  # Full IPC stream bytes — one batch
    has_more: bool


# ---------------------------------------------------------------------------
# Aggregate Window Function RPC Types
# ---------------------------------------------------------------------------
# Optional windowed-aggregate protocol: ``aggregate_window_init`` ships the
# partition once, ``aggregate_window`` evaluates one output row at a time
# (per-call flushing — DuckDB's window callback API has no per-Evaluate hook),
# ``aggregate_window_destructor`` evicts the partition from storage.


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateWindowInitRequest(ArrowSerializableDataclass):
    """Request for aggregate_window_init — ship a partition to the worker."""

    function_name: str
    execution_id: bytes
    partition_id: int
    row_count: int
    partition_batch: bytes  # Full IPC stream bytes (partition's input columns)
    output_schema: Annotated[pa.Schema, ArrowType(pa.binary())]
    filter_mask: bytes  # Packed-bit bool array, length == row_count
    frame_stats: bytes  # 4× int64: ((begin_delta,end_delta),(begin_delta,end_delta))
    all_valid: bytes  # 1 byte per input column
    attach_opaque_data: bytes | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateWindowInitResponse(ArrowSerializableDataclass):
    """Response from aggregate_window_init — empty ack."""

    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateWindowRequest(ArrowSerializableDataclass):
    """Request for aggregate_window — compute the aggregate for one output row.

    ``frame_starts`` and ``frame_ends`` are parallel arrays of length 1–3
    (one entry per subframe; 3 only for EXCLUDE TIES / EXCLUDE GROUP).
    """

    function_name: str
    execution_id: bytes
    partition_id: int
    rid: int
    frame_starts: list[int]
    frame_ends: list[int]
    attach_opaque_data: bytes | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateWindowResponse(ArrowSerializableDataclass):
    """Response from aggregate_window — one row RecordBatch with the scalar result."""

    result_batch: bytes  # Full IPC stream bytes (one row, output schema)


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateWindowDestructorRequest(ArrowSerializableDataclass):
    """Request for aggregate_window_destructor — evict a partition from storage."""

    function_name: str
    execution_id: bytes
    partition_id: int
    attach_opaque_data: bytes | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateWindowDestructorResponse(ArrowSerializableDataclass):
    """Response from aggregate_window_destructor — empty ack."""

    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateWindowBatchRequest(ArrowSerializableDataclass):
    """Request for aggregate_window_batch — compute ``count`` output rows in one RPC.

    ``frames_per_row[i]`` gives the subframe cardinality for output row ``i``
    (1 normally, 2–3 for EXCLUDE TIES / EXCLUDE GROUP). ``frame_starts`` and
    ``frame_ends`` are flat arrays of length ``sum(frames_per_row)``.
    """

    function_name: str
    execution_id: bytes
    partition_id: int
    row_idx: int
    count: int
    frames_per_row: list[int]
    frame_starts: list[int]
    frame_ends: list[int]
    attach_opaque_data: bytes | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateWindowBatchResponse(ArrowSerializableDataclass):
    """Response from aggregate_window_batch — count-row RecordBatch."""

    result_batch: bytes  # Full IPC stream bytes (count rows, output schema)


# ---------------------------------------------------------------------------
# Aggregate Streaming-Partitioned RPC Types
# ---------------------------------------------------------------------------
# Streaming protocol for partitioned aggregates whose state compresses
# heavily relative to input rows (e.g. portfolio_agg's positions dict vs
# millions of fills). DuckDB streams input chunks to the worker; the worker
# maintains concurrent per-partition state in a hash map keyed by partition
# key, dispatches each row to its partition's state, and emits one snapshot
# per input row. No DuckDB-side partition materialisation. Cumulative
# semantics only (UNBOUNDED PRECEDING -> CURRENT ROW); other frame shapes
# fall back to the non-streaming path.


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateStreamingOpenRequest(ArrowSerializableDataclass):
    """Request for aggregate_streaming_open — start a streaming session.

    The worker resolves the function, calls ``streaming_open`` to build the
    cross-partition global state, and returns an ``execution_id`` that
    subsequent chunk/close calls reference.

    ``input_schema`` is the schema of every chunk shipped via
    ``streaming_chunk``. The first ``partition_key_count`` columns are
    partition-key columns (used by the worker to dispatch rows to the right
    per-partition state). The next ``order_key_count`` columns are
    order-key columns (informational; the worker may verify monotonicity).
    Remaining columns are the function's value arguments, in declaration
    order.
    """

    function_name: str
    arguments: Annotated[Arguments, ArrowType(pa.binary())]
    input_schema: Annotated[pa.Schema, ArrowType(pa.binary())]
    partition_key_count: int
    order_key_count: int
    output_schema: Annotated[pa.Schema, ArrowType(pa.binary())]
    settings: Annotated[pa.RecordBatch | None, ArrowType(pa.binary())] = None
    secrets: Annotated[pa.RecordBatch | None, ArrowType(pa.binary())] = None
    attach_opaque_data: bytes | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateStreamingOpenResponse(ArrowSerializableDataclass):
    """Response from aggregate_streaming_open — session token."""

    execution_id: bytes


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateStreamingChunkRequest(ArrowSerializableDataclass):
    """Request for aggregate_streaming_chunk — process one input chunk.

    ``input_batch`` schema must match the ``input_schema`` agreed at
    ``streaming_open``. The worker iterates rows, dispatches to per-partition
    state by the partition-key columns, applies the function's update logic,
    and returns a same-length output array.
    """

    function_name: str
    execution_id: bytes
    input_batch: bytes  # Full IPC stream bytes
    attach_opaque_data: bytes | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateStreamingChunkResponse(ArrowSerializableDataclass):
    """Response from aggregate_streaming_chunk — same-length output batch."""

    result_batch: bytes  # Full IPC stream bytes (one row per input row)


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateStreamingCloseRequest(ArrowSerializableDataclass):
    """Request for aggregate_streaming_close — end the session, free state."""

    function_name: str
    execution_id: bytes
    attach_opaque_data: bytes | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class AggregateStreamingCloseResponse(ArrowSerializableDataclass):
    """Response from aggregate_streaming_close — empty ack."""

    pass


# ---------------------------------------------------------------------------
# VGI Protocol
# ---------------------------------------------------------------------------


class VgiProtocol(Protocol):
    """VGI wire protocol definition for vgi_rpc.

    Methods:
    - ``bind()`` / ``init()``: Function invocation protocol (scalar/table)
    - ``aggregate_*``: Aggregate function RPC methods (all unary)
    - ``catalog_*``: ~35 typed catalog interface methods

    ``vgi_rpc.RpcServer(VgiProtocol, worker)`` handles serialization,
    dispatching, error propagation, and stream lifecycle.

    """

    def bind(self, request: BindRequest) -> BindResponse:
        """Resolve output schema and validate arguments."""
        ...

    def init(self, request: InitRequest) -> Stream[ProcessState, GlobalInitResponse]:
        """Initialize a function execution and return a processing stream."""
        ...

    def table_function_cardinality(self, request: TableFunctionCardinalityRequest) -> TableCardinality:
        """Estimate the cardinality of a table function's output."""
        ...

    def table_function_statistics(self, request: TableFunctionStatisticsRequest) -> bytes | None:
        """Return per-column statistics for a table function's output.

        Returns IPC bytes of a RecordBatch with sparse-union min/max columns
        (same shape as catalog_table_column_statistics_get), or None if no
        statistics are available.
        """
        ...

    def table_function_dynamic_to_string(
        self, request: TableFunctionDynamicToStringRequest
    ) -> TableFunctionDynamicToStringResponse:
        """Return user-defined diagnostics for EXPLAIN ANALYZE Extra Info.

        Fired once per parallel scan thread at end-of-stream. The function
        class is responsible for persisting any diagnostics it wants to
        report and retrieving them by ``global_execution_id`` here.

        Best-effort: must not raise. The dispatcher catches exceptions and
        returns an empty response so EXPLAIN ANALYZE never breaks the query.
        """
        ...

    # ========== Aggregate Function Methods (all unary) ==========

    def aggregate_bind(self, request: AggregateBindRequest) -> AggregateBindResponse:
        """Bind an aggregate function, return output schema and execution_id."""
        ...

    def aggregate_update(self, request: AggregateUpdateRequest) -> AggregateUpdateResponse:
        """Accumulate rows from a DataChunk into per-group state."""
        ...

    def aggregate_combine(self, request: AggregateCombineRequest) -> AggregateCombineResponse:
        """Merge source states into target states."""
        ...

    def aggregate_finalize(self, request: AggregateFinalizeRequest) -> AggregateFinalizeResponse:
        """Produce results for a chunk of group_ids."""
        ...

    def aggregate_destructor(self, request: AggregateDestructorRequest) -> AggregateDestructorResponse:
        """Best-effort cleanup of aggregate states. Must not raise."""
        ...

    # ========== Buffered Table Function Methods (Sink+Source path) ==========

    def buffered_table_process(self, request: BufferedTableProcessRequest) -> BufferedTableProcessResponse:
        """Sink one input batch into per-(execution_id, state_id) state."""
        ...

    def buffered_table_combine(self, request: BufferedTableCombineRequest) -> BufferedTableCombineResponse:
        """Once-per-query end-of-input signal. Returns finalize_state_ids."""
        ...

    def buffered_table_finalize(self, request: BufferedTableFinalizeRequest) -> BufferedTableFinalizeResponse:
        """Pull one batch from a finalize_state_id's generator."""
        ...

    # ========== Aggregate Window Function Methods (optional, all unary) ==========

    def aggregate_window_init(self, request: AggregateWindowInitRequest) -> AggregateWindowInitResponse:
        """Ship a partition to the worker for windowed aggregation."""
        ...

    def aggregate_window(self, request: AggregateWindowRequest) -> AggregateWindowResponse:
        """Compute an aggregate value for one output row of the window."""
        ...

    def aggregate_window_destructor(
        self, request: AggregateWindowDestructorRequest
    ) -> AggregateWindowDestructorResponse:
        """Evict a cached partition from storage."""
        ...

    def aggregate_window_batch(self, request: AggregateWindowBatchRequest) -> AggregateWindowBatchResponse:
        """Compute ``count`` window output rows in one batched RPC."""
        ...

    # ========== Aggregate Streaming-Partitioned Methods (optional, all unary) ==========

    def aggregate_streaming_open(self, request: AggregateStreamingOpenRequest) -> AggregateStreamingOpenResponse:
        """Start a streaming-partitioned aggregate session."""
        ...

    def aggregate_streaming_chunk(self, request: AggregateStreamingChunkRequest) -> AggregateStreamingChunkResponse:
        """Process one input chunk; returns one output row per input row."""
        ...

    def aggregate_streaming_close(self, request: AggregateStreamingCloseRequest) -> AggregateStreamingCloseResponse:
        """End the streaming session, free per-session state."""
        ...

    # ========== Catalog - Discovery ==========

    def catalog_catalogs(self) -> CatalogsResponse:
        """List available catalog names."""
        ...

    # ========== Catalog - Lifecycle ==========

    def catalog_attach(self, request: CatalogAttachRequest) -> CatalogAttachResult:
        """Attach to a catalog with options."""
        ...

    def catalog_detach(self, attach_opaque_data: bytes) -> None:
        """Detach from a catalog."""
        ...

    def catalog_create(self, request: CatalogCreateRequest) -> None:
        """Create a new catalog."""
        ...

    def catalog_drop(self, name: str) -> None:
        """Drop a catalog."""
        ...

    def catalog_version(
        self, attach_opaque_data: bytes, transaction_opaque_data: bytes | None = None
    ) -> CatalogVersionResponse:
        """Get the current catalog version."""
        ...

    # ========== Catalog - Transactions ==========

    def catalog_transaction_begin(self, attach_opaque_data: bytes) -> TransactionBeginResponse:
        """Begin a new transaction."""
        ...

    def catalog_transaction_commit(self, attach_opaque_data: bytes, transaction_opaque_data: bytes) -> None:
        """Commit a transaction."""
        ...

    def catalog_transaction_rollback(self, attach_opaque_data: bytes, transaction_opaque_data: bytes) -> None:
        """Rollback a transaction."""
        ...

    # ========== Catalog - Schemas ==========

    def catalog_schemas(
        self, attach_opaque_data: bytes, transaction_opaque_data: bytes | None = None
    ) -> SchemasResponse:
        """List schemas in the catalog."""
        ...

    def catalog_schema_get(
        self, attach_opaque_data: bytes, name: str, transaction_opaque_data: bytes | None = None
    ) -> SchemasResponse:
        """Get information about a schema. Returns 0 or 1 items."""
        ...

    def catalog_schema_create(
        self,
        attach_opaque_data: bytes,
        name: str,
        on_conflict: OnConflict = OnConflict.ERROR,
        comment: str | None = None,
        tags: dict[str, str] | None = None,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Create a new schema."""
        ...

    def catalog_schema_drop(
        self,
        attach_opaque_data: bytes,
        name: str,
        ignore_not_found: bool = False,
        cascade: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Drop a schema."""
        ...

    def catalog_schema_contents_tables(
        self,
        attach_opaque_data: bytes,
        name: str,
        transaction_opaque_data: bytes | None = None,
    ) -> TablesResponse:
        """List tables in a schema."""
        ...

    def catalog_schema_contents_views(
        self,
        attach_opaque_data: bytes,
        name: str,
        transaction_opaque_data: bytes | None = None,
    ) -> ViewsResponse:
        """List views in a schema."""
        ...

    def catalog_schema_contents_functions(
        self,
        attach_opaque_data: bytes,
        name: str,
        type: SchemaObjectType,
        transaction_opaque_data: bytes | None = None,
    ) -> FunctionsResponse:
        """List functions in a schema (scalar or table)."""
        ...

    # ========== Catalog - Tables ==========

    def catalog_table_get(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
        transaction_opaque_data: bytes | None = None,
    ) -> TablesResponse:
        """Get information about a table. Returns 0 or 1 items."""
        ...

    def catalog_table_create(self, request: TableCreateRequest) -> None:
        """Create a new table."""
        ...

    def catalog_table_drop(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        ignore_not_found: bool = False,
        cascade: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Drop a table."""
        ...

    def catalog_table_scan_function_get(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
        transaction_opaque_data: bytes | None = None,
    ) -> bytes:
        """Get the scan function for a table. Returns ScanFunctionResult as IPC bytes."""
        ...

    def catalog_table_column_statistics_get(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        transaction_opaque_data: bytes | None = None,
    ) -> bytes | None:
        """Get column statistics for a table.

        Returns IPC bytes of a RecordBatch with sparse-union min/max columns,
        or None if statistics are not available.
        """
        ...

    def catalog_table_insert_function_get(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        transaction_opaque_data: bytes | None = None,
    ) -> bytes:
        """Get the insert function for a table. Returns WriteFunctionResult as IPC bytes."""
        ...

    def catalog_table_update_function_get(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        transaction_opaque_data: bytes | None = None,
    ) -> bytes:
        """Get the update function for a table. Returns WriteFunctionResult as IPC bytes."""
        ...

    def catalog_table_delete_function_get(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        transaction_opaque_data: bytes | None = None,
    ) -> bytes:
        """Get the delete function for a table. Returns WriteFunctionResult as IPC bytes."""
        ...

    def catalog_table_comment_set(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        comment: str | None = None,
        ignore_not_found: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Set or clear the comment on a table."""
        ...

    def catalog_table_column_comment_set(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        comment: str | None = None,
        ignore_not_found: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Set or clear the comment on a table column."""
        ...

    def catalog_table_rename(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        new_name: str,
        ignore_not_found: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Rename a table."""
        ...

    def catalog_table_column_add(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        column_definition: bytes,
        ignore_not_found: bool = False,
        if_column_not_exists: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Add a new column to a table."""
        ...

    def catalog_table_column_drop(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool = False,
        if_column_exists: bool = False,
        cascade: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Drop a column from a table."""
        ...

    def catalog_table_column_rename(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        new_column_name: str,
        ignore_not_found: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Rename a column."""
        ...

    def catalog_table_column_default_set(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        expression: str,
        ignore_not_found: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Set the default value expression for a column."""
        ...

    def catalog_table_column_default_drop(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Remove the default value from a column."""
        ...

    def catalog_table_column_type_change(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        column_definition: bytes,
        expression: str | None = None,
        ignore_not_found: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Change the type of a column."""
        ...

    def catalog_table_not_null_drop(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Remove NOT NULL constraint from a column."""
        ...

    def catalog_table_not_null_set(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Add NOT NULL constraint to a column."""
        ...

    # ========== Catalog - Views ==========

    def catalog_view_get(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        transaction_opaque_data: bytes | None = None,
    ) -> ViewsResponse:
        """Get information about a view. Returns 0 or 1 items."""
        ...

    def catalog_view_create(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        definition: str,
        on_conflict: OnConflict,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Create a new view."""
        ...

    def catalog_view_drop(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        ignore_not_found: bool = False,
        cascade: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Drop a view."""
        ...

    def catalog_view_rename(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        new_name: str,
        ignore_not_found: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Rename a view."""
        ...

    def catalog_view_comment_set(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        comment: str | None = None,
        ignore_not_found: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Set or clear the comment on a view."""
        ...

    # ========== Catalog - Macros ===========

    def catalog_macro_get(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        transaction_opaque_data: bytes | None = None,
    ) -> MacrosResponse:
        """Get information about a macro. Returns 0 or 1 items."""
        ...

    def catalog_macro_create(self, request: MacroCreateRequest) -> None:
        """Create a new macro."""
        ...

    def catalog_macro_drop(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        ignore_not_found: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Drop a macro."""
        ...

    def catalog_schema_contents_macros(
        self,
        attach_opaque_data: bytes,
        name: str,
        type: SchemaObjectType,
        transaction_opaque_data: bytes | None = None,
    ) -> MacrosResponse:
        """List macros in a schema (scalar or table)."""
        ...

    # ========== Catalog - Indexes ==========

    def catalog_index_get(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        transaction_opaque_data: bytes | None = None,
    ) -> IndexesResponse:
        """Get information about an index. Returns 0 or 1 items."""
        ...

    def catalog_index_create(self, request: IndexCreateRequest) -> None:
        """Create a new index."""
        ...

    def catalog_index_drop(
        self,
        attach_opaque_data: bytes,
        schema_name: str,
        name: str,
        ignore_not_found: bool = False,
        cascade: bool = False,
        transaction_opaque_data: bytes | None = None,
    ) -> None:
        """Drop an index."""
        ...

    def catalog_schema_contents_indexes(
        self,
        attach_opaque_data: bytes,
        name: str,
        transaction_opaque_data: bytes | None = None,
    ) -> IndexesResponse:
        """List indexes in a schema."""
        ...
