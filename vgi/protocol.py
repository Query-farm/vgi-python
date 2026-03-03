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

import logging
from dataclasses import dataclass, field
from typing import Annotated, Any, Protocol, get_args, get_origin

import pyarrow as pa
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
    FunctionInfo,
    MacroInfo,
    MacroType,
    OnConflict,
    SchemaInfo,
    SchemaObjectType,
    TableInfo,
    ViewInfo,
)
from vgi.function_storage import BoundStorage
from vgi.invocation import BindResponse, FunctionType, GlobalInitResponse
from vgi.scalar_function import ScalarFunctionGenerator
from vgi.table_function import (
    ProcessParams,
    TableCardinality,
    TableFunctionBase,
    TableFunctionGenerator,
    TableInOutFunctionInitPhase,
    _batch_to_scalar_dict,
    _batch_to_secret_dict,
    project_schema,
)
from vgi.table_in_out_function import TableInOutGenerator

__all__ = [
    "BindRequest",
    "CatalogAttachRequest",
    "CatalogCreateRequest",
    "CatalogsResponse",
    "MacroCreateRequest",
    "MacrosResponse",
    "TableCreateRequest",
    "CatalogVersionResponse",
    "FunctionsResponse",
    "InitRequest",
    "ProcessState",
    "ScalarExchangeState",
    "SchemasResponse",
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
    attach_id: bytes | None = None
    transaction_id: bytes | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class InitRequest(ArrowSerializableDataclass):
    """Consolidated init request for all function types.

    For secondary init requests, ``execution_id`` and ``init_opaque_data``
    are set; use :attr:`is_secondary` to distinguish.

    """

    # Core (always present)
    bind_call: BindRequest
    output_schema: Annotated[pa.Schema, ArrowType(pa.binary())]
    bind_opaque_data: Annotated[ArrowSerializableDataclass | None, ArrowType(pa.binary())] = None

    # Table function extras (None for scalar)
    projection_ids: list[int] | None = None
    pushdown_filters: Annotated[pa.RecordBatch | None, ArrowType(pa.binary())] = None

    # Table-in-out extras
    phase: TableInOutFunctionInitPhase | None = None

    # Secondary init (None = global init, set = secondary)
    execution_id: bytes | None = None
    init_opaque_data: Annotated[ArrowSerializableDataclass | None, ArrowType(pa.binary())] = None

    @property
    def is_secondary(self) -> bool:
        """True if this is a secondary init request."""
        return self.execution_id is not None


@dataclass(frozen=True, slots=True, kw_only=True)
class TableFunctionCardinalityRequest(ArrowSerializableDataclass):
    """Consolidated request for table function cardinality."""

    bind_call: BindRequest
    bind_opaque_data: Annotated[ArrowSerializableDataclass | None, ArrowType(pa.binary())] = None


# ---------------------------------------------------------------------------
# Catalog request types (for methods with complex parameters)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogAttachRequest(ArrowSerializableDataclass):
    """Request for catalog_attach. Uses RecordBatch for mixed-type options."""

    name: str
    options: Annotated[pa.RecordBatch | None, ArrowType(pa.binary())] = None


@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogCreateRequest(ArrowSerializableDataclass):
    """Request for catalog_create. Uses RecordBatch for mixed-type options."""

    name: str
    on_conflict: OnConflict
    options: Annotated[pa.RecordBatch | None, ArrowType(pa.binary())] = None


@dataclass(frozen=True, slots=True, kw_only=True)
class TableCreateRequest(ArrowSerializableDataclass):
    """Request for catalog_table_create with complex constraint types."""

    attach_id: bytes
    schema_name: str
    name: str
    columns: bytes  # SerializedSchema
    on_conflict: OnConflict
    not_null_constraints: Annotated[list[int], ArrowType(pa.list_(pa.int32()))] = field(default_factory=list)
    unique_constraints: Annotated[list[list[int]], ArrowType(pa.list_(pa.list_(pa.int32())))] = field(
        default_factory=list
    )
    check_constraints: list[str] = field(default_factory=list)
    transaction_id: bytes | None = None


# ---------------------------------------------------------------------------
# Catalog response types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogsResponse(ArrowSerializableDataclass):
    """Response wrapping list[str] for catalog_catalogs()."""

    items: list[str]


@dataclass(frozen=True, slots=True, kw_only=True)
class CatalogVersionResponse(ArrowSerializableDataclass):
    """Response wrapping int for catalog_version()."""

    version: int


@dataclass(frozen=True, slots=True, kw_only=True)
class TransactionBeginResponse(ArrowSerializableDataclass):
    """Response wrapping optional TransactionId for catalog_transaction_begin()."""

    transaction_id: bytes | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SchemasResponse(ArrowSerializableDataclass):
    """Response wrapping list of SchemaInfo.

    Also used for schema_get (0 or 1 items) and schemas (0+ items).
    """

    items: Annotated[list[bytes], ArrowType(pa.list_(pa.binary()))]

    @staticmethod
    def from_schema_infos(infos: list[SchemaInfo]) -> SchemasResponse:
        """Create from a list of SchemaInfo objects."""
        return SchemasResponse(items=[info.serialize_to_bytes() for info in infos])

    @staticmethod
    def from_optional(info: SchemaInfo | None) -> SchemasResponse:
        """Create from an optional SchemaInfo (0 or 1 items)."""
        if info is None:
            return SchemasResponse(items=[])
        return SchemasResponse(items=[info.serialize_to_bytes()])

    def to_schema_infos(self) -> list[SchemaInfo]:
        """Deserialize items to SchemaInfo objects."""
        return [SchemaInfo.deserialize_from_bytes(b) for b in self.items]

    def to_optional(self) -> SchemaInfo | None:
        """Deserialize single optional item."""
        if not self.items:
            return None
        return SchemaInfo.deserialize_from_bytes(self.items[0])


@dataclass(frozen=True, slots=True, kw_only=True)
class TablesResponse(ArrowSerializableDataclass):
    """Response wrapping list of TableInfo.

    Used for schema_contents_tables (0+ items) and table_get (0 or 1 items).
    """

    items: Annotated[list[bytes], ArrowType(pa.list_(pa.binary()))]

    @staticmethod
    def from_table_infos(infos: list[TableInfo]) -> TablesResponse:
        """Create from a list of TableInfo objects."""
        return TablesResponse(items=[info.serialize_to_bytes() for info in infos])

    @staticmethod
    def from_optional(info: TableInfo | None) -> TablesResponse:
        """Create from an optional TableInfo (0 or 1 items)."""
        if info is None:
            return TablesResponse(items=[])
        return TablesResponse(items=[info.serialize_to_bytes()])

    def to_table_infos(self) -> list[TableInfo]:
        """Deserialize items to TableInfo objects."""
        return [TableInfo.deserialize_from_bytes(b) for b in self.items]

    def to_optional(self) -> TableInfo | None:
        """Deserialize single optional item."""
        if not self.items:
            return None
        return TableInfo.deserialize_from_bytes(self.items[0])


@dataclass(frozen=True, slots=True, kw_only=True)
class ViewsResponse(ArrowSerializableDataclass):
    """Response wrapping list of ViewInfo.

    Used for schema_contents_views (0+ items) and view_get (0 or 1 items).
    """

    items: Annotated[list[bytes], ArrowType(pa.list_(pa.binary()))]

    @staticmethod
    def from_view_infos(infos: list[ViewInfo]) -> ViewsResponse:
        """Create from a list of ViewInfo objects."""
        return ViewsResponse(items=[info.serialize_to_bytes() for info in infos])

    @staticmethod
    def from_optional(info: ViewInfo | None) -> ViewsResponse:
        """Create from an optional ViewInfo (0 or 1 items)."""
        if info is None:
            return ViewsResponse(items=[])
        return ViewsResponse(items=[info.serialize_to_bytes()])

    def to_view_infos(self) -> list[ViewInfo]:
        """Deserialize items to ViewInfo objects."""
        return [ViewInfo.deserialize_from_bytes(b) for b in self.items]

    def to_optional(self) -> ViewInfo | None:
        """Deserialize single optional item."""
        if not self.items:
            return None
        return ViewInfo.deserialize_from_bytes(self.items[0])


@dataclass(frozen=True, slots=True, kw_only=True)
class FunctionsResponse(ArrowSerializableDataclass):
    """Response wrapping list of FunctionInfo for schema_contents_functions."""

    items: Annotated[list[bytes], ArrowType(pa.list_(pa.binary()))]

    @staticmethod
    def from_function_infos(infos: list[FunctionInfo]) -> FunctionsResponse:
        """Create from a list of FunctionInfo objects."""
        return FunctionsResponse(items=[info.serialize_to_bytes() for info in infos])

    def to_function_infos(self) -> list[FunctionInfo]:
        """Deserialize items to FunctionInfo objects."""
        return [FunctionInfo.deserialize_from_bytes(b) for b in self.items]


@dataclass(frozen=True, slots=True, kw_only=True)
class MacrosResponse(ArrowSerializableDataclass):
    """Response wrapping list of MacroInfo.

    Used for schema_contents_macros (0+ items) and macro_get (0 or 1 items).
    """

    items: Annotated[list[bytes], ArrowType(pa.list_(pa.binary()))]

    @staticmethod
    def from_macro_infos(infos: list[MacroInfo]) -> MacrosResponse:
        """Create from a list of MacroInfo objects."""
        return MacrosResponse(items=[info.serialize_to_bytes() for info in infos])

    @staticmethod
    def from_optional(info: MacroInfo | None) -> MacrosResponse:
        """Create from an optional MacroInfo (0 or 1 items)."""
        if info is None:
            return MacrosResponse(items=[])
        return MacrosResponse(items=[info.serialize_to_bytes()])

    def to_macro_infos(self) -> list[MacroInfo]:
        """Deserialize items to MacroInfo objects."""
        return [MacroInfo.deserialize_from_bytes(b) for b in self.items]

    def to_optional(self) -> MacroInfo | None:
        """Deserialize single optional item."""
        if not self.items:
            return None
        return MacroInfo.deserialize_from_bytes(self.items[0])


@dataclass(frozen=True, slots=True, kw_only=True)
class MacroCreateRequest(ArrowSerializableDataclass):
    """Request for catalog_macro_create with RecordBatch for parameter defaults."""

    attach_id: bytes
    schema_name: str
    name: str
    macro_type: MacroType
    parameters: list[str]
    definition: str
    on_conflict: OnConflict
    parameter_default_values: Annotated[pa.RecordBatch | None, ArrowType(pa.binary())] = None
    transaction_id: bytes | None = None


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

    def rehydrate(self, implementation: object) -> None:
        """Restore ``_func_cls`` from the worker's function registry."""
        from vgi.worker import Worker

        worker: Worker = implementation  # type: ignore[assignment]
        self._func_cls = worker._resolve_function(self._init_call.bind_call)  # type: ignore[assignment]

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Process one input batch through the scalar function."""
        cls = self._func_cls
        output = cls.process(
            batch=input.batch,
            init_call=self._init_call,
            init_response=self._init_response,
            storage=BoundStorage(cls.storage, self._init_response.execution_id),
        )
        cls._validate_row_count(output, input.batch)
        out.emit(output)


_log = logging.getLogger(__name__)


def _resolve_state_type(func_cls: type) -> type[ArrowSerializableDataclass] | None:
    """Extract the TState type parameter from a TableFunctionGenerator or TableInOutGenerator.

    Walks the MRO looking for ``TableFunctionGenerator[TArgs, TState]`` or
    ``TableInOutGenerator[TArgs, TState]`` and returns ``TState`` if it is a
    concrete ``ArrowSerializableDataclass`` subclass.
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
                    if (
                        isinstance(state_type, type)
                        and issubclass(state_type, ArrowSerializableDataclass)
                    ):
                        return state_type
    return None


class _FilteringOutputCollector:
    """Wrapper that applies pushdown filters to emitted data batches.

    Intercepts emit() calls and applies the pushdown filter before
    delegating to the real OutputCollector.
    """

    __slots__ = ("_inner", "_func_cls", "_filters")

    def __init__(self, inner: OutputCollector, func_cls: type[TableFunctionBase[Any]], filters: Any) -> None:
        self._inner = inner
        self._func_cls = func_cls
        self._filters = filters

    def emit(self, batch: pa.RecordBatch) -> None:
        filtered = self._func_cls._apply_pushdown_filter(batch, self._filters)
        self._inner.emit(filtered)

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

    def propagate(self) -> None:
        """No-op: state already propagated to inner collector."""

    @property
    def output_schema(self) -> pa.Schema:
        return self._inner.output_schema


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

    def __post_init__(self) -> None:
        """Resolve pushdown filters if auto_apply_filters is enabled."""
        if self._func_cls is not None and self._func_cls._should_auto_apply_filters():
            self._auto_apply = True
            if self._params is not None and self._params.init_call.pushdown_filters is not None:
                self._pushdown_filters = self._func_cls.pushdown_filters(self._params.init_call.pushdown_filters)

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
        output_schema = project_schema(self._init_call.projection_ids, self._init_call.output_schema)
        self._params = ProcessParams(
            args=func_cls._parse_arguments(func_cls.FunctionArguments, self._init_call.bind_call.arguments),
            init_call=self._init_call,
            init_response=self._init_response,
            output_schema=output_schema,
            settings=_batch_to_scalar_dict(self._init_call.bind_call.settings),
            secrets=_batch_to_secret_dict(self._init_call.bind_call.secrets),
            storage=BoundStorage(func_cls.storage, self._init_response.execution_id),
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
                self._pushdown_filters = func_cls.pushdown_filters(self._init_call.pushdown_filters)

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce the next output batch from the table function."""
        if self._auto_apply and self._pushdown_filters is not None:
            # Wrap the OutputCollector to auto-apply filters to emitted batches
            filtered_out = _FilteringOutputCollector(out, self._func_cls, self._pushdown_filters)
            self._func_cls.process(self._params, self._user_state, filtered_out)  # type: ignore[arg-type]
            filtered_out.propagate()
        else:
            self._func_cls.process(self._params, self._user_state, out)


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

    def __post_init__(self) -> None:
        """Resolve pushdown filters if auto_apply_filters is enabled."""
        if self._func_cls is not None and self._func_cls._should_auto_apply_filters():
            self._auto_apply = True
            if self._params is not None and self._params.init_call.pushdown_filters is not None:
                self._pushdown_filters = self._func_cls.pushdown_filters(self._params.init_call.pushdown_filters)

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
        output_schema = project_schema(self._init_call.projection_ids, self._init_call.output_schema)
        self._params = ProcessParams(
            args=func_cls._parse_arguments(func_cls.FunctionArguments, self._init_call.bind_call.arguments),
            init_call=self._init_call,
            init_response=self._init_response,
            output_schema=output_schema,
            settings=_batch_to_scalar_dict(self._init_call.bind_call.settings),
            secrets=_batch_to_secret_dict(self._init_call.bind_call.secrets),
            storage=BoundStorage(func_cls.storage, self._init_response.execution_id),
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
                self._pushdown_filters = func_cls.pushdown_filters(self._init_call.pushdown_filters)

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Process one input batch through the table-in-out function."""
        if self._auto_apply and self._pushdown_filters is not None:
            filtered_out = _FilteringOutputCollector(out, self._func_cls, self._pushdown_filters)
            self._func_cls.process(self._params, self._user_state, input.batch, filtered_out)  # type: ignore[arg-type]
            filtered_out.propagate()
        else:
            self._func_cls.process(self._params, self._user_state, input.batch, out)


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
# VGI Protocol
# ---------------------------------------------------------------------------


class VgiProtocol(Protocol):
    """VGI wire protocol definition for vgi_rpc.

    Methods:
    - ``bind()`` / ``init()``: Function invocation protocol
    - ``catalog_*``: ~35 typed catalog interface methods replacing opaque ``catalog_call``

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

    # ========== Catalog - Discovery ==========

    def catalog_catalogs(self) -> CatalogsResponse:
        """List available catalog names."""
        ...

    # ========== Catalog - Lifecycle ==========

    def catalog_attach(self, request: CatalogAttachRequest) -> CatalogAttachResult:
        """Attach to a catalog with options."""
        ...

    def catalog_detach(self, attach_id: bytes) -> None:
        """Detach from a catalog."""
        ...

    def catalog_create(self, request: CatalogCreateRequest) -> None:
        """Create a new catalog."""
        ...

    def catalog_drop(self, name: str) -> None:
        """Drop a catalog."""
        ...

    def catalog_version(self, attach_id: bytes, transaction_id: bytes | None = None) -> CatalogVersionResponse:
        """Get the current catalog version."""
        ...

    # ========== Catalog - Transactions ==========

    def catalog_transaction_begin(self, attach_id: bytes) -> TransactionBeginResponse:
        """Begin a new transaction."""
        ...

    def catalog_transaction_commit(self, attach_id: bytes, transaction_id: bytes) -> None:
        """Commit a transaction."""
        ...

    def catalog_transaction_rollback(self, attach_id: bytes, transaction_id: bytes) -> None:
        """Rollback a transaction."""
        ...

    # ========== Catalog - Schemas ==========

    def catalog_schemas(self, attach_id: bytes, transaction_id: bytes | None = None) -> SchemasResponse:
        """List schemas in the catalog."""
        ...

    def catalog_schema_get(self, attach_id: bytes, name: str, transaction_id: bytes | None = None) -> SchemasResponse:
        """Get information about a schema. Returns 0 or 1 items."""
        ...

    def catalog_schema_create(
        self,
        attach_id: bytes,
        name: str,
        comment: str | None = None,
        tags: dict[str, str] | None = None,
        transaction_id: bytes | None = None,
    ) -> None:
        """Create a new schema."""
        ...

    def catalog_schema_drop(
        self,
        attach_id: bytes,
        name: str,
        ignore_not_found: bool = False,
        cascade: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Drop a schema."""
        ...

    def catalog_schema_contents_tables(
        self,
        attach_id: bytes,
        name: str,
        transaction_id: bytes | None = None,
    ) -> TablesResponse:
        """List tables in a schema."""
        ...

    def catalog_schema_contents_views(
        self,
        attach_id: bytes,
        name: str,
        transaction_id: bytes | None = None,
    ) -> ViewsResponse:
        """List views in a schema."""
        ...

    def catalog_schema_contents_functions(
        self,
        attach_id: bytes,
        name: str,
        type: SchemaObjectType,
        transaction_id: bytes | None = None,
    ) -> FunctionsResponse:
        """List functions in a schema (scalar or table)."""
        ...

    # ========== Catalog - Tables ==========

    def catalog_table_get(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        transaction_id: bytes | None = None,
    ) -> TablesResponse:
        """Get information about a table. Returns 0 or 1 items."""
        ...

    def catalog_table_create(self, request: TableCreateRequest) -> None:
        """Create a new table."""
        ...

    def catalog_table_drop(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Drop a table."""
        ...

    def catalog_table_scan_function_get(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
        transaction_id: bytes | None = None,
    ) -> bytes:
        """Get the scan function for a table. Returns ScanFunctionResult as IPC bytes."""
        ...

    def catalog_table_comment_set(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        comment: str | None = None,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Set or clear the comment on a table."""
        ...

    def catalog_table_rename(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        new_name: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Rename a table."""
        ...

    def catalog_table_column_add(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        column_definition: bytes,
        ignore_not_found: bool = False,
        if_column_not_exists: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Add a new column to a table."""
        ...

    def catalog_table_column_drop(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool = False,
        if_column_exists: bool = False,
        cascade: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Drop a column from a table."""
        ...

    def catalog_table_column_rename(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        new_column_name: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Rename a column."""
        ...

    def catalog_table_column_default_set(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        expression: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Set the default value expression for a column."""
        ...

    def catalog_table_column_default_drop(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Remove the default value from a column."""
        ...

    def catalog_table_column_type_change(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        column_definition: bytes,
        expression: str | None = None,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Change the type of a column."""
        ...

    def catalog_table_not_null_drop(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Remove NOT NULL constraint from a column."""
        ...

    def catalog_table_not_null_set(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Add NOT NULL constraint to a column."""
        ...

    # ========== Catalog - Views ==========

    def catalog_view_get(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        transaction_id: bytes | None = None,
    ) -> ViewsResponse:
        """Get information about a view. Returns 0 or 1 items."""
        ...

    def catalog_view_create(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        definition: str,
        on_conflict: OnConflict,
        transaction_id: bytes | None = None,
    ) -> None:
        """Create a new view."""
        ...

    def catalog_view_drop(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Drop a view."""
        ...

    def catalog_view_rename(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        new_name: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Rename a view."""
        ...

    def catalog_view_comment_set(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        comment: str | None = None,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Set or clear the comment on a view."""
        ...

    # ========== Catalog - Macros ===========

    def catalog_macro_get(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        transaction_id: bytes | None = None,
    ) -> MacrosResponse:
        """Get information about a macro. Returns 0 or 1 items."""
        ...

    def catalog_macro_create(self, request: MacroCreateRequest) -> None:
        """Create a new macro."""
        ...

    def catalog_macro_drop(
        self,
        attach_id: bytes,
        schema_name: str,
        name: str,
        ignore_not_found: bool = False,
        transaction_id: bytes | None = None,
    ) -> None:
        """Drop a macro."""
        ...

    def catalog_schema_contents_macros(
        self,
        attach_id: bytes,
        name: str,
        type: SchemaObjectType,
        transaction_id: bytes | None = None,
    ) -> MacrosResponse:
        """List macros in a schema (scalar or table)."""
        ...
