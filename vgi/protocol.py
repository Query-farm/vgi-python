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

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Annotated, Any, Protocol

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType
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
    OnConflict,
    SchemaInfo,
    SchemaObjectType,
    TableInfo,
    ViewInfo,
)
from vgi.function_storage import BoundStorage
from vgi.invocation import BindResponse, FunctionType, GlobalInitResponse
from vgi.table_function import ProcessParams, TableFunctionGenerator, TableInOutFunctionInitPhase

if TYPE_CHECKING:
    from vgi.scalar_function import ScalarFunctionGenerator
    from vgi.table_in_out_function import TableInOutGenerator

__all__ = [
    "BindRequest",
    "CatalogAttachRequest",
    "CatalogCreateRequest",
    "CatalogsResponse",
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


# ---------------------------------------------------------------------------
# StreamState implementations
# ---------------------------------------------------------------------------


@dataclass
class ScalarExchangeState(ExchangeState):
    """Exchange state for scalar function streams.

    Calls ``ScalarFunctionGenerator.process()`` per batch. Each ``exchange()``
    call sends one input batch and receives one output batch.

    Transient attributes are set after construction by ``Worker.init()``.

    """

    _func_cls: type[ScalarFunctionGenerator] = field(default=None, repr=False)  # type: ignore[assignment]
    _init_call: InitRequest = field(default=None, repr=False)  # type: ignore[assignment]
    _init_response: GlobalInitResponse = field(default=None, repr=False)  # type: ignore[assignment]

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


@dataclass
class TableProducerState(ProducerState):
    """Producer state for table function streams.

    Calls ``TableFunctionGenerator.process()`` per tick. Each ``produce()``
    call delegates to the function's process method which uses ``out`` directly.

    Transient attributes are set after construction by ``Worker.init()``.

    """

    _func_cls: type[TableFunctionGenerator[Any]] = field(default=None, repr=False)  # type: ignore[assignment]
    _params: ProcessParams[Any] = field(default=None, repr=False)  # type: ignore[arg-type]
    _user_state: Any = field(default=None, repr=False)

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce the next output batch from the table function."""
        self._func_cls.process(self._params, self._user_state, out)


@dataclass
class TableInOutExchangeState(ExchangeState):
    """Exchange state for table-in-out function streams (INPUT phase).

    Calls ``TableInOutGenerator.process()`` per input batch. Each
    ``exchange()`` call sends one input batch and receives one output batch.

    Transient attributes are set after construction by ``Worker.init()``.

    """

    _func_cls: type[TableInOutGenerator[Any]] = field(default=None, repr=False)  # type: ignore[assignment]
    _params: ProcessParams[Any] = field(default=None, repr=False)  # type: ignore[arg-type]
    _user_state: Any = field(default=None, repr=False)

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Process one input batch through the table-in-out function."""
        self._func_cls.process(self._params, self._user_state, input.batch, out)


@dataclass
class TableInOutFinalizeState(ProducerState):
    """Producer state for table-in-out function finalize streams.

    Emits pre-computed finalize batches one per ``produce()`` call.
    Calls ``out.finish()`` when all batches have been emitted.

    """

    _batches: list[pa.RecordBatch] = field(default_factory=list, repr=False)
    _index: int = field(default=0, repr=False)

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Emit the next finalize batch, or finish if done."""
        if self._index >= len(self._batches):
            out.finish()
            return
        out.emit(self._batches[self._index])
        self._index += 1


# Type alias for the union of all stream state types
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
