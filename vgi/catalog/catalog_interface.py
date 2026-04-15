"""VGI Catalog Interface for exposing catalogs, schemas, tables, and views.

This module provides the abstract base class and data types for implementing
catalog interfaces in VGI workers, enabling DuckDB ATTACH support.
"""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Literal,
    NewType,
    Self,
    cast,
    overload,
)

if TYPE_CHECKING:
    from vgi.catalog.descriptors import Catalog, Index, Macro, Schema, Table, View
    from vgi.catalog.secret_type import SecretTypeSpec
    from vgi.catalog.setting import SettingSpec

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass, ArrowType
from vgi_rpc.utils import deserialize_record_batch, serialize_record_batch_bytes

from vgi.arguments import SecretLookupEntry
from vgi.exceptions import CatalogReadOnlyError
from vgi.metadata import (
    DistinctDependence,
    FunctionStability,
    NullHandling,
    OrderDependence,
    OrderPreservation,
)

__all__ = [
    # Re-exported from vgi.metadata
    "DistinctDependence",
    "FunctionStability",
    "NullHandling",
    "OrderDependence",
    "OrderPreservation",
    # Catalog-specific
    "CatalogExample",
    "ColumnStatistics",
    "IndexConstraintType",
    "IndexInfo",
    "SecretLookupEntry",
    "MacroType",
    "SchemaObjectType",
    "TableColumnStatisticsResult",
    "WriteFunctionResult",
]


def _validate_at_params(at_unit: str | None, at_value: str | None) -> None:
    """Validate that at_unit and at_value are both provided or both absent."""
    if bool(at_unit) != bool(at_value):
        raise ValueError("at_unit and at_value must both be provided or both be None")


@dataclass(frozen=True)
class CatalogExample(ArrowSerializableDataclass):
    """An example usage of a function for catalog serialization.

    Attributes:
        sql: SQL query demonstrating the function.
        description: What this example demonstrates.
        expected_output: Optional expected result description.

    """

    sql: str
    description: str = ""
    expected_output: str | None = None


# Type aliases for improved code clarity and type checking.
# At runtime, these are equivalent to their underlying types.
AttachId = NewType("AttachId", bytes)
TransactionId = NewType("TransactionId", bytes)
SerializedSchema = NewType("SerializedSchema", bytes)
SqlExpression = NewType("SqlExpression", str)


@dataclass(frozen=True)
class CatalogAttachResult(ArrowSerializableDataclass):
    """Result from attaching to a catalog."""

    # The unique id for the attached catalog.
    attach_id: AttachId
    # Indicate if the worker supports transactions or not.
    # If false, all transaction related methods will not be called and all
    # transaction_id parameters will be None.
    supports_transactions: bool
    # Indicate if tables support time travel
    supports_time_travel: bool
    # Indicate that the catalog version id is frozen and the schema
    # and object information will not change.
    catalog_version_frozen: bool
    # The initial catalog version, it increments when schemas, tables
    # or other objects change.
    catalog_version: int
    # Indicate if the attach_id must be persisted across commands.
    # True: Catalog is stateful; attach_id represents a session
    # False: Catalog is stateless; CLI can auto-attach on each command
    attach_id_required: bool = True
    # The name of the default schema for this catalog.
    default_schema: str = "main"
    # Extension options (settings) exposed by this catalog/worker.
    # Each ExtensionOption is serialized as bytes for Arrow compatibility.
    settings: list[bytes] = field(default_factory=list)
    # Secret types registered with DuckDB's SecretManager.
    # Each SecretTypeSpec is serialized as bytes for Arrow compatibility.
    secret_types: list[bytes] = field(default_factory=list)
    # Optional comment describing this catalog/database.
    comment: str | None = None
    # Optional key-value tags associated with this catalog/database.
    tags: dict[str, str] = field(default_factory=dict)
    # Whether any tables in this catalog can provide column statistics.
    # Global gate — if False, GetStatistics() returns nullptr for all tables.
    supports_column_statistics: bool = False


@dataclass(frozen=True)
class CatalogObject:
    """All objects have the following common properties."""

    # This is a generic comment about the object
    comment: str | None
    # These are key-value tags associated with the object
    tags: dict[str, str]


@dataclass(frozen=True)
class CatalogSchemaObject(CatalogObject):
    """Objects that exist within a schema have the following common properties."""

    # The name of the object
    name: str
    # The name of the schema containing the object
    schema_name: str


@dataclass(frozen=True)
class SchemaInfo(CatalogObject, ArrowSerializableDataclass):
    """Information about a schema in a catalog."""

    attach_id: AttachId
    name: str


@dataclass(frozen=True)
class TableInfo(CatalogSchemaObject, ArrowSerializableDataclass):
    """Information about a table in a schema."""

    # The columns of the table as a PyArrow schema
    # that is serialized as bytes.
    columns: SerializedSchema

    # Use ArrowType to specify int32 instead of default int64
    not_null_constraints: Annotated[list[int], ArrowType(pa.list_(pa.int32()))]
    unique_constraints: Annotated[list[list[int]], ArrowType(pa.list_(pa.list_(pa.int32())))]
    check_constraints: list[str]
    primary_key_constraints: Annotated[list[list[int]], ArrowType(pa.list_(pa.list_(pa.int32())))] = field(
        default_factory=list
    )
    foreign_key_constraints: Annotated[list[bytes], ArrowType(pa.list_(pa.binary()))] = field(default_factory=list)

    # Write support flags — indicate which DML operations the table supports.
    supports_insert: bool = False
    supports_update: bool = False
    supports_delete: bool = False

    # Statistics capability flag — indicates this table can provide column statistics.
    supports_column_statistics: bool = False


@dataclass(frozen=True)
class ViewInfo(CatalogSchemaObject, ArrowSerializableDataclass):
    """Information about a view in a schema."""

    # The definition of the view which is a SQL query string.
    definition: str


@dataclass(frozen=True)
class MacroInfo(CatalogSchemaObject, ArrowSerializableDataclass):
    """Information about a macro in a schema.

    Attributes:
        macro_type: Whether this is a scalar or table macro.
        parameters: Ordered list of parameter names.
        parameter_default_values: One-row RecordBatch where column names are parameter
            names and values are typed defaults. None if no defaults.
            Serialized as IPC bytes over the wire.
        definition: The SQL expression (scalar) or query (table).

    """

    macro_type: "MacroType"
    parameters: list[str]
    parameter_default_values: Annotated[pa.RecordBatch | None, ArrowType(pa.binary())] = None
    definition: str = ""


class FunctionType(Enum):
    """The type of function in a schema."""

    SCALAR = "scalar"
    TABLE = "table"
    AGGREGATE = "aggregate"


class MacroType(Enum):
    """The type of macro in a schema."""

    SCALAR = "scalar"
    TABLE = "table"


class IndexConstraintType(Enum):
    """The constraint type of an index.

    NONE: Regular index (no constraint enforcement).
    UNIQUE: Index enforces a UNIQUE constraint.
    PRIMARY: Index enforces a PRIMARY KEY constraint.
    """

    NONE = "none"
    UNIQUE = "unique"
    PRIMARY = "primary"


@dataclass(frozen=True)
class IndexInfo(CatalogSchemaObject, ArrowSerializableDataclass):
    """Information about an index in a schema.

    Attributes:
        table_name: The name of the table this index is on.
        index_type: The index type string (e.g., "ART", or empty for default).
        constraint_type: The constraint enforcement type (NONE, UNIQUE, PRIMARY).
        expressions: SQL expression strings defining the indexed expressions.
            For column-based indexes, these are column references (e.g., "col_a").
            For expression indexes, these are arbitrary SQL (e.g., "lower(col_a)").
        options: Key-value index options (WITH clause).

    """

    table_name: str
    index_type: str = ""
    constraint_type: IndexConstraintType = IndexConstraintType.NONE
    expressions: list[str] = field(default_factory=list)
    options: dict[str, str] = field(default_factory=dict)


class SchemaObjectType(Enum):
    """The type of object that can exist within a schema.

    Used to filter results from schema_contents().
    """

    TABLE = "table"
    VIEW = "view"
    SCALAR_FUNCTION = "scalar_function"
    TABLE_FUNCTION = "table_function"
    AGGREGATE_FUNCTION = "aggregate_function"
    SCALAR_MACRO = "scalar_macro"
    TABLE_MACRO = "table_macro"
    INDEX = "index"


class OnConflict(Enum):
    """Behavior when a conflict occurs during creation of an object.

    IGNORE: Do nothing if the object already exists.
    REPLACE: Replace the existing object if it already exists.
    ERROR: Raise an error if the object already exists.
    """

    ERROR = "error"
    IGNORE = "ignore"
    REPLACE = "replace"


@dataclass(frozen=True)
class FunctionInfo(CatalogSchemaObject, ArrowSerializableDataclass):
    """Information about a function in a schema."""

    # the type of function from VGI
    function_type: FunctionType

    # The arguments as a serialized Apache arrow schema using
    # schema.serialize().to_pybytes()
    arguments: SerializedSchema

    # The output schema as a serialized Apache arrow schema using
    # schema.serialize().to_pybytes()
    output_schema: SerializedSchema

    # Scalar function behavior fields (None for non-scalar functions)
    stability: FunctionStability | None = None
    null_handling: NullHandling | None = None

    # Documentation fields
    # description: intrinsic documentation from function metadata (Meta.description)
    # comment: user-settable comment (via COMMENT ON FUNCTION, inherited from base)
    description: str = ""
    examples: list[CatalogExample] = field(default_factory=list)
    categories: list[str] = field(default_factory=list)

    # Table function capabilities (None for scalar functions)
    projection_pushdown: bool | None = None
    filter_pushdown: bool | None = None
    sampling_pushdown: bool | None = None
    supported_expression_filters: list[str] = field(default_factory=list)
    order_preservation: OrderPreservation | None = None
    # Use ArrowType to specify int32 instead of default int64
    max_workers: Annotated[int | None, ArrowType(pa.int32())] = None

    # Aggregate function fields (future)
    order_dependent: OrderDependence = OrderDependence.NOT_ORDER_DEPENDENT
    distinct_dependent: DistinctDependence = DistinctDependence.NOT_DISTINCT_DEPENDENT

    # Settings required by the function
    required_settings: list[str] = field(default_factory=list)

    # Secrets required by the function (each entry has secret_type, optional secret_name, optional scope)
    required_secrets: list[SecretLookupEntry] = field(default_factory=list)


@dataclass(frozen=True)
class ScanFunctionResult:
    """Result from getting a table scan function.

    This result tells the VGI DuckDB extension which DuckDB function to call
    to obtain the data for a table. This enables catalogs to delegate scanning
    to any DuckDB function (e.g., read_parquet, iceberg_scan, or a custom VGI
    table function) with appropriate arguments.

    Attributes:
        function_name: The DuckDB function to call (e.g., "read_parquet").
        positional_arguments: Positional arguments as PyArrow scalars.
        named_arguments: Named arguments as PyArrow scalars.
        required_extensions: DuckDB extensions to load before calling.

    """

    # The name of the duckdb function to call to obtain the data
    # in the table.
    function_name: str

    # The positional arguments to the include in the function call.
    positional_arguments: list[pa.Scalar]  # type: ignore[type-arg]

    # The named arguments to include in the function call.
    named_arguments: dict[str, pa.Scalar]  # type: ignore[type-arg]

    # A list of extensions to require to be loaded.
    required_extensions: list[str] = field(default_factory=list)

    ARROW_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("function_name", pa.string(), nullable=False),
            pa.field("arguments", pa.binary(), nullable=False),
            pa.field("required_extensions", pa.list_(pa.string()), nullable=False),
        ]  # type: ignore[arg-type]
    )

    def to_row_dict(self) -> dict[str, Any]:
        """Convert to a dictionary for batch construction.

        The arguments field is serialized as nested Arrow IPC bytes.
        """
        # Build arguments as nested batch
        argument_values: dict[str, pa.Scalar] = {}  # type: ignore[type-arg]
        argument_schema = []
        for index, arg in enumerate(self.positional_arguments):
            argument_schema.append(pa.field(f"arg_{index}", arg.type))
            argument_values[f"arg_{index}"] = arg
        for name, value in self.named_arguments.items():
            argument_schema.append(pa.field(name, value.type))
            argument_values[name] = value

        argument_batch = pa.RecordBatch.from_pylist(
            [argument_values],
            schema=pa.schema(argument_schema),
        )

        return {
            "function_name": self.function_name,
            "arguments": serialize_record_batch_bytes(argument_batch),
            "required_extensions": list(self.required_extensions) if self.required_extensions is not None else None,
        }

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes."""
        batch = pa.RecordBatch.from_pylist(
            [self.to_row_dict()],
            schema=self.ARROW_SCHEMA,
        )
        return serialize_record_batch_bytes(batch)

    @classmethod
    def deserialize(cls, batch: pa.RecordBatch) -> Self:
        """Deserialize from Arrow RecordBatch."""
        from vgi_rpc.utils import _validate_single_row_batch

        row = _validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=["function_name", "arguments"],
        )

        # Deserialize the nested arguments batch.
        # row["arguments"] is already bytes (_validate_single_row_batch returns
        # Python values, not PyArrow scalars).
        arguments_bytes = cast(bytes, row["arguments"])
        arguments_batch, _ = deserialize_record_batch(arguments_bytes)

        # Extract positional and named arguments from the batch
        positional_arguments: list[pa.Scalar] = []  # type: ignore[type-arg]
        named_arguments: dict[str, pa.Scalar] = {}  # type: ignore[type-arg]

        for arg_field in arguments_batch.schema:
            value = arguments_batch.column(arg_field.name)[0]
            if arg_field.name.startswith("arg_"):
                positional_arguments.append(value)
            else:
                named_arguments[arg_field.name] = value

        return cls(
            function_name=cast(str, row["function_name"]),
            positional_arguments=positional_arguments,
            named_arguments=named_arguments,
            required_extensions=list(cast("list[str]", row.get("required_extensions") or [])),
        )


# Write function discovery uses the same wire format as scan function discovery.
WriteFunctionResult = ScanFunctionResult


# ============================================================================
# Column Statistics
# ============================================================================


@dataclass(frozen=True)
class ColumnStatistics:
    """Statistics for a single column in a table.

    Workers provide these to help DuckDB's optimizer make cost-based decisions
    (filter elimination, join reordering, etc.).

    Attributes:
        column_name: Name of the column these statistics describe.
        min: Minimum value as a typed PyArrow scalar (e.g., ``pa.scalar(0, pa.int64())``),
            or ``None`` if unknown.
        max: Maximum value as a typed PyArrow scalar, or ``None`` if unknown.
            Must have the same Arrow type as ``min``.
        has_null: Whether the column contains any null values.
        has_not_null: Whether the column contains any non-null values.
        distinct_count: Approximate count of distinct values, or ``None`` if unknown.
        contains_unicode: String/binary columns only — whether values contain non-ASCII
            characters. ``None`` for non-string columns.
        max_string_length: String/binary columns only — maximum byte length of values.
            ``None`` for non-string columns.

    """

    column_name: str
    min: pa.Scalar | None = None  # type: ignore[type-arg]
    max: pa.Scalar | None = None  # type: ignore[type-arg]
    has_null: bool = True
    has_not_null: bool = True
    distinct_count: int | None = None
    contains_unicode: bool | None = None
    max_string_length: int | None = None


@dataclass(frozen=True)
class TableColumnStatisticsResult:
    """Result from ``table_column_statistics_get`` with optional cache control.

    Attributes:
        statistics: Per-column statistics for the table.
        cache_max_age_seconds: How long the client may cache these statistics
            (in seconds). ``None`` means cache indefinitely (static data).
            ``0`` means do not cache (live/volatile data).

    """

    statistics: list[ColumnStatistics]
    cache_max_age_seconds: int | None = None


def _infer_stat_type(stat: ColumnStatistics) -> pa.DataType:
    """Infer the Arrow type for a ColumnStatistics entry from its min/max scalars."""
    if stat.min is not None and stat.min.is_valid:
        return stat.min.type  # type: ignore[no-any-return]
    if stat.max is not None and stat.max.is_valid:
        return stat.max.type  # type: ignore[no-any-return]
    return pa.null()


def serialize_column_statistics(
    stats: list[ColumnStatistics],
    cache_max_age_seconds: int | None = None,
) -> bytes:
    """Serialize column statistics into a single RecordBatch with sparse union min/max.

    The ``min`` and ``max`` columns use an Arrow sparse union whose child types
    are the distinct column types present in *stats*.  This keeps everything in
    a single IPC stream regardless of how many column types the table has.

    Args:
        stats: Per-column statistics to serialize.
        cache_max_age_seconds: Optional cache TTL embedded in schema metadata.

    Returns:
        IPC-serialized bytes of the statistics RecordBatch.

    """
    n = len(stats)
    if n == 0:
        # Return a minimal empty batch — must construct empty union arrays manually
        # since pa.array([], type=sparse_union) is not supported
        union_fields = [pa.field("0", pa.null())]
        union_type = pa.sparse_union(union_fields)
        empty_union = pa.UnionArray.from_sparse(
            pa.array([], type=pa.int8()),
            [pa.array([], type=pa.null())],
            field_names=["0"],
            type_codes=[0],  # type: ignore[arg-type]
        )
        schema = pa.schema(
            [
                pa.field("column_name", pa.utf8()),
                pa.field("min", union_type),
                pa.field("max", union_type),
                pa.field("has_null", pa.bool_()),
                pa.field("has_not_null", pa.bool_()),
                pa.field("distinct_count", pa.int64()),
                pa.field("contains_unicode", pa.bool_()),
                pa.field("max_string_length", pa.uint64()),
            ]
        )
        batch = pa.record_batch(
            [
                pa.array([], type=pa.utf8()),
                empty_union,
                empty_union,
                pa.array([], type=pa.bool_()),
                pa.array([], type=pa.bool_()),
                pa.array([], type=pa.int64()),
                pa.array([], type=pa.bool_()),
                pa.array([], type=pa.uint64()),
            ],
            schema=schema,
        )
        return serialize_record_batch_bytes(batch)

    # 1. Collect distinct Arrow types, assign type codes
    type_map: dict[pa.DataType, int] = {}
    row_type_codes: list[int] = []
    for s in stats:
        arrow_type = _infer_stat_type(s)
        if arrow_type not in type_map:
            type_map[arrow_type] = len(type_map)
        row_type_codes.append(type_map[arrow_type])

    # 2. Build sparse union child arrays (each child is length N)
    union_fields: list[pa.Field] = []  # type: ignore[type-arg]
    field_names: list[str] = []
    type_codes: list[int] = []
    min_children: list[pa.Array] = []  # type: ignore[type-arg]
    max_children: list[pa.Array] = []  # type: ignore[type-arg]
    for arrow_type, code in sorted(type_map.items(), key=lambda x: x[1]):
        union_fields.append(pa.field(str(code), arrow_type))
        field_names.append(str(code))
        type_codes.append(code)
        min_vals = [s.min if row_type_codes[i] == code else None for i, s in enumerate(stats)]
        max_vals = [s.max if row_type_codes[i] == code else None for i, s in enumerate(stats)]
        min_children.append(pa.array(min_vals, type=arrow_type))
        max_children.append(pa.array(max_vals, type=arrow_type))

    # 3. Build sparse union arrays
    codes_arr = pa.array(row_type_codes, type=pa.int8())
    min_union = pa.UnionArray.from_sparse(
        codes_arr,
        min_children,
        field_names=field_names,
        type_codes=type_codes,  # type: ignore[arg-type]
    )
    max_union = pa.UnionArray.from_sparse(
        codes_arr,
        max_children,
        field_names=field_names,
        type_codes=type_codes,  # type: ignore[arg-type]
    )

    # 4. Build schema and batch
    union_type = pa.sparse_union(union_fields)
    schema = pa.schema(
        [
            pa.field("column_name", pa.utf8()),
            pa.field("min", union_type),
            pa.field("max", union_type),
            pa.field("has_null", pa.bool_()),
            pa.field("has_not_null", pa.bool_()),
            pa.field("distinct_count", pa.int64()),
            pa.field("contains_unicode", pa.bool_()),
            pa.field("max_string_length", pa.uint64()),
        ],
    )

    batch = pa.record_batch(
        [
            pa.array([s.column_name for s in stats], type=pa.utf8()),
            min_union,
            max_union,
            pa.array([s.has_null for s in stats], type=pa.bool_()),
            pa.array([s.has_not_null for s in stats], type=pa.bool_()),
            pa.array([s.distinct_count for s in stats], type=pa.int64()),
            pa.array([s.contains_unicode for s in stats], type=pa.bool_()),
            pa.array([s.max_string_length for s in stats], type=pa.uint64()),
        ],
        schema=schema,
    )

    # 5. Serialize with cache TTL as IPC batch custom_metadata (not schema metadata)
    custom_metadata = None
    if cache_max_age_seconds is not None:
        custom_metadata = pa.KeyValueMetadata({b"cache_max_age_seconds": str(cache_max_age_seconds).encode()})
    return serialize_record_batch_bytes(batch, custom_metadata=custom_metadata)


class CatalogInterface(ABC):
    """Provides an interface to manage catalogs, schemas, tables, and views for VGI.

    This interface defines methods for creating, dropping, and managing catalogs,
    schemas, tables, and views. It also supports transactions and provides methods
    for discovering catalog contents.

    Implementors of this interface should provide concrete implementations for
    all abstract methods and properties.

    API limitations:
        - Functions are not able to be created or dropped.
        - Tags are not able to be updated on catalog objects.
        - Comments and tags are not updatable on schemas (SchemaInfo).
        - Constraints cannot be added/dropped (except NOT NULL).

    A VGI worker will offer a single implementation of this interface to clients
    to manage their catalogs.
    """

    @property
    def interface_feature_flags(self) -> set[str]:
        """Get the feature flags supported by this CatalogInterface.

        Feature flags indicate optional capabilities of the implementation.
        The default implementation returns an empty set.
        """
        return set()

    @abstractmethod
    def catalogs(self) -> list[str]:
        """Get a list of catalog names provided by the VGI worker.

        This is a discovery only method.
        """

    def catalog_create(self, *, name: str, on_conflict: OnConflict, options: dict[str, Any]) -> None:
        """Create a new catalog with the given name.

        If on_conflict is IGNORE and the catalog already exists, do nothing.
        If on_conflict is REPLACE and the catalog already exists, replace it.
        If on_conflict is ERROR and the catalog already exists, raise an error.

        """
        raise NotImplementedError("Catalog create not implemented.")

    # Drop a catalog
    def catalog_drop(self, *, name: str) -> None:
        """Drop the catalog with the given name."""
        raise NotImplementedError("Catalog drop not implemented.")

    # Transactions are initiated and driven by DuckDB it is rare for CatalogInterface
    # implementors to implement them, but I want to support them.
    #
    # Transaction Guarantees
    # - Transactions MAY span multiple worker processes
    # - Workers MUST treat transaction_id as opaque
    # - Workers MUST ensure idempotency of commit/rollback

    def catalog_transaction_begin(self, *, attach_id: AttachId) -> TransactionId | None:
        """Begin a new transaction for the given attach_id.

        If the implementation does not support transactions, it can return None.
        """
        raise NotImplementedError("Catalog transactions not implemented.")

    def catalog_transaction_commit(self, *, attach_id: AttachId, transaction_id: TransactionId) -> None:
        """Commit the transaction for the given attachment.

        If the transaction cannot be committed, an exception should be raised.
        """
        raise NotImplementedError("Catalog transactions not implemented.")

    def catalog_transaction_rollback(self, *, attach_id: AttachId, transaction_id: TransactionId) -> None:
        """Rollback the transaction for the given attachment.

        If the transaction cannot be rolled back, an exception should be raised.
        """
        raise NotImplementedError("Catalog transactions not implemented.")

    @abstractmethod
    def catalog_attach(self, *, name: str, options: dict[str, Any]) -> CatalogAttachResult:
        """Attach to a catalog with the given name and options.

        Returns a CatalogAttachResult containing the attach ID and other information
        about the attachment.
        """

    def catalog_detach(self, *, attach_id: AttachId) -> None:
        """Detach from the catalog with the given attach_id.

        Any open transactions should be rolled back.
        The default implementation does nothing.
        """
        return  # Default no-op

    def catalog_version(self, *, attach_id: AttachId, transaction_id: TransactionId | None) -> int:
        """Get the current catalog version for the given attach_id and transaction_id.

        Returns an integer representing the current catalog version.

        Changes to schemas, tables, and objects increment this version. It is used to
        expire cached catalog/schema/object information inside a VGI client or process.

        The default implementation returns 0.
        """
        return 0

    def schemas(self, *, attach_id: AttachId, transaction_id: TransactionId | None) -> list[SchemaInfo]:
        """Get a list of schemas for the given attach_id and transaction_id.

        The default returns a schema called "main" with no comment or tags.
        """
        return [
            SchemaInfo(
                attach_id=attach_id,
                name="main",
                comment=None,
                tags={},
            )
        ]

    def schema_create(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        on_conflict: OnConflict = OnConflict.ERROR,
        comment: str | None,
        tags: dict[str, str],
    ) -> None:
        """Create a new schema with the given name, comment, and tags."""
        raise NotImplementedError("Schema create not implemented.")

    def schema_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        ignore_not_found: bool,
        cascade: bool,
    ) -> None:
        """Drop the schema with the given name."""
        raise NotImplementedError("Schema drop not implemented.")

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: Literal[SchemaObjectType.TABLE],
    ) -> Sequence[TableInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: Literal[SchemaObjectType.VIEW],
    ) -> Sequence[ViewInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: Literal[
            SchemaObjectType.SCALAR_FUNCTION,
            SchemaObjectType.TABLE_FUNCTION,
            SchemaObjectType.AGGREGATE_FUNCTION,
        ],
    ) -> Sequence[FunctionInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: Literal[SchemaObjectType.SCALAR_MACRO, SchemaObjectType.TABLE_MACRO],
    ) -> Sequence[MacroInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: Literal[SchemaObjectType.INDEX],
    ) -> Sequence[IndexInfo]: ...

    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: SchemaObjectType,
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo | MacroInfo | IndexInfo]:
        """Get the contents of the schema with the given name.

        Schemas can contain tables, views, functions, macros, and indexes.

        Args:
            attach_id: The attachment identifier.
            transaction_id: The transaction identifier, if any.
            name: The name of the schema.
            type: The type of objects to return. Must be a SchemaObjectType enum:
                - SchemaObjectType.TABLE: Return only tables
                - SchemaObjectType.VIEW: Return only views
                - SchemaObjectType.SCALAR_FUNCTION: Scalar functions
                - SchemaObjectType.TABLE_FUNCTION: Table functions
                - SchemaObjectType.SCALAR_MACRO: Scalar macros
                - SchemaObjectType.TABLE_MACRO: Table macros
                - SchemaObjectType.INDEX: Indexes

        Returns:
            A list of TableInfo, ViewInfo, FunctionInfo, or MacroInfo objects
            depending on the type parameter.

        """
        raise NotImplementedError("Schema contents not implemented.")

    @abstractmethod
    def schema_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
    ) -> SchemaInfo | None:
        """Get information about the schema with the given name.

        Returns a SchemaInfo object if the schema exists, or None if it does not.
        """

    @abstractmethod
    def table_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        """Get information about the table with the given name in the specified schema.

        When ``at_unit`` / ``at_value`` are provided the implementation should
        return the table schema for the requested point in time (time travel).

        Returns a TableInfo object if the table exists, or None if it does not.
        """

    def table_create(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        # The contents of the table is a serialized PyArrow schema
        # the nullability for each field is ignored.
        # schema.serialize().to_pybytes()
        columns: SerializedSchema,
        on_conflict: OnConflict,
        # These are constraints listed by field index
        not_null_constraints: list[int],  # [] = no not null constraints
        unique_constraints: list[list[int]],  # [] = no unique constraints
        # These are general check constraints specified as SQL expressions.
        check_constraints: list[str],  # [] = no check constraints
        # Primary key constraints as column index groups
        primary_key_constraints: list[list[int]] | None = None,
        # Foreign key constraints as IPC-serialized bytes (same format as TableInfo)
        foreign_key_constraints: list[bytes] | None = None,
    ) -> None:
        """Create a new table with the given name and schema.

        Comments and tags are not supported on table creation.
        """
        raise NotImplementedError("Table create not implemented.")

    def table_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        ignore_not_found: bool,
        cascade: bool = False,
    ) -> None:
        """Drop the table with the given name."""
        raise NotImplementedError("Table drop not implemented.")

    def table_comment_set(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        comment: str | None,
        ignore_not_found: bool,
    ) -> None:
        """Set the comment for the table with the given name."""
        raise NotImplementedError("Table comment set not implemented.")

    def table_column_comment_set(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        column_name: str,
        comment: str | None,
        ignore_not_found: bool,
    ) -> None:
        """Set the comment for a column in the table."""
        raise NotImplementedError("Table column comment set not implemented.")

    def table_rename(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        new_name: str,
        ignore_not_found: bool,
    ) -> None:
        """Rename the table with the given name to the new name."""
        raise NotImplementedError("Table rename not implemented.")

    def table_column_add(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        # Arrow schema with single field for column to add.
        # Serialized via schema.serialize().to_pybytes()
        column_definition: SerializedSchema,
        ignore_not_found: bool,
        if_column_not_exists: bool,
    ) -> None:
        """Add a column to the table with the given name."""
        raise NotImplementedError("Table column add not implemented.")

    def table_column_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool,
        if_column_exists: bool,
        cascade: bool,
    ) -> None:
        """Drop the column from the table with the given name."""
        raise NotImplementedError("Table column drop not implemented.")

    def table_column_rename(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        column_name: str,
        new_column_name: str,
        ignore_not_found: bool,
    ) -> None:
        """Rename the column in the table with the given name."""
        raise NotImplementedError("Table column rename not implemented.")

    def table_column_default_set(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        column_name: str,
        expression: SqlExpression,
        ignore_not_found: bool,
    ) -> None:
        """Set the default expression for the column."""
        raise NotImplementedError("Table column default set not implemented.")

    def table_column_default_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool,
    ) -> None:
        """Drop the default expression for the column."""
        raise NotImplementedError("Table column default drop not implemented.")

    def table_column_type_change(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        # Arrow schema with single field for the new column type.
        # Serialized via schema.serialize().to_pybytes()
        column_definition: SerializedSchema,
        expression: SqlExpression | None,
        ignore_not_found: bool,
    ) -> None:
        """Change the type of the column in the table with the given name.

        The name of the column to change is taken from the field in the provided schema.
        """
        raise NotImplementedError("Table column type change not implemented.")

    def table_not_null_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool,
    ) -> None:
        """Drop the NOT NULL constraint from the column."""
        raise NotImplementedError("Table NOT NULL drop not implemented.")

    def table_not_null_set(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool,
    ) -> None:
        """Set the NOT NULL constraint on the column."""
        raise NotImplementedError("Table NOT NULL set not implemented.")

    def table_scan_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        # Time travel fields (iceberg style)
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        """Get the ScanFunctionResult for scanning the table.

        Returns information about the VGI table function to call when scanning
        this table. The at_unit and at_value support time travel queries.
        """
        raise NotImplementedError("Table scan function get not implemented.")

    def table_column_statistics_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> TableColumnStatisticsResult | None:
        """Get column statistics for all columns in a table.

        Returns a :class:`TableColumnStatisticsResult` containing per-column
        statistics and an optional cache TTL, or ``None`` if statistics are not
        available for this table.

        The default implementation returns ``None`` (no statistics).
        Workers that provide statistics should override this method.
        """
        return None

    def table_insert_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        """Get the write function for INSERT operations on the table.

        Returns a ScanFunctionResult identifying the TableInOutGenerator function
        to call for inserting rows into this table.
        """
        raise NotImplementedError("Table insert not supported.")

    def table_update_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        """Get the write function for UPDATE operations on the table.

        Returns a ScanFunctionResult identifying the TableInOutGenerator function
        to call for updating rows in this table. Input batches will include a
        rowid column plus the columns being updated.
        """
        raise NotImplementedError("Table update not supported.")

    def table_delete_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        """Get the write function for DELETE operations on the table.

        Returns a ScanFunctionResult identifying the TableInOutGenerator function
        to call for deleting rows from this table. Input batches will contain
        a rowid column identifying the rows to delete.
        """
        raise NotImplementedError("Table delete not supported.")

    def view_create(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        definition: str,
        on_conflict: OnConflict,
    ) -> None:
        """Create a new view with the given definition."""
        raise NotImplementedError("View create not implemented.")

    def view_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        ignore_not_found: bool,
        cascade: bool = False,
    ) -> None:
        """Drop the view with the given name."""
        raise NotImplementedError("View drop not implemented.")

    def view_rename(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        new_name: str,
        ignore_not_found: bool,
    ) -> None:
        """Rename the view to the new name."""
        raise NotImplementedError("View rename not implemented.")

    @abstractmethod
    def view_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ViewInfo | None:
        """Get information about the view with the given name.

        Returns a ViewInfo object if the view exists, or None if it does not.
        """

    def view_comment_set(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        comment: str | None,
        ignore_not_found: bool,
    ) -> None:
        """Set the comment for the view with the given name."""
        raise NotImplementedError("View comment set not implemented.")

    # ---- Macros ----

    @abstractmethod
    def macro_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> MacroInfo | None:
        """Get information about the macro with the given name.

        Returns a MacroInfo object if the macro exists, or None if it does not.
        """

    def macro_create(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        macro_type: "MacroType",
        parameters: list[str],
        definition: str,
        on_conflict: OnConflict,
        parameter_default_values: pa.RecordBatch | None = None,
    ) -> None:
        """Create a new macro with the given definition."""
        raise NotImplementedError("Macro create not implemented.")

    def macro_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        ignore_not_found: bool,
    ) -> None:
        """Drop the macro with the given name."""
        raise NotImplementedError("Macro drop not implemented.")

    # ---- Indexes ----

    def index_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> IndexInfo | None:
        """Get information about the index with the given name.

        Returns an IndexInfo object if the index exists, or None if it does not.
        The default implementation returns None (no indexes).
        """
        return None

    def index_create(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        table_name: str,
        index_type: str,
        constraint_type: IndexConstraintType,
        expressions: list[str],
        on_conflict: OnConflict,
        options: dict[str, str] | None = None,
    ) -> None:
        """Create a new index on the specified table."""
        raise NotImplementedError("Index create not implemented.")

    def index_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        ignore_not_found: bool,
        cascade: bool = False,
    ) -> None:
        """Drop the index with the given name."""
        raise NotImplementedError("Index drop not implemented.")


def _read_only(operation: str) -> Any:
    """Create a CatalogInterface method that raises CatalogReadOnlyError."""

    def method(self: Any, **kwargs: Any) -> Any:
        raise CatalogReadOnlyError(f"Cannot {operation}: catalog is read-only")

    method.__doc__ = "Not supported — raises CatalogReadOnlyError."
    return method


class ReadOnlyCatalogInterface(CatalogInterface):
    """A read-only catalog interface that does not support DDL operations.

    This is a convenience base class for catalogs that only support reading
    metadata and data, not creating or modifying objects.

    There are two ways to use this class:

    1. Subclass and implement abstract methods:
       - catalogs() - List available catalogs
       - catalog_attach() - Attach to a catalog
       - schema_get() - Get schema information
       - table_get() - Get table information (return None for function-only catalogs)
       - view_get() - Get view information (return None for function-only catalogs)

    2. Use with functions list (simpler for function-only catalogs):
       Set the `functions` class attribute to expose VGI functions:
       - catalog_name - Name of the catalog (default: "functions")
       - functions - List of function classes to expose in the "main" schema

       This provides automatic implementations of catalogs(), catalog_attach(),
       schema_get(), table_get(), view_get(), and schema_contents().

    Optional methods that can be overridden:
    - catalog_detach() - Custom detach logic
    - schemas() - Custom schema listing (default returns 'main')
    - schema_contents() - List schema contents
    - table_scan_function_get() - Get scan function for tables

    All DDL operations (create, drop, rename, modify) will raise
    CatalogReadOnlyError.

    """

    supports_transactions = False
    catalog_version_frozen = True

    # Class attributes for function-based catalogs
    catalog_name: str = "functions"
    functions: list[type] = []
    settings: list["SettingSpec"] = []
    secret_types: list["SecretTypeSpec"] = []

    # NEW: Optional Catalog object for declarative definition
    catalog: "Catalog | None" = None

    # Fixed attach_id for read-only catalogs (no need for unique IDs)
    _FIXED_ATTACH_ID: AttachId = AttachId(b"readonly-catalog-")

    # Instance-level registry caches (built lazily)
    # Keys are LOWERCASE for case-insensitive lookup
    _schema_registry: "dict[str, Schema] | None" = None
    _table_registry: "dict[tuple[str, str], Table] | None" = None
    _view_registry: "dict[tuple[str, str], View] | None" = None
    _function_registry: "dict[tuple[str, str], list[type]] | None" = None
    _macro_registry: "dict[tuple[str, str], Macro] | None" = None
    _index_registry: "dict[tuple[str, str], Index] | None" = None

    def _build_registries(self) -> None:
        """Build lookup dicts from Catalog or legacy patterns.

        All registry keys are lowercase for case-insensitive lookups.
        Raises ValueError if duplicate names detected within same schema.
        """
        if self._schema_registry is not None:
            return

        # Import here to avoid circular imports
        from vgi.catalog.descriptors import Schema

        self._schema_registry = {}
        self._table_registry = {}
        self._view_registry = {}
        self._function_registry = {}
        self._macro_registry = {}
        self._index_registry = {}

        def _register_table(schema_key: str, table: "Table") -> None:
            key = (schema_key, table.name.lower())
            if key in self._table_registry:  # type: ignore[operator]
                raise ValueError(f"Duplicate table '{table.name}' in schema '{schema_key}'")
            self._table_registry[key] = table  # type: ignore[index]

        def _register_view(schema_key: str, view: "View") -> None:
            key = (schema_key, view.name.lower())
            if key in self._view_registry:  # type: ignore[operator]
                raise ValueError(f"Duplicate view '{view.name}' in schema '{schema_key}'")
            self._view_registry[key] = view  # type: ignore[index]

        def _register_function(schema_key: str, func_cls: type) -> None:
            meta = func_cls.get_metadata()  # type: ignore[attr-defined]
            key = (schema_key, meta.name.lower())
            if key not in self._function_registry:  # type: ignore[operator]
                self._function_registry[key] = []  # type: ignore[index]
            self._function_registry[key].append(func_cls)  # type: ignore[index]

        def _register_macro(schema_key: str, macro: "Macro") -> None:
            key = (schema_key, macro.name.lower())
            if key in self._macro_registry:  # type: ignore[operator]
                raise ValueError(f"Duplicate macro '{macro.name}' in schema '{schema_key}'")
            self._macro_registry[key] = macro  # type: ignore[index]

        def _register_index(schema_key: str, index: "Index") -> None:
            key = (schema_key, index.name.lower())
            if key in self._index_registry:  # type: ignore[operator]
                raise ValueError(f"Duplicate index '{index.name}' in schema '{schema_key}'")
            self._index_registry[key] = index  # type: ignore[index]

        if self.catalog is not None:
            # Build from Catalog object
            for schema in self.catalog.schemas:
                schema_key = schema.name.lower()
                self._schema_registry[schema_key] = schema

                for table in schema.tables:
                    _register_table(schema_key, table)
                for view in schema.views:
                    _register_view(schema_key, view)
                for func_cls in schema.functions:
                    _register_function(schema_key, func_cls)
                for macro in schema.macros:
                    _register_macro(schema_key, macro)
                for index in schema.indexes:
                    _register_index(schema_key, index)
        else:
            # Backward compat: create "main" schema from legacy `functions` list
            main_schema = Schema(name="main", tables=(), views=(), functions=())
            self._schema_registry["main"] = main_schema

            for func_cls in self.functions:
                _register_function("main", func_cls)

    @property
    def _effective_catalog_name(self) -> str:
        """Get catalog name from Catalog object or class attribute."""
        if self.catalog is not None:
            return self.catalog.name
        return self.catalog_name

    @property
    def _default_schema_name(self) -> str:
        """Get default schema name."""
        if self.catalog is not None:
            return self.catalog.default_schema
        return "main"

    def catalogs(self) -> list[str]:
        """Return the list of available catalogs."""
        return [self._effective_catalog_name]

    def catalog_attach(self, *, name: str, options: dict[str, Any]) -> CatalogAttachResult:
        """Attach to the catalog."""
        effective_name = self._effective_catalog_name
        if name != effective_name:
            raise ValueError(f"Unknown catalog: {name!r}. Available: {effective_name}")

        # Serialize settings and secret types for the attach result
        serialized_settings = [s.serialize() for s in self.settings]
        serialized_secret_types = [st.serialize() for st in self.secret_types]

        # Auto-derive supports_time_travel and supports_column_statistics from tables
        self._build_registries()
        assert self._table_registry is not None
        has_time_travel = any(t.supports_time_travel for t in self._table_registry.values())
        has_column_statistics = any(bool(t.statistics) for t in self._table_registry.values())

        return CatalogAttachResult(
            attach_id=self._FIXED_ATTACH_ID,
            supports_transactions=getattr(self, "supports_transactions", False),
            supports_time_travel=has_time_travel,
            catalog_version_frozen=True,
            catalog_version=1,
            attach_id_required=False,
            default_schema=self._default_schema_name,
            settings=serialized_settings,
            secret_types=serialized_secret_types,
            comment=self.catalog.comment if self.catalog is not None else None,
            tags=dict(self.catalog.tags) if self.catalog is not None else {},
            supports_column_statistics=has_column_statistics,
        )

    def schemas(self, *, attach_id: AttachId, transaction_id: TransactionId | None) -> list[SchemaInfo]:
        """Get a list of schemas for the given attach_id."""
        self._build_registries()
        assert self._schema_registry is not None
        return [s.to_schema_info(attach_id) for s in self._schema_registry.values()]

    def schema_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
    ) -> SchemaInfo | None:
        """Get information about a schema (case-insensitive lookup)."""
        self._build_registries()
        assert self._schema_registry is not None
        schema = self._schema_registry.get(name.lower())
        return schema.to_schema_info(attach_id) if schema else None

    def table_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        """Get information about a table (case-insensitive lookup).

        When ``at_unit`` / ``at_value`` are provided, the default implementation
        returns the same table info (no schema evolution). Override this method
        to return version-specific schemas for time-travel queries.
        """
        _validate_at_params(at_unit, at_value)

        self._build_registries()
        assert self._table_registry is not None
        assert self._schema_registry is not None
        table = self._table_registry.get((schema_name.lower(), name.lower()))
        if table is None:
            return None

        # If AT clause present but table doesn't support time travel, error
        if at_unit and not table.supports_time_travel:
            raise ValueError(f"Table '{schema_name}.{name}' does not support time travel queries")

        schema = self._schema_registry.get(schema_name.lower())
        return table.to_table_info(schema.name if schema else schema_name)

    def view_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ViewInfo | None:
        """Get information about a view (case-insensitive lookup)."""
        self._build_registries()
        assert self._view_registry is not None
        assert self._schema_registry is not None
        view = self._view_registry.get((schema_name.lower(), name.lower()))
        if view:
            schema = self._schema_registry.get(schema_name.lower())
            return view.to_view_info(schema.name if schema else schema_name)
        return None

    def macro_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> MacroInfo | None:
        """Get information about a macro (case-insensitive lookup)."""
        self._build_registries()
        assert self._macro_registry is not None
        assert self._schema_registry is not None
        macro = self._macro_registry.get((schema_name.lower(), name.lower()))
        if macro:
            schema = self._schema_registry.get(schema_name.lower())
            return macro.to_macro_info(schema.name if schema else schema_name)
        return None

    def index_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> IndexInfo | None:
        """Get information about an index (case-insensitive lookup)."""
        self._build_registries()
        assert self._index_registry is not None
        assert self._schema_registry is not None
        index = self._index_registry.get((schema_name.lower(), name.lower()))
        if index is not None:
            schema = self._schema_registry.get(schema_name.lower())
            return index.to_index_info(schema.name if schema else schema_name)
        return None

    def table_column_statistics_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> TableColumnStatisticsResult | None:
        """Get column statistics from the Table descriptor's ``statistics`` dict.

        Automatically resolves plain Python values to typed PyArrow scalars
        using the column's Arrow type from the table schema.
        Override this method for dynamic or computed statistics.
        """
        self._build_registries()
        assert self._table_registry is not None
        table = self._table_registry.get((schema_name.lower(), name.lower()))
        if table is None:
            return None
        return table.resolve_column_statistics()

    def table_scan_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        """Get scan function for a table.

        For function-backed tables (Table.function is set), automatically returns
        a ScanFunctionResult that invokes the linked function.

        For tables with explicit columns, override this method in your Worker
        to provide scan functions.
        """
        _validate_at_params(at_unit, at_value)

        self._build_registries()
        assert self._table_registry is not None
        assert self._schema_registry is not None

        # Validate AT clause against table's supports_time_travel
        table = self._table_registry.get((schema_name.lower(), name.lower()))
        if table is not None and at_unit and not table.supports_time_travel:
            raise ValueError(f"Table '{schema_name}.{name}' does not support time travel queries")

        # Check if table exists and is function-backed
        if table is not None and table.function is not None:
            # Auto-implement for function-backed tables
            func_meta = table.function.get_metadata()
            return ScanFunctionResult(
                function_name=func_meta.name,
                positional_arguments=[],
                named_arguments={},
                required_extensions=[],
            )

        # No auto-implementation available - provide helpful error
        available = [
            f"{self._effective_catalog_name}.{s.name}.{t.name}"
            for s in self._schema_registry.values()
            for t in s.tables
        ]
        available_str = ", ".join(sorted(available)) if available else "(none)"

        raise NotImplementedError(
            f"table_scan_function_get not implemented for table "
            f"'{self._effective_catalog_name}.{schema_name}.{name}'. "
            f"Available tables: {available_str}. "
            f"Either use Table(function=...) for automatic scanning, "
            f"or override table_scan_function_get in your Worker."
        )

    def _write_function_get(
        self,
        *,
        schema_name: str,
        name: str,
        operation: str,
        attr_name: str,
    ) -> ScanFunctionResult:
        """Shared implementation for table_{insert,update,delete}_function_get."""
        self._build_registries()
        assert self._table_registry is not None

        table = self._table_registry.get((schema_name.lower(), name.lower()))
        if table is None:
            raise NotImplementedError(f"Table '{schema_name}.{name}' not found in catalog.")

        write_func = getattr(table, attr_name, None)
        if write_func is None:
            raise CatalogReadOnlyError(f"Table '{schema_name}.{name}' does not support {operation}.")

        func_meta = write_func.get_metadata()
        return ScanFunctionResult(
            function_name=func_meta.name,
            positional_arguments=[],
            named_arguments={},
            required_extensions=[],
        )

    def table_insert_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        """Get insert function for a table."""
        return self._write_function_get(
            schema_name=schema_name,
            name=name,
            operation="INSERT",
            attr_name="insert_function",
        )

    def table_update_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        """Get update function for a table."""
        return self._write_function_get(
            schema_name=schema_name,
            name=name,
            operation="UPDATE",
            attr_name="update_function",
        )

    def table_delete_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        """Get delete function for a table."""
        return self._write_function_get(
            schema_name=schema_name,
            name=name,
            operation="DELETE",
            attr_name="delete_function",
        )

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: Literal[SchemaObjectType.TABLE],
    ) -> Sequence[TableInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: Literal[SchemaObjectType.VIEW],
    ) -> Sequence[ViewInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: Literal[
            SchemaObjectType.SCALAR_FUNCTION,
            SchemaObjectType.TABLE_FUNCTION,
            SchemaObjectType.AGGREGATE_FUNCTION,
        ],
    ) -> Sequence[FunctionInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: Literal[SchemaObjectType.SCALAR_MACRO, SchemaObjectType.TABLE_MACRO],
    ) -> Sequence[MacroInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: Literal[SchemaObjectType.INDEX],
    ) -> Sequence[IndexInfo]: ...

    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: SchemaObjectType,
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo | MacroInfo | IndexInfo]:
        """List contents of a schema.

        Returns tables, views, functions, macros, or indexes based on the type parameter.
        Uses case-insensitive schema name lookup.

        Args:
            attach_id: The attachment identifier.
            transaction_id: The transaction identifier, if any.
            name: The name of the schema.
            type: The type of objects to return. Must be a SchemaObjectType enum.

        Returns:
            A list of TableInfo, ViewInfo, FunctionInfo, MacroInfo, or IndexInfo objects.

        """
        self._build_registries()
        assert self._schema_registry is not None
        assert self._table_registry is not None
        assert self._view_registry is not None
        assert self._function_registry is not None
        assert self._macro_registry is not None
        assert self._index_registry is not None

        # Case-insensitive schema lookup
        name_lower = name.lower()
        schema = self._schema_registry.get(name_lower)
        if schema is None:
            return []

        schema_name = schema.name

        # Normalize type parameter (may be string from wire protocol)
        type_enum = type if isinstance(type, SchemaObjectType) else SchemaObjectType(type)

        results: list[TableInfo | ViewInfo | FunctionInfo | MacroInfo | IndexInfo] = []

        if type_enum == SchemaObjectType.TABLE:
            for (sn, _), table in self._table_registry.items():
                if sn == name_lower:
                    results.append(table.to_table_info(schema_name))
        elif type_enum == SchemaObjectType.VIEW:
            for (sn, _), view in self._view_registry.items():
                if sn == name_lower:
                    results.append(view.to_view_info(schema_name))
        elif type_enum == SchemaObjectType.INDEX:
            for (sn, _), index in self._index_registry.items():
                if sn == name_lower:
                    results.append(index.to_index_info(schema_name))
        elif type_enum in (SchemaObjectType.SCALAR_MACRO, SchemaObjectType.TABLE_MACRO):
            target_macro_type = MacroType.SCALAR if type_enum == SchemaObjectType.SCALAR_MACRO else MacroType.TABLE
            for (sn, _), macro in self._macro_registry.items():
                if sn == name_lower and macro.macro_type == target_macro_type:
                    results.append(macro.to_macro_info(schema_name))
        else:
            # SCALAR_FUNCTION or TABLE_FUNCTION
            for (sn, _), func_classes in self._function_registry.items():
                if sn != name_lower:
                    continue
                for func_cls in func_classes:
                    func_info = self._function_to_info(func_cls, schema_name)
                    # Filter by function type
                    if type_enum == SchemaObjectType.SCALAR_FUNCTION and func_info.function_type != FunctionType.SCALAR:
                        continue
                    if type_enum == SchemaObjectType.TABLE_FUNCTION and func_info.function_type != FunctionType.TABLE:
                        continue
                    if (
                        type_enum == SchemaObjectType.AGGREGATE_FUNCTION
                        and func_info.function_type != FunctionType.AGGREGATE
                    ):
                        continue
                    results.append(func_info)

        return results

    def _function_to_info(self, func_cls: type, schema_name: str) -> FunctionInfo:
        """Convert a function class to FunctionInfo."""
        # Import here to avoid circular imports
        from vgi.argument_spec import (
            argument_specs_to_schema,
            extract_argument_specs,
        )
        from vgi.metadata import CatalogFunctionType as MetadataFunctionType
        from vgi.metadata import resolve_metadata

        meta = resolve_metadata(func_cls)

        # Map metadata function type to catalog function type
        func_type_map = {
            MetadataFunctionType.SCALAR: FunctionType.SCALAR,
            MetadataFunctionType.TABLE: FunctionType.TABLE,
            MetadataFunctionType.AGGREGATE: FunctionType.AGGREGATE,
        }
        func_type = func_type_map.get(meta.function_type, FunctionType.TABLE)

        # Extract argument specs with proper Arrow types
        arg_specs = extract_argument_specs(func_cls)
        args_schema = argument_specs_to_schema(arg_specs)
        args_bytes = SerializedSchema(args_schema.serialize().to_pybytes())

        # Get output schema from catalog introspection methods if available
        output_schema: pa.Schema = pa.schema([])
        has_catalog_schema = hasattr(func_cls, "catalog_output_schema")
        if func_type in (FunctionType.SCALAR, FunctionType.AGGREGATE) and has_catalog_schema:
            # ScalarFunction/AggregateFunction has catalog_output_schema() classmethod
            output_schema = func_cls.catalog_output_schema()  # type: ignore[attr-defined]
        output_bytes = SerializedSchema(output_schema.serialize().to_pybytes())

        is_scalar = func_type == FunctionType.SCALAR
        is_aggregate = func_type == FunctionType.AGGREGATE

        return FunctionInfo(
            name=meta.name,
            schema_name=schema_name,
            function_type=func_type,
            arguments=args_bytes,
            output_schema=output_bytes,
            comment=None,  # Functions don't use comment; use description instead
            tags=meta.tags,
            # Scalar/aggregate function behavior fields
            stability=meta.stability if is_scalar else None,
            null_handling=meta.null_handling if (is_scalar or is_aggregate) else None,
            # Documentation fields
            description=meta.description or "",  # Intrinsic from Meta.description
            examples=[
                CatalogExample(
                    sql=ex.sql,
                    description=ex.description,
                    expected_output=ex.expected_output,
                )
                for ex in meta.examples
            ],
            categories=meta.categories,
            # Table function capabilities (None for scalar)
            projection_pushdown=None if is_scalar else meta.projection_pushdown,
            filter_pushdown=None if is_scalar else meta.filter_pushdown,
            sampling_pushdown=None if is_scalar else meta.sampling_pushdown,
            supported_expression_filters=[] if is_scalar else meta.supported_expression_filters,
            order_preservation=None if is_scalar else meta.preserves_order,
            # Aggregate function fields
            order_dependent=meta.order_dependent,
            distinct_dependent=meta.distinct_dependent,
            # Settings
            required_settings=meta.required_settings,
            # Secrets
            required_secrets=list(meta.required_secrets),
        )

    # ========== DDL operations (not supported — read-only catalog) ==========

    catalog_create = _read_only("create catalog")
    catalog_drop = _read_only("drop catalog")
    catalog_transaction_begin = _read_only("begin transaction")
    catalog_transaction_commit = _read_only("commit transaction")
    catalog_transaction_rollback = _read_only("rollback transaction")
    schema_create = _read_only("create schema")
    schema_drop = _read_only("drop schema")
    table_create = _read_only("create table")
    table_drop = _read_only("drop table")
    table_comment_set = _read_only("set table comment")
    table_column_comment_set = _read_only("set column comment")
    table_rename = _read_only("rename table")
    table_column_add = _read_only("add column")
    table_column_drop = _read_only("drop column")
    table_column_rename = _read_only("rename column")
    table_column_default_set = _read_only("set column default")
    table_column_default_drop = _read_only("drop column default")
    table_column_type_change = _read_only("change column type")
    table_not_null_drop = _read_only("drop NOT NULL constraint")
    table_not_null_set = _read_only("set NOT NULL constraint")
    view_create = _read_only("create view")
    view_drop = _read_only("drop view")
    view_rename = _read_only("rename view")
    view_comment_set = _read_only("set view comment")
    macro_create = _read_only("create macro")
    macro_drop = _read_only("drop macro")
    index_create = _read_only("create index")
    index_drop = _read_only("drop index")
