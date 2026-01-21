"""VGI Catalog Interface for exposing catalogs, schemas, tables, and views.

This module provides the abstract base class and data types for implementing
catalog interfaces in VGI workers, enabling DuckDB ATTACH support.
"""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NewType, Self, overload

if TYPE_CHECKING:
    from vgi.catalog.descriptors import Catalog, Schema, Table, View
    from vgi.catalog.setting import SettingSpec

import pyarrow as pa

import vgi.ipc_utils
from vgi.metadata import (
    DEFAULT_MAX_WORKERS,
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
    "SchemaObjectType",
    # Schema mapping for catalog methods
    "CATALOG_METHOD_SCHEMAS",
    "get_catalog_method_schema",
]


@dataclass(frozen=True)
class CatalogExample:
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
class CatalogAttachResult:
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

    ARROW_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("attach_id", pa.binary(), nullable=False),
            pa.field("supports_transactions", pa.bool_(), nullable=False),
            pa.field("supports_time_travel", pa.bool_(), nullable=False),
            pa.field("catalog_version_frozen", pa.bool_(), nullable=False),
            pa.field("catalog_version", pa.int64(), nullable=False),
            pa.field("attach_id_required", pa.bool_(), nullable=False),
            pa.field("default_schema", pa.string(), nullable=False),
            pa.field("settings", pa.list_(pa.binary()), nullable=False),
        ]  # type: ignore[arg-type]
    )

    def to_row_dict(self) -> dict[str, Any]:
        """Convert to a dictionary for batch construction."""
        return {
            "attach_id": self.attach_id,
            "supports_transactions": self.supports_transactions,
            "supports_time_travel": self.supports_time_travel,
            "catalog_version_frozen": self.catalog_version_frozen,
            "catalog_version": self.catalog_version,
            "attach_id_required": self.attach_id_required,
            "default_schema": self.default_schema,
            "settings": self.settings,
        }

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes."""
        batch = pa.RecordBatch.from_pylist(
            [self.to_row_dict()],
            schema=self.ARROW_SCHEMA,
        )
        return vgi.ipc_utils.serialize_record_batch(batch)

    @classmethod
    def deserialize(cls, batch: pa.RecordBatch) -> Self:
        """Deserialize from Arrow RecordBatch."""
        row = vgi.ipc_utils.validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=[
                "attach_id",
                "supports_transactions",
                "supports_time_travel",
                "catalog_version_frozen",
                "catalog_version",
                "attach_id_required",
                "default_schema",
            ],
        )
        return cls(
            attach_id=AttachId(row["attach_id"]),
            supports_transactions=row["supports_transactions"],
            supports_time_travel=row["supports_time_travel"],
            catalog_version_frozen=row["catalog_version_frozen"],
            catalog_version=row["catalog_version"],
            attach_id_required=row["attach_id_required"],
            default_schema=row["default_schema"],
            settings=list(row.get("settings") or []),
        )


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
class SchemaInfo(CatalogObject):
    """Information about a schema in a catalog."""

    attach_id: AttachId
    name: str

    ARROW_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("attach_id", pa.binary(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
            pa.field("comment", pa.string(), nullable=True),
            pa.field("tags", pa.map_(pa.string(), pa.string()), nullable=False),
        ]  # type: ignore[arg-type]
    )

    def to_row_dict(self) -> dict[str, Any]:
        """Convert to a dictionary for batch construction."""
        return {
            "attach_id": self.attach_id,
            "name": self.name,
            "comment": self.comment,
            "tags": self.tags,
        }

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes."""
        batch = pa.RecordBatch.from_pylist(
            [self.to_row_dict()], schema=self.ARROW_SCHEMA
        )
        return vgi.ipc_utils.serialize_record_batch(batch)

    @classmethod
    def deserialize(cls, batch: pa.RecordBatch) -> Self:
        """Deserialize from Arrow RecordBatch."""
        row = vgi.ipc_utils.validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=["attach_id", "name", "tags"],
        )
        return cls(
            attach_id=AttachId(row["attach_id"]),
            name=row["name"],
            comment=row.get("comment"),
            tags=dict(row["tags"]) if row["tags"] else {},
        )


@dataclass(frozen=True)
class TableInfo(CatalogSchemaObject):
    """Information about a table in a schema."""

    # The columns of the table as a PyArrow schema
    # that is serialized as bytes.
    columns: SerializedSchema

    not_null_constraints: list[int]
    unique_constraints: list[list[int]]
    check_constraints: list[str]

    ARROW_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("name", pa.string(), nullable=False),
            pa.field("schema_name", pa.string(), nullable=False),
            pa.field("columns", pa.binary(), nullable=False),
            pa.field("not_null_constraints", pa.list_(pa.int32()), nullable=False),
            pa.field(
                "unique_constraints", pa.list_(pa.list_(pa.int32())), nullable=False
            ),
            pa.field("check_constraints", pa.list_(pa.string()), nullable=False),
            pa.field("comment", pa.string(), nullable=True),
            pa.field("tags", pa.map_(pa.string(), pa.string()), nullable=False),
        ]  # type: ignore[arg-type]
    )

    def to_row_dict(self) -> dict[str, Any]:
        """Convert to a dictionary for batch construction."""
        return {
            "name": self.name,
            "schema_name": self.schema_name,
            "columns": self.columns,
            "not_null_constraints": self.not_null_constraints,
            "unique_constraints": self.unique_constraints,
            "check_constraints": self.check_constraints,
            "comment": self.comment,
            "tags": self.tags,
        }

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes."""
        batch = pa.RecordBatch.from_pylist(
            [self.to_row_dict()], schema=self.ARROW_SCHEMA
        )
        return vgi.ipc_utils.serialize_record_batch(batch)

    @classmethod
    def deserialize(cls, batch: pa.RecordBatch) -> Self:
        """Deserialize from Arrow RecordBatch."""
        row = vgi.ipc_utils.validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=[
                "name",
                "schema_name",
                "columns",
                "not_null_constraints",
                "unique_constraints",
                "check_constraints",
                "tags",
            ],
        )
        return cls(
            name=row["name"],
            schema_name=row["schema_name"],
            columns=SerializedSchema(row["columns"]),
            not_null_constraints=list(row["not_null_constraints"]),
            unique_constraints=[list(c) for c in row["unique_constraints"]],
            check_constraints=list(row["check_constraints"]),
            comment=row.get("comment"),
            tags=dict(row["tags"]) if row["tags"] else {},
        )


@dataclass(frozen=True)
class ViewInfo(CatalogSchemaObject):
    """Information about a view in a schema."""

    # The definition of the view which is a SQL query string.
    definition: str

    ARROW_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("name", pa.string(), nullable=False),
            pa.field("schema_name", pa.string(), nullable=False),
            pa.field("definition", pa.string(), nullable=False),
            pa.field("comment", pa.string(), nullable=True),
            pa.field("tags", pa.map_(pa.string(), pa.string()), nullable=False),
        ]
    )

    def to_row_dict(self) -> dict[str, Any]:
        """Convert to a dictionary for batch construction."""
        return {
            "name": self.name,
            "schema_name": self.schema_name,
            "definition": self.definition,
            "comment": self.comment,
            "tags": self.tags,
        }

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes."""
        batch = pa.RecordBatch.from_pylist(
            [self.to_row_dict()], schema=self.ARROW_SCHEMA
        )
        return vgi.ipc_utils.serialize_record_batch(batch)

    @classmethod
    def deserialize(cls, batch: pa.RecordBatch) -> Self:
        """Deserialize from Arrow RecordBatch."""
        row = vgi.ipc_utils.validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=["name", "schema_name", "definition", "tags"],
        )
        return cls(
            name=row["name"],
            schema_name=row["schema_name"],
            definition=row["definition"],
            comment=row.get("comment"),
            tags=dict(row["tags"]) if row["tags"] else {},
        )


class FunctionType(Enum):
    """The type of function in a schema."""

    SCALAR = "scalar"
    TABLE = "table"
    AGGREGATE = "aggregate"


class SchemaObjectType(Enum):
    """The type of object that can exist within a schema.

    Used to filter results from schema_contents().
    """

    TABLE = "table"
    VIEW = "view"
    SCALAR_FUNCTION = "scalar_function"
    TABLE_FUNCTION = "table_function"


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
class FunctionInfo(CatalogSchemaObject):
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
    examples: list[CatalogExample | str] = field(default_factory=list)
    categories: list[str] = field(default_factory=list)

    # Table function capabilities (None for scalar functions)
    projection_pushdown: bool | None = None
    filter_pushdown: bool | None = None
    order_preservation: OrderPreservation | None = None
    max_workers: int | None = None

    # Aggregate function fields (future)
    order_dependent: OrderDependence = OrderDependence.NOT_ORDER_DEPENDENT
    distinct_dependent: DistinctDependence = DistinctDependence.NOT_DISTINCT_DEPENDENT

    # Settings required by the function
    required_settings: list[str] = field(default_factory=list)

    ARROW_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("name", pa.string(), nullable=False),
            pa.field(
                "schema_name", pa.dictionary(pa.int8(), pa.string()), nullable=False
            ),
            pa.field(
                "function_type", pa.dictionary(pa.int8(), pa.string()), nullable=False
            ),
            pa.field("arguments", pa.binary(), nullable=False),
            pa.field("output_schema", pa.binary(), nullable=False),
            pa.field("tags", pa.map_(pa.string(), pa.string()), nullable=False),
            # Scalar function behavior fields (nullable for non-scalar functions)
            pa.field("stability", pa.dictionary(pa.int8(), pa.string()), nullable=True),
            pa.field(
                "null_handling", pa.dictionary(pa.int8(), pa.string()), nullable=True
            ),
            # Documentation fields
            pa.field("description", pa.string(), nullable=False),
            pa.field(
                "examples",
                pa.list_(
                    pa.struct(
                        [
                            pa.field("sql", pa.string(), nullable=False),
                            pa.field("description", pa.string(), nullable=False),
                            pa.field("expected_output", pa.string(), nullable=True),
                        ]
                    )
                ),
                nullable=False,
            ),
            pa.field("categories", pa.list_(pa.string()), nullable=False),
            # Table function capabilities (nullable for scalar functions)
            pa.field("projection_pushdown", pa.bool_(), nullable=True),
            pa.field("filter_pushdown", pa.bool_(), nullable=True),
            pa.field(
                "order_preservation",
                pa.dictionary(pa.int8(), pa.string()),
                nullable=True,
            ),
            pa.field("max_workers", pa.int32(), nullable=True),
            # Aggregate function fields
            pa.field(
                "order_dependent", pa.dictionary(pa.int8(), pa.string()), nullable=False
            ),
            pa.field(
                "distinct_dependent",
                pa.dictionary(pa.int8(), pa.string()),
                nullable=False,
            ),
            # Settings
            pa.field("required_settings", pa.list_(pa.string()), nullable=False),
        ]
    )

    def to_row_dict(self) -> dict[str, Any]:
        """Convert to a dictionary for batch construction."""
        return {
            "name": self.name,
            "schema_name": self.schema_name,
            "function_type": self.function_type.value,
            "arguments": self.arguments,
            "output_schema": self.output_schema,
            "tags": self.tags,
            # Scalar function behavior fields (None for non-scalar)
            "stability": self.stability.name if self.stability else None,
            "null_handling": (self.null_handling.name if self.null_handling else None),
            # Documentation fields
            "description": self.description,
            "examples": [
                (
                    {
                        "sql": ex.sql,
                        "description": ex.description,
                        "expected_output": ex.expected_output,
                    }
                    if isinstance(ex, CatalogExample)
                    else {"sql": ex, "description": "", "expected_output": None}
                )
                for ex in self.examples
            ],
            "categories": self.categories,
            # Table function capabilities (None for scalar)
            "projection_pushdown": self.projection_pushdown,
            "filter_pushdown": self.filter_pushdown,
            "order_preservation": (
                self.order_preservation.name if self.order_preservation else None
            ),
            "max_workers": self.max_workers,
            # Aggregate function fields
            "order_dependent": self.order_dependent.name,
            "distinct_dependent": self.distinct_dependent.name,
            # Settings
            "required_settings": self.required_settings,
        }

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes."""
        batch = pa.RecordBatch.from_pylist(
            [self.to_row_dict()], schema=self.ARROW_SCHEMA
        )
        return vgi.ipc_utils.serialize_record_batch(batch)

    @classmethod
    def deserialize(cls, batch: pa.RecordBatch) -> Self:
        """Deserialize from Arrow RecordBatch.

        Supports backward compatibility with data serialized before new fields
        were added by using sensible defaults for missing fields.
        """
        row = vgi.ipc_utils.validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=[
                "name",
                "schema_name",
                "function_type",
                "arguments",
                "output_schema",
                "tags",
            ],
        )
        return cls(
            name=row["name"],
            schema_name=row["schema_name"],
            function_type=FunctionType(row["function_type"]),
            arguments=SerializedSchema(row["arguments"]),
            output_schema=SerializedSchema(row["output_schema"]),
            comment=None,  # Functions don't use comment; use description instead
            tags=dict(row["tags"]) if row["tags"] else {},
            # Scalar function behavior fields (None for non-scalar functions)
            stability=(
                FunctionStability[row["stability"]]
                if row.get("stability") is not None
                else None
            ),
            null_handling=(
                NullHandling[row["null_handling"]]
                if row.get("null_handling") is not None
                else None
            ),
            # Documentation fields
            description=row.get("description", ""),
            examples=[
                CatalogExample(
                    sql=ex["sql"],
                    description=ex.get("description", ""),
                    expected_output=ex.get("expected_output"),
                )
                for ex in (row.get("examples") or [])
            ],
            categories=list(row.get("categories") or []),
            # Table function capabilities (None for scalar functions)
            projection_pushdown=row.get("projection_pushdown"),
            filter_pushdown=row.get("filter_pushdown"),
            order_preservation=(
                OrderPreservation[row["order_preservation"]]
                if row.get("order_preservation") is not None
                else None
            ),
            max_workers=row.get("max_workers"),
            # Aggregate function fields
            order_dependent=OrderDependence[
                row.get("order_dependent", "NOT_ORDER_DEPENDENT")
            ],
            distinct_dependent=DistinctDependence[
                row.get("distinct_dependent", "NOT_DISTINCT_DEPENDENT")
            ],
            # Settings
            required_settings=list(row.get("required_settings") or []),
        )


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

    Example:
        >>> result = ScanFunctionResult(
        ...     function_name="read_parquet",
        ...     positional_arguments=[pa.scalar("data.parquet")],
        ...     named_arguments={"hive_partitioning": pa.scalar(True)},
        ...     required_extensions=["parquet"],
        ... )

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
            "arguments": vgi.ipc_utils.serialize_record_batch(argument_batch),
            "required_extensions": list(self.required_extensions)
            if self.required_extensions is not None
            else None,
        }

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes."""
        batch = pa.RecordBatch.from_pylist(
            [self.to_row_dict()],
            schema=self.ARROW_SCHEMA,
        )
        return vgi.ipc_utils.serialize_record_batch(batch)

    @classmethod
    def deserialize(cls, batch: pa.RecordBatch) -> Self:
        """Deserialize from Arrow RecordBatch."""
        row = vgi.ipc_utils.validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=["function_name", "arguments"],
        )

        # Deserialize the nested arguments batch.
        # row["arguments"] is already bytes (validate_single_row_batch returns
        # Python values, not PyArrow scalars).
        arguments_bytes: bytes = row["arguments"]
        arguments_batch, _ = vgi.ipc_utils.deserialize_record_batch(arguments_bytes)

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
            function_name=row["function_name"],
            positional_arguments=positional_arguments,
            named_arguments=named_arguments,
            required_extensions=list(row.get("required_extensions") or []),
        )


# Mapping of catalog method names to their expected Arrow schemas.
# Used by workers to determine the schema for empty results without
# needing special-case code for each method.
#
# - Methods returning None (DDL operations) don't need entries - use empty schema.
# - Methods with type-dependent schemas use a dict keyed by the type parameter value.
CATALOG_METHOD_SCHEMAS: dict[str, pa.Schema | dict[str, pa.Schema]] = {}


def _init_catalog_method_schemas() -> None:
    """Initialize CATALOG_METHOD_SCHEMAS after all dataclasses are defined.

    This is called at module load time to populate the schema mapping.
    We use a function to avoid forward reference issues.
    """
    global CATALOG_METHOD_SCHEMAS
    CATALOG_METHOD_SCHEMAS = {
        "catalogs": pa.schema([("value", pa.string())]),
        "catalog_attach": CatalogAttachResult.ARROW_SCHEMA,
        "catalog_version": pa.schema([("value", pa.int64())]),
        "schemas": SchemaInfo.ARROW_SCHEMA,
        "schema_get": SchemaInfo.ARROW_SCHEMA,
        "schema_contents": {
            "table": TableInfo.ARROW_SCHEMA,
            "view": ViewInfo.ARROW_SCHEMA,
            "scalar_function": FunctionInfo.ARROW_SCHEMA,
            "table_function": FunctionInfo.ARROW_SCHEMA,
        },
        "table_get": TableInfo.ARROW_SCHEMA,
        "table_scan_function_get": ScanFunctionResult.ARROW_SCHEMA,
        "view_get": ViewInfo.ARROW_SCHEMA,
    }


# Initialize the schema mapping
_init_catalog_method_schemas()


def get_catalog_method_schema(method_name: str, kwargs: dict[str, Any]) -> pa.Schema:
    """Get the Arrow schema for a catalog method's result.

    Args:
        method_name: The name of the catalog method.
        kwargs: The keyword arguments passed to the method (used to determine
            type-dependent schemas like schema_contents).

    Returns:
        The Arrow schema for the method's result. Returns an empty schema
        for DDL operations that return None.

    """
    schema_or_map = CATALOG_METHOD_SCHEMAS.get(method_name)
    if schema_or_map is None:
        return pa.schema([])  # DDL operations return None
    if isinstance(schema_or_map, dict):
        # schema_contents - lookup by type parameter
        type_value = kwargs.get("type")
        if type_value is None:
            return pa.schema([])
        if isinstance(type_value, SchemaObjectType):
            type_value = type_value.value
        return schema_or_map.get(str(type_value), pa.schema([]))
    return schema_or_map


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

    def catalog_create(
        self, *, name: str, on_conflict: OnConflict, options: dict[str, Any]
    ) -> None:
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

    def catalog_transaction_commit(
        self, *, attach_id: AttachId, transaction_id: TransactionId
    ) -> None:
        """Commit the transaction for the given attachment.

        If the transaction cannot be committed, an exception should be raised.
        """
        raise NotImplementedError("Catalog transactions not implemented.")

    def catalog_transaction_rollback(
        self, *, attach_id: AttachId, transaction_id: TransactionId
    ) -> None:
        """Rollback the transaction for the given attachment.

        If the transaction cannot be rolled back, an exception should be raised.
        """
        raise NotImplementedError("Catalog transactions not implemented.")

    @abstractmethod
    def catalog_attach(
        self, *, name: str, options: dict[str, Any]
    ) -> CatalogAttachResult:
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

    def catalog_version(
        self, *, attach_id: AttachId, transaction_id: TransactionId | None
    ) -> int:
        """Get the current catalog version for the given attach_id and transaction_id.

        Returns an integer representing the current catalog version.

        Changes to schemas, tables, and objects increment this version. It is used to
        expire cached catalog/schema/object information inside a VGI client or process.

        The default implementation returns 0.
        """
        return 0

    def schemas(
        self, *, attach_id: AttachId, transaction_id: TransactionId | None
    ) -> list[SchemaInfo]:
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
            SchemaObjectType.SCALAR_FUNCTION, SchemaObjectType.TABLE_FUNCTION
        ],
    ) -> Sequence[FunctionInfo]: ...

    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: SchemaObjectType,
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo]:
        """Get the contents of the schema with the given name.

        Schemas can contain tables, views, and various types of functions.

        Args:
            attach_id: The attachment identifier.
            transaction_id: The transaction identifier, if any.
            name: The name of the schema.
            type: The type of objects to return. Must be a SchemaObjectType enum:
                - SchemaObjectType.TABLE: Return only tables
                - SchemaObjectType.VIEW: Return only views
                - SchemaObjectType.SCALAR_FUNCTION: Scalar functions
                - SchemaObjectType.TABLE_FUNCTION: Table functions

        Returns:
            A list of TableInfo, ViewInfo, or FunctionInfo objects
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
    ) -> TableInfo | None:
        """Get information about the table with the given name in the specified schema.

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

    Example:
        class MyFunctionCatalog(ReadOnlyCatalogInterface):
            catalog_name = "my_catalog"
            functions = [MyFunction, OtherFunction]

    """

    supports_transactions = False
    catalog_version_frozen = True

    # Class attributes for function-based catalogs
    catalog_name: str = "functions"
    functions: list[type] = []
    settings: list["SettingSpec"] = []

    # NEW: Optional Catalog object for declarative definition
    catalog: "Catalog | None" = None

    # Fixed attach_id for read-only catalogs (no need for unique IDs)
    _FIXED_ATTACH_ID: AttachId = AttachId(b"readonly-catalog-")

    # Instance-level registry caches (built lazily)
    # Keys are LOWERCASE for case-insensitive lookup
    _schema_registry: "dict[str, Schema] | None" = None
    _table_registry: "dict[tuple[str, str], Table] | None" = None
    _view_registry: "dict[tuple[str, str], View] | None" = None
    _function_registry: "dict[tuple[str, str], type] | None" = None

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

        def _register_table(schema_key: str, table: "Table") -> None:
            key = (schema_key, table.name.lower())
            if key in self._table_registry:  # type: ignore[operator]
                raise ValueError(
                    f"Duplicate table '{table.name}' in schema '{schema_key}'"
                )
            self._table_registry[key] = table  # type: ignore[index]

        def _register_view(schema_key: str, view: "View") -> None:
            key = (schema_key, view.name.lower())
            if key in self._view_registry:  # type: ignore[operator]
                raise ValueError(
                    f"Duplicate view '{view.name}' in schema '{schema_key}'"
                )
            self._view_registry[key] = view  # type: ignore[index]

        def _register_function(schema_key: str, func_cls: type) -> None:
            meta = func_cls.get_metadata()  # type: ignore[attr-defined]
            key = (schema_key, meta.name.lower())
            if key in self._function_registry:  # type: ignore[operator]
                raise ValueError(
                    f"Duplicate function '{meta.name}' in schema '{schema_key}'"
                )
            self._function_registry[key] = func_cls  # type: ignore[index]

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

    def catalog_attach(
        self, *, name: str, options: dict[str, Any]
    ) -> CatalogAttachResult:
        """Attach to the catalog."""
        effective_name = self._effective_catalog_name
        if name != effective_name:
            raise ValueError(f"Unknown catalog: {name!r}. Available: {effective_name}")

        # Serialize settings for the attach result
        serialized_settings = [s.serialize() for s in self.settings]

        return CatalogAttachResult(
            attach_id=self._FIXED_ATTACH_ID,
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=True,
            catalog_version=1,
            attach_id_required=False,
            default_schema=self._default_schema_name,
            settings=serialized_settings,
        )

    def schemas(
        self, *, attach_id: AttachId, transaction_id: TransactionId | None
    ) -> list[SchemaInfo]:
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
    ) -> TableInfo | None:
        """Get information about a table (case-insensitive lookup)."""
        self._build_registries()
        assert self._table_registry is not None
        assert self._schema_registry is not None
        table = self._table_registry.get((schema_name.lower(), name.lower()))
        if table:
            schema = self._schema_registry.get(schema_name.lower())
            return table.to_table_info(schema.name if schema else schema_name)
        return None

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
        self._build_registries()
        assert self._table_registry is not None
        assert self._schema_registry is not None

        # Check if table exists and is function-backed
        table = self._table_registry.get((schema_name.lower(), name.lower()))
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
            SchemaObjectType.SCALAR_FUNCTION, SchemaObjectType.TABLE_FUNCTION
        ],
    ) -> Sequence[FunctionInfo]: ...

    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: SchemaObjectType,
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo]:
        """List contents of a schema.

        Returns tables, views, or functions based on the type parameter.
        Uses case-insensitive schema name lookup.

        Args:
            attach_id: The attachment identifier.
            transaction_id: The transaction identifier, if any.
            name: The name of the schema.
            type: The type of objects to return. Must be a SchemaObjectType enum.

        Returns:
            A list of TableInfo, ViewInfo, or FunctionInfo objects.

        """
        self._build_registries()
        assert self._schema_registry is not None
        assert self._table_registry is not None
        assert self._view_registry is not None
        assert self._function_registry is not None

        # Case-insensitive schema lookup
        name_lower = name.lower()
        schema = self._schema_registry.get(name_lower)
        if schema is None:
            return []

        schema_name = schema.name

        # Normalize type parameter (may be string from wire protocol)
        if isinstance(type, SchemaObjectType):
            type_enum = type
        else:
            type_enum = SchemaObjectType(type)

        results: list[TableInfo | ViewInfo | FunctionInfo] = []

        if type_enum == SchemaObjectType.TABLE:
            for (sn, _), table in self._table_registry.items():
                if sn == name_lower:
                    results.append(table.to_table_info(schema_name))
        elif type_enum == SchemaObjectType.VIEW:
            for (sn, _), view in self._view_registry.items():
                if sn == name_lower:
                    results.append(view.to_view_info(schema_name))
        else:
            # SCALAR_FUNCTION or TABLE_FUNCTION
            for (sn, _), func_cls in self._function_registry.items():
                if sn != name_lower:
                    continue
                func_info = self._function_to_info(func_cls, schema_name)
                # Filter by function type
                if (
                    type_enum == SchemaObjectType.SCALAR_FUNCTION
                    and func_info.function_type != FunctionType.SCALAR
                ):
                    continue
                if (
                    type_enum == SchemaObjectType.TABLE_FUNCTION
                    and func_info.function_type
                    not in (FunctionType.TABLE, FunctionType.AGGREGATE)
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
        from vgi.metadata import FunctionType as MetadataFunctionType
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
        if func_type == FunctionType.SCALAR and has_catalog_schema:
            # ScalarFunction has catalog_output_schema() classmethod
            output_schema = func_cls.catalog_output_schema()  # type: ignore[attr-defined]
        output_bytes = SerializedSchema(output_schema.serialize().to_pybytes())

        is_scalar = func_type == FunctionType.SCALAR

        return FunctionInfo(
            name=meta.name,
            schema_name=schema_name,
            function_type=func_type,
            arguments=args_bytes,
            output_schema=output_bytes,
            comment=None,  # Functions don't use comment; use description instead
            tags=meta.tags,
            # Scalar function behavior fields (None for non-scalar)
            stability=meta.stability if is_scalar else None,
            null_handling=meta.null_handling if is_scalar else None,
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
            order_preservation=None if is_scalar else meta.preserves_order,
            max_workers=(
                None
                if is_scalar
                else (
                    meta.max_workers
                    if meta.max_workers is not None
                    else DEFAULT_MAX_WORKERS
                )
            ),
            # Aggregate function fields
            order_dependent=meta.order_dependent,
            distinct_dependent=meta.distinct_dependent,
            # Settings
            required_settings=meta.required_settings,
        )

    # ========== Catalog DDL (not supported) ==========

    def catalog_create(
        self, *, name: str, on_conflict: OnConflict, options: dict[str, Any]
    ) -> None:
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot create catalog: catalog is read-only")

    def catalog_drop(self, *, name: str) -> None:
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot drop catalog: catalog is read-only")

    # ========== Transaction methods (not supported) ==========

    def catalog_transaction_begin(self, *, attach_id: AttachId) -> TransactionId | None:
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError(
            "Cannot begin transaction: catalog is read-only and does not support "
            "transactions"
        )

    def catalog_transaction_commit(
        self, *, attach_id: AttachId, transaction_id: TransactionId
    ) -> None:
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError(
            "Cannot commit transaction: catalog is read-only and does not support "
            "transactions"
        )

    def catalog_transaction_rollback(
        self, *, attach_id: AttachId, transaction_id: TransactionId
    ) -> None:
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError(
            "Cannot rollback transaction: catalog is read-only and does not support "
            "transactions"
        )

    # ========== Schema DDL (not supported) ==========

    def schema_create(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        comment: str | None,
        tags: dict[str, str],
    ) -> None:
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot create schema: catalog is read-only")

    def schema_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        ignore_not_found: bool,
        cascade: bool,
    ) -> None:
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot drop schema: catalog is read-only")

    # ========== Table DDL (not supported) ==========

    def table_create(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        columns: SerializedSchema,
        on_conflict: OnConflict,
        not_null_constraints: list[int],
        unique_constraints: list[list[int]],
        check_constraints: list[str],
    ) -> None:
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot create table: catalog is read-only")

    def table_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        ignore_not_found: bool,
    ) -> None:
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot drop table: catalog is read-only")

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
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot set table comment: catalog is read-only")

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
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot rename table: catalog is read-only")

    def table_column_add(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        column_definition: SerializedSchema,
        ignore_not_found: bool,
        if_column_not_exists: bool,
    ) -> None:
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot add column: catalog is read-only")

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
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot drop column: catalog is read-only")

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
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot rename column: catalog is read-only")

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
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot set column default: catalog is read-only")

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
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot drop column default: catalog is read-only")

    def table_column_type_change(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        column_definition: SerializedSchema,
        expression: SqlExpression | None,
        ignore_not_found: bool,
    ) -> None:
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot change column type: catalog is read-only")

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
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError(
            "Cannot drop NOT NULL constraint: catalog is read-only"
        )

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
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError(
            "Cannot set NOT NULL constraint: catalog is read-only"
        )

    # ========== View DDL (not supported) ==========

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
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot create view: catalog is read-only")

    def view_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        ignore_not_found: bool,
    ) -> None:
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot drop view: catalog is read-only")

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
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot rename view: catalog is read-only")

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
        """Not supported - raises CatalogReadOnlyError."""
        from vgi.exceptions import CatalogReadOnlyError

        raise CatalogReadOnlyError("Cannot set view comment: catalog is read-only")
