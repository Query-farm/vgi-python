"""VGI Catalog Interface for exposing catalogs, schemas, tables, and views.

This module provides the abstract base class and data types for implementing
catalog interfaces in VGI workers, enabling DuckDB ATTACH support.
"""

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ClassVar, NewType, Self

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
]

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

    ARROW_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("attach_id", pa.binary(), nullable=False),
            pa.field("supports_transactions", pa.bool_(), nullable=False),
            pa.field("supports_time_travel", pa.bool_(), nullable=False),
            pa.field("catalog_version_frozen", pa.bool_(), nullable=False),
            pa.field("catalog_version", pa.int64(), nullable=False),
            pa.field("attach_id_required", pa.bool_(), nullable=False),
        ]  # type: ignore[arg-type]
    )

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes."""
        batch = pa.RecordBatch.from_pylist(
            [
                {
                    "attach_id": self.attach_id,
                    "supports_transactions": self.supports_transactions,
                    "supports_time_travel": self.supports_time_travel,
                    "catalog_version_frozen": self.catalog_version_frozen,
                    "catalog_version": self.catalog_version,
                    "attach_id_required": self.attach_id_required,
                }
            ],
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
            ],
        )
        return cls(
            attach_id=AttachId(row["attach_id"]),
            supports_transactions=row["supports_transactions"],
            supports_time_travel=row["supports_time_travel"],
            catalog_version_frozen=row["catalog_version_frozen"],
            catalog_version=row["catalog_version"],
            attach_id_required=row["attach_id_required"],
        )


@dataclass(frozen=True)
class CatalogObject:
    """All objects have the following common properties."""

    # This is a generic comment about the object
    comment: str | None
    # These are tags associated with the object (simple string labels)
    tags: set[str]


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

    # Is this the default schema of the catalog
    is_default: bool

    ARROW_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("attach_id", pa.binary(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
            pa.field("is_default", pa.bool_(), nullable=False),
            pa.field("comment", pa.string(), nullable=True),
            pa.field("tags", pa.list_(pa.string()), nullable=False),
        ]  # type: ignore[arg-type]
    )

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes."""
        batch = pa.RecordBatch.from_pylist(
            [
                {
                    "attach_id": self.attach_id,
                    "name": self.name,
                    "is_default": self.is_default,
                    "comment": self.comment,
                    "tags": list(self.tags),
                }
            ],
            schema=self.ARROW_SCHEMA,
        )
        return vgi.ipc_utils.serialize_record_batch(batch)

    @classmethod
    def deserialize(cls, batch: pa.RecordBatch) -> Self:
        """Deserialize from Arrow RecordBatch."""
        row = vgi.ipc_utils.validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=["attach_id", "name", "is_default", "tags"],
        )
        return cls(
            attach_id=AttachId(row["attach_id"]),
            name=row["name"],
            is_default=row["is_default"],
            comment=row.get("comment"),
            tags=set(row["tags"]) if row["tags"] else set(),
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
            pa.field("tags", pa.list_(pa.string()), nullable=False),
        ]  # type: ignore[arg-type]
    )

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes."""
        batch = pa.RecordBatch.from_pylist(
            [
                {
                    "name": self.name,
                    "schema_name": self.schema_name,
                    "columns": self.columns,
                    "not_null_constraints": self.not_null_constraints,
                    "unique_constraints": self.unique_constraints,
                    "check_constraints": self.check_constraints,
                    "comment": self.comment,
                    "tags": list(self.tags),
                }
            ],
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
            tags=set(row["tags"]) if row["tags"] else set(),
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
            pa.field("tags", pa.list_(pa.string()), nullable=False),
        ]
    )

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes."""
        batch = pa.RecordBatch.from_pylist(
            [
                {
                    "name": self.name,
                    "schema_name": self.schema_name,
                    "definition": self.definition,
                    "comment": self.comment,
                    "tags": list(self.tags),
                }
            ],
            schema=self.ARROW_SCHEMA,
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
            tags=set(row["tags"]) if row["tags"] else set(),
        )


class FunctionType(Enum):
    """The type of function in a schema."""

    SCALAR = "scalar"
    TABLE = "table"
    AGGREGATE = "aggregate"


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
    examples: list[str] = field(default_factory=list)
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
            pa.field("schema_name", pa.string(), nullable=False),
            pa.field("function_type", pa.string(), nullable=False),
            pa.field("arguments", pa.binary(), nullable=False),
            pa.field("output_schema", pa.binary(), nullable=False),
            pa.field("comment", pa.string(), nullable=True),
            pa.field("tags", pa.list_(pa.string()), nullable=False),
            # Scalar function behavior fields (nullable for non-scalar functions)
            pa.field("stability", pa.string(), nullable=True),
            pa.field("null_handling", pa.string(), nullable=True),
            # Documentation fields
            pa.field("examples", pa.list_(pa.string()), nullable=False),
            pa.field("categories", pa.list_(pa.string()), nullable=False),
            # Table function capabilities (nullable for scalar functions)
            pa.field("projection_pushdown", pa.bool_(), nullable=True),
            pa.field("filter_pushdown", pa.bool_(), nullable=True),
            pa.field("order_preservation", pa.string(), nullable=True),
            pa.field("max_workers", pa.int32(), nullable=True),
            # Aggregate function fields
            pa.field("order_dependent", pa.string(), nullable=False),
            pa.field("distinct_dependent", pa.string(), nullable=False),
            # Settings
            pa.field("required_settings", pa.list_(pa.string()), nullable=False),
        ]  # type: ignore[arg-type]
    )

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes."""
        batch = pa.RecordBatch.from_pylist(
            [
                {
                    "name": self.name,
                    "schema_name": self.schema_name,
                    "function_type": self.function_type.value,
                    "arguments": self.arguments,
                    "output_schema": self.output_schema,
                    "comment": self.comment,
                    "tags": list(self.tags),
                    # Scalar function behavior fields (None for non-scalar)
                    "stability": self.stability.name if self.stability else None,
                    "null_handling": (
                        self.null_handling.name if self.null_handling else None
                    ),
                    # Documentation fields
                    "examples": self.examples,
                    "categories": self.categories,
                    # Table function capabilities (None for scalar)
                    "projection_pushdown": self.projection_pushdown,
                    "filter_pushdown": self.filter_pushdown,
                    "order_preservation": (
                        self.order_preservation.name
                        if self.order_preservation
                        else None
                    ),
                    "max_workers": self.max_workers,
                    # Aggregate function fields
                    "order_dependent": self.order_dependent.name,
                    "distinct_dependent": self.distinct_dependent.name,
                    # Settings
                    "required_settings": self.required_settings,
                }
            ],
            schema=self.ARROW_SCHEMA,
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
            comment=row.get("comment"),
            tags=set(row["tags"]) if row["tags"] else set(),
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
            examples=list(row.get("examples") or []),
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
    """Result from getting a table scan function."""

    # The name of the VGI table function to call to scan data from the table,
    # when duckdb attempts to scan a table it will change that call into a
    # call to VGI to call this named table function.
    function_name: str
    max_processes: int
    # In the DuckDB extension, it will proceed to the BIND phase of the
    # VGI table function, the invocation_id returned from that phase will be
    # persisted here, so that when DuckDB actually wants to retrieve
    # data from the table, it just starts at the init phase and not the bind
    # phase again.
    invocation_id: bytes | None

    ARROW_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("function_name", pa.string(), nullable=False),
            pa.field("max_processes", pa.int32(), nullable=False),
            pa.field("invocation_id", pa.binary(), nullable=True),
        ]  # type: ignore[arg-type]
    )

    def serialize(self) -> bytes:
        """Serialize to Arrow IPC bytes."""
        batch = pa.RecordBatch.from_pylist(
            [
                {
                    "function_name": self.function_name,
                    "max_processes": self.max_processes,
                    "invocation_id": self.invocation_id,
                }
            ],
            schema=self.ARROW_SCHEMA,
        )
        return vgi.ipc_utils.serialize_record_batch(batch)

    @classmethod
    def deserialize(cls, batch: pa.RecordBatch) -> Self:
        """Deserialize from Arrow RecordBatch."""
        row = vgi.ipc_utils.validate_single_row_batch(
            batch,
            cls.__name__,
            required_fields=["function_name", "max_processes"],
        )
        return cls(
            function_name=row["function_name"],
            max_processes=row["max_processes"],
            invocation_id=row.get("invocation_id"),
        )


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
    def catalogs(self) -> Iterable[str]:
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
    ) -> Iterable[SchemaInfo]:
        """Get a list of schemas for the given attach_id and transaction_id.

        The default returns a schema called "main" with no comment or tags.
        """
        return iter(
            [
                SchemaInfo(
                    attach_id=attach_id,
                    name="main",
                    comment=None,
                    tags=set(),
                    is_default=True,
                )
            ]
        )

    def schema_create(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        comment: str | None,
        tags: set[str],
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

    def schema_contents(
        self, *, attach_id: AttachId, transaction_id: TransactionId | None, name: str
    ) -> Iterable[TableInfo | ViewInfo | FunctionInfo]:
        """Get the contents of the schema with the given name.

        Schemas can contain tables, views, and various types of functions.
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

    # Fixed attach_id for read-only catalogs (no need for unique IDs)
    _FIXED_ATTACH_ID: AttachId = AttachId(b"readonly-catalog-")

    def catalogs(self) -> Iterable[str]:
        """Return the list of available catalogs."""
        return [self.catalog_name]

    def catalog_attach(
        self, *, name: str, options: dict[str, Any]
    ) -> CatalogAttachResult:
        """Attach to the catalog."""
        if name != self.catalog_name:
            raise ValueError(
                f"Unknown catalog: {name!r}. Available: {self.catalog_name}"
            )

        return CatalogAttachResult(
            attach_id=self._FIXED_ATTACH_ID,
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=True,
            catalog_version=1,
            attach_id_required=False,
        )

    def schema_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
    ) -> SchemaInfo | None:
        """Get information about a schema."""
        if name != "main":
            return None
        return SchemaInfo(
            attach_id=attach_id,
            name="main",
            is_default=True,
            comment=None,
            tags=set(),
        )

    def table_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> TableInfo | None:
        """Get information about a table (none in function-only catalogs)."""
        return None

    def view_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ViewInfo | None:
        """Get information about a view (none in function-only catalogs)."""
        return None

    def schema_contents(
        self, *, attach_id: AttachId, transaction_id: TransactionId | None, name: str
    ) -> Iterable[TableInfo | ViewInfo | FunctionInfo]:
        """List all functions in the schema."""
        if name != "main" or not self.functions:
            return

        for func_cls in self.functions:
            yield self._function_to_info(func_cls, name)

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
            comment=meta.description or None,
            tags=meta.tags,
            # Scalar function behavior fields (None for non-scalar)
            stability=meta.stability if is_scalar else None,
            null_handling=meta.null_handling if is_scalar else None,
            # Documentation fields
            examples=[ex.sql for ex in meta.examples],
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
        tags: set[str],
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
