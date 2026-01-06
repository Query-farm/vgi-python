from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, NewType
from enum import Enum

import pyarrow as pa


# Type aliases for improved code clarity and type checking.
# At runtime, these are equivalent to their underlying types.
AttachId = NewType("AttachId", bytes)
TransactionId = NewType("TransactionId", bytes)
SerializedSchema = NewType("SerializedSchema", bytes)
SqlExpression = NewType("SqlExpression", str)


@dataclass(frozen=True)
class CatalogAttachResult:
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


@dataclass(frozen=True)
class CatalogObject:
    """All objects have the following common properties."""

    # This is a generic comment about the object
    comment: str | None
    # These are tags associated with the object
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

    # Is this the default schema of the catalog
    is_default: bool


@dataclass(frozen=True)
class TableInfo(CatalogSchemaObject):
    """Information about a table in a schema."""

    # The columns of the table as a PyArrow schema
    # that is serialized as bytes.
    columns: SerializedSchema

    not_null_constraints: list[int]
    unique_constraints: list[list[int]]
    check_constraints: list[str]


@dataclass(frozen=True)
class ViewInfo(CatalogSchemaObject):
    """Information about a view in a schema."""

    # The definition of the view which is a SQL query string.
    definition: str


class FunctionType(Enum):
    """The type of function in a schema."""

    SCALAR = "scalar"
    TABLE = "table"


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


@dataclass(frozen=True)
class ScanFunctionResult:
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
        - Comments and tags are not able to be updated or created on schemas (SchemaInfo).
        - Constraints are not able to be added or dropped on tables (with the exception of not null constraints).

    A VGI worker will offer a single implementation of this interface to clients
    to manage their catalogs.
    """

    @property
    def interface_feature_flags(self) -> set[str]:
        """Get the set of feature flags supported by this CatalogInterface implementation.

        Feature flags are used to indicate optional capabilities of the implementation.
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
        """Commit the transaction with the given transaction_id for the given attachment.

        If the transaction cannot be committed, an exception should be raised.
        """
        raise NotImplementedError("Catalog transactions not implemented.")

    def catalog_transaction_rollback(
        self, *, attach_id: AttachId, transaction_id: TransactionId
    ) -> None:
        """Rollback the transaction with the given transaction_id for the given attachment.

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
        pass

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

        The default implementation returns a schema called "main" with no comment or tags.
        """
        return iter(
            [
                SchemaInfo(
                    attach_id=attach_id,
                    name="main",
                    comment=None,
                    tags={},
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

    def schema_contents(
        self, *, attach_id: AttachId, transaction_id: TransactionId | None, name: str
    ) -> Iterable[TableInfo | ViewInfo | FunctionInfo]:
        """Get the contents of the schema with the given name.

        Schemas can contain tables, views, and various types of functions.

        The default implementation returns everything registered to the Worker.
        """
        # FIXME: write this implementation for the worker.

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

    # Add a column to a table, the name is serialized, but the column_type is the Arrow data type
    # of the column to add.
    def table_column_add(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        # column should be Arrow schema with a single field representing the column to add.
        # the name and type are taken from that field. it is serialized as bytes using:
        # schema.serialize().to_pybytes()
        # the schema can only have one field, if it has more than one field an error should be raised.
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
        """Set the default expression for the column in the table with the given name."""
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
        """Drop the default expression for the column in the table with the given name."""
        raise NotImplementedError("Table column default drop not implemented.")

    def table_column_type_change(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        # This is an Arrow schema with a single field representing the column to change
        # the type of.  The new type is taken from the single field in this schema.
        # it is serialized as bytes using:
        # schema.serialize().to_pybytes()
        # The schema can only have one field
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
        """Drop the NOT NULL constraint from the column in the table with the given name."""
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
        """Set the NOT NULL constraint on the column in the table with the given name."""
        raise NotImplementedError("Table NOT NULL set not implemented.")

    def table_scan_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        # These fields are used for iceberg style time travel.
        # provided later on.
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        """Get the ScanFunctionResult for scanning the table with the given name.

        Get the ScanFunctionResult of the table function to call to read data from a particular table.
        This is necessary since this method may yield the bind data identifier for the scan function.

        The at_unit and at_value will be passed by DuckDB, basically there is a bind function called
        in the duckdb process and the additional parameters will be sent to the CatalogInterface,
        the projection pushdown and (later on predicate pushdown) will be done in the init phase of the call
        to the actual VGI function to scan the table.
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
    supports_transactions = False
    catalog_version_frozen = True
