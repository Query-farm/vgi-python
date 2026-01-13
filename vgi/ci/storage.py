"""Per-attachment isolated state storage for CI catalog.

This module provides in-memory state storage with:
- Per-attachment isolation (each attachment has its own namespace)
- Actual table data storage (not just metadata)
- Transaction support with snapshot-based rollback
- Version tracking per attachment
"""

from __future__ import annotations

import copy
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa

from vgi.catalog import (
    AttachId,
    SchemaInfo,
    SerializedSchema,
    TableInfo,
    TransactionId,
    ViewInfo,
)

if TYPE_CHECKING:
    from collections.abc import Iterable


@dataclass
class TableData:
    """Storage for table metadata and data."""

    info: TableInfo
    data: pa.Table  # Actual table data


@dataclass
class ViewData:
    """Storage for view metadata."""

    info: ViewInfo


@dataclass
class SchemaData:
    """Storage for schema metadata and contents."""

    info: SchemaInfo
    tables: dict[str, TableData] = field(default_factory=dict)
    views: dict[str, ViewData] = field(default_factory=dict)


@dataclass
class AttachmentState:
    """Complete state for a single attachment.

    Each attachment has isolated state including:
    - catalog_name: The name of the catalog this attachment is connected to
    - schemas: All schemas with their tables and views
    - version: Incremented on any DDL operation (for cache invalidation)
    - pending_tx: Current transaction ID if a transaction is active
    - tx_snapshot: Deep copy of schemas for rollback support
    """

    catalog_name: str
    schemas: dict[str, SchemaData] = field(default_factory=dict)
    version: int = 1
    pending_tx: TransactionId | None = None
    tx_snapshot: dict[str, SchemaData] | None = None


class AttachmentNotFoundError(Exception):
    """Raised when an attachment is not found."""


class TransactionError(Exception):
    """Raised for transaction-related errors."""


class AttachmentStorage:
    """Per-attachment isolated state storage.

    This class manages state for multiple attachments, where each attachment
    has its own isolated namespace for schemas, tables, and views.

    Key features:
    - Create/delete attachments with isolated state
    - Schema CRUD operations
    - Table CRUD with actual data storage
    - View CRUD operations
    - Transaction begin/commit/rollback with snapshot-based rollback
    """

    def __init__(self) -> None:
        """Initialize the attachment storage."""
        self._attachments: dict[AttachId, AttachmentState] = {}

    # Attachment lifecycle

    def create_attachment(
        self, attach_id: AttachId, catalog_name: str
    ) -> AttachmentState:
        """Create a new attachment with isolated state.

        Args:
            attach_id: Unique identifier for this attachment.
            catalog_name: Name of the catalog to attach to.

        Returns:
            The newly created attachment state.

        """
        state = AttachmentState(catalog_name=catalog_name)
        # Create default "main" schema
        state.schemas["main"] = SchemaData(
            info=SchemaInfo(
                attach_id=attach_id,
                name="main",
                is_default=True,
                comment=None,
                tags={},
            )
        )
        self._attachments[attach_id] = state
        return state

    def get_attachment(self, attach_id: AttachId) -> AttachmentState:
        """Get the state for an attachment.

        Args:
            attach_id: The attachment identifier.

        Returns:
            The attachment state.

        Raises:
            AttachmentNotFoundError: If the attachment does not exist.

        """
        state = self._attachments.get(attach_id)
        if state is None:
            msg = f"Attachment {attach_id!r} not found"
            raise AttachmentNotFoundError(msg)
        return state

    def delete_attachment(self, attach_id: AttachId) -> None:
        """Delete an attachment and all its state.

        Args:
            attach_id: The attachment identifier.

        """
        self._attachments.pop(attach_id, None)

    def list_attachments(self) -> list[AttachId]:
        """List all attachment IDs."""
        return list(self._attachments.keys())

    # Version management

    def increment_version(self, attach_id: AttachId) -> None:
        """Increment the version number for an attachment."""
        state = self.get_attachment(attach_id)
        state.version += 1

    # Schema operations

    def create_schema(
        self,
        attach_id: AttachId,
        name: str,
        *,
        comment: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Create a new schema in the attachment.

        Args:
            attach_id: The attachment identifier.
            name: Schema name.
            comment: Optional description.
            tags: Optional metadata tags.

        Raises:
            ValueError: If schema already exists.

        """
        state = self.get_attachment(attach_id)
        if name in state.schemas:
            msg = f"Schema {name!r} already exists"
            raise ValueError(msg)
        state.schemas[name] = SchemaData(
            info=SchemaInfo(
                attach_id=attach_id,
                name=name,
                is_default=False,
                comment=comment,
                tags=tags or {},
            )
        )
        self.increment_version(attach_id)

    def drop_schema(
        self,
        attach_id: AttachId,
        name: str,
        *,
        ignore_not_found: bool = False,
        cascade: bool = False,
    ) -> None:
        """Drop a schema from the attachment.

        Args:
            attach_id: The attachment identifier.
            name: Schema name.
            ignore_not_found: If True, don't error if schema doesn't exist.
            cascade: If True, drop all contained tables and views.

        Raises:
            ValueError: If schema not found (and ignore_not_found is False).
            ValueError: If schema is not empty and cascade is False.

        """
        state = self.get_attachment(attach_id)
        if name not in state.schemas:
            if ignore_not_found:
                return
            msg = f"Schema {name!r} not found"
            raise ValueError(msg)
        schema = state.schemas[name]
        if not cascade and (schema.tables or schema.views):
            msg = f"Schema {name!r} is not empty, use cascade=True to drop"
            raise ValueError(msg)
        del state.schemas[name]
        self.increment_version(attach_id)

    def get_schema(self, attach_id: AttachId, name: str) -> SchemaData | None:
        """Get a schema by name.

        Args:
            attach_id: The attachment identifier.
            name: Schema name.

        Returns:
            The schema data, or None if not found.

        """
        state = self.get_attachment(attach_id)
        return state.schemas.get(name)

    def list_schemas(self, attach_id: AttachId) -> Iterable[SchemaInfo]:
        """List all schemas in an attachment.

        Args:
            attach_id: The attachment identifier.

        Returns:
            Iterator of schema info objects.

        """
        state = self.get_attachment(attach_id)
        for schema in state.schemas.values():
            # Return a copy with the current attach_id
            yield SchemaInfo(
                attach_id=attach_id,
                name=schema.info.name,
                is_default=schema.info.is_default,
                comment=schema.info.comment,
                tags=schema.info.tags,
            )

    # Table operations

    def create_table(
        self,
        attach_id: AttachId,
        schema_name: str,
        name: str,
        columns: SerializedSchema,
        *,
        not_null_constraints: list[int] | None = None,
        unique_constraints: list[list[int]] | None = None,
        check_constraints: list[str] | None = None,
    ) -> None:
        """Create a new table.

        Args:
            attach_id: The attachment identifier.
            schema_name: Schema containing the table.
            name: Table name.
            columns: Serialized Arrow schema for columns.
            not_null_constraints: Column indices with NOT NULL.
            unique_constraints: Lists of column indices for unique constraints.
            check_constraints: SQL check expressions.

        Raises:
            ValueError: If schema not found or table already exists.

        """
        state = self.get_attachment(attach_id)
        schema = state.schemas.get(schema_name)
        if schema is None:
            msg = f"Schema {schema_name!r} not found"
            raise ValueError(msg)
        if name in schema.tables:
            msg = f"Table {name!r} already exists in schema {schema_name!r}"
            raise ValueError(msg)

        # Deserialize schema to create empty table
        arrow_schema = pa.ipc.read_schema(pa.py_buffer(columns))
        empty_table = pa.Table.from_batches([], schema=arrow_schema)

        schema.tables[name] = TableData(
            info=TableInfo(
                name=name,
                schema_name=schema_name,
                columns=columns,
                not_null_constraints=not_null_constraints or [],
                unique_constraints=unique_constraints or [],
                check_constraints=check_constraints or [],
                comment=None,
                tags={},
            ),
            data=empty_table,
        )
        self.increment_version(attach_id)

    def drop_table(
        self,
        attach_id: AttachId,
        schema_name: str,
        name: str,
        *,
        ignore_not_found: bool = False,
    ) -> None:
        """Drop a table.

        Args:
            attach_id: The attachment identifier.
            schema_name: Schema containing the table.
            name: Table name.
            ignore_not_found: If True, don't error if table doesn't exist.

        Raises:
            ValueError: If schema or table not found.

        """
        state = self.get_attachment(attach_id)
        schema = state.schemas.get(schema_name)
        if schema is None:
            msg = f"Schema {schema_name!r} not found"
            raise ValueError(msg)
        if name not in schema.tables:
            if ignore_not_found:
                return
            msg = f"Table {name!r} not found in schema {schema_name!r}"
            raise ValueError(msg)
        del schema.tables[name]
        self.increment_version(attach_id)

    def get_table(
        self, attach_id: AttachId, schema_name: str, name: str
    ) -> TableData | None:
        """Get a table by name.

        Args:
            attach_id: The attachment identifier.
            schema_name: Schema containing the table.
            name: Table name.

        Returns:
            The table data, or None if not found.

        """
        state = self.get_attachment(attach_id)
        schema = state.schemas.get(schema_name)
        if schema is None:
            return None
        return schema.tables.get(name)

    def insert_data(
        self, attach_id: AttachId, schema_name: str, name: str, data: pa.Table
    ) -> None:
        """Insert data into a table.

        Args:
            attach_id: The attachment identifier.
            schema_name: Schema containing the table.
            name: Table name.
            data: Table data to insert.

        Raises:
            ValueError: If table not found.

        """
        table_data = self.get_table(attach_id, schema_name, name)
        if table_data is None:
            msg = f"Table {name!r} not found in schema {schema_name!r}"
            raise ValueError(msg)
        # Concatenate new data with existing data
        if table_data.data.num_rows == 0:
            table_data.data = data
        else:
            table_data.data = pa.concat_tables([table_data.data, data])

    def scan_table(self, attach_id: AttachId, schema_name: str, name: str) -> pa.Table:
        """Scan all data from a table.

        Args:
            attach_id: The attachment identifier.
            schema_name: Schema containing the table.
            name: Table name.

        Returns:
            The table data.

        Raises:
            ValueError: If table not found.

        """
        table_data = self.get_table(attach_id, schema_name, name)
        if table_data is None:
            msg = f"Table {name!r} not found in schema {schema_name!r}"
            raise ValueError(msg)
        return table_data.data

    def rename_table(
        self,
        attach_id: AttachId,
        schema_name: str,
        name: str,
        new_name: str,
        *,
        ignore_not_found: bool = False,
    ) -> None:
        """Rename a table.

        Args:
            attach_id: The attachment identifier.
            schema_name: Schema containing the table.
            name: Current table name.
            new_name: New table name.
            ignore_not_found: If True, don't error if table doesn't exist.

        Raises:
            ValueError: If table not found or new name already exists.

        """
        state = self.get_attachment(attach_id)
        schema = state.schemas.get(schema_name)
        if schema is None:
            msg = f"Schema {schema_name!r} not found"
            raise ValueError(msg)
        if name not in schema.tables:
            if ignore_not_found:
                return
            msg = f"Table {name!r} not found in schema {schema_name!r}"
            raise ValueError(msg)
        if new_name in schema.tables:
            msg = f"Table {new_name!r} already exists in schema {schema_name!r}"
            raise ValueError(msg)

        table_data = schema.tables.pop(name)
        old_info = table_data.info
        schema.tables[new_name] = TableData(
            info=TableInfo(
                name=new_name,
                schema_name=schema_name,
                columns=old_info.columns,
                not_null_constraints=old_info.not_null_constraints,
                unique_constraints=old_info.unique_constraints,
                check_constraints=old_info.check_constraints,
                comment=old_info.comment,
                tags=old_info.tags,
            ),
            data=table_data.data,
        )
        self.increment_version(attach_id)

    def set_table_comment(
        self,
        attach_id: AttachId,
        schema_name: str,
        name: str,
        comment: str | None,
        *,
        ignore_not_found: bool = False,
    ) -> None:
        """Set the comment for a table.

        Args:
            attach_id: The attachment identifier.
            schema_name: Schema containing the table.
            name: Table name.
            comment: New comment (or None to clear).
            ignore_not_found: If True, don't error if table doesn't exist.

        """
        table_data = self.get_table(attach_id, schema_name, name)
        if table_data is None:
            if ignore_not_found:
                return
            msg = f"Table {name!r} not found in schema {schema_name!r}"
            raise ValueError(msg)

        old_info = table_data.info
        table_data.info = TableInfo(
            name=old_info.name,
            schema_name=old_info.schema_name,
            columns=old_info.columns,
            not_null_constraints=old_info.not_null_constraints,
            unique_constraints=old_info.unique_constraints,
            check_constraints=old_info.check_constraints,
            comment=comment,
            tags=old_info.tags,
        )
        self.increment_version(attach_id)

    # View operations

    def create_view(
        self,
        attach_id: AttachId,
        schema_name: str,
        name: str,
        definition: str,
    ) -> None:
        """Create a new view.

        Args:
            attach_id: The attachment identifier.
            schema_name: Schema containing the view.
            name: View name.
            definition: SQL definition.

        Raises:
            ValueError: If schema not found or view already exists.

        """
        state = self.get_attachment(attach_id)
        schema = state.schemas.get(schema_name)
        if schema is None:
            msg = f"Schema {schema_name!r} not found"
            raise ValueError(msg)
        if name in schema.views:
            msg = f"View {name!r} already exists in schema {schema_name!r}"
            raise ValueError(msg)

        schema.views[name] = ViewData(
            info=ViewInfo(
                name=name,
                schema_name=schema_name,
                definition=definition,
                comment=None,
                tags={},
            )
        )
        self.increment_version(attach_id)

    def drop_view(
        self,
        attach_id: AttachId,
        schema_name: str,
        name: str,
        *,
        ignore_not_found: bool = False,
    ) -> None:
        """Drop a view.

        Args:
            attach_id: The attachment identifier.
            schema_name: Schema containing the view.
            name: View name.
            ignore_not_found: If True, don't error if view doesn't exist.

        Raises:
            ValueError: If schema or view not found.

        """
        state = self.get_attachment(attach_id)
        schema = state.schemas.get(schema_name)
        if schema is None:
            msg = f"Schema {schema_name!r} not found"
            raise ValueError(msg)
        if name not in schema.views:
            if ignore_not_found:
                return
            msg = f"View {name!r} not found in schema {schema_name!r}"
            raise ValueError(msg)
        del schema.views[name]
        self.increment_version(attach_id)

    def get_view(
        self, attach_id: AttachId, schema_name: str, name: str
    ) -> ViewData | None:
        """Get a view by name.

        Args:
            attach_id: The attachment identifier.
            schema_name: Schema containing the view.
            name: View name.

        Returns:
            The view data, or None if not found.

        """
        state = self.get_attachment(attach_id)
        schema = state.schemas.get(schema_name)
        if schema is None:
            return None
        return schema.views.get(name)

    # Transaction operations

    def begin_transaction(self, attach_id: AttachId) -> TransactionId:
        """Begin a new transaction.

        Creates a snapshot of the current state for rollback.

        Args:
            attach_id: The attachment identifier.

        Returns:
            The transaction ID.

        Raises:
            TransactionError: If a transaction is already active.

        """
        state = self.get_attachment(attach_id)
        if state.pending_tx is not None:
            msg = "Transaction already active (nested transactions not supported)"
            raise TransactionError(msg)

        tx_id = TransactionId(uuid.uuid4().bytes)
        state.pending_tx = tx_id
        # Deep copy the schemas for rollback
        state.tx_snapshot = copy.deepcopy(state.schemas)
        return tx_id

    def commit_transaction(self, attach_id: AttachId, tx_id: TransactionId) -> None:
        """Commit a transaction.

        Clears the snapshot and transaction state.

        Args:
            attach_id: The attachment identifier.
            tx_id: The transaction ID to commit.

        Raises:
            TransactionError: If no transaction is active or tx_id doesn't match.

        """
        state = self.get_attachment(attach_id)
        if state.pending_tx is None:
            msg = "No transaction active"
            raise TransactionError(msg)
        if state.pending_tx != tx_id:
            msg = "Transaction ID mismatch"
            raise TransactionError(msg)

        state.pending_tx = None
        state.tx_snapshot = None

    def rollback_transaction(self, attach_id: AttachId, tx_id: TransactionId) -> None:
        """Rollback a transaction.

        Restores the state from the snapshot.

        Args:
            attach_id: The attachment identifier.
            tx_id: The transaction ID to rollback.

        Raises:
            TransactionError: If no transaction is active or tx_id doesn't match.

        """
        state = self.get_attachment(attach_id)
        if state.pending_tx is None:
            msg = "No transaction active"
            raise TransactionError(msg)
        if state.pending_tx != tx_id:
            msg = "Transaction ID mismatch"
            raise TransactionError(msg)
        if state.tx_snapshot is None:
            msg = "No snapshot available for rollback"
            raise TransactionError(msg)

        # Restore the snapshot
        state.schemas = state.tx_snapshot
        state.pending_tx = None
        state.tx_snapshot = None
