"""CI Catalog implementation with full DDL and transaction support.

This module provides a CatalogInterface implementation that:
- Uses AttachmentStorage for per-attachment isolated state
- Supports transactions with rollback
- Stores actual table data (not just metadata)
- Tracks version per attachment for cache invalidation
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import Any, Literal, overload

from vgi.catalog import (
    AttachId,
    CatalogAttachResult,
    CatalogInterface,
    FunctionInfo,
    OnConflict,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    SqlExpression,
    TableInfo,
    TransactionId,
    ViewInfo,
)
from vgi.ci.storage import AttachmentNotFoundError, AttachmentStorage, TransactionError


class CICatalog(CatalogInterface):
    """Full-featured catalog implementation for CI testing.

    This catalog provides:
    - Per-attachment isolated state (each attachment has its own namespace)
    - Transaction support with begin/commit/rollback
    - Actual table data storage (not just metadata)
    - Version tracking for cache invalidation
    - Full DDL operations for schemas, tables, and views

    Available catalogs: "ci", "test"
    """

    def __init__(self) -> None:
        """Initialize the CI catalog."""
        self._storage = AttachmentStorage()
        self._available_catalogs = {"ci", "test"}

    @property
    def interface_feature_flags(self) -> set[str]:
        """Return feature flags supported by this catalog."""
        return {"transactions", "table_data"}

    # Required abstract methods

    def catalogs(self) -> list[str]:
        """Get a list of available catalog names."""
        return list(self._available_catalogs)

    def catalog_attach(
        self, *, name: str, options: dict[str, Any]
    ) -> CatalogAttachResult:
        """Attach to a catalog with the given name.

        Creates a new attachment with isolated state.

        Args:
            name: Catalog name to attach to.
            options: Optional configuration (unused).

        Returns:
            Attachment result with attach_id and capabilities.

        Raises:
            ValueError: If catalog name is not available.

        """
        if name not in self._available_catalogs:
            msg = f"Catalog {name!r} not found. Available: {self._available_catalogs}"
            raise ValueError(msg)

        attach_id = AttachId(uuid.uuid4().bytes)
        state = self._storage.create_attachment(attach_id, name)

        return CatalogAttachResult(
            attach_id=attach_id,
            supports_transactions=True,
            supports_time_travel=False,
            catalog_version_frozen=False,
            catalog_version=state.version,
            attach_id_required=True,  # Stateful catalog
        )

    def catalog_detach(self, *, attach_id: AttachId) -> None:
        """Detach from the catalog and clean up state."""
        self._storage.delete_attachment(attach_id)

    def schema_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
    ) -> SchemaInfo | None:
        """Get information about a schema."""
        try:
            schema = self._storage.get_schema(attach_id, name)
            if schema is None:
                return None
            # Return info with current attach_id
            return SchemaInfo(
                attach_id=attach_id,
                name=schema.info.name,
                comment=schema.info.comment,
                tags=schema.info.tags,
            )
        except AttachmentNotFoundError:
            return None

    def table_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> TableInfo | None:
        """Get information about a table."""
        try:
            table = self._storage.get_table(attach_id, schema_name, name)
            return table.info if table else None
        except AttachmentNotFoundError:
            return None

    def view_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ViewInfo | None:
        """Get information about a view."""
        try:
            view = self._storage.get_view(attach_id, schema_name, name)
            return view.info if view else None
        except AttachmentNotFoundError:
            return None

    # Catalog DDL

    def catalog_create(
        self, *, name: str, on_conflict: OnConflict, options: dict[str, Any]
    ) -> None:
        """Create a new catalog (adds to available catalogs)."""
        if name in self._available_catalogs:
            if on_conflict == OnConflict.ERROR:
                msg = f"Catalog {name!r} already exists"
                raise ValueError(msg)
            if on_conflict == OnConflict.IGNORE:
                return
            # REPLACE: fall through
        self._available_catalogs.add(name)

    def catalog_drop(self, *, name: str) -> None:
        """Drop a catalog (removes from available catalogs)."""
        if name not in self._available_catalogs:
            msg = f"Catalog {name!r} not found"
            raise ValueError(msg)
        self._available_catalogs.discard(name)

    def catalog_version(
        self, *, attach_id: AttachId, transaction_id: TransactionId | None
    ) -> int:
        """Get the current catalog version."""
        state = self._storage.get_attachment(attach_id)
        return state.version

    # Transaction methods

    def catalog_transaction_begin(self, *, attach_id: AttachId) -> TransactionId | None:
        """Begin a new transaction."""
        try:
            return self._storage.begin_transaction(attach_id)
        except TransactionError as e:
            msg = str(e)
            raise ValueError(msg) from e

    def catalog_transaction_commit(
        self, *, attach_id: AttachId, transaction_id: TransactionId
    ) -> None:
        """Commit a transaction."""
        try:
            self._storage.commit_transaction(attach_id, transaction_id)
        except TransactionError as e:
            msg = str(e)
            raise ValueError(msg) from e

    def catalog_transaction_rollback(
        self, *, attach_id: AttachId, transaction_id: TransactionId
    ) -> None:
        """Rollback a transaction."""
        try:
            self._storage.rollback_transaction(attach_id, transaction_id)
        except TransactionError as e:
            msg = str(e)
            raise ValueError(msg) from e

    # Schema DDL

    def schemas(
        self, *, attach_id: AttachId, transaction_id: TransactionId | None
    ) -> list[SchemaInfo]:
        """Get a list of schemas in the catalog."""
        return self._storage.list_schemas(attach_id)

    def schema_create(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        comment: str | None,
        tags: dict[str, str],
    ) -> None:
        """Create a new schema."""
        self._storage.create_schema(attach_id, name, comment=comment, tags=tags)

    def schema_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        ignore_not_found: bool,
        cascade: bool,
    ) -> None:
        """Drop a schema."""
        self._storage.drop_schema(
            attach_id, name, ignore_not_found=ignore_not_found, cascade=cascade
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
        """Get the contents of a schema.

        Args:
            attach_id: The attachment identifier.
            transaction_id: The transaction identifier, if any.
            name: The name of the schema.
            type: The type of objects to return. Must be a SchemaObjectType enum.

        Returns:
            An iterable of TableInfo, ViewInfo, or FunctionInfo objects
            depending on the type parameter.

        """
        schema = self._storage.get_schema(attach_id, name)
        if schema is None:
            msg = f"Schema {name!r} not found"
            raise ValueError(msg)

        # Normalize type parameter (may be string from wire protocol)
        if isinstance(type, SchemaObjectType):
            type_enum = type
        else:
            type_enum = SchemaObjectType(type)

        result: list[TableInfo | ViewInfo | FunctionInfo] = []

        # Return tables for TABLE type
        if type_enum == SchemaObjectType.TABLE:
            for table in schema.tables.values():
                result.append(table.info)

        # Return views for VIEW type
        elif type_enum == SchemaObjectType.VIEW:
            for view in schema.views.values():
                result.append(view.info)

        # Note: This CI catalog doesn't store functions,
        # so SCALAR_FUNCTION and TABLE_FUNCTION types return nothing

        return result

    # Table DDL

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
        """Create a new table."""
        existing = self._storage.get_table(attach_id, schema_name, name)
        if existing is not None:
            if on_conflict == OnConflict.ERROR:
                msg = f"Table {name!r} already exists in schema {schema_name!r}"
                raise ValueError(msg)
            if on_conflict == OnConflict.IGNORE:
                return
            # REPLACE: drop and recreate
            self._storage.drop_table(attach_id, schema_name, name)

        self._storage.create_table(
            attach_id,
            schema_name,
            name,
            columns,
            not_null_constraints=not_null_constraints,
            unique_constraints=unique_constraints,
            check_constraints=check_constraints,
        )

    def table_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        ignore_not_found: bool,
    ) -> None:
        """Drop a table."""
        self._storage.drop_table(
            attach_id, schema_name, name, ignore_not_found=ignore_not_found
        )

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
        """Rename a table."""
        self._storage.rename_table(
            attach_id, schema_name, name, new_name, ignore_not_found=ignore_not_found
        )

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
        """Set the comment for a table."""
        self._storage.set_table_comment(
            attach_id, schema_name, name, comment, ignore_not_found=ignore_not_found
        )

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
        """Add a column to a table."""
        # For now, just raise NotImplementedError as column operations
        # require more complex schema manipulation
        raise NotImplementedError("Column add not yet implemented in CI catalog")

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
        """Drop a column from a table."""
        raise NotImplementedError("Column drop not yet implemented in CI catalog")

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
        """Rename a column."""
        raise NotImplementedError("Column rename not yet implemented in CI catalog")

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
        """Change a column's type."""
        msg = "Column type change not yet implemented in CI catalog"
        raise NotImplementedError(msg)

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
        """Add NOT NULL constraint to a column."""
        raise NotImplementedError("NOT NULL set not yet implemented in CI catalog")

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
        """Remove NOT NULL constraint from a column."""
        raise NotImplementedError("NOT NULL drop not yet implemented in CI catalog")

    # View DDL

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
        """Create a new view."""
        existing = self._storage.get_view(attach_id, schema_name, name)
        if existing is not None:
            if on_conflict == OnConflict.ERROR:
                msg = f"View {name!r} already exists in schema {schema_name!r}"
                raise ValueError(msg)
            if on_conflict == OnConflict.IGNORE:
                return
            # REPLACE: drop and recreate
            self._storage.drop_view(attach_id, schema_name, name)

        self._storage.create_view(attach_id, schema_name, name, definition)

    def view_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        ignore_not_found: bool,
    ) -> None:
        """Drop a view."""
        self._storage.drop_view(
            attach_id, schema_name, name, ignore_not_found=ignore_not_found
        )

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
        """Rename a view."""
        view = self._storage.get_view(attach_id, schema_name, name)
        if view is None:
            if ignore_not_found:
                return
            msg = f"View {name!r} not found in schema {schema_name!r}"
            raise ValueError(msg)

        # Check new name doesn't exist
        if self._storage.get_view(attach_id, schema_name, new_name) is not None:
            msg = f"View {new_name!r} already exists in schema {schema_name!r}"
            raise ValueError(msg)

        # Create new view with new name, then drop old
        self._storage.create_view(
            attach_id, schema_name, new_name, view.info.definition
        )
        self._storage.drop_view(attach_id, schema_name, name)

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
        """Set the comment for a view."""
        view = self._storage.get_view(attach_id, schema_name, name)
        if view is None:
            if ignore_not_found:
                return
            msg = f"View {name!r} not found in schema {schema_name!r}"
            raise ValueError(msg)

        # Update comment by recreating view info
        old_info = view.info
        view.info = ViewInfo(
            name=old_info.name,
            schema_name=old_info.schema_name,
            definition=old_info.definition,
            comment=comment,
            tags=old_info.tags,
        )
        self._storage.increment_version(attach_id)
