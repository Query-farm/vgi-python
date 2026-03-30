"""CatalogClientMixin for adding catalog operations to Client.

This module provides a mixin class that adds catalog operation methods
to the VGI Client. It handles the ephemeral subprocess pattern for
catalog calls while using the Client's server_path and correlation_id.

Usage:
    class CatalogEnabledClient(CatalogClientMixin, Client):
        pass

    client = CatalogEnabledClient("vgi-my-worker")

    # List available catalogs
    catalogs = client.catalogs()

    # Attach to a catalog and work with schemas
    result = client.catalog_attach(name="my_catalog")
    schemas = client.schemas(attach_id=result.attach_id)

    # Use transactions for atomic operations
    tx_id = client.catalog_transaction_begin(attach_id=result.attach_id)
    client.schema_create(
        attach_id=result.attach_id, transaction_id=tx_id, name="new_schema"
    )
    client.catalog_transaction_commit(
        attach_id=result.attach_id, transaction_id=tx_id
    )

Error Handling:
    Worker exceptions are propagated via vgi_rpc's RpcError mechanism.
    These are wrapped in CatalogClientError for a consistent client API.

"""

from __future__ import annotations

import shlex
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any, Literal, overload

import pyarrow as pa
from vgi_rpc import WorkerPool
from vgi_rpc.rpc import RpcError
from vgi_rpc.utils import deserialize_record_batch

from vgi.catalog import (
    AttachId,
    CatalogAttachResult,
    FunctionInfo,
    MacroInfo,
    MacroType,
    OnConflict,
    ScanFunctionResult,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    SqlExpression,
    TableInfo,
    TransactionId,
    ViewInfo,
)
from vgi.protocol import (
    CatalogAttachRequest,
    CatalogCreateRequest,
    MacroCreateRequest,
    TableCreateRequest,
    VgiProtocol,
)

# Module-level worker pool shared across all CatalogClientMixin instances.
# Workers are cached by command and reused across catalog calls, avoiding
# the overhead of spawning/tearing down a subprocess for each call.
_catalog_pool = WorkerPool(max_idle=4, idle_timeout=30.0)


class CatalogClientError(Exception):
    """Error raised by catalog operations."""


class CatalogClientMixin:
    """Mixin that adds catalog operations to a VGI Client.

    This mixin provides the core infrastructure for catalog operations.
    Worker subprocesses are pooled and reused across calls via a shared
    WorkerPool.

    Expected attributes from Client:
        server_path: str - Worker command (shell command)

    """

    # Type hints for attributes expected from Client
    server_path: str

    @contextmanager
    def _catalog_connect(self) -> Iterator[VgiProtocol]:
        """Get a typed proxy to the worker via the connection pool.

        Yields a VgiProtocol proxy. Worker errors are caught and
        re-raised as CatalogClientError.

        """
        cmd = shlex.split(self.server_path)
        try:
            with _catalog_pool.connect(VgiProtocol, cmd) as proxy:  # type: ignore[type-abstract]
                yield proxy
        except RpcError as e:
            raise CatalogClientError(str(e)) from e
        except CatalogClientError:
            raise
        except Exception as e:
            raise CatalogClientError(f"Failed catalog call: {e}") from e

    @staticmethod
    def _options_to_batch(options: dict[str, Any] | None) -> pa.RecordBatch | None:
        """Convert an options dict to a one-row RecordBatch for wire transport.

        Returns None if options is empty/None.
        """
        if not options:
            return None
        return pa.RecordBatch.from_pylist([options])

    # ========== Discovery Methods ==========

    def catalogs(self) -> list[str]:
        """Get list of catalog names from the worker.

        Returns:
            List of catalog names available in the worker.

        """
        with self._catalog_connect() as proxy:
            return proxy.catalog_catalogs().items

    # ========== Catalog Lifecycle Methods ==========

    def catalog_attach(self, *, name: str, options: dict[str, Any] | None = None) -> CatalogAttachResult:
        """Attach to a catalog.

        Args:
            name: The catalog name to attach to.
            options: Optional dictionary of catalog-specific options.

        Returns:
            CatalogAttachResult with attach_id and catalog capabilities.

        """
        with self._catalog_connect() as proxy:
            return proxy.catalog_attach(
                request=CatalogAttachRequest(
                    name=name,
                    options=self._options_to_batch(options),
                )
            )

    def catalog_detach(self, *, attach_id: AttachId) -> None:
        """Detach from a catalog.

        Args:
            attach_id: The attachment ID from catalog_attach.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_detach(attach_id=attach_id)

    def catalog_create(
        self,
        *,
        name: str,
        on_conflict: OnConflict = OnConflict.ERROR,
        options: dict[str, Any] | None = None,
    ) -> None:
        """Create a new catalog.

        Args:
            name: The name for the new catalog.
            on_conflict: Behavior if catalog already exists.
            options: Optional dictionary of catalog-specific options.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_create(
                request=CatalogCreateRequest(
                    name=name,
                    on_conflict=on_conflict,
                    options=self._options_to_batch(options),
                )
            )

    def catalog_drop(self, *, name: str) -> None:
        """Drop a catalog.

        Args:
            name: The name of the catalog to drop.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_drop(name=name)

    def catalog_version(self, *, attach_id: AttachId, transaction_id: TransactionId | None = None) -> int:
        """Get the current catalog version.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID for transactional reads.

        Returns:
            The current catalog version number, or 0 if empty.

        """
        with self._catalog_connect() as proxy:
            return proxy.catalog_version(
                attach_id=attach_id,
                transaction_id=transaction_id,
            ).version

    # ========== Transaction Methods ==========

    def catalog_transaction_begin(self, *, attach_id: AttachId) -> TransactionId | None:
        """Begin a new transaction.

        Args:
            attach_id: The attachment ID from catalog_attach.

        Returns:
            TransactionId for the new transaction, or None if transactions
            are not supported by this catalog.

        """
        with self._catalog_connect() as proxy:
            tx_id = proxy.catalog_transaction_begin(attach_id=attach_id).transaction_id
            return TransactionId(tx_id) if tx_id else None

    def catalog_transaction_commit(self, *, attach_id: AttachId, transaction_id: TransactionId) -> None:
        """Commit a transaction.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: The transaction ID to commit.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_transaction_commit(
                attach_id=attach_id,
                transaction_id=transaction_id,
            )

    def catalog_transaction_rollback(self, *, attach_id: AttachId, transaction_id: TransactionId) -> None:
        """Rollback a transaction.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: The transaction ID to rollback.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_transaction_rollback(
                attach_id=attach_id,
                transaction_id=transaction_id,
            )

    # ========== Schema Methods ==========

    def schemas(self, *, attach_id: AttachId, transaction_id: TransactionId | None = None) -> list[SchemaInfo]:
        """List schemas in the catalog.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID for transactional reads.

        Returns:
            List of SchemaInfo for each schema in the catalog.

        """
        with self._catalog_connect() as proxy:
            return proxy.catalog_schemas(
                attach_id=attach_id,
                transaction_id=transaction_id,
            ).to_infos()

    def schema_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        name: str,
    ) -> SchemaInfo | None:
        """Get information about a schema.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID for transactional reads.
            name: The schema name.

        Returns:
            SchemaInfo for the schema, or None if not found.

        """
        with self._catalog_connect() as proxy:
            return proxy.catalog_schema_get(  # type: ignore[no-any-return]
                attach_id=attach_id,
                name=name,
                transaction_id=transaction_id,
            ).to_optional()

    def schema_create(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        name: str,
        comment: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Create a new schema.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            name: The name for the new schema.
            comment: Optional description of the schema.
            tags: Optional key-value tags for the schema.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_schema_create(
                attach_id=attach_id,
                name=name,
                comment=comment,
                tags=tags,
                transaction_id=transaction_id,
            )

    def schema_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        name: str,
        ignore_not_found: bool = False,
        cascade: bool = False,
    ) -> None:
        """Drop a schema.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            name: The name of the schema to drop.
            ignore_not_found: If True, don't error if schema doesn't exist.
            cascade: If True, drop all contained tables and views.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_schema_drop(
                attach_id=attach_id,
                name=name,
                ignore_not_found=ignore_not_found,
                cascade=cascade,
                transaction_id=transaction_id,
            )

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        name: str,
        type: Literal[SchemaObjectType.TABLE],
    ) -> Sequence[TableInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        name: str,
        type: Literal[SchemaObjectType.VIEW],
    ) -> Sequence[ViewInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        name: str,
        type: Literal[SchemaObjectType.SCALAR_FUNCTION, SchemaObjectType.TABLE_FUNCTION],
    ) -> Sequence[FunctionInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        name: str,
        type: Literal[SchemaObjectType.SCALAR_MACRO, SchemaObjectType.TABLE_MACRO],
    ) -> Sequence[MacroInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        name: str,
        type: SchemaObjectType,
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo | MacroInfo]: ...

    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        name: str,
        type: SchemaObjectType,
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo | MacroInfo]:
        """List contents of a schema (tables, views, functions, macros).

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID for transactional reads.
            name: The schema name.
            type: The type of objects to return. Must be a SchemaObjectType enum:
                - SchemaObjectType.TABLE: Return only tables
                - SchemaObjectType.VIEW: Return only views
                - SchemaObjectType.SCALAR_FUNCTION: Return only scalar functions
                - SchemaObjectType.TABLE_FUNCTION: Return only table functions
                - SchemaObjectType.SCALAR_MACRO: Return only scalar macros
                - SchemaObjectType.TABLE_MACRO: Return only table macros

        Returns:
            List of TableInfo, ViewInfo, FunctionInfo, or MacroInfo depending on the type.

        """
        with self._catalog_connect() as proxy:
            if type == SchemaObjectType.TABLE:
                return proxy.catalog_schema_contents_tables(
                    attach_id=attach_id,
                    name=name,
                    transaction_id=transaction_id,
                ).to_infos()
            elif type == SchemaObjectType.VIEW:
                return proxy.catalog_schema_contents_views(
                    attach_id=attach_id,
                    name=name,
                    transaction_id=transaction_id,
                ).to_infos()
            elif type in (SchemaObjectType.SCALAR_MACRO, SchemaObjectType.TABLE_MACRO):
                return proxy.catalog_schema_contents_macros(
                    attach_id=attach_id,
                    name=name,
                    type=type,
                    transaction_id=transaction_id,
                ).to_infos()
            else:
                return proxy.catalog_schema_contents_functions(
                    attach_id=attach_id,
                    name=name,
                    type=type,
                    transaction_id=transaction_id,
                ).to_infos()

    # ========== Table Methods ==========

    def table_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
    ) -> TableInfo | None:
        """Get information about a table.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID for transactional reads.
            schema_name: The schema containing the table.
            name: The table name.

        Returns:
            TableInfo for the table, or None if not found.

        """
        with self._catalog_connect() as proxy:
            return proxy.catalog_table_get(  # type: ignore[no-any-return]
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                transaction_id=transaction_id,
            ).to_optional()

    def table_create(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        columns: SerializedSchema,
        on_conflict: OnConflict = OnConflict.ERROR,
        not_null_constraints: list[int] | None = None,
        unique_constraints: list[list[int]] | None = None,
        check_constraints: list[str] | None = None,
    ) -> None:
        """Create a new table.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema to create the table in.
            name: The name for the new table.
            columns: Serialized PyArrow schema for the table columns.
            on_conflict: Behavior if table already exists.
            not_null_constraints: Column indices that must not be null.
            unique_constraints: Lists of column indices for unique constraints.
            check_constraints: SQL expressions for check constraints.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_table_create(
                request=TableCreateRequest(
                    attach_id=attach_id,
                    schema_name=schema_name,
                    name=name,
                    columns=columns,
                    on_conflict=on_conflict,
                    not_null_constraints=not_null_constraints or [],
                    unique_constraints=unique_constraints or [],
                    check_constraints=check_constraints or [],
                    transaction_id=transaction_id,
                )
            )

    def table_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        ignore_not_found: bool = False,
        cascade: bool = False,
    ) -> None:
        """Drop a table.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema containing the table.
            name: The name of the table to drop.
            ignore_not_found: If True, don't error if table doesn't exist.
            cascade: If True, also drop dependent objects.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_table_drop(
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                ignore_not_found=ignore_not_found,
                cascade=cascade,
                transaction_id=transaction_id,
            )

    def table_scan_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> ScanFunctionResult:
        """Get the scan function for a table.

        Returns a ScanFunctionResult that tells the VGI DuckDB extension which
        DuckDB function to call to obtain the table data.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID for transactional reads.
            schema_name: The schema containing the table.
            name: The table name.
            at_unit: Optional time travel unit (e.g., 'timestamp', 'version').
            at_value: Optional time travel value.

        Returns:
            ScanFunctionResult with function_name, arguments, and extensions.

        Raises:
            CatalogClientError: If table_scan_function_get returned no result.

        """
        with self._catalog_connect() as proxy:
            result_bytes = proxy.catalog_table_scan_function_get(
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                at_unit=at_unit,
                at_value=at_value,
                transaction_id=transaction_id,
            )
            batch, _ = deserialize_record_batch(result_bytes)
            return ScanFunctionResult.deserialize(batch)

    def table_comment_set(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        comment: str | None,
        ignore_not_found: bool = False,
    ) -> None:
        """Set or clear the comment on a table.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema containing the table.
            name: The table name.
            comment: The new comment, or None to clear.
            ignore_not_found: If True, don't error if table doesn't exist.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_table_comment_set(
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                comment=comment,
                ignore_not_found=ignore_not_found,
                transaction_id=transaction_id,
            )

    def table_rename(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        new_name: str,
        ignore_not_found: bool = False,
    ) -> None:
        """Rename a table.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema containing the table.
            name: The current name of the table.
            new_name: The new name for the table.
            ignore_not_found: If True, don't error if table doesn't exist.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_table_rename(
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                new_name=new_name,
                ignore_not_found=ignore_not_found,
                transaction_id=transaction_id,
            )

    def table_column_add(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        column_definition: SerializedSchema,
        ignore_not_found: bool = False,
        if_column_not_exists: bool = False,
    ) -> None:
        """Add a new column to a table.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema containing the table.
            name: The table name.
            column_definition: Serialized schema with single field for the new column.
            ignore_not_found: If True, don't error if table doesn't exist.
            if_column_not_exists: If True, don't error if column already exists.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_table_column_add(
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                column_definition=column_definition,
                ignore_not_found=ignore_not_found,
                if_column_not_exists=if_column_not_exists,
                transaction_id=transaction_id,
            )

    def table_column_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool = False,
        if_column_exists: bool = False,
        cascade: bool = False,
    ) -> None:
        """Drop a column from a table.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema containing the table.
            name: The table name.
            column_name: The name of the column to drop.
            ignore_not_found: If True, don't error if table doesn't exist.
            if_column_exists: If True, don't error if column doesn't exist.
            cascade: If True, drop dependent constraints.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_table_column_drop(
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                column_name=column_name,
                ignore_not_found=ignore_not_found,
                if_column_exists=if_column_exists,
                cascade=cascade,
                transaction_id=transaction_id,
            )

    def table_column_rename(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        column_name: str,
        new_column_name: str,
        ignore_not_found: bool = False,
    ) -> None:
        """Rename a column.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema containing the table.
            name: The table name.
            column_name: The current name of the column.
            new_column_name: The new name for the column.
            ignore_not_found: If True, don't error if table doesn't exist.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_table_column_rename(
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                column_name=column_name,
                new_column_name=new_column_name,
                ignore_not_found=ignore_not_found,
                transaction_id=transaction_id,
            )

    def table_column_default_set(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        column_name: str,
        expression: SqlExpression,
        ignore_not_found: bool = False,
    ) -> None:
        """Set the default value expression for a column.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema containing the table.
            name: The table name.
            column_name: The column to set the default for.
            expression: The SQL expression for the default value.
            ignore_not_found: If True, don't error if table doesn't exist.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_table_column_default_set(
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                column_name=column_name,
                expression=expression,
                ignore_not_found=ignore_not_found,
                transaction_id=transaction_id,
            )

    def table_column_default_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool = False,
    ) -> None:
        """Remove the default value from a column.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema containing the table.
            name: The table name.
            column_name: The column to remove the default from.
            ignore_not_found: If True, don't error if table doesn't exist.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_table_column_default_drop(
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                column_name=column_name,
                ignore_not_found=ignore_not_found,
                transaction_id=transaction_id,
            )

    def table_column_type_change(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        column_definition: SerializedSchema,
        expression: SqlExpression | None = None,
        ignore_not_found: bool = False,
    ) -> None:
        """Change the type of a column.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema containing the table.
            name: The table name.
            column_definition: Serialized schema with single field defining the
                new type. Column name is taken from the schema field name.
            expression: Optional SQL expression to convert existing values.
            ignore_not_found: If True, don't error if table doesn't exist.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_table_column_type_change(
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                column_definition=column_definition,
                expression=expression,
                ignore_not_found=ignore_not_found,
                transaction_id=transaction_id,
            )

    def table_not_null_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool = False,
    ) -> None:
        """Remove NOT NULL constraint from a column.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema containing the table.
            name: The table name.
            column_name: The column to remove NOT NULL from.
            ignore_not_found: If True, don't error if table doesn't exist.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_table_not_null_drop(
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                column_name=column_name,
                ignore_not_found=ignore_not_found,
                transaction_id=transaction_id,
            )

    def table_not_null_set(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool = False,
    ) -> None:
        """Add NOT NULL constraint to a column.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema containing the table.
            name: The table name.
            column_name: The column to add NOT NULL to.
            ignore_not_found: If True, don't error if table doesn't exist.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_table_not_null_set(
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                column_name=column_name,
                ignore_not_found=ignore_not_found,
                transaction_id=transaction_id,
            )

    # ========== View Methods ==========

    def view_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
    ) -> ViewInfo | None:
        """Get information about a view.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID for transactional reads.
            schema_name: The schema containing the view.
            name: The view name.

        Returns:
            ViewInfo for the view, or None if not found.

        """
        with self._catalog_connect() as proxy:
            return proxy.catalog_view_get(  # type: ignore[no-any-return]
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                transaction_id=transaction_id,
            ).to_optional()

    def view_create(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        definition: str,
        on_conflict: OnConflict = OnConflict.ERROR,
    ) -> None:
        """Create a new view.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema to create the view in.
            name: The name for the new view.
            definition: The SQL SELECT statement defining the view.
            on_conflict: Behavior if view already exists.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_view_create(
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                definition=definition,
                on_conflict=on_conflict,
                transaction_id=transaction_id,
            )

    def view_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        ignore_not_found: bool = False,
        cascade: bool = False,
    ) -> None:
        """Drop a view.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema containing the view.
            name: The name of the view to drop.
            ignore_not_found: If True, don't error if view doesn't exist.
            cascade: If True, also drop dependent objects.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_view_drop(
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                ignore_not_found=ignore_not_found,
                cascade=cascade,
                transaction_id=transaction_id,
            )

    def view_rename(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        new_name: str,
        ignore_not_found: bool = False,
    ) -> None:
        """Rename a view.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema containing the view.
            name: The current name of the view.
            new_name: The new name for the view.
            ignore_not_found: If True, don't error if view doesn't exist.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_view_rename(
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                new_name=new_name,
                ignore_not_found=ignore_not_found,
                transaction_id=transaction_id,
            )

    def view_comment_set(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        comment: str | None,
        ignore_not_found: bool = False,
    ) -> None:
        """Set or clear the comment on a view.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema containing the view.
            name: The view name.
            comment: The new comment, or None to clear.
            ignore_not_found: If True, don't error if view doesn't exist.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_view_comment_set(
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                comment=comment,
                ignore_not_found=ignore_not_found,
                transaction_id=transaction_id,
            )

    # ========== Macro Methods ==========

    def macro_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
    ) -> MacroInfo | None:
        """Get information about a macro.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID for transactional reads.
            schema_name: The schema containing the macro.
            name: The macro name.

        Returns:
            MacroInfo for the macro, or None if not found.

        """
        with self._catalog_connect() as proxy:
            return proxy.catalog_macro_get(  # type: ignore[no-any-return]
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                transaction_id=transaction_id,
            ).to_optional()

    def macro_create(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        macro_type: MacroType,
        parameters: list[str],
        definition: str,
        on_conflict: OnConflict = OnConflict.ERROR,
        parameter_default_values: pa.RecordBatch | None = None,
    ) -> None:
        """Create a new macro.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema to create the macro in.
            name: The name for the new macro.
            macro_type: Whether this is a scalar or table macro.
            parameters: Ordered list of parameter names.
            definition: SQL expression (scalar) or query (table).
            on_conflict: Behavior if macro already exists.
            parameter_default_values: One-row RecordBatch with typed defaults.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_macro_create(
                request=MacroCreateRequest(
                    attach_id=attach_id,
                    schema_name=schema_name,
                    name=name,
                    macro_type=macro_type,
                    parameters=parameters,
                    definition=definition,
                    on_conflict=on_conflict,
                    parameter_default_values=parameter_default_values,
                    transaction_id=transaction_id,
                )
            )

    def macro_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        ignore_not_found: bool = False,
    ) -> None:
        """Drop a macro.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema containing the macro.
            name: The name of the macro to drop.
            ignore_not_found: If True, don't error if macro doesn't exist.

        """
        with self._catalog_connect() as proxy:
            proxy.catalog_macro_drop(
                attach_id=attach_id,
                schema_name=schema_name,
                name=name,
                ignore_not_found=ignore_not_found,
                transaction_id=transaction_id,
            )
