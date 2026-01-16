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
    Worker exceptions are signaled via empty batches (0 rows) with
    vgi.log_level metadata set to "exception". The error message and
    traceback are extracted and raised as CatalogClientError.

"""

from __future__ import annotations

import io
import subprocess
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal, cast, overload

import pyarrow as pa

from vgi.arguments import Arguments
from vgi.catalog import (
    AttachId,
    CatalogAttachResult,
    FunctionInfo,
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
from vgi.invocation import Invocation, InvocationType
from vgi.ipc_utils import read_single_record_batch

if TYPE_CHECKING:
    import pyarrow as pa_typing
    import structlog.stdlib


class CatalogClientError(Exception):
    """Error raised by catalog operations."""


class CatalogClientMixin:
    """Mixin that adds catalog operations to a VGI Client.

    This mixin provides the core infrastructure for catalog operations.
    Each catalog method call spawns an ephemeral worker subprocess.

    Expected attributes from Client:
        server_path: str - Worker command (shell command)
        correlation_id: str - For distributed tracing

    """

    # Type hints for attributes expected from Client
    server_path: str
    correlation_id: str

    def _get_catalog_logger(self) -> structlog.stdlib.BoundLogger:
        """Get a logger for catalog operations.

        Returns a structlog logger bound with component="catalog_mixin".
        Import is done lazily to avoid circular imports.

        """
        import structlog

        return cast(
            "structlog.stdlib.BoundLogger",
            structlog.get_logger().bind(component="catalog_mixin"),
        )

    def _check_catalog_error(
        self,
        result_batch: pa.RecordBatch,
        result_metadata: pa_typing.KeyValueMetadata | None,
    ) -> None:
        """Check for error metadata in a catalog result and raise if found.

        Worker exceptions are signaled via empty batches (0 rows) with
        vgi.log_level metadata set to "exception".

        Args:
            result_batch: The result batch from the catalog call.
            result_metadata: Custom metadata from the batch.

        Raises:
            CatalogClientError: If the batch contains an exception message.

        """
        if result_metadata is None:
            return

        if not (
            result_batch.num_rows == 0
            and result_metadata.get(b"vgi.log_level") is not None
        ):
            return

        level_name = result_metadata[b"vgi.log_level"].decode().lower()
        if level_name != "exception":
            return

        import contextlib
        import json

        message = result_metadata.get(b"vgi.log_message", b"").decode()
        extra: dict[str, Any] = {}
        if result_metadata.get(b"vgi.log_extra") is not None:
            with contextlib.suppress(json.JSONDecodeError):
                extra = json.loads(result_metadata[b"vgi.log_extra"].decode())

        traceback_str = extra.get("traceback", "")
        raise CatalogClientError(f"Worker Exception: {message}\n{traceback_str}")

    def _send_catalog_invocation(
        self,
        method_name: str,
        kwargs: dict[str, Any],
    ) -> tuple[subprocess.Popen[bytes], io.BufferedReader[Any]]:
        """Spawn worker subprocess and send catalog invocation.

        This helper handles the common setup for catalog operations:
        spawning the worker, sending the invocation header, and sending
        the arguments batch.

        Args:
            method_name: CatalogInterface method name (e.g., 'catalog_attach').
            kwargs: Method keyword arguments to send.

        Returns:
            Tuple of (process, buffered_stdout) for reading results.

        Raises:
            CatalogClientError: If subprocess creation or invocation send fails.

        """
        proc = subprocess.Popen(
            self.server_path,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=False,
            shell=True,
        )

        if proc.stdin is None or proc.stdout is None:
            raise CatalogClientError("Failed to create pipes for worker process")

        stdout_buffered = io.BufferedReader(cast(io.RawIOBase, proc.stdout))

        # Create and send invocation
        invocation = Invocation(
            function_name=method_name,
            input_schema=None,
            function_type=InvocationType.CATALOG,
            correlation_id=self.correlation_id,
            invocation_id=None,
            arguments=Arguments(),
        )
        invocation_bytes = invocation.serialize()
        try:
            proc.stdin.write(invocation_bytes)
        except BrokenPipeError:
            proc.poll()
            raise CatalogClientError(
                f"Worker terminated unexpectedly during {method_name} invocation "
                f"(exit code: {proc.returncode})"
            ) from None

        # Create and send arguments batch
        args_batch = self._create_catalog_args_batch(kwargs)
        args_bytes = (
            args_batch.schema.serialize().to_pybytes()
            + args_batch.serialize().to_pybytes()
        )
        try:
            proc.stdin.write(args_bytes)
            proc.stdin.flush()
        except BrokenPipeError:
            proc.poll()
            raise CatalogClientError(
                f"Worker terminated unexpectedly during {method_name} arguments "
                f"(exit code: {proc.returncode})"
            ) from None
        proc.stdin.close()

        return proc, stdout_buffered

    def _catalog_invoke(
        self,
        method_name: str,
        **kwargs: Any,
    ) -> pa.RecordBatch | None:
        """Invoke a catalog method and return the result batch.

        Spawns an ephemeral worker subprocess, sends the invocation with
        method name and arguments, reads a single result batch using
        read_single_record_batch, and returns it.

        Args:
            method_name: CatalogInterface method name (e.g., 'catalog_attach').
            **kwargs: Method keyword arguments.

        Returns:
            RecordBatch with the result, or None for methods that return None.

        Raises:
            CatalogClientError: If worker subprocess fails or returns an error.

        """
        log = self._get_catalog_logger()
        log.debug("catalog_invoke", method=method_name, kwargs=kwargs)

        proc, stdout_buffered = self._send_catalog_invocation(method_name, kwargs)

        try:
            result_batch, result_metadata = read_single_record_batch(
                stdout_buffered, "catalog_result"
            )
            self._check_catalog_error(result_batch, result_metadata)

            log.debug(
                "catalog_result",
                method=method_name,
                num_rows=result_batch.num_rows,
                num_columns=result_batch.num_columns,
            )
            return result_batch
        except CatalogClientError:
            raise
        except Exception as e:
            stderr_output = proc.stderr.read().decode() if proc.stderr else ""
            if stderr_output:
                log.error("worker_stderr", stderr=stderr_output)
            raise CatalogClientError(
                f"Failed to read catalog result: {e}\n{stderr_output}"
            ) from e
        finally:
            proc.wait()

    def _catalog_invoke_batch(
        self,
        method_name: str,
        **kwargs: Any,
    ) -> pa.RecordBatch:
        """Invoke a catalog method and return a batch with multiple rows.

        For methods that return lists (schemas, schema_contents), this uses
        an IPC stream reader to read a single batch containing all results.
        Unlike _catalog_invoke which uses read_single_record_batch, this
        method handles the streaming protocol used for list-returning methods.

        Args:
            method_name: CatalogInterface method name.
            **kwargs: Method keyword arguments.

        Returns:
            RecordBatch with all results (may have 0 rows for empty results).

        Raises:
            CatalogClientError: If worker subprocess fails or returns an error.

        """
        log = self._get_catalog_logger()
        log.debug("catalog_invoke_batch", method=method_name, kwargs=kwargs)

        proc, stdout_buffered = self._send_catalog_invocation(method_name, kwargs)

        try:
            with pa.ipc.open_stream(stdout_buffered) as reader:
                result_batch = reader.read_next_batch()
                result_metadata = cast(
                    "pa_typing.KeyValueMetadata | None",
                    result_batch.schema.metadata,
                )
                self._check_catalog_error(result_batch, result_metadata)

                log.debug(
                    "catalog_batch_result",
                    method=method_name,
                    num_rows=result_batch.num_rows,
                )
                return result_batch
        except StopIteration:
            return pa.RecordBatch.from_pydict({})
        except CatalogClientError:
            raise
        except Exception as e:
            stderr_output = proc.stderr.read().decode() if proc.stderr else ""
            if stderr_output:
                log.error("worker_stderr", stderr=stderr_output)
            raise CatalogClientError(
                f"Failed to read catalog result: {e}\n{stderr_output}"
            ) from e
        finally:
            proc.wait()

    def _create_catalog_args_batch(self, kwargs: dict[str, Any]) -> pa.RecordBatch:
        """Create a batch from method keyword arguments.

        Converts method kwargs into an Arrow RecordBatch where each column
        corresponds to a kwarg key/value pair.

        Args:
            kwargs: Dictionary of method keyword arguments.

        Returns:
            A RecordBatch with 0 or 1 rows. Empty batch (0 rows) for methods
            with no arguments, 1-row batch otherwise.

        """
        if not kwargs:
            # Empty batch for methods with no arguments
            return pa.RecordBatch.from_pydict({})
        return pa.RecordBatch.from_pylist([kwargs])

    # ========== Discovery Methods ==========

    def catalogs(self) -> list[str]:
        """Get list of catalog names from the worker.

        Returns:
            List of catalog names available in the worker.

        """
        result = self._catalog_invoke("catalogs")
        if result is None or result.num_rows == 0:
            return []
        return cast(list[str], result.column(0).to_pylist())

    # ========== Catalog Lifecycle Methods ==========

    def catalog_attach(
        self, *, name: str, options: dict[str, Any] | None = None
    ) -> CatalogAttachResult:
        """Attach to a catalog.

        Args:
            name: The catalog name to attach to.
            options: Optional dictionary of catalog-specific options.

        Returns:
            CatalogAttachResult with attach_id and catalog capabilities.

        Raises:
            CatalogClientError: If catalog_attach returned no result.

        """
        result = self._catalog_invoke(
            "catalog_attach", name=name, options=options or {}
        )
        if result is None:
            raise CatalogClientError("catalog_attach returned no result")
        return CatalogAttachResult.deserialize(result)

    def catalog_detach(self, *, attach_id: AttachId) -> None:
        """Detach from a catalog.

        Args:
            attach_id: The attachment ID from catalog_attach.

        """
        self._catalog_invoke("catalog_detach", attach_id=attach_id)

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
        self._catalog_invoke(
            "catalog_create",
            name=name,
            on_conflict=on_conflict.value,
            options=options or {},
        )

    def catalog_drop(self, *, name: str) -> None:
        """Drop a catalog.

        Args:
            name: The name of the catalog to drop.

        """
        self._catalog_invoke("catalog_drop", name=name)

    def catalog_version(
        self, *, attach_id: AttachId, transaction_id: TransactionId | None = None
    ) -> int:
        """Get the current catalog version.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID for transactional reads.

        Returns:
            The current catalog version number, or 0 if empty.

        """
        result = self._catalog_invoke(
            "catalog_version", attach_id=attach_id, transaction_id=transaction_id
        )
        if result is None or result.num_rows == 0:
            return 0
        return cast(int, result.column(0).to_pylist()[0])

    # ========== Transaction Methods ==========

    def catalog_transaction_begin(self, *, attach_id: AttachId) -> TransactionId | None:
        """Begin a new transaction.

        Args:
            attach_id: The attachment ID from catalog_attach.

        Returns:
            TransactionId for the new transaction, or None if transactions
            are not supported by this catalog.

        """
        result = self._catalog_invoke("catalog_transaction_begin", attach_id=attach_id)
        if result is None or result.num_rows == 0:
            return None
        value = result.column(0).to_pylist()[0]
        return TransactionId(value) if value else None

    def catalog_transaction_commit(
        self, *, attach_id: AttachId, transaction_id: TransactionId
    ) -> None:
        """Commit a transaction.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: The transaction ID to commit.

        """
        self._catalog_invoke(
            "catalog_transaction_commit",
            attach_id=attach_id,
            transaction_id=transaction_id,
        )

    def catalog_transaction_rollback(
        self, *, attach_id: AttachId, transaction_id: TransactionId
    ) -> None:
        """Rollback a transaction.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: The transaction ID to rollback.

        """
        self._catalog_invoke(
            "catalog_transaction_rollback",
            attach_id=attach_id,
            transaction_id=transaction_id,
        )

    # ========== Schema Methods ==========

    def schemas(
        self, *, attach_id: AttachId, transaction_id: TransactionId | None = None
    ) -> list[SchemaInfo]:
        """List schemas in the catalog.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID for transactional reads.

        Returns:
            List of SchemaInfo for each schema in the catalog.

        """
        batch = self._catalog_invoke_batch(
            "schemas", attach_id=attach_id, transaction_id=transaction_id
        )
        results: list[SchemaInfo] = []
        for i in range(batch.num_rows):
            row_batch = batch.slice(i, 1)
            results.append(SchemaInfo.deserialize(row_batch))
        return results

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
        result = self._catalog_invoke(
            "schema_get",
            attach_id=attach_id,
            transaction_id=transaction_id,
            name=name,
        )
        if result is None or result.num_rows == 0:
            return None
        return SchemaInfo.deserialize(result)

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
        self._catalog_invoke(
            "schema_create",
            attach_id=attach_id,
            transaction_id=transaction_id,
            name=name,
            comment=comment,
            tags=tags or {},
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
        self._catalog_invoke(
            "schema_drop",
            attach_id=attach_id,
            transaction_id=transaction_id,
            name=name,
            ignore_not_found=ignore_not_found,
            cascade=cascade,
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
        type: Literal[
            SchemaObjectType.SCALAR_FUNCTION, SchemaObjectType.TABLE_FUNCTION
        ],
    ) -> Sequence[FunctionInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        name: str,
        type: SchemaObjectType,
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo]: ...

    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        name: str,
        type: SchemaObjectType,
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo]:
        """List contents of a schema (tables, views, functions).

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID for transactional reads.
            name: The schema name.
            type: The type of objects to return. Must be a SchemaObjectType enum:
                - SchemaObjectType.TABLE: Return only tables
                - SchemaObjectType.VIEW: Return only views
                - SchemaObjectType.SCALAR_FUNCTION: Return only scalar functions
                - SchemaObjectType.TABLE_FUNCTION: Return only table functions

        Returns:
            List of TableInfo, ViewInfo, or FunctionInfo depending on the type.

        """
        kwargs: dict[str, Any] = {
            "attach_id": attach_id,
            "transaction_id": transaction_id,
            "name": name,
            "type": type.value,
        }

        batch = self._catalog_invoke_batch("schema_contents", **kwargs)
        results: list[TableInfo | ViewInfo | FunctionInfo] = []

        for i in range(batch.num_rows):
            row_batch = batch.slice(i, 1)
            # Deserialize based on requested type
            if type == SchemaObjectType.TABLE:
                results.append(TableInfo.deserialize(row_batch))
            elif type == SchemaObjectType.VIEW:
                results.append(ViewInfo.deserialize(row_batch))
            else:
                results.append(FunctionInfo.deserialize(row_batch))

        return results

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
        result = self._catalog_invoke(
            "table_get",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
        )
        if result is None or result.num_rows == 0:
            return None
        return TableInfo.deserialize(result)

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
        self._catalog_invoke(
            "table_create",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            columns=columns,
            on_conflict=on_conflict.value,
            not_null_constraints=not_null_constraints or [],
            unique_constraints=unique_constraints or [],
            check_constraints=check_constraints or [],
        )

    def table_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        ignore_not_found: bool = False,
    ) -> None:
        """Drop a table.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema containing the table.
            name: The name of the table to drop.
            ignore_not_found: If True, don't error if table doesn't exist.

        """
        self._catalog_invoke(
            "table_drop",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            ignore_not_found=ignore_not_found,
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

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID for transactional reads.
            schema_name: The schema containing the table.
            name: The table name.
            at_unit: Optional time travel unit (e.g., 'timestamp', 'version').
            at_value: Optional time travel value.

        Returns:
            ScanFunctionResult with function_name, max_processes, invocation_id.

        Raises:
            CatalogClientError: If table_scan_function_get returned no result.

        """
        result = self._catalog_invoke(
            "table_scan_function_get",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            at_unit=at_unit,
            at_value=at_value,
        )
        if result is None:
            raise CatalogClientError("table_scan_function_get returned no result")
        return ScanFunctionResult.deserialize(result)

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
        self._catalog_invoke(
            "table_comment_set",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            comment=comment,
            ignore_not_found=ignore_not_found,
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
        self._catalog_invoke(
            "table_rename",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            new_name=new_name,
            ignore_not_found=ignore_not_found,
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
        self._catalog_invoke(
            "table_column_add",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            column_definition=column_definition,
            ignore_not_found=ignore_not_found,
            if_column_not_exists=if_column_not_exists,
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
        self._catalog_invoke(
            "table_column_drop",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            column_name=column_name,
            ignore_not_found=ignore_not_found,
            if_column_exists=if_column_exists,
            cascade=cascade,
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
        self._catalog_invoke(
            "table_column_rename",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            column_name=column_name,
            new_column_name=new_column_name,
            ignore_not_found=ignore_not_found,
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
        self._catalog_invoke(
            "table_column_default_set",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            column_name=column_name,
            expression=expression,
            ignore_not_found=ignore_not_found,
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
        self._catalog_invoke(
            "table_column_default_drop",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            column_name=column_name,
            ignore_not_found=ignore_not_found,
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
        self._catalog_invoke(
            "table_column_type_change",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            column_definition=column_definition,
            expression=expression,
            ignore_not_found=ignore_not_found,
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
        self._catalog_invoke(
            "table_not_null_drop",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            column_name=column_name,
            ignore_not_found=ignore_not_found,
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
        self._catalog_invoke(
            "table_not_null_set",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            column_name=column_name,
            ignore_not_found=ignore_not_found,
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
        result = self._catalog_invoke(
            "view_get",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
        )
        if result is None or result.num_rows == 0:
            return None
        return ViewInfo.deserialize(result)

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
        self._catalog_invoke(
            "view_create",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            definition=definition,
            on_conflict=on_conflict.value,
        )

    def view_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
        ignore_not_found: bool = False,
    ) -> None:
        """Drop a view.

        Args:
            attach_id: The attachment ID from catalog_attach.
            transaction_id: Optional transaction ID.
            schema_name: The schema containing the view.
            name: The name of the view to drop.
            ignore_not_found: If True, don't error if view doesn't exist.

        """
        self._catalog_invoke(
            "view_drop",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            ignore_not_found=ignore_not_found,
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
        self._catalog_invoke(
            "view_rename",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            new_name=new_name,
            ignore_not_found=ignore_not_found,
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
        self._catalog_invoke(
            "view_comment_set",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            comment=comment,
            ignore_not_found=ignore_not_found,
        )
