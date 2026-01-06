"""VGI CatalogClient for catalog operations.

This module provides the CatalogClient class for invoking CatalogInterface methods
on VGI workers. Each method call spawns a new worker process for simplicity.

QUICK START
-----------
Use CatalogClient for catalog operations:

    from vgi.client import CatalogClient

    client = CatalogClient("vgi-my-worker")

    # List available catalogs
    catalogs = client.catalogs()

    # Attach to a catalog
    result = client.catalog_attach(name="my_catalog", options={})

    # List schemas
    for schema in client.schemas(attach_id=result.attach_id, transaction_id=None):
        print(schema.name)

See Also
--------
vgi.catalog.CatalogInterface : The interface that workers implement
vgi.worker.Worker : Workers with catalog_interface set

"""

from __future__ import annotations

import io
import subprocess
import sys
from collections.abc import Iterator
from typing import Any, cast

import pyarrow as pa
import structlog
import structlog.stdlib

from vgi.arguments import Arguments
from vgi.catalog import (
    AttachId,
    CatalogAttachResult,
    FunctionInfo,
    OnConflict,
    ScanFunctionResult,
    SchemaInfo,
    SerializedSchema,
    TableInfo,
    TransactionId,
    ViewInfo,
)
from vgi.invocation import Invocation, InvocationType
from vgi.ipc_utils import read_ipc_batch

# Configure structlog to write to stderr
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),
    logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
)

log: structlog.stdlib.BoundLogger = structlog.get_logger().bind(
    component="catalog_client"
)


class CatalogClientError(Exception):
    """Error raised by CatalogClient operations."""


class CatalogClient:
    """Client for invoking CatalogInterface methods on VGI workers.

    Each method call spawns a new worker process, matching VGI's short-lived
    worker pattern. The catalog protocol is simplified compared to function
    invocations: there's no bind/init phase, just invoke → stream.

    Example:
        client = CatalogClient("./my_worker")

        # Attach to a catalog
        result = client.catalog_attach(name="my_catalog", options={})

        # List schemas
        for schema in client.schemas(attach_id=result.attach_id, transaction_id=None):
            print(schema.name)

    """

    def __init__(self, worker_command: str | list[str]) -> None:
        """Initialize the CatalogClient.

        Args:
            worker_command: Command to spawn the worker. Can be a string
                (shell command) or list of arguments.

        """
        if isinstance(worker_command, str):
            self.server_path: list[str] = worker_command.split()
        else:
            self.server_path = worker_command

    def _invoke(
        self,
        method_name: str,
        **kwargs: Any,
    ) -> pa.RecordBatch | None:
        """Invoke a catalog method and return the result batch.

        Spawns a worker, sends the invocation with method name and args,
        reads the result, and returns the deserialized batch.

        Args:
            method_name: CatalogInterface method name (e.g., 'catalog_attach').
            **kwargs: Method keyword arguments.

        Returns:
            RecordBatch with the result, or None for methods that return None.

        """
        log.debug("catalog_invoke", method=method_name, kwargs=kwargs)

        # Start worker process
        proc = subprocess.Popen(
            self.server_path,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=False,
        )

        if proc.stdin is None or proc.stdout is None:
            raise CatalogClientError("Failed to create pipes for worker process")

        stdout_buffered = io.BufferedReader(cast(io.RawIOBase, proc.stdout))

        try:
            # Create and send invocation
            invocation = Invocation(
                function_name=method_name,
                input_schema=None,
                function_type=InvocationType.CATALOG,
                correlation_id="catalog",
                invocation_id=None,
                arguments=Arguments(),
            )
            invocation_bytes = invocation.serialize()
            proc.stdin.write(invocation_bytes)

            # Create and send arguments batch (1 row with kwargs as columns)
            args_batch = self._create_args_batch(kwargs)
            args_bytes = (
                args_batch.schema.serialize().to_pybytes()
                + args_batch.serialize().to_pybytes()
            )
            proc.stdin.write(args_bytes)
            proc.stdin.flush()
            proc.stdin.close()

            # Read result
            try:
                result_batch = read_ipc_batch(stdout_buffered, "catalog_result")
                log.debug(
                    "catalog_result",
                    method=method_name,
                    num_rows=result_batch.num_rows,
                    num_columns=result_batch.num_columns,
                )
                return result_batch
            except Exception as e:
                # Check if worker had an error
                stderr_output = proc.stderr.read().decode() if proc.stderr else ""
                if stderr_output:
                    log.error("worker_stderr", stderr=stderr_output)
                raise CatalogClientError(
                    f"Failed to read catalog result: {e}\n{stderr_output}"
                ) from e

        finally:
            proc.wait()

    def _invoke_stream(
        self,
        method_name: str,
        **kwargs: Any,
    ) -> Iterator[pa.RecordBatch]:
        """Invoke a catalog method and stream result batches.

        For methods that return iterables (schemas, schema_contents, etc.),
        this yields each result batch.

        Args:
            method_name: CatalogInterface method name.
            **kwargs: Method keyword arguments.

        Yields:
            RecordBatch for each result item.

        """
        log.debug("catalog_invoke_stream", method=method_name, kwargs=kwargs)

        # Start worker process
        proc = subprocess.Popen(
            self.server_path,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=False,
        )

        if proc.stdin is None or proc.stdout is None:
            raise CatalogClientError("Failed to create pipes for worker process")

        stdout_buffered = io.BufferedReader(cast(io.RawIOBase, proc.stdout))

        try:
            # Create and send invocation
            invocation = Invocation(
                function_name=method_name,
                input_schema=None,
                function_type=InvocationType.CATALOG,
                correlation_id="catalog",
                invocation_id=None,
                arguments=Arguments(),
            )
            invocation_bytes = invocation.serialize()
            proc.stdin.write(invocation_bytes)

            # Create and send arguments batch
            args_batch = self._create_args_batch(kwargs)
            args_bytes = (
                args_batch.schema.serialize().to_pybytes()
                + args_batch.serialize().to_pybytes()
            )
            proc.stdin.write(args_bytes)
            proc.stdin.flush()
            proc.stdin.close()

            # Stream results - read batches until EOF
            while True:
                try:
                    result_batch = read_ipc_batch(stdout_buffered, "catalog_result")
                    # Empty batch (0 rows, 0 columns) signals end
                    if result_batch.num_rows == 0 and result_batch.num_columns == 0:
                        break
                    yield result_batch
                except Exception:
                    # EOF or error - stop iteration
                    break

        finally:
            proc.wait()

    def _create_args_batch(self, kwargs: dict[str, Any]) -> pa.RecordBatch:
        """Create a single-row batch from method keyword arguments."""
        if not kwargs:
            return pa.RecordBatch.from_pydict({})

        # Build column arrays from kwargs
        data: dict[str, list[Any]] = {}
        for name, value in kwargs.items():
            data[name] = [value]

        return pa.RecordBatch.from_pylist([kwargs])

    # ========== Discovery Methods ==========

    def catalogs(self) -> list[str]:
        """Get list of catalog names from the worker."""
        result = self._invoke("catalogs")
        if result is None or result.num_rows == 0:
            return []
        # Result should have a column with catalog names
        return cast(list[str], result.column(0).to_pylist())

    # ========== Catalog Lifecycle Methods ==========

    def catalog_attach(
        self, *, name: str, options: dict[str, Any] | None = None
    ) -> CatalogAttachResult:
        """Attach to a catalog."""
        result = self._invoke("catalog_attach", name=name, options=options or {})
        if result is None:
            raise CatalogClientError("catalog_attach returned no result")
        return CatalogAttachResult.deserialize(result)

    def catalog_detach(self, *, attach_id: AttachId) -> None:
        """Detach from a catalog."""
        self._invoke("catalog_detach", attach_id=attach_id)

    def catalog_create(
        self,
        *,
        name: str,
        on_conflict: OnConflict = OnConflict.ERROR,
        options: dict[str, Any] | None = None,
    ) -> None:
        """Create a new catalog."""
        self._invoke(
            "catalog_create",
            name=name,
            on_conflict=on_conflict.value,
            options=options or {},
        )

    def catalog_drop(self, *, name: str) -> None:
        """Drop a catalog."""
        self._invoke("catalog_drop", name=name)

    def catalog_version(
        self, *, attach_id: AttachId, transaction_id: TransactionId | None = None
    ) -> int:
        """Get the current catalog version."""
        result = self._invoke(
            "catalog_version", attach_id=attach_id, transaction_id=transaction_id
        )
        if result is None or result.num_rows == 0:
            return 0
        return cast(int, result.column(0).to_pylist()[0])

    # ========== Transaction Methods ==========

    def catalog_transaction_begin(self, *, attach_id: AttachId) -> TransactionId | None:
        """Begin a new transaction."""
        result = self._invoke("catalog_transaction_begin", attach_id=attach_id)
        if result is None or result.num_rows == 0:
            return None
        value = result.column(0).to_pylist()[0]
        return TransactionId(value) if value else None

    def catalog_transaction_commit(
        self, *, attach_id: AttachId, transaction_id: TransactionId
    ) -> None:
        """Commit a transaction."""
        self._invoke(
            "catalog_transaction_commit",
            attach_id=attach_id,
            transaction_id=transaction_id,
        )

    def catalog_transaction_rollback(
        self, *, attach_id: AttachId, transaction_id: TransactionId
    ) -> None:
        """Rollback a transaction."""
        self._invoke(
            "catalog_transaction_rollback",
            attach_id=attach_id,
            transaction_id=transaction_id,
        )

    # ========== Schema Methods ==========

    def schemas(
        self, *, attach_id: AttachId, transaction_id: TransactionId | None = None
    ) -> Iterator[SchemaInfo]:
        """List schemas in the catalog."""
        for batch in self._invoke_stream(
            "schemas", attach_id=attach_id, transaction_id=transaction_id
        ):
            yield SchemaInfo.deserialize(batch)

    def schema_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        name: str,
    ) -> SchemaInfo | None:
        """Get information about a schema."""
        result = self._invoke(
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
        """Create a new schema."""
        self._invoke(
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
        """Drop a schema."""
        self._invoke(
            "schema_drop",
            attach_id=attach_id,
            transaction_id=transaction_id,
            name=name,
            ignore_not_found=ignore_not_found,
            cascade=cascade,
        )

    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        name: str,
    ) -> Iterator[TableInfo | ViewInfo | FunctionInfo]:
        """List contents of a schema (tables, views, functions)."""
        for batch in self._invoke_stream(
            "schema_contents",
            attach_id=attach_id,
            transaction_id=transaction_id,
            name=name,
        ):
            # Determine type from batch schema or content
            # For now, assume schema column indicates type
            if "columns" in batch.schema.names:
                yield TableInfo.deserialize(batch)
            elif "definition" in batch.schema.names:
                yield ViewInfo.deserialize(batch)
            else:
                yield FunctionInfo.deserialize(batch)

    # ========== Table Methods ==========

    def table_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
    ) -> TableInfo | None:
        """Get information about a table."""
        result = self._invoke(
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
        """Create a new table."""
        self._invoke(
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
        """Drop a table."""
        self._invoke(
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
        """Get the scan function for a table."""
        result = self._invoke(
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

    # ========== View Methods ==========

    def view_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None = None,
        schema_name: str,
        name: str,
    ) -> ViewInfo | None:
        """Get information about a view."""
        result = self._invoke(
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
        """Create a new view."""
        self._invoke(
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
        """Drop a view."""
        self._invoke(
            "view_drop",
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            ignore_not_found=ignore_not_found,
        )
