# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Generic writable functions parameterized by table_name.

These functions extend the base writable scan/insert/update/delete classes
from ``writable_table`` but determine the table name dynamically from the
first positional argument instead of a hardcoded class variable. This allows
the same function classes to serve any table in the transactor's DuckDB
database, which is essential for DDL-created tables.

The table_name is passed as the first positional argument via the
``ScanFunctionResult`` when the catalog dispatches scan/insert/update/delete
for dynamically discovered tables.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
from vgi_rpc import AnnotatedBatch
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.writable.table import (
    _COUNT_SCHEMA,
    WritableScanState,
    _get_attach_opaque_data,
    _get_pushdown_filters,
    _get_tx_id,
    _is_returning,
    transactor_proxy,
)
from vgi.invocation import BindResponse, GlobalInitResponse
from vgi.table_function import BindParams, InitParams, ProcessParams, TableFunctionGenerator
from vgi.table_in_out_function import TableInOutGenerator

__all__ = [
    "GenericTableDelete",
    "GenericTableInsert",
    "GenericTableScan",
    "GenericTableUpdate",
]


def _get_table_name_from_bind(params: BindParams[None]) -> str:
    """Extract the table name from the first positional argument at bind time."""
    args = params.bind_call.arguments
    if not args.positional or args.positional[0] is None:
        msg = "table_name positional argument is required"
        raise ValueError(msg)
    return str(args.positional[0].as_py())


def _get_table_name_from_process(params: ProcessParams[None]) -> str:
    """Extract the table name from the first positional argument at process time."""
    assert params.init_call is not None
    args = params.init_call.bind_call.arguments
    if not args.positional or args.positional[0] is None:
        msg = "table_name positional argument is required"
        raise ValueError(msg)
    return str(args.positional[0].as_py())


def _get_table_schema_from_transactor(table_name: str, attach_opaque_data: bytes, tx_id: bytes) -> pa.Schema:
    """Query the transactor for the table's Arrow schema (returned as IPC bytes)."""
    proxy = transactor_proxy._get_proxy()
    schema_bytes = proxy.table_schema(attach_opaque_data=attach_opaque_data, table_name=table_name, tx_id=tx_id)
    return pa.ipc.read_schema(pa.BufferReader(schema_bytes))  # type: ignore[arg-type]


# ============================================================================
# Generic scan — dynamic table name + schema from transactor
# ============================================================================


class GenericTableScan(TableFunctionGenerator[None, WritableScanState]):
    """Scan function for any table — determines table name from first positional arg."""

    class Meta:
        """Metadata for GenericTableScan."""

        name = "generic_writable_scan"
        projection_pushdown = True
        filter_pushdown = True

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """Bind: query transactor for table schema (already includes rowid)."""
        table_name = _get_table_name_from_bind(params)
        attach_opaque_data = params.bind_call.attach_opaque_data
        tx_id = params.bind_call.transaction_opaque_data
        assert attach_opaque_data is not None and tx_id is not None
        table_schema = _get_table_schema_from_transactor(table_name, attach_opaque_data, tx_id)
        return BindResponse(output_schema=table_schema)

    @classmethod
    def on_init(cls, params: InitParams[None]) -> GlobalInitResponse:
        """Limit to a single worker."""
        return GlobalInitResponse(max_workers=1)

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> WritableScanState:
        """Open the transactor scan stream once before processing begins."""
        table_name = _get_table_name_from_process(params)
        attach_opaque_data = _get_attach_opaque_data(params)
        tx_id = _get_tx_id(params)
        proxy = transactor_proxy._get_proxy()
        columns = list(params.output_schema.names)
        scan_iter = iter(
            proxy.scan(
                attach_opaque_data=attach_opaque_data,
                tx_id=tx_id,
                schema_name="",
                table_name=table_name,
                columns=columns,
                pushdown_filters=_get_pushdown_filters(params),
            )
        )
        return WritableScanState(scan_iter=scan_iter)

    @classmethod
    def process(cls, params: ProcessParams[None], state: WritableScanState, out: OutputCollector) -> None:
        """Read the next batch from the scan stream."""
        assert state.scan_iter is not None
        try:
            batch = next(state.scan_iter)
            out.emit(batch.batch)
        except StopIteration:
            out.finish()


# ============================================================================
# Generic write base — shared INSERT/UPDATE/DELETE logic
# ============================================================================


class _GenericWriteBase(TableInOutGenerator[None, None]):
    """Base for generic write handlers. Subclasses set _operation."""

    _operation: str  # "insert" | "update" | "delete"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """Bind: query transactor for table schema to use for RETURNING."""
        table_name = _get_table_name_from_bind(params)
        if _is_returning(params):
            attach_opaque_data = params.bind_call.attach_opaque_data
            tx_id = params.bind_call.transaction_opaque_data
            assert attach_opaque_data is not None and tx_id is not None
            table_schema = _get_table_schema_from_transactor(table_name, attach_opaque_data, tx_id)
            user_fields = [f for f in table_schema if f.name not in ("rowid", "row_id")]
            return BindResponse(output_schema=pa.schema(user_fields))
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def _open_stream(
        cls,
        proxy: Any,
        attach_opaque_data: bytes,
        tx_id: bytes,
        table_name: str,
        returning: bool,
        batch: pa.RecordBatch,
    ) -> Any:
        """Open a write stream. Override for operations needing extra args."""
        return getattr(proxy, cls._operation)(
            attach_opaque_data=attach_opaque_data,
            tx_id=tx_id,
            table_name=table_name,
            returning=returning,
        )

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        """Forward batch to transactor write stream."""
        table_name = _get_table_name_from_process(params)
        attach_opaque_data = _get_attach_opaque_data(params)
        tx_id = _get_tx_id(params)
        returning = params.output_schema != _COUNT_SCHEMA
        proxy = transactor_proxy._get_proxy()
        with cls._open_stream(proxy, attach_opaque_data, tx_id, table_name, returning, batch) as stream:
            response = stream.exchange(AnnotatedBatch(batch=batch))
            out.emit(response.batch)


class GenericTableInsert(_GenericWriteBase):
    """INSERT handler for any table — determines table name from first positional arg."""

    _operation = "insert"

    class Meta:
        """Metadata for GenericTableInsert."""

        name = "generic_writable_insert"


class GenericTableUpdate(_GenericWriteBase):
    """UPDATE handler for any table — determines table name from first positional arg."""

    _operation = "update"

    class Meta:
        """Metadata for GenericTableUpdate."""

        name = "generic_writable_update"

    @classmethod
    def _open_stream(
        cls,
        proxy: Any,
        attach_opaque_data: bytes,
        tx_id: bytes,
        table_name: str,
        returning: bool,
        batch: pa.RecordBatch,
    ) -> Any:
        """Open an update stream with column list derived from the batch."""
        update_cols = [name for name in batch.schema.names if name != "rowid"]
        return proxy.update(
            attach_opaque_data=attach_opaque_data,
            tx_id=tx_id,
            table_name=table_name,
            columns=update_cols,
            returning=returning,
        )


class GenericTableDelete(_GenericWriteBase):
    """DELETE handler for any table — determines table name from first positional arg."""

    _operation = "delete"

    class Meta:
        """Metadata for GenericTableDelete."""

        name = "generic_writable_delete"
