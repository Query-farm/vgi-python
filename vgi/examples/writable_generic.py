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

import pyarrow as pa
from vgi_rpc import AnnotatedBatch
from vgi_rpc.rpc import OutputCollector

from vgi.invocation import BindResponse, GlobalInitResponse
from vgi.table_function import BindParams, InitParams, ProcessParams, TableFunctionGenerator
from vgi.table_in_out_function import TableInOutGenerator

from vgi.examples.writable_table import (
    WritableScanState,
    _COUNT_SCHEMA,
    _get_pushdown_filters,
    _get_tx_id,
    _is_returning,
    transactor_proxy,
)

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
    return args.positional[0].as_py()


def _get_table_name_from_process(params: ProcessParams[None]) -> str:
    """Extract the table name from the first positional argument at process time."""
    args = params.init_call.bind_call.arguments
    if not args.positional or args.positional[0] is None:
        msg = "table_name positional argument is required"
        raise ValueError(msg)
    return args.positional[0].as_py()


def _get_table_schema_from_transactor(table_name: str, tx_id: bytes) -> pa.Schema:
    """Query the transactor for the table's Arrow schema (returned as IPC bytes)."""
    proxy = transactor_proxy._get_proxy()
    schema_bytes = proxy.table_schema(table_name=table_name, tx_id=tx_id)
    return pa.ipc.read_schema(pa.BufferReader(schema_bytes))


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
        table_schema = _get_table_schema_from_transactor(table_name, params.bind_call.transaction_id)
        return BindResponse(output_schema=table_schema)

    @classmethod
    def on_init(cls, params: InitParams[None]) -> GlobalInitResponse:
        """Limit to a single worker."""
        return GlobalInitResponse(max_workers=1)

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> WritableScanState:
        """Open the transactor scan stream once before processing begins."""
        table_name = _get_table_name_from_process(params)
        tx_id = _get_tx_id(params)
        proxy = transactor_proxy._get_proxy()
        columns = list(params.output_schema.names)
        scan_iter = iter(
            proxy.scan(
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
# Generic insert — dynamic table name from first positional arg
# ============================================================================


class GenericTableInsert(TableInOutGenerator[None, None]):
    """INSERT handler for any table — determines table name from first positional arg."""

    class Meta:
        """Metadata for GenericTableInsert."""

        name = "generic_writable_insert"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """Bind: query transactor for table schema to use for RETURNING."""
        table_name = _get_table_name_from_bind(params)
        if _is_returning(params):
            table_schema = _get_table_schema_from_transactor(table_name, params.bind_call.transaction_id)
            # Exclude row_id from RETURNING schema (it's auto-generated)
            user_fields = [f for f in table_schema if f.name not in ("rowid", "row_id")]
            return BindResponse(output_schema=pa.schema(user_fields))
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        """Forward batch to transactor insert stream."""
        table_name = _get_table_name_from_process(params)
        tx_id = _get_tx_id(params)
        returning = params.output_schema != _COUNT_SCHEMA
        proxy = transactor_proxy._get_proxy()
        with proxy.insert(tx_id=tx_id, table_name=table_name, returning=returning) as stream:
            response = stream.exchange(AnnotatedBatch(batch=batch))
            out.emit(response.batch)


# ============================================================================
# Generic update — dynamic table name from first positional arg
# ============================================================================


class GenericTableUpdate(TableInOutGenerator[None, None]):
    """UPDATE handler for any table — determines table name from first positional arg."""

    class Meta:
        """Metadata for GenericTableUpdate."""

        name = "generic_writable_update"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """Bind: query transactor for table schema to use for RETURNING."""
        table_name = _get_table_name_from_bind(params)
        if _is_returning(params):
            table_schema = _get_table_schema_from_transactor(table_name, params.bind_call.transaction_id)
            user_fields = [f for f in table_schema if f.name not in ("rowid", "row_id")]
            return BindResponse(output_schema=pa.schema(user_fields))
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        """Forward batch to transactor update stream."""
        table_name = _get_table_name_from_process(params)
        tx_id = _get_tx_id(params)
        returning = params.output_schema != _COUNT_SCHEMA
        update_cols = [name for name in batch.schema.names if name != "rowid"]
        proxy = transactor_proxy._get_proxy()
        with proxy.update(tx_id=tx_id, table_name=table_name, columns=update_cols, returning=returning) as stream:
            response = stream.exchange(AnnotatedBatch(batch=batch))
            out.emit(response.batch)


# ============================================================================
# Generic delete — dynamic table name from first positional arg
# ============================================================================


class GenericTableDelete(TableInOutGenerator[None, None]):
    """DELETE handler for any table — determines table name from first positional arg."""

    class Meta:
        """Metadata for GenericTableDelete."""

        name = "generic_writable_delete"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """Bind: query transactor for table schema to use for RETURNING."""
        table_name = _get_table_name_from_bind(params)
        if _is_returning(params):
            table_schema = _get_table_schema_from_transactor(table_name, params.bind_call.transaction_id)
            user_fields = [f for f in table_schema if f.name not in ("rowid", "row_id")]
            return BindResponse(output_schema=pa.schema(user_fields))
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        """Forward batch to transactor delete stream."""
        table_name = _get_table_name_from_process(params)
        tx_id = _get_tx_id(params)
        returning = params.output_schema != _COUNT_SCHEMA
        proxy = transactor_proxy._get_proxy()
        with proxy.delete(tx_id=tx_id, table_name=table_name, returning=returning) as stream:
            response = stream.exchange(AnnotatedBatch(batch=batch))
            out.emit(response.batch)
