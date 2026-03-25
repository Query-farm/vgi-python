"""Example writable tables with INSERT, UPDATE, DELETE, and RETURNING support.

Demonstrates how to implement write operations using TableInOutGenerator functions
that proxy Arrow batches through a **db-transactor** subprocess. The transactor
owns the single DuckDB connection and provides transactional access to multiple
VGI worker processes.

Two tables are provided:

- **writable_data** — simple two-column table (id, name)
- **writable_products** — table with defaults, constraints, and server-side
  modification of RETURNING values (auto-assigned product_id and timestamp)

Usage in a Table descriptor::

    Table(
        name="writable_data",
        function=WritableTableScan,
        insert_function=WritableTableInsert,
        update_function=WritableTableUpdate,
        delete_function=WritableTableDelete,
    )
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, ClassVar

import pyarrow as pa
from vgi_rpc import AnnotatedBatch, ArrowSerializableDataclass, Transient
from vgi_rpc.rpc import OutputCollector

from vgi.invocation import BindResponse, GlobalInitResponse
from vgi.protocol import BindRequest
from vgi.schema_utils import schema
from vgi.table_function import BindParams, InitParams, ProcessParams, TableFunctionGenerator
from vgi.table_in_out_function import TableInOutGenerator
from vgi.transactor.client import TransactorClient
from vgi.transactor.protocol import TransactorProtocol

__all__ = [
    "TransactorProxy",
    "WritableOrdersDelete",
    "WritableOrdersInsert",
    "WritableOrdersScan",
    "WritableOrdersUpdate",
    "WritableProductsDelete",
    "WritableProductsInsert",
    "WritableProductsScan",
    "WritableProductsUpdate",
    "WritableTableDelete",
    "WritableTableInsert",
    "WritableTableScan",
    "WritableTableUpdate",
]

# Output schema for write functions returning affected row counts.
_COUNT_SCHEMA = schema(count=pa.int64())

# DuckDB's native rowid pseudocolumn, marked with is_row_id metadata so the
# C++ extension knows which column carries the physical row identifier.
_ROWID_FIELD = pa.field("rowid", pa.int64(), metadata={b"is_row_id": b""})


def _scan_schema(table_schema: pa.Schema) -> pa.Schema:
    """Build a scan schema by prepending the rowid field to a table schema."""
    return pa.schema([_ROWID_FIELD, *table_schema])


def _parse_write_options(bind_call: BindRequest) -> dict[str, bool | str | list[str]]:
    """Parse the write_options RecordBatch from the bind call's named arguments."""
    defaults: dict[str, bool | str | list[str]] = {
        "return_chunks": False,
        "on_conflict": "throw",
        "on_conflict_columns": [],
    }
    if not (bind_call.arguments and bind_call.arguments.named):
        return defaults
    val = bind_call.arguments.named.get("write_options")
    if val is None:
        return defaults
    from vgi_rpc.utils import deserialize_record_batch

    options_bytes = val.as_py()
    batch, _ = deserialize_record_batch(options_bytes)
    result = dict(defaults)
    if "return_chunks" in batch.schema.names:
        result["return_chunks"] = batch.column("return_chunks")[0].as_py()
    if "on_conflict" in batch.schema.names:
        result["on_conflict"] = batch.column("on_conflict")[0].as_py()
    if "on_conflict_columns" in batch.schema.names:
        result["on_conflict_columns"] = batch.column("on_conflict_columns")[0].as_py()
    return result


def _get_write_options_from_bind(params: BindParams[None]) -> dict[str, bool | str | list[str]]:
    """Get write options from BindParams."""
    return _parse_write_options(params.bind_call)


def _get_write_options_from_process(params: ProcessParams[None]) -> dict[str, bool | str | list[str]]:
    """Get write options from ProcessParams."""
    return _parse_write_options(params.init_call.bind_call)


def _is_returning(params: BindParams[None]) -> bool:
    """Check if the C++ operator requested RETURNING rows."""
    opts = _get_write_options_from_bind(params)
    return bool(opts.get("return_chunks", False))


def _get_tx_id(params: ProcessParams[None]) -> bytes:
    """Get transaction_id from the bind request.

    The C++ extension threads the transaction_id from VgiTransaction through the
    BindRequest protocol. It arrives in params.init_call.bind_call.transaction_id.
    """
    tx_id = params.init_call.bind_call.transaction_id
    if tx_id:
        return tx_id
    msg = "transaction_id is required but was not provided in the bind request"
    raise ValueError(msg)


# ============================================================================
# TransactorProxy — manages the db-transactor connection
# ============================================================================

_DEFAULT_DB_PATH = Path("~/.local/state/vgi/writable_store.duckdb")


def _get_db_path() -> Path:
    """Get the database path from env var or default.

    Set VGI_WRITABLE_STORE to a unique temp path per test run to ensure
    each test suite starts with a fresh database.
    """
    env_path = os.environ.get("VGI_WRITABLE_STORE")
    return Path(env_path) if env_path else _DEFAULT_DB_PATH.expanduser()


class TransactorProxy:
    """Manages connections to the db-transactor subprocess.

    Each worker process gets its own transactor with a unique database,
    ensuring no state leaks between test runs or independent workers.
    """

    def __init__(self, db_path: Path | None = None, ddl_statements: list[str] | None = None) -> None:
        """Initialize the proxy."""
        self._db_path = db_path or _get_db_path()
        self._ddl = ddl_statements or []
        self._client: TransactorClient | None = None
        self._tables_created = False

    def _get_proxy(self) -> TransactorProtocol:
        """Get the transactor RPC proxy (auto-spawn if needed)."""
        if self._client is None:
            self._client = TransactorClient(str(self._db_path))
        proxy: TransactorProtocol = self._client.get_proxy()
        if not self._tables_created:
            for ddl in self._ddl:
                proxy.execute_ddl(sql=ddl)
            self._tables_created = True
        return proxy

    def close(self) -> None:
        """Close the transactor connection."""
        if self._client is not None:
            self._client.close()
            self._client = None


# Module-level proxy — DDL is sent to the transactor on first use.
_WRITABLE_DATA_DDL = [
    ("CREATE TABLE IF NOT EXISTS writable_data (\n  id BIGINT NOT NULL,\n  name VARCHAR NOT NULL\n)"),
]

_WRITABLE_PRODUCTS_DDL = [
    (
        "CREATE TABLE IF NOT EXISTS writable_products (\n"
        "  product_id BIGINT NOT NULL UNIQUE,\n"
        "  name VARCHAR NOT NULL,\n"
        "  price DOUBLE NOT NULL DEFAULT 0.0 CHECK(price >= 0),\n"
        "  status VARCHAR NOT NULL DEFAULT 'draft',\n"
        "  created_at VARCHAR NOT NULL DEFAULT 'server-assigned'\n"
        ")"
    ),
]

_WRITABLE_ORDERS_DDL = [
    (
        "CREATE TABLE IF NOT EXISTS writable_orders (\n"
        "  order_id BIGINT NOT NULL UNIQUE,\n"
        "  product_id BIGINT NOT NULL REFERENCES writable_products(product_id),\n"
        "  quantity BIGINT NOT NULL DEFAULT 1\n"
        ")"
    ),
]

transactor_proxy = TransactorProxy(ddl_statements=_WRITABLE_DATA_DDL + _WRITABLE_PRODUCTS_DDL + _WRITABLE_ORDERS_DDL)


# ============================================================================
# writable_data — simple two-column table
# ============================================================================

_TABLE_SCHEMA = schema(id=pa.int64(), name=pa.string())
_SCAN_SCHEMA = _scan_schema(_TABLE_SCHEMA)


def _get_pushdown_filters(params: ProcessParams[None]) -> bytes | None:
    """Get pushdown_filters as serialized IPC bytes from params (or None)."""
    pf_batch = params.init_call.pushdown_filters
    if pf_batch is None:
        return None
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, pf_batch.schema)
    writer.write_batch(pf_batch)
    writer.close()
    return sink.getvalue().to_pybytes()


@dataclass(kw_only=True)
class WritableScanState(ArrowSerializableDataclass):
    """State for writable table scans — holds the live transactor scan iterator."""

    scan_iter: Annotated[Iterator[AnnotatedBatch] | None, Transient()] = None


class _WritableScanBase(TableFunctionGenerator[None, WritableScanState]):
    """Base class for writable table scans — shared init/process logic."""

    _table_name: ClassVar[str]
    _scan_schema: ClassVar[pa.Schema]

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """Bind: return scan schema with row_id column."""
        return BindResponse(output_schema=cls._scan_schema)

    @classmethod
    def on_init(cls, params: InitParams[None]) -> GlobalInitResponse:
        """Limit to a single worker."""
        return GlobalInitResponse(max_workers=1)

    @classmethod
    def initial_state(cls, params: ProcessParams[None]) -> WritableScanState:
        """Open the transactor scan stream once before processing begins."""
        tx_id = _get_tx_id(params)
        proxy = transactor_proxy._get_proxy()
        columns = list(params.output_schema.names)
        scan_iter = iter(
            proxy.scan(
                tx_id=tx_id,
                schema_name="",
                table_name=cls._table_name,
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


class WritableTableScan(_WritableScanBase):
    """Scan function for writable_data — proxies to transactor scan stream."""

    _table_name = "writable_data"
    _scan_schema = _SCAN_SCHEMA

    class Meta:
        """Metadata for WritableTableScan."""

        name = "writable_table_scan"
        projection_pushdown = True
        filter_pushdown = True


# ============================================================================
# Write operation base classes — shared INSERT/UPDATE/DELETE logic
# ============================================================================


class _WritableInsertBase(TableInOutGenerator[None, None]):
    """Base class for INSERT handlers — subclasses set _table_name and _table_schema."""

    _table_name: ClassVar[str]
    _table_schema: ClassVar[pa.Schema]

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """Bind: return table schema for RETURNING, count schema otherwise."""
        if _is_returning(params):
            return BindResponse(output_schema=cls._table_schema)
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        """Forward batch to transactor insert stream."""
        tx_id = _get_tx_id(params)
        returning = params.output_schema != _COUNT_SCHEMA
        proxy = transactor_proxy._get_proxy()
        with proxy.insert(tx_id=tx_id, table_name=cls._table_name, returning=returning) as stream:
            response = stream.exchange(AnnotatedBatch(batch=batch))
            out.emit(response.batch)


class _WritableUpdateBase(TableInOutGenerator[None, None]):
    """Base class for UPDATE handlers — subclasses set _table_name and _table_schema."""

    _table_name: ClassVar[str]
    _table_schema: ClassVar[pa.Schema]

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """Bind: return table schema for RETURNING, count schema otherwise."""
        if _is_returning(params):
            return BindResponse(output_schema=cls._table_schema)
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        """Forward batch to transactor update stream."""
        tx_id = _get_tx_id(params)
        returning = params.output_schema != _COUNT_SCHEMA
        update_cols = [name for name in batch.schema.names if name != "rowid"]
        proxy = transactor_proxy._get_proxy()
        with proxy.update(tx_id=tx_id, table_name=cls._table_name, columns=update_cols, returning=returning) as stream:
            response = stream.exchange(AnnotatedBatch(batch=batch))
            out.emit(response.batch)


class _WritableDeleteBase(TableInOutGenerator[None, None]):
    """Base class for DELETE handlers — subclasses set _table_name and _table_schema."""

    _table_name: ClassVar[str]
    _table_schema: ClassVar[pa.Schema]

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """Bind: return table schema for RETURNING, count schema otherwise."""
        if _is_returning(params):
            return BindResponse(output_schema=cls._table_schema)
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, batch: pa.RecordBatch, out: OutputCollector) -> None:
        """Forward batch to transactor delete stream."""
        tx_id = _get_tx_id(params)
        returning = params.output_schema != _COUNT_SCHEMA
        proxy = transactor_proxy._get_proxy()
        with proxy.delete(tx_id=tx_id, table_name=cls._table_name, returning=returning) as stream:
            response = stream.exchange(AnnotatedBatch(batch=batch))
            out.emit(response.batch)


# ============================================================================
# writable_data — concrete classes
# ============================================================================


class WritableTableInsert(_WritableInsertBase):
    """INSERT handler."""

    _table_name = "writable_data"
    _table_schema = _TABLE_SCHEMA

    class Meta:
        """Metadata."""

        name = "writable_table_insert"


class WritableTableUpdate(_WritableUpdateBase):
    """UPDATE handler."""

    _table_name = "writable_data"
    _table_schema = _TABLE_SCHEMA

    class Meta:
        """Metadata."""

        name = "writable_table_update"


class WritableTableDelete(_WritableDeleteBase):
    """DELETE handler."""

    _table_name = "writable_data"
    _table_schema = _TABLE_SCHEMA

    class Meta:
        """Metadata."""

        name = "writable_table_delete"


# ============================================================================
# writable_products — table with defaults, constraints, server-side modification
# ============================================================================

_PRODUCTS_TABLE_SCHEMA = schema(
    product_id=pa.int64(),
    name=pa.string(),
    price=pa.float64(),
    status=pa.string(),
    created_at=pa.string(),
)
_PRODUCTS_SCAN_SCHEMA = _scan_schema(_PRODUCTS_TABLE_SCHEMA)


class WritableProductsScan(_WritableScanBase):
    """Scan handler."""

    _table_name = "writable_products"
    _scan_schema = _PRODUCTS_SCAN_SCHEMA

    class Meta:
        """Metadata."""

        name = "writable_products_scan"
        projection_pushdown = True
        filter_pushdown = True


class WritableProductsInsert(_WritableInsertBase):
    """INSERT handler."""

    _table_name = "writable_products"
    _table_schema = _PRODUCTS_TABLE_SCHEMA

    class Meta:
        """Metadata."""

        name = "writable_products_insert"


class WritableProductsUpdate(_WritableUpdateBase):
    """UPDATE handler."""

    _table_name = "writable_products"
    _table_schema = _PRODUCTS_TABLE_SCHEMA

    class Meta:
        """Metadata."""

        name = "writable_products_update"


class WritableProductsDelete(_WritableDeleteBase):
    """DELETE handler."""

    _table_name = "writable_products"
    _table_schema = _PRODUCTS_TABLE_SCHEMA

    class Meta:
        """Metadata."""

        name = "writable_products_delete"


# ============================================================================
# writable_orders — table with foreign key to writable_products
# ============================================================================

_ORDERS_TABLE_SCHEMA = schema(
    order_id=pa.int64(),
    product_id=pa.int64(),
    quantity=pa.int64(),
)
_ORDERS_SCAN_SCHEMA = _scan_schema(_ORDERS_TABLE_SCHEMA)


class WritableOrdersScan(_WritableScanBase):
    """Scan handler."""

    _table_name = "writable_orders"
    _scan_schema = _ORDERS_SCAN_SCHEMA

    class Meta:
        """Metadata."""

        name = "writable_orders_scan"
        projection_pushdown = True
        filter_pushdown = True


class WritableOrdersInsert(_WritableInsertBase):
    """INSERT handler."""

    _table_name = "writable_orders"
    _table_schema = _ORDERS_TABLE_SCHEMA

    class Meta:
        """Metadata."""

        name = "writable_orders_insert"


class WritableOrdersUpdate(_WritableUpdateBase):
    """UPDATE handler."""

    _table_name = "writable_orders"
    _table_schema = _ORDERS_TABLE_SCHEMA

    class Meta:
        """Metadata."""

        name = "writable_orders_update"


class WritableOrdersDelete(_WritableDeleteBase):
    """DELETE handler."""

    _table_name = "writable_orders"
    _table_schema = _ORDERS_TABLE_SCHEMA

    class Meta:
        """Metadata."""

        name = "writable_orders_delete"
