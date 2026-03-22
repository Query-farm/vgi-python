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
from pathlib import Path
from typing import Any

import pyarrow as pa
from vgi_rpc import AnnotatedBatch

from vgi.invocation import BindResponse, GlobalInitResponse
from vgi.table_function import TableFunctionGenerator
from vgi.table_in_out_function import TableInOutGenerator
from vgi.transactor.client import TransactorClient

__all__ = [
    "TransactorProxy",
    "WritableProductsInsert",
    "WritableProductsScan",
    "WritableTableDelete",
    "WritableTableInsert",
    "WritableTableScan",
    "WritableTableUpdate",
]

# Output schema for write functions returning affected row counts.
_COUNT_SCHEMA = pa.schema([("count", pa.int64())])


def _parse_write_options(bind_call: Any) -> dict[str, Any]:
    """Parse the write_options RecordBatch from the bind call's named arguments."""
    defaults: dict[str, Any] = {
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


def _get_write_options(params: Any) -> dict[str, Any]:
    """Get write options from either BindParams or ProcessParams."""
    if hasattr(params, "bind_call"):
        return _parse_write_options(params.bind_call)
    if hasattr(params, "init_call") and hasattr(params.init_call, "bind_call"):
        return _parse_write_options(params.init_call.bind_call)
    return {"return_chunks": False, "on_conflict": "throw", "on_conflict_columns": []}


def _is_returning(params: Any) -> bool:
    """Check if the C++ operator requested RETURNING rows."""
    return _get_write_options(params).get("return_chunks", False)  # type: ignore[no-any-return]


# ============================================================================
# TransactorProxy — manages the db-transactor connection
# ============================================================================

_DEFAULT_DB_PATH = Path("~/.local/state/vgi/writable_store.duckdb")


def _get_db_path() -> Path:
    """Get the database path from env var or default."""
    env_path = os.environ.get("VGI_WRITABLE_STORE")
    return Path(env_path) if env_path else _DEFAULT_DB_PATH.expanduser()


class TransactorProxy:
    """Manages connections to the db-transactor subprocess.

    Lazily spawns the transactor on first use. Provides methods to open
    exchange streams (insert/delete/update) and producer streams (scan)
    that proxy Arrow batches to the transactor's DuckDB connection.
    """

    def __init__(self, db_path: Path | None = None, ddl_statements: list[str] | None = None) -> None:
        """Initialize the proxy."""
        self._db_path = db_path or _get_db_path()
        self._ddl = ddl_statements or []
        self._client: TransactorClient | None = None
        self._tables_created = False

    def _get_proxy(self) -> Any:
        """Get the transactor RPC proxy (auto-spawn if needed)."""
        if self._client is None:
            self._client = TransactorClient(str(self._db_path))
        proxy = self._client.get_proxy()
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
    "CREATE SEQUENCE IF NOT EXISTS writable_data_seq START 1",
    (
        "CREATE TABLE IF NOT EXISTS writable_data ("
        "  row_id BIGINT DEFAULT nextval('writable_data_seq') PRIMARY KEY,"
        "  id BIGINT NOT NULL,"
        "  name VARCHAR NOT NULL"
        ")"
    ),
]

_WRITABLE_PRODUCTS_DDL = [
    "CREATE SEQUENCE IF NOT EXISTS writable_products_seq START 1",
    (
        "CREATE TABLE IF NOT EXISTS writable_products ("
        "  row_id BIGINT DEFAULT nextval('writable_products_seq') PRIMARY KEY,"
        "  product_id BIGINT NOT NULL UNIQUE,"
        "  name VARCHAR NOT NULL,"
        "  price DOUBLE NOT NULL DEFAULT 0.0 CHECK(price >= 0),"
        "  status VARCHAR NOT NULL DEFAULT 'draft',"
        "  created_at VARCHAR NOT NULL DEFAULT 'server-assigned'"
        ")"
    ),
]

transactor_proxy = TransactorProxy(ddl_statements=_WRITABLE_DATA_DDL + _WRITABLE_PRODUCTS_DDL)


# ============================================================================
# writable_data — simple two-column table
# ============================================================================

_TABLE_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
    ]
)

_SCAN_SCHEMA = pa.schema(
    [  # type: ignore[arg-type]  # pyarrow stubs: mixed-type fields
        pa.field("row_id", pa.int64(), metadata={b"is_row_id": b""}),
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
    ]
)


class WritableTableScan(TableFunctionGenerator[None, None]):
    """Scan function — proxies to transactor scan stream."""

    class Meta:
        """Metadata for WritableTableScan."""

        name = "writable_table_scan"
        projection_pushdown = True

    @classmethod
    def on_bind(cls, params):  # type: ignore[override]
        """Bind: return scan schema with row_id column."""
        return BindResponse(output_schema=_SCAN_SCHEMA)

    @classmethod
    def on_init(cls, params):  # type: ignore[override]
        """Limit to a single worker."""
        return GlobalInitResponse(max_workers=1)

    @classmethod
    def process(cls, params, state, out):  # type: ignore[override]
        """Proxy scan through the transactor."""
        proxy = transactor_proxy._get_proxy()
        columns = list(params.output_schema.names)
        for batch in proxy.scan(tx_id=b"\x00", schema_name="", table_name="writable_data", columns=columns):
            out.emit(batch.batch)
        out.finish()


class WritableTableInsert(TableInOutGenerator[None, None]):
    """INSERT handler — proxies batches to transactor insert stream."""

    class Meta:
        """Metadata for WritableTableInsert."""

        name = "writable_table_insert"

    @classmethod
    def on_bind(cls, params):  # type: ignore[override]
        """Bind: return table schema for RETURNING, count schema otherwise."""
        if _is_returning(params):
            return BindResponse(output_schema=_TABLE_SCHEMA)
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(cls, params, state, batch, out):  # type: ignore[override]
        """Forward batch to transactor insert stream."""
        returning = params.output_schema != _COUNT_SCHEMA
        proxy = transactor_proxy._get_proxy()
        with proxy.insert(tx_id=b"\x00", schema_name="", table_name="writable_data", returning=returning) as stream:
            response = stream.exchange(AnnotatedBatch(batch=batch))
            out.emit(response.batch)


class WritableTableUpdate(TableInOutGenerator[None, None]):
    """UPDATE handler — proxies batches to transactor update stream."""

    class Meta:
        """Metadata for WritableTableUpdate."""

        name = "writable_table_update"

    @classmethod
    def on_bind(cls, params):  # type: ignore[override]
        """Bind: return count output schema."""
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(cls, params, state, batch, out):  # type: ignore[override]
        """Forward batch to transactor update stream."""
        proxy = transactor_proxy._get_proxy()
        with proxy.update(tx_id=b"\x00", schema_name="", table_name="writable_data") as stream:
            response = stream.exchange(AnnotatedBatch(batch=batch))
            out.emit(response.batch)


class WritableTableDelete(TableInOutGenerator[None, None]):
    """DELETE handler — proxies batches to transactor delete stream."""

    class Meta:
        """Metadata for WritableTableDelete."""

        name = "writable_table_delete"

    @classmethod
    def on_bind(cls, params):  # type: ignore[override]
        """Bind: return count output schema."""
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(cls, params, state, batch, out):  # type: ignore[override]
        """Forward batch to transactor delete stream."""
        proxy = transactor_proxy._get_proxy()
        with proxy.delete(tx_id=b"\x00", schema_name="", table_name="writable_data") as stream:
            response = stream.exchange(AnnotatedBatch(batch=batch))
            out.emit(response.batch)


# ============================================================================
# writable_products — table with defaults, constraints, server-side modification
# ============================================================================

_PRODUCTS_TABLE_SCHEMA = pa.schema(
    [
        pa.field("product_id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("status", pa.string()),
        pa.field("created_at", pa.string()),
    ]
)

_PRODUCTS_SCAN_SCHEMA = pa.schema(
    [  # type: ignore[arg-type]
        pa.field("row_id", pa.int64(), metadata={b"is_row_id": b""}),
        pa.field("product_id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("status", pa.string()),
        pa.field("created_at", pa.string()),
    ]
)


class WritableProductsScan(TableFunctionGenerator[None, None]):
    """Scan function — proxies to transactor scan stream."""

    class Meta:
        """Metadata for WritableProductsScan."""

        name = "writable_products_scan"
        projection_pushdown = True

    @classmethod
    def on_bind(cls, params):  # type: ignore[override]
        """Bind: return products scan schema with row_id."""
        return BindResponse(output_schema=_PRODUCTS_SCAN_SCHEMA)

    @classmethod
    def on_init(cls, params):  # type: ignore[override]
        """Limit to single worker."""
        return GlobalInitResponse(max_workers=1)

    @classmethod
    def process(cls, params, state, out):  # type: ignore[override]
        """Proxy scan through the transactor."""
        proxy = transactor_proxy._get_proxy()
        columns = list(params.output_schema.names)
        for batch in proxy.scan(tx_id=b"\x00", schema_name="", table_name="writable_products", columns=columns):
            out.emit(batch.batch)
        out.finish()


class WritableProductsInsert(TableInOutGenerator[None, None]):
    """INSERT handler for writable_products — proxies to transactor.

    Server-side modification (auto product_id, timestamp) is handled by
    DuckDB DEFAULT expressions in the transactor's schema. The RETURNING
    response from the transactor includes the server-assigned values.
    """

    class Meta:
        """Metadata for WritableProductsInsert."""

        name = "writable_products_insert"

    @classmethod
    def on_bind(cls, params):  # type: ignore[override]
        """Bind: return products schema for RETURNING, count schema otherwise."""
        opts = _get_write_options(params)
        if opts["return_chunks"]:
            return BindResponse(output_schema=_PRODUCTS_TABLE_SCHEMA)
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(cls, params, state, batch, out):  # type: ignore[override]
        """Forward batch to transactor insert stream."""
        returning = params.output_schema != _COUNT_SCHEMA
        proxy = transactor_proxy._get_proxy()
        with proxy.insert(tx_id=b"\x00", schema_name="", table_name="writable_products", returning=returning) as stream:
            response = stream.exchange(AnnotatedBatch(batch=batch))
            out.emit(response.batch)
