"""Example writable tables with INSERT, UPDATE, DELETE, and RETURNING support.

Demonstrates how to implement write operations using TableInOutGenerator functions
backed by a shared DuckDB database. The scan functions emit a ``row_id`` column marked
with ``is_row_id`` metadata so that UPDATE and DELETE can identify target rows.

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
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from vgi.invocation import BindResponse, GlobalInitResponse
from vgi.table_function import TableFunctionGenerator
from vgi.table_in_out_function import TableInOutGenerator

__all__ = [
    "DuckDBStore",
    "ProductsStore",
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
    """Parse the write_options RecordBatch from the bind call's named arguments.

    Works with a BindRequest object. Returns a dict with keys: return_chunks (bool),
    on_conflict (str), on_conflict_columns (list[str]). Missing keys get defaults.
    """
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
# DuckDBStore — shared base for DuckDB-backed stores
# ============================================================================


class DuckDBStore:
    """Base class for DuckDB-backed stores with lazy initialization.

    Connection is created lazily on first use and reused for the lifetime of the
    store instance. Subclasses override ``_create_tables`` to define their schema.

    The database path is determined by (in order):
    1. The ``path`` constructor argument
    2. The ``VGI_WRITABLE_STORE`` environment variable
    3. ``~/.local/state/vgi/writable_store.duckdb``
    """

    _DEFAULT_PATH = Path("~/.local/state/vgi/writable_store.duckdb")

    def __init__(self, path: Path | None = None) -> None:
        """Initialize the store. Database is created lazily on first access."""
        env_path = os.environ.get("VGI_WRITABLE_STORE")
        self._path = path or (Path(env_path) if env_path else self._DEFAULT_PATH.expanduser())
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: duckdb.DuckDBPyConnection | None = None

    def _get_conn(self) -> duckdb.DuckDBPyConnection:
        """Get or create the DuckDB connection."""
        if self._conn is None:
            self._conn = duckdb.connect(str(self._path))
            self._create_tables(self._conn)
        return self._conn

    def _create_tables(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Override to create tables. Called once on first connection."""
        raise NotImplementedError


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


class MemoryStore(DuckDBStore):
    """DuckDB-backed store for the writable_data table."""

    def _create_tables(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create the writable_data table."""
        conn.execute(
            "CREATE SEQUENCE IF NOT EXISTS writable_data_seq START 1;"
            "CREATE TABLE IF NOT EXISTS writable_data ("
            "  row_id BIGINT DEFAULT nextval('writable_data_seq') PRIMARY KEY,"
            "  id BIGINT NOT NULL,"
            "  name VARCHAR NOT NULL"
            ")"
        )

    def insert(self, rows: list[dict[str, Any]]) -> list[int]:
        """Insert rows and return their assigned row IDs."""
        conn = self._get_conn()
        ids: list[int] = []
        for row in rows:
            result = conn.execute(
                "INSERT INTO writable_data (id, name) VALUES ($1, $2) RETURNING row_id",
                [row["id"], row["name"]],
            ).fetchone()
            ids.append(result[0] if result else 0)
        return ids

    def update(self, row_id: int, values: dict[str, Any]) -> bool:
        """Update a row by row_id. Returns True if the row was found."""
        set_parts = []
        params: list[Any] = []
        for i, (col, val) in enumerate(values.items()):
            set_parts.append(f"{col} = ${i + 1}")
            params.append(val)
        params.append(row_id)
        conn = self._get_conn()
        result = conn.execute(
            f"UPDATE writable_data SET {', '.join(set_parts)} WHERE row_id = ${len(params)} RETURNING row_id",  # noqa: S608
            params,
        )
        return result.fetchone() is not None

    def delete(self, row_id: int) -> bool:
        """Delete a row by row_id. Returns True if the row was found."""
        conn = self._get_conn()
        result = conn.execute("DELETE FROM writable_data WHERE row_id = $1 RETURNING row_id", [row_id])
        return result.fetchone() is not None

    def scan(self) -> list[tuple[int, dict[str, Any]]]:
        """Return all (row_id, row_dict) pairs.

        Note: loads all rows into memory. Production code should use cursors
        and batch the output for large tables.
        """
        conn = self._get_conn()
        rows = conn.execute("SELECT row_id, id, name FROM writable_data").fetchall()
        return [(r[0], {"id": r[1], "name": r[2]}) for r in rows]

    def reset(self) -> None:
        """Clear all data."""
        conn = self._get_conn()
        conn.execute("DELETE FROM writable_data")


# Module-level store — database is created lazily on first access.
writable_store = MemoryStore()


class WritableTableScan(TableFunctionGenerator[None, None]):
    """Scan function for the writable table. Emits row_id + data columns."""

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
        """Limit to a single worker — DuckDB store is not partitioned."""
        return GlobalInitResponse(max_workers=1)

    @classmethod
    def process(cls, params, state, out):  # type: ignore[override]
        """Emit rows from the store, respecting projection pushdown."""
        rows = writable_store.scan()
        if rows:
            full_data: dict[str, list[Any]] = {
                "row_id": [r[0] for r in rows],
                "id": [r[1]["id"] for r in rows],
                "name": [r[1]["name"] for r in rows],
            }
            projected = {col: full_data[col] for col in params.output_schema.names}
            batch = pa.record_batch(projected, schema=params.output_schema)
            out.emit(batch)
        out.finish()


class WritableTableInsert(TableInOutGenerator[None, None]):
    """INSERT handler: receives rows with table columns, appends to store.

    Supports RETURNING: when ``return_chunks=true`` is passed via write_options,
    the function returns the inserted rows instead of counts.
    """

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
        """Insert all rows from the batch into the store."""
        rows = batch.to_pylist()
        writable_store.insert(rows)
        if params.output_schema != _COUNT_SCHEMA:
            out.emit(batch)
        else:
            out.emit(pa.record_batch({"count": [len(rows)]}, schema=_COUNT_SCHEMA))


class WritableTableUpdate(TableInOutGenerator[None, None]):
    """UPDATE handler: receives row_id + updated columns."""

    class Meta:
        """Metadata for WritableTableUpdate."""

        name = "writable_table_update"

    @classmethod
    def on_bind(cls, params):  # type: ignore[override]
        """Bind: return count output schema."""
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(cls, params, state, batch, out):  # type: ignore[override]
        """Update rows identified by row_id with new column values."""
        count = 0
        for i in range(batch.num_rows):
            row_id = batch.column("row_id")[i].as_py()
            values = {col: batch.column(col)[i].as_py() for col in batch.schema.names if col != "row_id"}
            if writable_store.update(row_id, values):
                count += 1
        out.emit(pa.record_batch({"count": [count]}, schema=_COUNT_SCHEMA))


class WritableTableDelete(TableInOutGenerator[None, None]):
    """DELETE handler: receives row_id column, removes matching rows."""

    class Meta:
        """Metadata for WritableTableDelete."""

        name = "writable_table_delete"

    @classmethod
    def on_bind(cls, params):  # type: ignore[override]
        """Bind: return count output schema."""
        return BindResponse(output_schema=_COUNT_SCHEMA)

    @classmethod
    def process(cls, params, state, batch, out):  # type: ignore[override]
        """Delete rows identified by row_id from the store."""
        count = 0
        for i in range(batch.num_rows):
            row_id = batch.column("row_id")[i].as_py()
            if writable_store.delete(row_id):
                count += 1
        out.emit(pa.record_batch({"count": [count]}, schema=_COUNT_SCHEMA))


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


class ProductsStore(DuckDBStore):
    """DuckDB-backed products store with server-side ID and timestamp assignment."""

    def _create_tables(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create the writable_products table with constraints."""
        conn.execute(
            "CREATE SEQUENCE IF NOT EXISTS writable_products_seq START 1;"
            "CREATE TABLE IF NOT EXISTS writable_products ("
            "  row_id BIGINT DEFAULT nextval('writable_products_seq') PRIMARY KEY,"
            "  product_id BIGINT NOT NULL UNIQUE,"
            "  name VARCHAR NOT NULL,"
            "  price DOUBLE NOT NULL DEFAULT 0.0 CHECK(price >= 0),"
            "  status VARCHAR NOT NULL DEFAULT 'draft',"
            "  created_at VARCHAR NOT NULL"
            ")"
        )

    def insert(
        self,
        rows: list[dict[str, Any]],
        on_conflict: str = "throw",
        on_conflict_columns: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Insert rows with server-side modification. Returns the actual inserted rows.

        Server-side behavior:
        - ``created_at`` is always overwritten with the server UTC timestamp
        - ``product_id`` of 0 is auto-assigned via a sequence
        - ``on_conflict="nothing"`` uses targeted ``ON CONFLICT(...) DO NOTHING``
        """
        result_rows: list[dict[str, Any]] = []
        now = datetime.now(tz=UTC).isoformat()
        conn = self._get_conn()

        # Build the INSERT SQL with optional ON CONFLICT clause
        base_sql = (
            "INSERT INTO writable_products (product_id, name, price, status, created_at) VALUES ($1, $2, $3, $4, $5)"
        )
        if on_conflict == "nothing":
            target_cols = ", ".join(on_conflict_columns or ["product_id"])
            base_sql += f" ON CONFLICT({target_cols}) DO NOTHING"

        # Use COALESCE(NULLIF($1, 0), nextval(...)) to auto-assign product_id when 0.
        # Use RETURNING to detect which rows were actually inserted
        # (ON CONFLICT DO NOTHING silently skips — RETURNING returns nothing for them).
        auto_id_sql = base_sql.replace(
            "$1",
            "COALESCE(NULLIF($1, 0), nextval('writable_products_seq'))",
        )
        returning_sql = auto_id_sql + " RETURNING product_id, name, price, status, created_at"

        for row in rows:
            created_at = now
            product_id = row.get("product_id", 0)

            inserted = conn.execute(
                returning_sql,
                [product_id, row["name"], row["price"], row["status"], created_at],
            ).fetchone()

            if inserted is None:
                # Row was skipped due to ON CONFLICT DO NOTHING
                continue

            result_rows.append(
                {
                    "product_id": inserted[0],
                    "name": inserted[1],
                    "price": inserted[2],
                    "status": inserted[3],
                    "created_at": inserted[4],
                }
            )
        return result_rows

    def scan(self) -> list[tuple[int, dict[str, Any]]]:
        """Return all (row_id, row_dict) pairs."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT row_id, product_id, name, price, status, created_at FROM writable_products"
        ).fetchall()
        return [
            (r[0], {"product_id": r[1], "name": r[2], "price": r[3], "status": r[4], "created_at": r[5]}) for r in rows
        ]


# Module-level store — database is created lazily on first access.
products_store = ProductsStore()


class WritableProductsScan(TableFunctionGenerator[None, None]):
    """Scan function for writable_products table."""

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
        """Emit rows from the products store, respecting projection pushdown."""
        rows = products_store.scan()
        if rows:
            full_data: dict[str, list[Any]] = {
                "row_id": [r[0] for r in rows],
                "product_id": [r[1]["product_id"] for r in rows],
                "name": [r[1]["name"] for r in rows],
                "price": [r[1]["price"] for r in rows],
                "status": [r[1]["status"] for r in rows],
                "created_at": [r[1]["created_at"] for r in rows],
            }
            projected = {col: full_data[col] for col in params.output_schema.names}
            batch = pa.record_batch(projected, schema=params.output_schema)
            out.emit(batch)
        out.finish()


class WritableProductsInsert(TableInOutGenerator[None, None]):
    """INSERT handler for writable_products with server-side modification.

    Server-side behavior:
    - ``created_at`` is always overwritten with the server timestamp
    - ``product_id`` of 0 is auto-assigned via a sequence
    - RETURNING reflects the server-modified values
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
        """Insert products with server-side modification."""
        opts = _get_write_options(params)
        rows = batch.to_pylist()
        actual_rows = products_store.insert(
            rows,
            on_conflict=opts["on_conflict"],
            on_conflict_columns=opts["on_conflict_columns"],
        )

        if params.output_schema != _COUNT_SCHEMA:
            # RETURNING: emit only the successfully inserted rows
            if actual_rows:
                out.emit(
                    pa.record_batch(
                        {
                            "product_id": [r["product_id"] for r in actual_rows],
                            "name": [r["name"] for r in actual_rows],
                            "price": [r["price"] for r in actual_rows],
                            "status": [r["status"] for r in actual_rows],
                            "created_at": [r["created_at"] for r in actual_rows],
                        },
                        schema=_PRODUCTS_TABLE_SCHEMA,
                    )
                )
            else:
                empty = {col: [] for col in _PRODUCTS_TABLE_SCHEMA.names}
                out.emit(pa.record_batch(empty, schema=_PRODUCTS_TABLE_SCHEMA))
        else:
            out.emit(pa.record_batch({"count": [len(actual_rows)]}, schema=_COUNT_SCHEMA))
