"""Writable worker with transactional INSERT, UPDATE, DELETE, and DDL support.

This worker exposes writable tables backed by a db-transactor subprocess.
It supports transactions — scan and write workers share the same DuckDB
transaction through the transactor.

DDL operations (CREATE TABLE, DROP TABLE, ALTER TABLE, etc.) are forwarded to
the transactor. Dynamically created tables are discovered via the transactor's
metadata methods and served through generic writable functions.

Usage::

    vgi-writable-worker

Tables:
    writable_data — simple two-column table (id, name)
    writable_products — table with defaults, constraints, server-side modification
    writable_orders — table with foreign key to writable_products
    (plus any dynamically created tables via CREATE TABLE)
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from typing import Any, Literal, overload

import pyarrow as pa

from vgi.catalog import (
    AttachId,
    Catalog,
    CatalogAttachResult,
    OnConflict,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    Schema,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    Sql,
    Table,
    TableInfo,
    TransactionId,
    FunctionInfo,
    MacroInfo,
    ViewInfo,
)
from vgi.examples.writable_generic import (
    GenericTableDelete,
    GenericTableInsert,
    GenericTableScan,
    GenericTableUpdate,
)
from vgi.examples.writable_table import (
    WritableOrdersDelete,
    WritableOrdersInsert,
    WritableOrdersScan,
    WritableOrdersUpdate,
    WritableProductsDelete,
    WritableProductsInsert,
    WritableProductsScan,
    WritableProductsUpdate,
    WritableTableDelete,
    WritableTableInsert,
    WritableTableScan,
    WritableTableUpdate,
    transactor_proxy,
)
from vgi.worker import Worker

logger = logging.getLogger("vgi.writable_worker")


def _qi(name: str) -> str:
    """Quote a SQL identifier with double quotes, escaping internal double quotes."""
    return '"' + name.replace('"', '""') + '"'


def _qn(schema_name: str, name: str) -> str:
    """Build a schema-qualified, quoted identifier."""
    return f'{_qi(schema_name)}.{_qi(name)}'


# ============================================================================
# Arrow type to DuckDB SQL type mapping
# ============================================================================


def _arrow_type_to_sql(arrow_type: pa.DataType) -> str:
    """Map a PyArrow type to a DuckDB SQL type string."""
    _SIMPLE_MAP: dict[pa.DataType, str] = {
        pa.int8(): "TINYINT",
        pa.int16(): "SMALLINT",
        pa.int32(): "INTEGER",
        pa.int64(): "BIGINT",
        pa.uint8(): "UTINYINT",
        pa.uint16(): "USMALLINT",
        pa.uint32(): "UINTEGER",
        pa.uint64(): "UBIGINT",
        pa.float16(): "FLOAT",
        pa.float32(): "FLOAT",
        pa.float64(): "DOUBLE",
        pa.string(): "VARCHAR",
        pa.utf8(): "VARCHAR",
        pa.large_utf8(): "VARCHAR",
        pa.binary(): "BLOB",
        pa.large_binary(): "BLOB",
        pa.bool_(): "BOOLEAN",
        pa.date32(): "DATE",
        pa.date64(): "DATE",
    }

    if arrow_type in _SIMPLE_MAP:
        return _SIMPLE_MAP[arrow_type]

    # Timestamp types
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMP"

    # Time types
    if pa.types.is_time(arrow_type):
        return "TIME"

    # Duration types
    if pa.types.is_duration(arrow_type):
        return "INTERVAL"

    # Decimal types
    if pa.types.is_decimal(arrow_type):
        return f"DECIMAL({arrow_type.precision}, {arrow_type.scale})"

    # List types
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        inner = _arrow_type_to_sql(arrow_type.value_type)
        return f"{inner}[]"

    # Struct types
    if pa.types.is_struct(arrow_type):
        fields = []
        for i in range(arrow_type.num_fields):
            f = arrow_type.field(i)
            fields.append(f"{f.name} {_arrow_type_to_sql(f.type)}")
        return "STRUCT(" + ", ".join(fields) + ")"

    # Map types
    if pa.types.is_map(arrow_type):
        key_type = _arrow_type_to_sql(arrow_type.key_type)
        item_type = _arrow_type_to_sql(arrow_type.item_type)
        return f"MAP({key_type}, {item_type})"

    # Fallback
    logger.warning("_arrow_type_to_sql: unmapped Arrow type %s, falling back to VARCHAR", arrow_type)
    return "VARCHAR"


# ============================================================================
# FK constraint deserialization
# ============================================================================


def _deserialize_fk(fk_bytes: bytes) -> dict[str, Any]:
    """Deserialize a foreign key constraint from IPC bytes.

    Returns a dict with keys: fk_columns, pk_columns, referenced_table, referenced_schema.
    """
    reader = pa.ipc.open_stream(fk_bytes)
    batch = reader.read_next_batch()
    return {
        "fk_columns": batch.column("fk_columns")[0].as_py(),
        "pk_columns": batch.column("pk_columns")[0].as_py(),
        "referenced_table": batch.column("referenced_table")[0].as_py(),
        "referenced_schema": batch.column("referenced_schema")[0].as_py(),
    }


# ============================================================================
# Static catalog definition
# ============================================================================


_WRITABLE_CATALOG = Catalog(
    name="writable",
    default_schema="main",
    schemas=[
        Schema(
            name="main",
            comment="Writable tables backed by db-transactor",
            functions=[
                # Scan functions registered here for projection_pushdown metadata
                WritableTableScan,
                WritableProductsScan,
                WritableOrdersScan,
                # Generic functions for DDL-created tables
                GenericTableScan,
                GenericTableInsert,
                GenericTableUpdate,
                GenericTableDelete,
            ],
            tables=[
                Table(
                    name="writable_data",
                    function=WritableTableScan,
                    insert_function=WritableTableInsert,
                    update_function=WritableTableUpdate,
                    delete_function=WritableTableDelete,
                    comment="Simple writable table (id, name)",
                ),
                Table(
                    name="writable_products",
                    function=WritableProductsScan,
                    insert_function=WritableProductsInsert,
                    update_function=WritableProductsUpdate,
                    delete_function=WritableProductsDelete,
                    primary_key=(("product_id",),),
                    not_null=("product_id", "name"),
                    check=("price >= 0",),
                    defaults={
                        "price": 0.0,
                        "status": "draft",
                        "created_at": Sql("'server-assigned'"),
                    },
                    comment="Writable products with defaults, constraints, server-side modification",
                ),
                Table(
                    name="writable_orders",
                    function=WritableOrdersScan,
                    insert_function=WritableOrdersInsert,
                    update_function=WritableOrdersUpdate,
                    delete_function=WritableOrdersDelete,
                    not_null=("order_id", "product_id"),
                    defaults={"quantity": 1},
                    comment="Writable orders with FK to writable_products",
                ),
            ],
        ),
    ],
)

# Set of static table names (lowercase) for quick lookup
_STATIC_TABLE_NAMES: set[str] = set()
for _schema in _WRITABLE_CATALOG.schemas:
    for _table in _schema.tables:
        _STATIC_TABLE_NAMES.add(_table.name.lower())


# ============================================================================
# WritableCatalog — catalog interface with DDL support
# ============================================================================


class WritableCatalog(ReadOnlyCatalogInterface):
    """Catalog interface with transaction and DDL support for writable tables.

    Transactions are managed by the db-transactor subprocess. The transactor
    owns the single DuckDB connection and serializes all operations.

    DDL operations (CREATE/DROP/ALTER TABLE) are forwarded to the transactor
    and dynamically discovered tables are served through generic writable
    functions.
    """

    catalog = _WRITABLE_CATALOG
    _FIXED_ATTACH_ID = AttachId(b"writable-catalog-")
    supports_transactions = True
    catalog_version_frozen = False

    def __init__(self) -> None:
        # Track catalog version — incremented on DDL operations
        self._catalog_version: int = 1

    def catalog_attach(self, *, name: str, options: dict[str, Any]) -> CatalogAttachResult:
        """Attach to the catalog with catalog_version_frozen=False."""
        result = super().catalog_attach(name=name, options=options)
        # Override frozen flag to signal dynamic catalog
        return CatalogAttachResult(
            attach_id=result.attach_id,
            supports_transactions=result.supports_transactions,
            supports_time_travel=result.supports_time_travel,
            catalog_version_frozen=False,
            catalog_version=self._catalog_version,
            attach_id_required=result.attach_id_required,
            default_schema=result.default_schema,
            settings=result.settings,
            secret_types=result.secret_types,
        )

    def catalog_version(self, *, attach_id: AttachId, transaction_id: TransactionId | None) -> int:
        """Return current catalog version, incremented on DDL changes."""
        return self._catalog_version

    # ========== Transaction lifecycle ==========

    def catalog_transaction_begin(self, *, attach_id: AttachId) -> TransactionId | None:
        """Begin a transaction via the transactor."""
        tx_id = TransactionId(uuid.uuid4().bytes)
        logger.info("catalog_transaction_begin: tx_id=%s", tx_id.hex())
        proxy = transactor_proxy._get_proxy()
        proxy.begin(tx_id=tx_id)
        return tx_id

    def catalog_transaction_commit(self, *, attach_id: AttachId, transaction_id: TransactionId) -> None:
        """Commit a transaction via the transactor."""
        logger.info("catalog_transaction_commit: tx_id=%s", transaction_id.hex())
        proxy = transactor_proxy._get_proxy()
        proxy.commit(tx_id=transaction_id)

    def catalog_transaction_rollback(self, *, attach_id: AttachId, transaction_id: TransactionId) -> None:
        """Rollback a transaction via the transactor."""
        proxy = transactor_proxy._get_proxy()
        proxy.rollback(tx_id=transaction_id)

    # ========== DDL: Table operations ==========

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
        primary_key_constraints: list[list[int]] | None = None,
        foreign_key_constraints: list[bytes] | None = None,
    ) -> None:
        """Create a new table in the transactor's DuckDB database.

        Builds CREATE TABLE DDL from column definitions and constraints.
        DuckDB's built-in rowid pseudocolumn provides the row identifier
        automatically — no extra column or sequence needed.
        """
        # Deserialize columns schema
        col_schema = pa.ipc.read_schema(pa.BufferReader(columns))

        if_not_exists = " IF NOT EXISTS" if on_conflict == OnConflict.IGNORE else ""

        # Build column definitions (including defaults from Arrow field metadata)
        col_defs: list[str] = []
        for i, field in enumerate(col_schema):
            col_def = f"{_qi(field.name)} {_arrow_type_to_sql(field.type)}"
            if i in not_null_constraints:
                col_def += " NOT NULL"
            # Default values are passed as Arrow field metadata with key "default"
            if field.metadata and b"default" in field.metadata:
                default_expr = field.metadata[b"default"].decode("utf-8")
                col_def += f" DEFAULT {default_expr}"
            col_defs.append(col_def)

        # Table-level constraints
        constraints: list[str] = []

        # Primary key
        if primary_key_constraints:
            for pk_group in primary_key_constraints:
                pk_cols = ", ".join(_qi(col_schema.field(i).name) for i in pk_group)
                constraints.append(f"PRIMARY KEY ({pk_cols})")

        # Unique constraints
        for unique_group in unique_constraints:
            uq_cols = ", ".join(_qi(col_schema.field(i).name) for i in unique_group)
            constraints.append(f"UNIQUE ({uq_cols})")

        # Check constraints
        for check_expr in check_constraints:
            constraints.append(f"CHECK ({check_expr})")

        # Foreign key constraints
        if foreign_key_constraints:
            for fk_bytes in foreign_key_constraints:
                fk = _deserialize_fk(fk_bytes)
                fk_cols = ", ".join(_qi(c) for c in fk["fk_columns"])
                pk_cols = ", ".join(_qi(c) for c in fk["pk_columns"])
                ref_table = _qi(fk["referenced_table"])
                constraints.append(
                    f"FOREIGN KEY ({fk_cols}) REFERENCES {ref_table}({pk_cols})"
                )

        # Combine into CREATE TABLE
        all_parts = col_defs + constraints
        columns_sql = ",\n  ".join(all_parts)
        ddl = f"CREATE TABLE{if_not_exists} {_qn(schema_name, name)} (\n  {columns_sql}\n);"

        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=ddl)

        self._catalog_version += 1
        logger.info("table_create: %s (on_conflict=%s)", name, on_conflict.value)

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
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        if_exists = " IF EXISTS" if ignore_not_found else ""
        ddl = f"DROP TABLE{if_exists} {_qn(schema_name, name)};"

        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=ddl)

        self._catalog_version += 1
        logger.info("table_drop: %s (ignore_not_found=%s)", name, ignore_not_found)

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
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        sql = f"ALTER TABLE {_qn(schema_name, name)} RENAME TO {_qi(new_name)};"
        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=sql)

        self._catalog_version += 1
        logger.info("table_rename: %s -> %s", name, new_name)

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
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        col_schema = pa.ipc.read_schema(pa.BufferReader(column_definition))
        field = col_schema.field(0)
        if_not_exists = " IF NOT EXISTS" if if_column_not_exists else ""
        sql = f"ALTER TABLE {_qn(schema_name, name)} ADD COLUMN{if_not_exists} {_qi(field.name)} {_arrow_type_to_sql(field.type)};"

        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=sql)

        self._catalog_version += 1
        logger.info("table_column_add: %s.%s", name, field.name)

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
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        if_exists = " IF EXISTS" if if_column_exists else ""
        cascade_sql = " CASCADE" if cascade else ""
        sql = f"ALTER TABLE {_qn(schema_name, name)} DROP COLUMN{if_exists} {_qi(column_name)}{cascade_sql};"

        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=sql)

        self._catalog_version += 1
        logger.info("table_column_drop: %s.%s", name, column_name)

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
        """Rename a column in a table."""
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        sql = f"ALTER TABLE {_qn(schema_name, name)} RENAME COLUMN {_qi(column_name)} TO {_qi(new_column_name)};"

        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=sql)

        self._catalog_version += 1
        logger.info("table_column_rename: %s.%s -> %s", name, column_name, new_column_name)

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
        """Set or clear the comment on a table."""
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        if comment is None:
            sql = f"COMMENT ON TABLE {_qn(schema_name, name)} IS NULL;"
        else:
            escaped = comment.replace("'", "''")
            sql = f"COMMENT ON TABLE {_qn(schema_name, name)} IS '{escaped}';"
        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=sql)

        self._catalog_version += 1
        logger.info("table_comment_set: %s comment=%s", name, comment)

    def table_column_comment_set(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        column_name: str,
        comment: str | None,
        ignore_not_found: bool,
    ) -> None:
        """Set or clear the comment on a table column."""
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        if comment is None:
            sql = f"COMMENT ON COLUMN {_qn(schema_name, name)}.{_qi(column_name)} IS NULL;"
        else:
            escaped = comment.replace("'", "''")
            sql = f"COMMENT ON COLUMN {_qn(schema_name, name)}.{_qi(column_name)} IS '{escaped}';"
        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=sql)
        self._catalog_version += 1
        logger.info("table_column_comment_set: %s.%s comment=%s", name, column_name, comment)

    def table_column_type_change(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        column_definition: SerializedSchema,
        expression: str | None,
        ignore_not_found: bool,
    ) -> None:
        """Change the type of a column in a table."""
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        col_schema = pa.ipc.read_schema(pa.BufferReader(column_definition))
        field = col_schema.field(0)
        sql = f"ALTER TABLE {_qn(schema_name, name)} ALTER COLUMN {_qi(field.name)} TYPE {_arrow_type_to_sql(field.type)}"
        if expression:
            # expression comes from DuckDB's binder (serialized AST), not raw user input
            sql += f" USING {expression}"
        sql += ";"
        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=sql)
        self._catalog_version += 1
        logger.info("table_column_type_change: %s.%s -> %s", name, field.name, field.type)

    def table_column_default_set(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        column_name: str,
        expression: str,
        ignore_not_found: bool,
    ) -> None:
        """Set the default expression for a column."""
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        sql = f"ALTER TABLE {_qn(schema_name, name)} ALTER COLUMN {_qi(column_name)} SET DEFAULT {expression};"
        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=sql)
        self._catalog_version += 1
        logger.info("table_column_default_set: %s.%s = %s", name, column_name, expression)

    def table_column_default_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        column_name: str,
        ignore_not_found: bool,
    ) -> None:
        """Drop the default expression for a column."""
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        sql = f"ALTER TABLE {_qn(schema_name, name)} ALTER COLUMN {_qi(column_name)} DROP DEFAULT;"
        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=sql)
        self._catalog_version += 1
        logger.info("table_column_default_drop: %s.%s", name, column_name)

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
        """Set NOT NULL constraint on a column."""
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        sql = f"ALTER TABLE {_qn(schema_name, name)} ALTER COLUMN {_qi(column_name)} SET NOT NULL;"
        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=sql)
        self._catalog_version += 1
        logger.info("table_not_null_set: %s.%s", name, column_name)

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
        """Drop NOT NULL constraint from a column."""
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        sql = f"ALTER TABLE {_qn(schema_name, name)} ALTER COLUMN {_qi(column_name)} DROP NOT NULL;"
        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=sql)
        self._catalog_version += 1
        logger.info("table_not_null_drop: %s.%s", name, column_name)

    # ========== Schema discovery (merge static + dynamic) ==========

    def schemas(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
    ) -> list[SchemaInfo]:
        """List schemas — merge static catalog schemas with transactor schemas."""
        static_schemas = super().schemas(attach_id=attach_id, transaction_id=transaction_id)
        static_names = {s.name.lower() for s in static_schemas}

        if not transaction_id:
            return static_schemas

        try:
            proxy = transactor_proxy._get_proxy()
            dynamic_names = proxy.list_schemas(tx_id=transaction_id)
        except Exception:
            logger.debug("schemas: failed to list schemas from transactor")
            return static_schemas

        result = list(static_schemas)
        for name in dynamic_names:
            if name.lower() not in static_names:
                result.append(SchemaInfo(
                    attach_id=attach_id,
                    name=name,
                    comment=None,
                    tags={},
                ))
        return result

    def schema_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
    ) -> SchemaInfo | None:
        """Get schema info — check static first, then transactor."""
        result = super().schema_get(attach_id=attach_id, transaction_id=transaction_id, name=name)
        if result is not None:
            return result

        if not transaction_id:
            return None

        try:
            proxy = transactor_proxy._get_proxy()
            schema_names = proxy.list_schemas(tx_id=transaction_id)
        except Exception:
            return None

        if name.lower() in {n.lower() for n in schema_names}:
            return SchemaInfo(
                attach_id=attach_id,
                name=name,
                comment=None,
                tags={},
            )
        return None

    # ========== DDL: Schema operations ==========

    def schema_create(self, *, attach_id: AttachId, transaction_id: TransactionId | None,
                      name: str, on_conflict: OnConflict = OnConflict.ERROR,
                      comment: str | None, tags: dict[str, str] | None) -> None:
        """Create a new schema in the transactor's DuckDB database."""
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        if_not_exists = " IF NOT EXISTS" if on_conflict == OnConflict.IGNORE else ""
        sql = f"CREATE SCHEMA{if_not_exists} {_qi(name)};"
        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=sql)
        self._catalog_version += 1
        logger.info("schema_create: %s (on_conflict=%s)", name, on_conflict.value)

    def schema_drop(self, *, attach_id: AttachId, transaction_id: TransactionId | None,
                    name: str, ignore_not_found: bool, cascade: bool) -> None:
        """Drop a schema from the transactor's DuckDB database."""
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        if_exists = " IF EXISTS" if ignore_not_found else ""
        cascade_sql = " CASCADE" if cascade else ""
        sql = f"DROP SCHEMA{if_exists} {_qi(name)}{cascade_sql};"
        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=sql)
        self._catalog_version += 1
        logger.info("schema_drop: %s (ignore_not_found=%s, cascade=%s)", name, ignore_not_found, cascade)

    # ========== DDL: View operations ==========

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
        """Create a new view in the transactor's DuckDB database."""
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        if_replace = ""
        if_not_exists = ""
        if on_conflict == OnConflict.REPLACE:
            if_replace = " OR REPLACE"
        elif on_conflict == OnConflict.IGNORE:
            if_not_exists = " IF NOT EXISTS"
        sql = f"CREATE{if_replace} VIEW{if_not_exists} {_qn(schema_name, name)} AS {definition};"
        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=sql,
                              strip_catalog=self._effective_catalog_name)
        self._catalog_version += 1
        logger.info("view_create: %s (on_conflict=%s)", name, on_conflict.value)

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
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        if_exists = " IF EXISTS" if ignore_not_found else ""
        sql = f"DROP VIEW{if_exists} {_qn(schema_name, name)};"
        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=sql)
        self._catalog_version += 1
        logger.info("view_drop: %s (ignore_not_found=%s)", name, ignore_not_found)

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
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        sql = f"ALTER VIEW {_qn(schema_name, name)} RENAME TO {_qi(new_name)};"
        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=sql)
        self._catalog_version += 1
        logger.info("view_rename: %s -> %s", name, new_name)

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
        """Set or clear the comment on a view."""
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        if comment is None:
            sql = f"COMMENT ON VIEW {_qn(schema_name, name)} IS NULL;"
        else:
            escaped = comment.replace("'", "''")
            sql = f"COMMENT ON VIEW {_qn(schema_name, name)} IS '{escaped}';"
        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(tx_id=transaction_id, sql=sql)
        self._catalog_version += 1
        logger.info("view_comment_set: %s comment=%s", name, comment)

    # ========== Dynamic view discovery ==========

    def view_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ViewInfo | None:
        """Get view info — check static catalog first, then transactor."""
        result = super().view_get(
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
        )
        if result is not None:
            return result

        if not transaction_id:
            return None

        try:
            proxy = transactor_proxy._get_proxy()
            import json
            info_json = proxy.view_info(view_name=name, tx_id=transaction_id)
            info = json.loads(info_json)
        except ValueError:
            logger.debug("view_get: dynamic view '%s' not found in transactor", name)
            return None
        except Exception:
            logger.warning("view_get: unexpected error for dynamic view '%s'", name, exc_info=True)
            return None

        return ViewInfo(
            name=name,
            schema_name=schema_name,
            definition=info["definition"],
            comment=info.get("comment"),
            tags={},
        )

    # ========== Dynamic table discovery ==========

    def table_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        """Get table info — check static catalog first, then transactor."""
        # Try static catalog first
        result = super().table_get(
            attach_id=attach_id,
            transaction_id=transaction_id,
            schema_name=schema_name,
            name=name,
            at_unit=at_unit,
            at_value=at_value,
        )
        if result is not None:
            return result

        # Query transactor for dynamic table (requires transaction context)
        if not transaction_id:
            return None
        try:
            proxy = transactor_proxy._get_proxy()
            # Use schema-qualified name for non-default schemas
            # Don't quote — the transactor handles its own quoting internally
            tx_table_name = f"{schema_name}.{name}" if schema_name else name
            schema_bytes = proxy.table_schema(
                table_name=tx_table_name,
                tx_id=transaction_id,
            )
            table_schema = pa.ipc.read_schema(pa.BufferReader(schema_bytes))
        except ValueError:
            logger.debug("table_get: dynamic table '%s' not found in transactor", name)
            return None
        except Exception:
            logger.warning("table_get: unexpected error for dynamic table '%s'", name, exc_info=True)
            return None

        # Build TableInfo from the transactor schema.
        # The columns schema includes rowid with is_row_id metadata from the transactor.
        # Constraints are embedded in schema-level metadata as JSON.
        serialized = SerializedSchema(table_schema.serialize().to_pybytes())

        # Parse constraints from schema-level metadata
        not_null_constraints: list[int] = []
        unique_constraints: list[list[int]] = []
        check_constraints: list[str] = []
        primary_key_constraints: list[list[int]] = []
        foreign_key_constraints: list[bytes] = []

        schema_meta = table_schema.metadata or {}
        if b"vgi.constraints" in schema_meta:
            import json
            from vgi_rpc.utils import serialize_record_batch_bytes
            constraints = json.loads(schema_meta[b"vgi.constraints"].decode("utf-8"))
            # Build column name → index map (excluding rowid)
            col_names = [f.name for f in table_schema if f.name != "rowid"]
            col_index = {name: i for i, name in enumerate(col_names)}
            for c in constraints:
                ctype = c["type"]
                cols = c.get("columns") or []
                text = c.get("text") or ""
                col_indices = [col_index[cn] for cn in cols if cn in col_index]
                if ctype == "NOT NULL":
                    not_null_constraints.extend(col_indices)
                elif ctype == "UNIQUE":
                    unique_constraints.append(col_indices)
                elif ctype == "PRIMARY KEY":
                    primary_key_constraints.append(col_indices)
                elif ctype == "CHECK":
                    # Extract the check expression from constraint_text
                    # Format: "CHECK(expr)"
                    if text.startswith("CHECK(") and text.endswith(")"):
                        check_constraints.append(text[6:-1])
                    elif text:
                        check_constraints.append(text)
                elif ctype == "FOREIGN KEY":
                    ref_table = c.get("referenced_table") or ""
                    ref_cols = c.get("referenced_columns") or []
                    if ref_table and cols and ref_cols:
                        fk_batch = pa.RecordBatch.from_pydict(
                            {
                                "fk_columns": [list(cols)],
                                "pk_columns": [list(ref_cols)],
                                "referenced_table": [ref_table],
                                "referenced_schema": [schema_name],
                            },
                            schema=pa.schema([
                                ("fk_columns", pa.list_(pa.utf8())),
                                ("pk_columns", pa.list_(pa.utf8())),
                                ("referenced_table", pa.utf8()),
                                ("referenced_schema", pa.utf8()),
                            ]),
                        )
                        foreign_key_constraints.append(
                            serialize_record_batch_bytes(fk_batch)
                        )

        # Also fetch the table comment
        try:
            table_comment = proxy.table_comment(table_name=name, tx_id=transaction_id)
        except Exception:
            table_comment = None

        return TableInfo(
            name=name,
            schema_name=schema_name,
            columns=serialized,
            not_null_constraints=not_null_constraints,
            unique_constraints=unique_constraints,
            check_constraints=check_constraints,
            primary_key_constraints=primary_key_constraints,
            foreign_key_constraints=foreign_key_constraints,
            supports_insert=True,
            supports_update=True,
            supports_delete=True,
            comment=table_comment,
            tags={},
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
        type: Literal[SchemaObjectType.SCALAR_FUNCTION, SchemaObjectType.TABLE_FUNCTION],
    ) -> Sequence[FunctionInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: Literal[SchemaObjectType.SCALAR_MACRO, SchemaObjectType.TABLE_MACRO],
    ) -> Sequence[MacroInfo]: ...

    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: SchemaObjectType,
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo | MacroInfo]:
        """List schema contents, merging static + dynamic tables for TABLE type."""
        # Normalize type parameter
        type_enum = type if isinstance(type, SchemaObjectType) else SchemaObjectType(type)

        if type_enum == SchemaObjectType.TABLE:
            # Get static tables from parent
            static_results = list(super().schema_contents(
                attach_id=attach_id,
                transaction_id=transaction_id,
                name=name,
                type=type,
            ))
            static_names = {r.name.lower() for r in static_results if isinstance(r, TableInfo)}

            # Get dynamic tables from transactor
            try:
                proxy = transactor_proxy._get_proxy()
                user_tables = proxy.list_user_tables(tx_id=transaction_id, schema_name=name) if transaction_id else []
            except ValueError:
                logger.debug("schema_contents: failed to list user tables from transactor")
                user_tables = []
            except Exception:
                logger.warning("schema_contents: unexpected error listing user tables", exc_info=True)
                user_tables = []

            # Merge: add dynamic tables not already in static
            for tbl_name in user_tables:
                if tbl_name.lower() not in static_names:
                    tbl_info = self.table_get(
                        attach_id=attach_id,
                        transaction_id=transaction_id,
                        schema_name=name,
                        name=tbl_name,
                    )
                    if tbl_info is not None:
                        static_results.append(tbl_info)

            return static_results

        if type_enum == SchemaObjectType.VIEW:
            # Get static views from parent
            static_results = list(super().schema_contents(
                attach_id=attach_id,
                transaction_id=transaction_id,
                name=name,
                type=type,
            ))
            static_names = {r.name.lower() for r in static_results if isinstance(r, ViewInfo)}

            # Get dynamic views from transactor
            try:
                proxy = transactor_proxy._get_proxy()
                user_views = proxy.list_user_views(tx_id=transaction_id, schema_name=name) if transaction_id else []
            except ValueError:
                logger.debug("schema_contents: failed to list user views from transactor")
                user_views = []
            except Exception:
                logger.warning("schema_contents: unexpected error listing user views", exc_info=True)
                user_views = []

            # Merge: add dynamic views not already in static
            for vw_name in user_views:
                if vw_name.lower() not in static_names:
                    vw_info = self.view_get(
                        attach_id=attach_id,
                        transaction_id=transaction_id,
                        schema_name=name,
                        name=vw_name,
                    )
                    if vw_info is not None:
                        static_results.append(vw_info)

            return static_results

        # Other types: delegate to parent
        return super().schema_contents(
            attach_id=attach_id,
            transaction_id=transaction_id,
            name=name,
            type=type,
        )

    # ========== Dynamic scan/write function dispatch ==========

    def table_scan_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        """Get scan function — static tables use parent, dynamic use generic_writable_scan."""
        # Try static catalog first
        if name.lower() in _STATIC_TABLE_NAMES:
            return super().table_scan_function_get(
                attach_id=attach_id,
                transaction_id=transaction_id,
                schema_name=schema_name,
                name=name,
                at_unit=at_unit,
                at_value=at_value,
            )

        # Dynamic table — use generic scan with schema-qualified table_name as positional arg
        qualified = f"{schema_name}.{name}" if schema_name else name
        return ScanFunctionResult(
            function_name="generic_writable_scan",
            positional_arguments=[pa.scalar(qualified)],
            named_arguments={},
        )

    def table_insert_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        """Get insert function — static tables use parent, dynamic use generic."""
        if name.lower() in _STATIC_TABLE_NAMES:
            return super().table_insert_function_get(
                attach_id=attach_id,
                transaction_id=transaction_id,
                schema_name=schema_name,
                name=name,
            )

        qualified = f"{schema_name}.{name}" if schema_name else name
        return ScanFunctionResult(
            function_name="generic_writable_insert",
            positional_arguments=[pa.scalar(qualified)],
            named_arguments={},
        )

    def table_update_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        """Get update function — static tables use parent, dynamic use generic."""
        if name.lower() in _STATIC_TABLE_NAMES:
            return super().table_update_function_get(
                attach_id=attach_id,
                transaction_id=transaction_id,
                schema_name=schema_name,
                name=name,
            )

        qualified = f"{schema_name}.{name}" if schema_name else name
        return ScanFunctionResult(
            function_name="generic_writable_update",
            positional_arguments=[pa.scalar(qualified)],
            named_arguments={},
        )

    def table_delete_function_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ScanFunctionResult:
        """Get delete function — static tables use parent, dynamic use generic."""
        if name.lower() in _STATIC_TABLE_NAMES:
            return super().table_delete_function_get(
                attach_id=attach_id,
                transaction_id=transaction_id,
                schema_name=schema_name,
                name=name,
            )

        qualified = f"{schema_name}.{name}" if schema_name else name
        return ScanFunctionResult(
            function_name="generic_writable_delete",
            positional_arguments=[pa.scalar(qualified)],
            named_arguments={},
        )


class WritableWorker(Worker):
    """Worker with transactional writable tables and DDL support.

    Exposes writable_data, writable_products, and writable_orders tables
    via the WritableCatalog. Also supports CREATE TABLE, DROP TABLE, and
    ALTER TABLE for dynamically created tables.
    """

    catalog_interface = WritableCatalog
    catalog = _WRITABLE_CATALOG


def main() -> None:
    """Run the writable worker process."""
    WritableWorker.main()


if __name__ == "__main__":
    main()
