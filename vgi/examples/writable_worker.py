"""Writable worker with transactional INSERT, UPDATE, DELETE, and DDL support.

This worker exposes a fully dynamic writable catalog backed by a db-transactor
subprocess. All tables are created via CREATE TABLE DDL — there are no static
table definitions. Generic scan/insert/update/delete functions serve any table.

Usage::

    vgi-writable-worker
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
from vgi.examples.writable_table import transactor_proxy
from vgi.worker import Worker

logger = logging.getLogger("vgi.writable_worker")


def _qi(name: str) -> str:
    """Quote a SQL identifier with double quotes, escaping internal double quotes."""
    return '"' + name.replace('"', '""') + '"'


def _qn(schema_name: str, name: str) -> str:
    """Build a schema-qualified, quoted identifier."""
    return f'{_qi(schema_name)}.{_qi(name)}'


def _comment_sql(target: str, comment: str | None) -> str:
    """Build COMMENT ON <target> IS ... SQL."""
    if comment is None:
        return f"COMMENT ON {target} IS NULL;"
    escaped = comment.replace("'", "''")
    return f"COMMENT ON {target} IS '{escaped}';"


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
# Catalog definition — generic functions only, no static tables
# ============================================================================


_WRITABLE_CATALOG = Catalog(
    name="writable",
    default_schema="main",
    schemas=[
        Schema(
            name="main",
            functions=[
                GenericTableScan,
                GenericTableInsert,
                GenericTableUpdate,
                GenericTableDelete,
            ],
            tables=[],
        ),
    ],
)


# ============================================================================
# WritableCatalog — fully dynamic catalog interface
# ============================================================================


class WritableCatalog(ReadOnlyCatalogInterface):
    """Fully dynamic catalog — all tables created via DDL, served by generic functions."""

    catalog = _WRITABLE_CATALOG
    supports_transactions = True
    catalog_version_frozen = False

    def catalog_attach(self, *, name: str, options: dict[str, Any]) -> CatalogAttachResult:
        """Attach: generate unique attach_id and register a fresh database in the transactor."""
        attach_id = AttachId(uuid.uuid4().bytes)
        transactor_proxy.register(attach_id=attach_id, catalog_name=name)
        return CatalogAttachResult(
            attach_id=attach_id,
            supports_transactions=True,
            supports_time_travel=False,
            catalog_version_frozen=False,
            catalog_version=1,
            attach_id_required=True,
            default_schema="main",
            settings=[],
            secret_types=[],
        )

    def catalog_version(self, *, attach_id: AttachId, transaction_id: TransactionId | None) -> int:
        proxy = transactor_proxy._get_proxy()
        return proxy.catalog_version(attach_id=attach_id)

    # ========== Transaction lifecycle ==========

    def catalog_transaction_begin(self, *, attach_id: AttachId) -> TransactionId | None:
        """Begin a transaction — transactor generates the tx_id."""
        proxy = transactor_proxy._get_proxy()
        tx_id = proxy.begin(attach_id=attach_id)
        return TransactionId(tx_id)

    def catalog_transaction_commit(self, *, attach_id: AttachId, transaction_id: TransactionId) -> None:
        proxy = transactor_proxy._get_proxy()
        proxy.commit(attach_id=attach_id, tx_id=transaction_id)

    def catalog_transaction_rollback(self, *, attach_id: AttachId, transaction_id: TransactionId) -> None:
        proxy = transactor_proxy._get_proxy()
        proxy.rollback(attach_id=attach_id, tx_id=transaction_id)

    # ========== DDL helpers ==========

    def _execute_ddl(self, attach_id: AttachId, transaction_id: TransactionId | None, sql: str) -> None:
        """Validate transaction and execute DDL. Version is tracked by the transactor."""
        if not transaction_id:
            raise ValueError("transaction_id is required for DDL operations")
        proxy = transactor_proxy._get_proxy()
        proxy.execute_ddl_tx(attach_id=attach_id, tx_id=transaction_id, sql=sql)

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
        self._execute_ddl(attach_id, transaction_id, ddl)
        logger.info("table_create: %s (on_conflict=%s)", name, on_conflict.value)

    def table_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        ignore_not_found: bool,
        cascade: bool = False,
    ) -> None:
        """Drop a table."""
        if_exists = " IF EXISTS" if ignore_not_found else ""
        cascade_sql = " CASCADE" if cascade else ""
        self._execute_ddl(attach_id, transaction_id, f"DROP TABLE{if_exists} {_qn(schema_name, name)}{cascade_sql};")
        logger.info("table_drop: %s", name)

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
        self._execute_ddl(attach_id, transaction_id, f"ALTER TABLE {_qn(schema_name, name)} RENAME TO {_qi(new_name)};")
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
        col_schema = pa.ipc.read_schema(pa.BufferReader(column_definition))
        field = col_schema.field(0)
        if_not_exists = " IF NOT EXISTS" if if_column_not_exists else ""
        self._execute_ddl(attach_id, transaction_id, f"ALTER TABLE {_qn(schema_name, name)} ADD COLUMN{if_not_exists} {_qi(field.name)} {_arrow_type_to_sql(field.type)};")
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
        if_exists = " IF EXISTS" if if_column_exists else ""
        cascade_sql = " CASCADE" if cascade else ""
        self._execute_ddl(attach_id, transaction_id, f"ALTER TABLE {_qn(schema_name, name)} DROP COLUMN{if_exists} {_qi(column_name)}{cascade_sql};")
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
        self._execute_ddl(attach_id, transaction_id, f"ALTER TABLE {_qn(schema_name, name)} RENAME COLUMN {_qi(column_name)} TO {_qi(new_column_name)};")
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
        self._execute_ddl(attach_id, transaction_id, _comment_sql(f"TABLE {_qn(schema_name, name)}", comment))

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
        self._execute_ddl(attach_id, transaction_id, _comment_sql(f"COLUMN {_qn(schema_name, name)}.{_qi(column_name)}", comment))

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
        col_schema = pa.ipc.read_schema(pa.BufferReader(column_definition))
        field = col_schema.field(0)
        sql = f"ALTER TABLE {_qn(schema_name, name)} ALTER COLUMN {_qi(field.name)} TYPE {_arrow_type_to_sql(field.type)}"
        if expression:
            # expression comes from DuckDB's binder (serialized AST), not raw user input
            sql += f" USING {expression}"
        self._execute_ddl(attach_id, transaction_id, sql + ";")

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
        self._execute_ddl(attach_id, transaction_id, f"ALTER TABLE {_qn(schema_name, name)} ALTER COLUMN {_qi(column_name)} SET DEFAULT {expression};")

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
        self._execute_ddl(attach_id, transaction_id, f"ALTER TABLE {_qn(schema_name, name)} ALTER COLUMN {_qi(column_name)} DROP DEFAULT;")

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
        self._execute_ddl(attach_id, transaction_id, f"ALTER TABLE {_qn(schema_name, name)} ALTER COLUMN {_qi(column_name)} SET NOT NULL;")

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
        self._execute_ddl(attach_id, transaction_id, f"ALTER TABLE {_qn(schema_name, name)} ALTER COLUMN {_qi(column_name)} DROP NOT NULL;")

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
            dynamic_names = proxy.list_schemas(attach_id=attach_id, tx_id=transaction_id)
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
            schema_names = proxy.list_schemas(attach_id=attach_id, tx_id=transaction_id)
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
        if_not_exists = " IF NOT EXISTS" if on_conflict == OnConflict.IGNORE else ""
        self._execute_ddl(attach_id, transaction_id, f"CREATE SCHEMA{if_not_exists} {_qi(name)};")

    def schema_drop(self, *, attach_id: AttachId, transaction_id: TransactionId | None,
                    name: str, ignore_not_found: bool, cascade: bool) -> None:
        """Drop a schema from the transactor's DuckDB database."""
        if_exists = " IF EXISTS" if ignore_not_found else ""
        cascade_sql = " CASCADE" if cascade else ""
        self._execute_ddl(attach_id, transaction_id, f"DROP SCHEMA{if_exists} {_qi(name)}{cascade_sql};")

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
        if_replace = " OR REPLACE" if on_conflict == OnConflict.REPLACE else ""
        if_not_exists = " IF NOT EXISTS" if on_conflict == OnConflict.IGNORE else ""
        sql = f"CREATE{if_replace} VIEW{if_not_exists} {_qn(schema_name, name)} AS {definition};"
        self._execute_ddl(attach_id, transaction_id, sql)

    def view_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        ignore_not_found: bool,
        cascade: bool = False,
    ) -> None:
        """Drop a view."""
        if_exists = " IF EXISTS" if ignore_not_found else ""
        cascade_sql = " CASCADE" if cascade else ""
        self._execute_ddl(attach_id, transaction_id, f"DROP VIEW{if_exists} {_qn(schema_name, name)}{cascade_sql};")

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
        self._execute_ddl(attach_id, transaction_id, f"ALTER VIEW {_qn(schema_name, name)} RENAME TO {_qi(new_name)};")

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
        self._execute_ddl(attach_id, transaction_id, _comment_sql(f"VIEW {_qn(schema_name, name)}", comment))

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
            info_json = proxy.view_info(attach_id=attach_id, view_name=name, tx_id=transaction_id)
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
                attach_id=attach_id,
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
            # Build column name → index map in Arrow schema space (including rowid at index 0).
            # Constraint indices must be in Arrow space so the C++ adjust_col lambda
            # can correctly shift them to physical space (excluding rowid).
            col_index = {f.name: i for i, f in enumerate(table_schema) if f.name != "rowid"}
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
            table_comment = proxy.table_comment(attach_id=attach_id, table_name=name, tx_id=transaction_id)
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

    def _merge_dynamic_contents(
        self, *, attach_id: AttachId, transaction_id: TransactionId | None,
        schema_name: str, type: SchemaObjectType, info_type: type,
        list_method: str, get_method: str,
    ) -> list:
        """Merge static catalog contents with dynamic entries from the transactor."""
        static_results = list(super().schema_contents(
            attach_id=attach_id, transaction_id=transaction_id, name=schema_name, type=type,
        ))
        static_names = {r.name.lower() for r in static_results if isinstance(r, info_type)}
        try:
            proxy = transactor_proxy._get_proxy()
            dynamic_names = getattr(proxy, list_method)(attach_id=attach_id, tx_id=transaction_id, schema_name=schema_name) if transaction_id else []
        except ValueError:
            dynamic_names = []
        except Exception:
            logger.warning("schema_contents: error listing dynamic %s", type, exc_info=True)
            dynamic_names = []
        for item_name in dynamic_names:
            if item_name.lower() not in static_names:
                item = getattr(self, get_method)(
                    attach_id=attach_id, transaction_id=transaction_id,
                    schema_name=schema_name, name=item_name,
                )
                if item is not None:
                    static_results.append(item)
        return static_results

    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: SchemaObjectType,
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo | MacroInfo]:
        """List schema contents, merging static + dynamic entries."""
        type_enum = type if isinstance(type, SchemaObjectType) else SchemaObjectType(type)

        if type_enum == SchemaObjectType.TABLE:
            return self._merge_dynamic_contents(
                attach_id=attach_id, transaction_id=transaction_id, schema_name=name,
                type=type, info_type=TableInfo, list_method="list_user_tables", get_method="table_get",
            )
        if type_enum == SchemaObjectType.VIEW:
            return self._merge_dynamic_contents(
                attach_id=attach_id, transaction_id=transaction_id, schema_name=name,
                type=type, info_type=ViewInfo, list_method="list_user_views", get_method="view_get",
            )
        return super().schema_contents(
            attach_id=attach_id, transaction_id=transaction_id, name=name, type=type,
        )

    # ========== Dynamic scan/write function dispatch ==========

    def _function_get(self, kind: str, *, schema_name: str, name: str, **kwargs: Any) -> ScanFunctionResult:
        """Dispatch all tables to generic functions."""
        qualified = f"{schema_name}.{name}" if schema_name else name
        return ScanFunctionResult(
            function_name=f"generic_writable_{kind}",
            positional_arguments=[pa.scalar(qualified)],
            named_arguments={},
        )

    def table_scan_function_get(self, *, attach_id, transaction_id, schema_name, name,
                                at_unit, at_value) -> ScanFunctionResult:
        return self._function_get("scan", schema_name=schema_name, name=name)

    def table_insert_function_get(self, *, attach_id, transaction_id, schema_name, name) -> ScanFunctionResult:
        return self._function_get("insert", schema_name=schema_name, name=name)

    def table_update_function_get(self, *, attach_id, transaction_id, schema_name, name) -> ScanFunctionResult:
        return self._function_get("update", schema_name=schema_name, name=name)

    def table_delete_function_get(self, *, attach_id, transaction_id, schema_name, name) -> ScanFunctionResult:
        return self._function_get("delete", schema_name=schema_name, name=name)


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
