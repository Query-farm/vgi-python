# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D107

"""Durable column-edit reference with real nested data changes on the 1.5 engine.

This fixture intentionally supports primitive JSON values and structs, refuses
layout dependencies and generated columns, and advertises no public DML path.
Its transaction snapshots exercise DDL rollback without claiming distributed or
mixed-write atomicity. General expression profiles remain a separate contract.
"""

from __future__ import annotations

import base64
import copy
import json
import sqlite3
from contextlib import closing
from dataclasses import replace
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from vgi.catalog import TableInfo
from vgi.catalog.capabilities import (
    COLUMN_NESTED_ADD,
    COLUMN_NESTED_COMMENT,
    COLUMN_NESTED_DEFAULT,
    COLUMN_NESTED_DROP,
    COLUMN_NESTED_NULLABILITY,
    COLUMN_NESTED_RENAME,
    COLUMN_NESTED_TYPE,
    ROW_SCALAR_EXPRESSION,
    CatalogCapabilities,
)
from vgi.catalog.layout import SqlExpression
from vgi.column_path import column_definition, column_path_key, replace_column_field, resolve_column_path
from vgi.worker import Worker

from .table_layout_reference import TABLE_PROFILES, TableLayoutCatalog, _table_key, _validate_expression

COLUMN_PROFILES = CatalogCapabilities(
    read=TABLE_PROFILES.read,
    ddl=sorted(
        [
            *TABLE_PROFILES.ddl,
            COLUMN_NESTED_ADD,
            COLUMN_NESTED_DROP,
            COLUMN_NESTED_RENAME,
            COLUMN_NESTED_TYPE,
            COLUMN_NESTED_DEFAULT,
            COLUMN_NESTED_NULLABILITY,
            COLUMN_NESTED_COMMENT,
        ]
    ),
    transaction=TABLE_PROFILES.transaction,
)


def _eval(
    sql: str, path: list[str], schema: pa.Schema | None = None, rows: list[dict[str, Any]] | None = None
) -> pa.Array:
    with duckdb.connect(config={"enable_external_access": False}) as connection:
        if schema is not None:
            connection.register("input_data", pa.Table.from_pylist(rows or [], schema=schema))
        _validate_expression(connection, SqlExpression(sql=sql, profile=ROW_SCALAR_EXPRESSION, schema_path=path), path)
        query = f"SELECT ({sql}) AS v" + (" FROM input_data" if schema is not None else "")
        return connection.execute(query).to_arrow_table().column(0).combine_chunks()


def _fields(fields: Any, path: list[str]) -> None:
    """Validate fixture support before storing any metadata or rows."""
    names = [field.name for field in fields]
    if len(names) != len(set(names)):
        raise ValueError("duplicate field names")
    for field in fields:
        column_path_key([field.name])
        if pa.types.is_struct(field.type):
            if not len(field.type):
                raise ValueError("empty structs are not supported")
            _fields(field.type, path)
        elif not any(
            (
                pa.types.is_integer(field.type),
                pa.types.is_floating(field.type),
                pa.types.is_string(field.type),
                pa.types.is_boolean(field.type),
            )
        ):
            raise NotImplementedError("reference supports primitive JSON values and structs")
        metadata = field.metadata or {}
        if b"generated_expression" in metadata or metadata.get(b"compression", b"auto") != b"auto":
            raise NotImplementedError("generated expressions and nondefault compression are not supported")
        if any(key.startswith(b"ARROW:extension:") for key in metadata):
            raise NotImplementedError("logical extensions need their own semantic qualification")
        if b"default" in metadata:
            pc.cast(_eval(metadata[b"default"].decode(), path), field.type, safe=True)


def _validate_values(fields: Any, value: dict[str, Any]) -> None:
    if set(value) - {field.name for field in fields}:
        raise ValueError("row contains unknown fields")
    for field in fields:
        item = value.get(field.name)
        if item is None:
            if not field.nullable:
                raise ValueError(f"NOT NULL violation for {field.name}")
        elif pa.types.is_struct(field.type):
            _validate_values(field.type, item)


def _defaulted(fields: Any, row: dict[str, Any], schema_path: list[str]) -> dict[str, Any]:
    result = copy.deepcopy(row)
    for field in fields:
        if field.name not in result and b"default" in (field.metadata or {}):
            result[field.name] = pc.cast(_eval(field.metadata[b"default"].decode(), schema_path), field.type)[0].as_py()
        if pa.types.is_struct(field.type) and result.get(field.name) is not None:
            result[field.name] = _defaulted(field.type, result[field.name], schema_path)
    return result


def _parent(row: dict[str, Any], path: list[str]) -> dict[str, Any] | None:
    value: dict[str, Any] | None = row
    for part in path[:-1]:
        value = value.get(part) if value is not None else None
    return value


class ColumnPathsCatalog(TableLayoutCatalog):
    """Store each metadata/data edit in the same SQLite transaction snapshot."""

    catalog_name = "columns"
    profiles = COLUMN_PROFILES

    def table_create(self, **kwargs: Any) -> None:
        _fields(pa.ipc.read_schema(pa.BufferReader(kwargs["columns"])), kwargs["schema_path"])
        super().table_create(**kwargs)

    def _arrange(self, rows: list[dict[str, Any]], info: TableInfo) -> list[dict[str, Any]]:
        schema = pa.ipc.read_schema(pa.BufferReader(info.columns))
        # Resolve defaults from the same locked snapshot that stores the rows.
        # Reading table metadata before _change would race concurrent DDL.
        rows = [_defaulted(schema, row, info.schema_path) for row in rows]
        for row in rows:
            _validate_values(schema, row)
        return super()._arrange(rows, info)

    def _column_change(self, operation: str, args: dict[str, Any]) -> None:
        path = list(column_path_key(args["column_path"]))
        key = _table_key(args["schema_path"], args["name"])
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, args["attach_opaque_data"], args["transaction_opaque_data"])
            if key not in data:
                if args.get("ignore_not_found"):
                    return
                raise ValueError("table does not exist")
            info = self._info(data[key])
            schema = pa.ipc.read_schema(pa.BufferReader(info.columns))
            parent_fields = schema if len(path) == 1 else resolve_column_path(schema, path[:-1]).type
            if not isinstance(parent_fields, pa.Schema) and not pa.types.is_struct(parent_fields):
                raise ValueError("column path has a non-struct parent")
            matches = [field for field in parent_fields if field.name == path[-1]]
            if len(matches) > 1:
                raise ValueError("column path is ambiguous")
            existing = matches[0] if matches else None
            if operation == "add" and existing is not None:
                if args.get("if_column_not_exists"):
                    return
                raise ValueError("column already exists")
            if operation != "add" and existing is None:
                if operation == "drop" and args.get("if_column_exists"):
                    return
                raise ValueError("column does not exist")
            if (
                operation in ("drop", "rename", "type")
                and info.layout is not None
                and (info.layout.partition_by or info.layout.sorted_by)
            ):
                raise NotImplementedError("layout dependencies must be resolved before changing this column")
            record = json.loads(data[key])
            rows = [copy.deepcopy(row) for part in record["partitions"] for row in part["rows"]]
            replacement = existing
            computed = None
            if operation == "add":
                replacement = column_definition(args["column_definition"], path)
                _fields([replacement], info.schema_path)
                default = (replacement.metadata or {}).get(b"default")
                value = (
                    pc.cast(_eval(default.decode(), info.schema_path), replacement.type)[0].as_py() if default else None
                )
                for row in rows:
                    parent = _parent(row, path)
                    if parent is not None:
                        parent[path[-1]] = copy.deepcopy(value)
            elif operation == "drop":
                replacement = None
                for row in rows:
                    parent = _parent(row, path)
                    if parent is not None:
                        parent.pop(path[-1], None)
            elif operation == "rename":
                assert existing is not None
                replacement = existing.with_name(args["new_column_name"])
                for row in rows:
                    parent = _parent(row, path)
                    if parent is not None:
                        parent[replacement.name] = parent.pop(path[-1], None)
            elif operation == "type":
                assert existing is not None
                target = column_definition(args["column_definition"], path, type_only=True)
                # This reference rejects logical extensions; retain all other
                # annotations when replacing the primitive physical type.
                metadata = dict(existing.metadata or {})
                metadata.update(target.metadata or {})
                replacement = existing.with_type(target.type).with_metadata(metadata or None)
                _fields([replacement], info.schema_path)
                if args["expression"] is not None:
                    # Bind before changes even if the table has no rows.
                    computed = pc.cast(
                        _eval(args["expression"], info.schema_path, schema, rows), target.type, safe=True
                    )
                else:
                    values = [
                        parent.get(path[-1]) if (parent := _parent(row, path)) is not None else None for row in rows
                    ]
                    computed = pc.cast(pa.array(values, type=existing.type), target.type, safe=True)
                for index, row in enumerate(rows):
                    parent = _parent(row, path)
                    if parent is not None:
                        parent[path[-1]] = computed[index].as_py()
            else:
                assert existing is not None
                metadata = dict(existing.metadata or {})
                if operation == "default_set":
                    pc.cast(_eval(args["expression"], info.schema_path), existing.type, safe=True)
                    metadata[b"default"] = args["expression"].encode()
                elif operation == "default_drop":
                    metadata.pop(b"default", None)
                elif operation == "comment":
                    if args["comment"] is None:
                        metadata.pop(b"comment", None)
                    else:
                        metadata[b"comment"] = args["comment"].encode()
                elif operation in ("not_null_set", "not_null_drop"):
                    replacement = existing.with_nullable(operation == "not_null_drop")
                else:
                    raise AssertionError(operation)
                assert replacement is not None
                replacement = replacement.with_metadata(metadata or None)
            updated = replace_column_field(schema, path, replacement, add=operation == "add")
            info = replace(
                info,
                columns=updated.serialize().to_pybytes(),
                not_null_constraints=[i for i, field in enumerate(updated) if not field.nullable],
            )
            # Metadata-only edits must preserve physical layout, especially
            # when a future-write layout differs from existing partitions.
            for row in rows:
                _validate_values(updated, row)
            partitions = (
                self._arrange(rows, info) if operation in ("add", "drop", "rename", "type") else record["partitions"]
            )
            data[key] = json.dumps(
                {"info": base64.b64encode(info.serialize_to_bytes()).decode(), "partitions": partitions}
            )
            tx = args["transaction_opaque_data"]
            if tx is None:
                db.execute("UPDATE catalog SET version=?, data=? WHERE id=1", (version + 1, json.dumps(data)))
            else:
                db.execute("UPDATE transactions SET version=?, data=? WHERE id=?", (version + 1, json.dumps(data), tx))

    def table_column_add(self, **kwargs: Any) -> None:
        self._column_change("add", kwargs)

    def table_column_drop(self, **kwargs: Any) -> None:
        self._column_change("drop", kwargs)

    def table_column_rename(self, **kwargs: Any) -> None:
        self._column_change("rename", kwargs)

    def table_column_type_change(self, **kwargs: Any) -> None:
        self._column_change("type", kwargs)

    def table_column_default_set(self, **kwargs: Any) -> None:
        self._column_change("default_set", kwargs)

    def table_column_default_drop(self, **kwargs: Any) -> None:
        self._column_change("default_drop", kwargs)

    def table_not_null_set(self, **kwargs: Any) -> None:
        self._column_change("not_null_set", kwargs)

    def table_not_null_drop(self, **kwargs: Any) -> None:
        self._column_change("not_null_drop", kwargs)

    def table_column_comment_set(self, **kwargs: Any) -> None:
        self._column_change("comment", kwargs)


class ColumnPathsWorker(Worker):
    """Expose the durable column reference through the normal protocol worker."""

    catalog_interface = ColumnPathsCatalog
