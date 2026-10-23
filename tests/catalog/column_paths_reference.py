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
import uuid
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
    CONSTRAINT_CREATE,
    CONSTRAINT_ENFORCE,
    CONSTRAINT_MUTATE,
    CONSTRAINT_RECORDS_READ,
    CatalogCapabilities,
)
from vgi.catalog.column_metadata import column_metadata
from vgi.catalog.constraints import (
    ConstraintDefinition,
    ConstraintEnforcement,
    ConstraintInfo,
    ConstraintKind,
    ConstraintOperation,
    ConstraintsApplyRequest,
    ConstraintSnapshot,
    ConstraintValidation,
    constraint_definitions,
    validate_constraint_schema,
)
from vgi.catalog.dependencies import CatalogObjectRef
from vgi.catalog.layout import SqlExpression
from vgi.catalog.sql import ColumnExpressions, column_expressions, with_column_expressions
from vgi.column_path import column_definition, column_path_key, replace_column_field, resolve_column_path
from vgi.worker import Worker

from .table_layout_reference import TABLE_PROFILES, TableLayoutCatalog, _table_key, _validate_expression

COLUMN_PROFILES = CatalogCapabilities(
    read=sorted([*TABLE_PROFILES.read, CONSTRAINT_RECORDS_READ]),
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
            CONSTRAINT_CREATE,
            CONSTRAINT_MUTATE,
        ]
    ),
    write=[CONSTRAINT_ENFORCE],
    transaction=TABLE_PROFILES.transaction,
)


def _eval(
    expression: SqlExpression,
    path: list[str],
    schema: pa.Schema | None = None,
    rows: list[dict[str, Any]] | None = None,
) -> pa.Array:
    expression.validate_owner(path)
    with duckdb.connect(config={"enable_external_access": False}) as connection:
        connection.execute("SET TimeZone='UTC'")
        connection.execute("SET default_collation='binary'")
        connection.execute("SET default_order='asc'")
        connection.execute("SET default_null_order='nulls_last'")
        connection.execute("SET preserve_insertion_order=true")
        if schema is not None:
            connection.register("input_data", pa.Table.from_pylist(rows or [], schema=schema))
        _validate_expression(
            connection,
            expression,
            path,
            role="using" if schema is not None else "default",
        )
        query = f"SELECT ({expression.sql}) AS v" + (" FROM input_data" if schema is not None else "")
        return connection.execute(query).to_arrow_table().column(0).combine_chunks()


def _fields(fields: Any, path: list[str]) -> None:
    """Validate fixture support before storing any metadata or rows."""
    names = [field.name for field in fields]
    if len(names) != len(set(names)):
        raise ValueError("duplicate field names")
    for field in fields:
        column_metadata(field)
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
        expressions = column_expressions(field)
        if expressions.generated is not None or metadata.get(b"compression", b"auto") != b"auto":
            raise NotImplementedError("generated expressions and nondefault compression are not supported")
        if {b"default", b"generated_expression"}.intersection(metadata):
            raise ValueError("column reference requires canonical expression metadata")
        if any(key.startswith(b"ARROW:extension:") for key in metadata):
            raise NotImplementedError("logical extensions need their own semantic qualification")
        if expressions.default is not None:
            pc.cast(_eval(expressions.default, path), field.type, safe=True)


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
        default = column_expressions(field).default
        if field.name not in result and default is not None:
            result[field.name] = pc.cast(_eval(default, schema_path), field.type, safe=True)[0].as_py()
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

    def _create_constraints(self, args: dict[str, Any]) -> tuple[list[bytes], bytes | None]:
        table = CatalogObjectRef(kind="table", schema_path=args["schema_path"], name=args["name"])
        definitions = constraint_definitions(args["constraint_definitions"], table)
        if any(definition.kind != ConstraintKind.NOT_NULL for definition in definitions):
            raise NotImplementedError("column reference only enforces NOT_NULL constraints")
        validate_constraint_schema(pa.ipc.read_schema(pa.BufferReader(args["columns"])), definitions)
        return [
            self._not_null_info(table, definition).serialize_to_bytes() for definition in definitions
        ], uuid.uuid4().bytes

    @staticmethod
    def _not_null_info(table: CatalogObjectRef, definition: ConstraintDefinition) -> ConstraintInfo:
        return ConstraintInfo(
            ref=CatalogObjectRef(
                kind="constraint", schema_path=table.schema_path, owner_table=table.name, name=uuid.uuid4().hex
            ),
            definition=definition,
            enforcement=ConstraintEnforcement.ENFORCED,
            validation=ConstraintValidation.ALL_ROWS,
        )

    def _changed_constraints(
        self, info: TableInfo, schema: pa.Schema, operation: str, path: list[str], replacement: pa.Field | None
    ) -> list[bytes]:
        existing = {}
        for constraint in info.constraint_infos():
            key = constraint.definition.column_paths[0]
            if key[: len(path)] == path:
                if operation == "drop":
                    continue
                if operation == "rename":
                    assert replacement is not None
                    key = [*path[:-1], replacement.name, *key[len(path) :]]
                    constraint = replace(constraint, definition=replace(constraint.definition, column_paths=[key]))
            existing[tuple(key)] = constraint
        table = CatalogObjectRef(kind="table", schema_path=info.schema_path, name=info.name)
        result = []

        def collect(fields: Any, prefix: list[str]) -> None:
            for field in fields:
                key = [*prefix, field.name]
                if not field.nullable:
                    constraint = existing.get(tuple(key))
                    if constraint is None:
                        constraint = self._not_null_info(
                            table, ConstraintDefinition(kind=ConstraintKind.NOT_NULL, column_paths=[key])
                        )
                    result.append(constraint.serialize_to_bytes())
                if pa.types.is_struct(field.type):
                    collect(field.type, key)

        collect(schema, [])
        return result

    def _arrange(self, rows: list[dict[str, Any]], info: TableInfo) -> list[dict[str, Any]]:
        schema = pa.ipc.read_schema(pa.BufferReader(info.columns))
        # Resolve defaults from the same locked snapshot that stores the rows.
        # Reading table metadata before _change would race concurrent DDL.
        rows = [_defaulted(schema, row, info.schema_path) for row in rows]
        for row in rows:
            _validate_values(schema, row)
        return super()._arrange(rows, info)

    def _column_change(self, operation: str, args: dict[str, Any]) -> ConstraintSnapshot | None:
        path = [] if "constraint_request" in args else list(column_path_key(args["column_path"]))
        key = _table_key(args["schema_path"], args["name"])
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, args["attach_opaque_data"], args["transaction_opaque_data"])
            if key not in data:
                if args.get("ignore_not_found"):
                    return
                raise ValueError("table does not exist")
            info = self._info(data[key])
            request = args.get("constraint_request")
            if request is not None:
                if request.expected_revision != info.definition_revision:
                    raise ValueError("stale table definition revision")
                change = request.constraint_changes()[0]
                if change.operation == ConstraintOperation.ADD:
                    path = change.definition.column_paths[0]
                    if any(item.definition.column_paths == [path] for item in info.constraint_infos()):
                        raise ValueError("NOT_NULL constraint already exists")
                    operation = "not_null_set"
                else:
                    matches = [item for item in info.constraint_infos() if item.ref.key() == change.ref.key()]
                    if len(matches) != 1 or matches[0].definition.kind != ConstraintKind.NOT_NULL:
                        raise ValueError("unknown NOT_NULL constraint identity")
                    path = matches[0].definition.column_paths[0]
                    operation = "not_null_drop"
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
                default = column_expressions(replacement).default
                for row in rows:
                    parent = _parent(row, path)
                    if parent is not None:
                        parent[path[-1]] = (
                            pc.cast(_eval(default, info.schema_path), replacement.type, safe=True)[0].as_py()
                            if default is not None
                            else None
                        )
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
                    metadata = dict(
                        with_column_expressions(existing, ColumnExpressions(default=args["expression"])).metadata or {}
                    )
                elif operation == "default_drop":
                    metadata = dict(with_column_expressions(existing, ColumnExpressions()).metadata or {})
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
            constraints = self._changed_constraints(info, updated, operation, path, replacement)
            changed = not updated.equals(schema, check_metadata=True) or constraints != info.constraints
            info = replace(
                info,
                columns=updated.serialize().to_pybytes(),
                constraints=constraints,
                definition_revision=uuid.uuid4().bytes
                if changed and operation != "comment"
                else info.definition_revision,
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
            if request is not None:
                return ConstraintSnapshot(
                    table=request.table, definition_revision=info.definition_revision, constraints=info.constraints
                )
        return None

    def table_constraints_apply(self, request: ConstraintsApplyRequest) -> ConstraintSnapshot:
        """Apply the native single NOT_NULL edit in the same locked data snapshot."""
        request.__post_init__()
        changes = request.constraint_changes()
        if len(changes) != 1:
            raise NotImplementedError("column reference supports one NOT_NULL change per request")
        change = changes[0]
        if change.operation == ConstraintOperation.ADD:
            definition = change.definition
            if (
                definition.kind != ConstraintKind.NOT_NULL
                or definition.declared_name is not None
                or change.enforcement != ConstraintEnforcement.ENFORCED
                or change.validation != ConstraintValidation.ALL_ROWS
            ):
                raise NotImplementedError("column reference supports unnamed enforced ALL_ROWS NOT_NULL only")
        elif change.operation != ConstraintOperation.DROP or change.cascade:
            raise NotImplementedError("column reference supports NOT_NULL ADD and non-cascading DROP only")
        result = self._column_change(
            "constraint",
            dict(
                attach_opaque_data=request.attach_opaque_data,
                transaction_opaque_data=request.transaction_opaque_data,
                schema_path=request.table.schema_path,
                name=request.table.name,
                constraint_request=request,
            ),
        )
        assert result is not None
        return result

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
