# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D103
"""Durable dependency authority for metadata-only tables, generated columns and views.

The view binder deliberately accepts only SELECT * FROM one table/view, with
an optional single raw schema identifier. Other SQL is rejected, never treated
as dependency-free. This reference does not execute or advertise table writes.
"""

from __future__ import annotations

import base64
import json
import sqlite3
from contextlib import closing
from dataclasses import replace
from typing import Any

import duckdb
import pyarrow as pa

from vgi.catalog import (
    CatalogObjectRef,
    DependencySnapshot,
    ObjectDependency,
    OnConflict,
    SchemaInfo,
    TableInfo,
    ViewInfo,
)
from vgi.catalog.capabilities import (
    CATALOG_DEPENDENCIES_READ,
    COLUMN_NESTED_COMMENT,
    COLUMN_NESTED_DROP,
    COLUMN_NESTED_RENAME,
    COLUMN_NESTED_TYPE,
    ROW_SCALAR_EXPRESSION,
    SCHEMA_LIFECYCLE,
    TRANSACTION_DDL,
)
from vgi.catalog.catalog_interface import CatalogContentsResult, SchemaContentsInfo, SchemaObjectType
from vgi.catalog.sql import column_expressions
from vgi.column_path import column_definition, column_path_key, replace_column_field, resolve_column_path
from vgi.schema_path import schema_path_key
from vgi.worker import Worker

from .expression_reference import evaluate_expression, validate_expression
from .schema_options_reference import DEFINITIONS, PROFILES, SchemaOptionsCatalog, _key

DEPENDENCY_PROFILES = replace(
    PROFILES,
    read=sorted([*PROFILES.read, CATALOG_DEPENDENCIES_READ, ROW_SCALAR_EXPRESSION]),
    ddl=sorted(
        [
            *PROFILES.ddl,
            SCHEMA_LIFECYCLE,
            COLUMN_NESTED_COMMENT,
            COLUMN_NESTED_DROP,
            COLUMN_NESTED_RENAME,
            COLUMN_NESTED_TYPE,
        ]
    ),
)


def schema_ref(path: list[str]) -> CatalogObjectRef:
    schema_path_key(path)
    return CatalogObjectRef(kind="schema", schema_path=path[:-1], name=path[-1])


def object_key(ref: CatalogObjectRef) -> str:
    ref.__post_init__()
    if ref.kind == "schema":
        return _key([*ref.schema_path, ref.name])
    # This reference follows the existing schema reference's case-insensitive
    # key policy. The public graph returns original canonical spelling.
    return "object:" + json.dumps([ref.kind, schema_path_key(ref.schema_path), ref.name.lower(), ref.owner_table])


def edge(
    dependent: CatalogObjectRef,
    subject: CatalogObjectRef,
    kind: str = "requires",
    *,
    blocks_alter: bool = False,
    subject_column_paths: list[list[str]] | None = None,
    dependent_column_path: list[str] | None = None,
) -> ObjectDependency:
    return ObjectDependency(
        dependent=dependent.serialize_to_bytes(),
        subject=subject.serialize_to_bytes(),
        kind=kind,
        blocks_alter=blocks_alter,
        subject_column_paths=subject_column_paths,
        dependent_column_path=dependent_column_path,
    )


def _read_schema(payload: bytes) -> pa.Schema:
    reader = pa.BufferReader(payload)
    schema = pa.ipc.read_schema(reader)
    if reader.tell() != len(payload):
        raise ValueError("table schema contains trailing bytes")
    return schema


def _schema(info: TableInfo) -> pa.Schema:
    return _read_schema(info.columns)


def _primitive(dtype: pa.DataType) -> bool:
    return pa.types.is_signed_integer(dtype) or pa.types.is_boolean(dtype) or pa.types.is_string(dtype)


def _generated(schema: pa.Schema, owner: list[str]) -> dict[str, list[list[str]]]:
    """Bind the qualified virtual DAG and derive every exact column edge."""
    definitions = {}

    def fields(items: Any, parent: list[str]) -> None:
        names = [item.name.casefold() for item in items]
        if len(set(names)) != len(names):
            raise ValueError("duplicate or case-ambiguous column names")
        for item in items:
            column_path_key([item.name])
            if not item.nullable:
                raise ValueError("metadata-only reference cannot claim NOT NULL enforcement")
            metadata = item.metadata or {}
            if any(key.startswith((b"ARROW:extension:", b"vgi.logical_type.")) for key in metadata):
                raise ValueError("reference does not qualify logical or extension types")
            if any(key in metadata for key in (b"default", b"generated_expression", b"collation", b"compression")):
                raise ValueError("legacy or unsupported column semantics")
            exprs = column_expressions(item)
            if exprs.default is not None:
                raise ValueError("reference does not implement default expressions")
            if exprs.generated is not None:
                if parent or exprs.generation != "virtual" or not _primitive(item.type):
                    raise ValueError("reference supports top-level primitive virtual generated columns only")
                definitions[item.name] = exprs.generated
            if pa.types.is_struct(item.type):
                if not len(item.type):
                    raise ValueError("empty structs are unsupported")
                fields(item.type, [*parent, item.name])
            elif not _primitive(item.type):
                raise ValueError("reference supports ordinary structs and primitive signed integer/bool/string fields")

    fields(schema, [])
    if not definitions:
        return {}
    canonical = {item.name.casefold(): item for item in schema}
    dependencies = {}
    with duckdb.connect(config={"enable_external_access": False}) as con:
        for name, expression in definitions.items():
            references = validate_expression(con, expression, owner, role="generated")
            paths = []
            for reference in references:
                field = canonical.get(reference.casefold())
                if field is None or not _primitive(field.type):
                    raise ValueError("generated expression references an unknown or unsupported column")
                paths.append([field.name])
            dependencies[name] = [list(path) for path in sorted({tuple(path) for path in paths})]
    pending = set(definitions)
    # Bind with actual Arrow types, even for a metadata-only table with no rows.
    base = pa.schema([item for item in schema if item.name not in definitions and _primitive(item.type)])
    candidate = pa.Table.from_pylist([], schema=base)
    while pending:
        ready = sorted(name for name in pending if not {path[0] for path in dependencies[name]} & pending)
        if not ready:
            raise ValueError("generated column dependency cycle")
        for name in ready:
            result = evaluate_expression(definitions[name], candidate, owner, role="generated")
            if result.type != schema.field(name).type:
                raise ValueError("generated expression result type does not match declared column type")
            candidate = candidate.append_column(name, result)
        pending.difference_update(ready)
    return dependencies


class DependencyCatalog(SchemaOptionsCatalog):
    """Persist authoritative dependency and metadata snapshots atomically."""

    catalog_name = "dependencies"
    profiles = DEPENDENCY_PROFILES

    def _objects(self, data: dict[str, str]) -> dict[str, CatalogObjectRef]:
        result = {}
        for key, value in data.items():
            if key.startswith("["):
                result[key] = schema_ref(self._decode(value, b"").path)
            else:
                result[key] = CatalogObjectRef.deserialize_from_bytes(base64.b64decode(json.loads(value)["ref"]))
        return result

    def _graph(self, version: int, data: dict[str, str]) -> DependencySnapshot:
        objects = self._objects(data)
        dependencies = []
        schemas = {}
        for key, ref in objects.items():
            if ref.schema_path:
                dependencies.append(edge(ref, objects[_key(ref.schema_path)]))
            if ref.kind == "view":
                for subject in json.loads(data[key])["subjects"]:
                    dependencies.append(edge(ref, objects[subject], blocks_alter=True))
            if ref.kind == "table":
                info = self._info(data[key])
                assert isinstance(info, TableInfo)
                schema = _schema(info)
                schemas[ref.key()] = schema
                for name, paths in _generated(schema, ref.schema_path).items():
                    if paths:
                        dependencies.append(
                            edge(ref, ref, blocks_alter=True, subject_column_paths=paths, dependent_column_path=[name])
                        )
        graph = DependencySnapshot(
            catalog_version=version,
            objects=[ref.serialize_to_bytes() for ref in objects.values()],
            dependencies=[item.serialize_to_bytes() for item in dependencies],
        )
        graph.validate_columns(schemas)
        return graph

    def _column_schemas(self, data: dict[str, str]) -> dict[Any, pa.Schema]:
        return {
            ref.key(): _schema(self._info(data[key])) for key, ref in self._objects(data).items() if ref.kind == "table"
        }

    def _require_transaction_profile(self, transaction: bytes | None) -> None:
        if transaction is not None and TRANSACTION_DDL not in self.profiles.transaction:
            raise ValueError("semantic mutation requires transaction DDL profile")

    def catalog_dependencies(self, **kwargs: Any) -> DependencySnapshot:
        with closing(sqlite3.connect(self.database_path)) as db:
            return self._graph(*self._snapshot(db, kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"]))

    def schemas(self, **kwargs: Any) -> list[SchemaInfo]:
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
            return [self._decode(v, kwargs["attach_opaque_data"]) for k, v in data.items() if k.startswith("[")]

    @staticmethod
    def _info(value: str) -> TableInfo | ViewInfo:
        item = json.loads(value)
        cls = TableInfo if item["kind"] == "table" else ViewInfo
        return cls.deserialize_from_bytes(base64.b64decode(item["info"]))

    def _get(self, kind: str, kwargs: dict[str, Any]) -> Any:
        ref = CatalogObjectRef(kind=kind, schema_path=kwargs["schema_path"], name=kwargs["name"])
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
            value = data.get(object_key(ref))
            return self._info(value) if value is not None else None

    def table_get(self, **kwargs: Any) -> TableInfo | None:
        return self._get("table", kwargs)

    def view_get(self, **kwargs: Any) -> ViewInfo | None:
        return self._get("view", kwargs)

    def schema_contents(self, **kwargs: Any) -> Any:
        expected = {SchemaObjectType.TABLE: "table", SchemaObjectType.VIEW: "view"}.get(kwargs["type"])
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
            return [
                self._info(data[k])
                for k, r in self._objects(data).items()
                if r.kind == expected and schema_path_key(r.schema_path) == schema_path_key(kwargs["path"])
            ]

    def catalog_contents(self, **kwargs: Any) -> CatalogContentsResult:
        # All related metadata comes from the same SQLite snapshot.
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, kwargs["attach_opaque_data"], None)
        objects = self._objects(data)
        schemas = []
        for key, ref in objects.items():
            if ref.kind != "schema":
                continue
            info = self._decode(data[key], kwargs["attach_opaque_data"])
            items = [
                (r.kind, self._info(data[k]))
                for k, r in objects.items()
                if r.kind != "schema" and _key(r.schema_path) == key
            ]
            schemas.append(
                SchemaContentsInfo(
                    schema=info,
                    tables=[v for kind, v in items if kind == "table"],
                    views=[v for kind, v in items if kind == "view"],
                )
            )
        return CatalogContentsResult(schemas=schemas, etag="dependency-reference")

    def _authorize_drop(self, ref: CatalogObjectRef) -> None:
        """Provider authorization hook; tests deny one member of a cascade."""

    def _after_write(self) -> None:
        """Failure injection after SQLite UPDATE, before transaction commit."""

    def _publish(self, db: sqlite3.Connection, version: int, data: dict[str, str], tx: bytes | None) -> None:
        self._require_transaction_profile(tx)
        self._graph(version + 1, data)  # Never persist dangling references.
        if tx is None:
            db.execute("UPDATE catalog SET version=?, data=? WHERE id=1", (version + 1, json.dumps(data)))
        else:
            db.execute("UPDATE transactions SET version=?, data=? WHERE id=?", (version + 1, json.dumps(data), tx))
        self._after_write()

    def _drop_keys(self, version: int, data: dict[str, str], target: CatalogObjectRef, cascade: bool) -> list[str]:
        canonical = self._objects(data)[object_key(target)]
        drops = self._graph(version, data).drop_set(canonical, cascade=cascade)
        for ref in drops:
            self._authorize_drop(ref)
        return [object_key(ref) for ref in drops]

    def _mutate(self, attach: Any, tx: Any, path: list[str], **kwargs: Any) -> None:
        ref = schema_ref(path)
        key = object_key(ref)
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, attach, tx)
            if kwargs["create"]:
                options = DEFINITIONS.create(kwargs.get("options"))
                conflict = OnConflict(kwargs["on_conflict"])
                if ref.schema_path and _key(ref.schema_path) not in data:
                    raise ValueError("parent schema does not exist")
                if ref.schema_path:
                    parent = self._decode(data[_key(ref.schema_path)], b"").path
                    path = [*parent, ref.name]
                if key in data:
                    if conflict == OnConflict.IGNORE:
                        return
                    if conflict != OnConflict.REPLACE:
                        raise ValueError("schema already exists")
                    for drop in self._drop_keys(version, data, ref, False):
                        del data[drop]
                info = SchemaInfo(
                    attach_opaque_data=b"",
                    path=path,
                    comment=kwargs.get("comment"),
                    tags=kwargs.get("tags") or {},
                    options=options,
                )
            else:
                if key not in data:
                    raise ValueError("schema does not exist")
                info = self._decode(data[key], b"")
                info = replace(info, options=DEFINITIONS.apply(info.options, kwargs["changes"]))
            data[key] = self._encode(info)
            self._publish(db, version, data, tx)

    def _drop(self, ref: CatalogObjectRef, **kwargs: Any) -> None:
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
            if object_key(ref) not in data:
                if kwargs["ignore_not_found"]:
                    return
                raise ValueError("catalog object does not exist")
            for key in self._drop_keys(version, data, ref, kwargs["cascade"]):
                del data[key]
            self._publish(db, version, data, kwargs["transaction_opaque_data"])

    def schema_drop(self, **kwargs: Any) -> None:
        self._drop(schema_ref(kwargs["path"]), **kwargs)

    def table_drop(self, **kwargs: Any) -> None:
        self._drop(CatalogObjectRef(kind="table", schema_path=kwargs["schema_path"], name=kwargs["name"]), **kwargs)

    def view_drop(self, **kwargs: Any) -> None:
        self._drop(CatalogObjectRef(kind="view", schema_path=kwargs["schema_path"], name=kwargs["name"]), **kwargs)

    @staticmethod
    def _bind_view(definition: str, path: list[str], data: dict[str, str]) -> tuple[str, TableInfo | ViewInfo]:
        with duckdb.connect(config={"enable_external_access": False}) as con:
            parsed = json.loads(con.execute("SELECT json_serialize_sql(?)", [definition]).fetchone()[0])
        if parsed.get("error") or len(parsed.get("statements", [])) != 1:
            raise ValueError("reference requires SELECT * FROM one relation")
        statement = parsed["statements"][0]
        node = statement["node"]
        source = node.get("from_table") or {}
        stars = node.get("select_list", [])
        if (
            node["type"] != "SELECT_NODE"
            or len(stars) != 1
            or stars[0].get("class") != "STAR"
            or any(
                stars[0].get(k)
                for k in [
                    "relation_name",
                    "exclude_list",
                    "replace_list",
                    "columns",
                    "expr",
                    "qualified_exclude_list",
                    "rename_list",
                    "alias",
                ]
            )
            or source.get("type") != "BASE_TABLE"
            or source.get("catalog_name")
            or any(source.get(k) for k in ["sample", "at_clause", "column_name_alias"])
            or any(
                node.get(k)
                for k in ["modifiers", "where_clause", "group_expressions", "group_sets", "having", "qualify", "sample"]
            )
            or node.get("cte_map", {}).get("map")
            or statement.get("named_param_map")
        ):
            raise ValueError("reference requires SELECT * FROM one relation")
        owner = [source["schema_name"]] if source.get("schema_name") else path
        for kind in ["table", "view"]:
            key = object_key(CatalogObjectRef(kind=kind, schema_path=owner, name=source["table_name"]))
            if key in data:
                return key, DependencyCatalog._info(data[key])
        raise ValueError("view dependency does not exist")

    def _create_object(self, kind: str, **kwargs: Any) -> None:
        ref = CatalogObjectRef(kind=kind, schema_path=kwargs["schema_path"], name=kwargs["name"])
        key = object_key(ref)
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, kwargs["attach_opaque_data"], kwargs["transaction_opaque_data"])
            if _key(ref.schema_path) not in data:
                raise ValueError("schema does not exist")
            # Resolve the complete owning path to the provider's spelling.
            # Case-insensitive request aliases must not leak into graph identity.
            ref = replace(ref, schema_path=self._decode(data[_key(ref.schema_path)], b"").path)
            other_kind = "view" if kind == "table" else "table"
            if object_key(replace(ref, kind=other_kind)) in data:
                raise ValueError("relation name is already in use")
            conflict = OnConflict(kwargs["on_conflict"])
            if key in data:
                if conflict == OnConflict.IGNORE:
                    return
                if conflict != OnConflict.REPLACE:
                    raise ValueError("relation already exists")
                self._drop_keys(version, data, ref, False)
            subjects = []
            if kind == "table":
                if any(
                    kwargs.get(k)
                    for k in [
                        "constraint_definitions",
                        "options",
                        "layout",
                    ]
                ):
                    raise ValueError("reference does not implement table constraints/options/layout")
                _generated(_read_schema(kwargs["columns"]), ref.schema_path)
                info = TableInfo(
                    name=ref.name,
                    schema_path=ref.schema_path,
                    columns=kwargs["columns"],
                    comment=kwargs.get("comment"),
                    tags=kwargs.get("tags") or {},
                    constraints=[],
                )
            else:
                subject, bound = self._bind_view(kwargs["definition"], ref.schema_path, data)
                if subject == key:
                    raise ValueError("self-referential views are invalid")
                subjects = [subject]
                count = (
                    len(pa.ipc.read_schema(pa.BufferReader(bound.columns)))
                    if isinstance(bound, TableInfo)
                    else len(json.loads(data[subject])["output_names"])
                )
                output_names = kwargs.get("aliases") or (
                    pa.ipc.read_schema(pa.BufferReader(bound.columns)).names
                    if isinstance(bound, TableInfo)
                    else json.loads(data[subject])["output_names"]
                )
                if len(output_names) != count:
                    raise ValueError("view alias count does not match its output")
                info = ViewInfo(
                    name=ref.name,
                    schema_path=ref.schema_path,
                    definition=kwargs["definition"],
                    aliases=kwargs.get("aliases") or [],
                    security=kwargs.get("security", "regular"),
                    comment=kwargs.get("comment"),
                    tags=kwargs.get("tags") or {},
                    column_comments=kwargs.get("column_comments") or {},
                )
            record = dict(
                kind=kind,
                ref=base64.b64encode(ref.serialize_to_bytes()).decode(),
                info=base64.b64encode(info.serialize_to_bytes()).decode(),
                subjects=subjects,
            )
            if kind == "view":
                record["output_names"] = output_names
            data[key] = json.dumps(record)
            self._publish(db, version, data, kwargs["transaction_opaque_data"])

    def _authorize_alter(self, ref: CatalogObjectRef, path: list[str] | None, action: str) -> None:
        """Provider authorization hook for every semantic target before effects."""

    @staticmethod
    def _store_info(
        data: dict[str, str], key: str, info: TableInfo | ViewInfo, ref: CatalogObjectRef | None = None
    ) -> None:
        record = json.loads(data[key])
        record["info"] = base64.b64encode(info.serialize_to_bytes()).decode()
        if ref is not None:
            record["ref"] = base64.b64encode(ref.serialize_to_bytes()).decode()
        data[key] = json.dumps(record)

    def _alter_table(self, operation: str, **kwargs: Any) -> None:
        tx = kwargs["transaction_opaque_data"]
        self._require_transaction_profile(tx)
        requested = CatalogObjectRef(kind="table", schema_path=kwargs["schema_path"], name=kwargs["name"])
        key = object_key(requested)
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, kwargs["attach_opaque_data"], tx)
            if key not in data:
                if kwargs.get("ignore_not_found"):
                    return
                raise ValueError("table does not exist")
            canonical = self._objects(data)[key]
            info = self._info(data[key])
            assert isinstance(info, TableInfo)
            schema = _schema(info)
            path = kwargs.get("column_path")
            field = None
            if path is not None:
                column_path_key(path)
                try:
                    field = resolve_column_path(schema, path)
                except ValueError:
                    if operation == "drop" and kwargs.get("if_column_exists"):
                        return
                    raise
            if operation == "comment":
                if path is None:
                    info = replace(info, comment=kwargs["comment"])
                else:
                    assert field is not None
                    metadata = dict(field.metadata or {})
                    if kwargs["comment"] is None:
                        metadata.pop(b"comment", None)
                    else:
                        metadata[b"comment"] = kwargs["comment"].encode()
                    schema = replace_column_field(schema, path, field.with_metadata(metadata))
                    info = replace(info, columns=schema.serialize().to_pybytes())
                self._store_info(data, key, info)
                self._publish(db, version, data, tx)
                return
            graph = self._graph(version, data)
            if operation == "drop" and kwargs.get("cascade"):
                self._drop_column_cascade(graph, data, canonical, path)
                self._publish(db, version, data, tx)
                return
            graph.assert_alter_allowed(
                canonical, column_path=path, action=operation, schemas=self._column_schemas(data)
            )
            self._authorize_alter(canonical, path, operation)
            if path is None:
                new_ref = replace(canonical, name=kwargs["new_name"])
                new_key = object_key(new_ref)
                if new_key != key and (new_key in data or object_key(replace(new_ref, kind="view")) in data):
                    raise ValueError("relation already exists")
                self._store_info(data, key, replace(info, name=new_ref.name), new_ref)
                if new_key != key:
                    data[new_key] = data.pop(key)
            else:
                assert field is not None
                if operation == "rename":
                    replacement = field.with_name(kwargs["new_column_name"])
                elif operation == "type":
                    if kwargs.get("expression") is not None:
                        raise ValueError("metadata-only reference does not execute USING conversions")
                    replacement_type = column_definition(kwargs["column_definition"], path, type_only=True)
                    if replacement_type.metadata:
                        raise ValueError("type carrier cannot alter metadata")
                    replacement = field.with_type(replacement_type.type)
                elif operation == "drop":
                    replacement = None
                else:
                    raise ValueError("unsupported semantic mutation")
                schema = replace_column_field(schema, path, replacement)
                self._store_info(data, key, replace(info, columns=schema.serialize().to_pybytes()))
            self._publish(db, version, data, tx)

    def _drop_column_cascade(
        self, graph: DependencySnapshot, data: dict[str, str], target: CatalogObjectRef, path: list[str]
    ) -> None:
        # Core planner provides separate object and column closures. Never turn
        # one generated-column dependency into deletion of its owning table.
        plan = graph.plan_alter(
            target, column_path=path, action="drop", cascade=True, schemas=self._column_schemas(data)
        )
        for ref in plan.drop_objects:
            self._authorize_drop(ref)
        for column in plan.drop_columns:
            self._authorize_alter(column.table, column.column_path, "drop")
        for ref in plan.drop_objects:
            del data[object_key(ref)]
        for column in sorted(plan.drop_columns, key=lambda item: len(item.column_path), reverse=True):
            if object_key(column.table) not in data:
                continue
            key = object_key(column.table)
            info = self._info(data[key])
            assert isinstance(info, TableInfo)
            changed = replace_column_field(_schema(info), column.column_path, None)
            self._store_info(data, key, replace(info, columns=changed.serialize().to_pybytes()))

    def table_rename(self, **kwargs: Any) -> None:
        self._alter_table("rename", **kwargs)

    def table_column_rename(self, **kwargs: Any) -> None:
        self._alter_table("rename", **kwargs)

    def table_column_type_change(self, **kwargs: Any) -> None:
        self._alter_table("type", **kwargs)

    def table_column_drop(self, **kwargs: Any) -> None:
        self._alter_table("drop", **kwargs)

    def table_comment_set(self, **kwargs: Any) -> None:
        self._alter_table("comment", **kwargs)

    def table_column_comment_set(self, **kwargs: Any) -> None:
        self._alter_table("comment", **kwargs)

    def table_create(self, **kwargs: Any) -> None:
        self._create_object("table", **kwargs)

    def view_create(self, **kwargs: Any) -> None:
        self._create_object("view", **kwargs)


class DependencyWorker(Worker):
    """Expose the durable dependency provider through public RPC."""

    catalog_interface = DependencyCatalog
