# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D102, D107

"""Persistent table-option/layout reference with real existing-row rewrites.

The fixture uses the pinned 1.5 SQL parser locally to prove a closed scalar
expression profile. Its AST is only a validation aid and never crosses VGI.
It advertises schema/table DDL transactions, not general data-write atomicity.
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

from vgi.catalog import SchemaInfo, TableInfo
from vgi.catalog.capabilities import (
    ROW_SCALAR_EXPRESSION,
    TABLE_LAYOUT_ALTER_FUTURE,
    TABLE_LAYOUT_ALTER_REWRITE,
    TABLE_LAYOUT_CREATE,
    TABLE_LAYOUT_READ,
    TABLE_METADATA_CREATE,
    TABLE_OPTIONS_ALTER,
    TABLE_OPTIONS_CREATE,
    TABLE_OPTIONS_READ,
    CatalogCapabilities,
)
from vgi.catalog.catalog_interface import CatalogContentsResult, OnConflict, SchemaContentsInfo, SchemaObjectType
from vgi.catalog.layout import LayoutChangeMode, SqlExpression, TableLayout, TableLayoutChange
from vgi.schema_path import schema_path_key
from vgi.worker import Worker

from .schema_options_reference import DEFINITIONS, PROFILES, SchemaOptionsCatalog

TABLE_PROFILES = CatalogCapabilities(
    read=sorted([*PROFILES.read, TABLE_OPTIONS_READ, TABLE_LAYOUT_READ, ROW_SCALAR_EXPRESSION]),
    ddl=sorted(
        [
            *PROFILES.ddl,
            TABLE_OPTIONS_CREATE,
            TABLE_OPTIONS_ALTER,
            TABLE_METADATA_CREATE,
            TABLE_LAYOUT_CREATE,
            TABLE_LAYOUT_ALTER_FUTURE,
            TABLE_LAYOUT_ALTER_REWRITE,
        ]
    ),
    transaction=PROFILES.transaction,
)


def _table_key(path: list[str], name: str) -> str:
    if not name or "\x00" in name:
        raise ValueError("invalid table name")
    return json.dumps({"schema": schema_path_key(path), "table": name.lower()})


def _validate_expression(connection: Any, expr: SqlExpression, path: list[str]) -> None:
    if expr.profile != ROW_SCALAR_EXPRESSION or schema_path_key(expr.schema_path) != schema_path_key(path):
        raise ValueError("unsupported expression profile or owner schema")
    sql = f"SELECT ({expr.sql}) AS v FROM input_data"
    parsed = json.loads(connection.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()[0])
    if parsed.get("error") or len(parsed.get("statements", [])) != 1:
        raise ValueError("expected one scalar expression")
    node = parsed["statements"][0]["node"]
    source = node.get("from_table", {})
    if (
        node.get("type") != "SELECT_NODE"
        or source.get("type") != "BASE_TABLE"
        or source.get("table_name") != "input_data"
        or source.get("schema_name")
        or source.get("catalog_name")
        or len(node.get("select_list", [])) != 1
        or node.get("modifiers")
        or node.get("cte_map", {}).get("map")
        or any(
            node.get(key) for key in ("where_clause", "group_expressions", "group_sets", "having", "qualify", "sample")
        )
    ):
        raise ValueError("expected one row-local scalar expression")

    def check(expression: dict[str, Any]) -> None:
        kind = expression.get("class")
        if kind in ("COLUMN_REF", "CONSTANT"):
            return
        if kind == "FUNCTION" and expression.get("function_name") in ("+", "-", "*", "%", "abs", "lower", "upper"):
            if (
                expression.get("schema")
                or expression.get("catalog")
                or expression.get("distinct")
                or expression.get("filter")
                or expression.get("order_bys", {}).get("orders")
            ):
                raise ValueError("unsupported scalar function modifiers")
            for child in expression["children"]:
                check(child)
            return
        if kind == "OPERATOR" and expression.get("type") == "OPERATOR_COALESCE":
            for child in expression["children"]:
                check(child)
            return
        raise ValueError("expression is outside the deterministic row-scalar profile")

    check(node["select_list"][0])


def arrange_rows(rows: list[dict[str, Any]], info: TableInfo) -> list[dict[str, Any]]:
    """Validate/bind keys, then partition and sort actual rows in one snapshot."""
    columns = pa.ipc.read_schema(pa.BufferReader(info.columns))
    layout = info.layout or TableLayout()
    partitions, sorts = layout.partition_expressions(), layout.sort_keys()
    with duckdb.connect(config={"enable_external_access": False}) as connection:
        connection.register("input_data", pa.Table.from_pylist(rows, schema=columns))
        for expr in partitions + [key.expression for key in sorts]:
            _validate_expression(connection, expr, info.schema_path)
        select = "*" + "".join(f", ({expr.sql}) AS __vgi_partition_{i}" for i, expr in enumerate(partitions))
        order = ", ".join(f"({key.expression.sql}) {key.direction.value} NULLS {key.null_order.value}" for key in sorts)
        result = connection.execute(
            f"SELECT {select} FROM input_data" + (f" ORDER BY {order}" if order else "")
        ).to_arrow_table()
    groups: dict[str, dict[str, Any]] = {}
    for row in range(len(result)):
        key = [result.column(len(columns) + i)[row].as_py() for i in range(len(partitions))]
        group = groups.setdefault(json.dumps(key), {"key": key, "rows": []})
        group["rows"].append({name: result.column(i)[row].as_py() for i, name in enumerate(columns.names)})
    return list(groups.values())


class TableLayoutCatalog(SchemaOptionsCatalog):
    """Schema/table metadata and data-layout snapshots in one durable transaction."""

    catalog_name = "layout"
    profiles = TABLE_PROFILES

    def schemas(self, *, attach_opaque_data: Any, transaction_opaque_data: Any) -> list[SchemaInfo]:
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, attach_opaque_data, transaction_opaque_data)
            return [self._decode(value, attach_opaque_data) for key, value in data.items() if key.startswith("[")]

    @staticmethod
    def _info(value: str) -> TableInfo:
        return TableInfo.deserialize_from_bytes(base64.b64decode(json.loads(value)["info"]))

    def schema_contents(
        self, *, attach_opaque_data: Any, transaction_opaque_data: Any, path: list[str], type: SchemaObjectType
    ) -> list[TableInfo]:
        if type != SchemaObjectType.TABLE:
            return []
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, attach_opaque_data, transaction_opaque_data)
            return [
                self._info(value)
                for key, value in data.items()
                if key.startswith("{") and schema_path_key(self._info(value).schema_path) == schema_path_key(path)
            ]

    def table_get(
        self, *, attach_opaque_data: Any, transaction_opaque_data: Any, schema_path: list[str], name: str, **kwargs: Any
    ) -> TableInfo | None:
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, attach_opaque_data, transaction_opaque_data)
            value = data.get(_table_key(schema_path, name))
            return self._info(value) if value is not None else None

    def catalog_contents(self, *, attach_opaque_data: Any, if_none_match: str | None = None) -> CatalogContentsResult:
        return CatalogContentsResult(
            schemas=[
                SchemaContentsInfo(
                    schema=info,
                    tables=self.schema_contents(
                        attach_opaque_data=attach_opaque_data,
                        transaction_opaque_data=None,
                        path=info.path,
                        type=SchemaObjectType.TABLE,
                    ),
                )
                for info in self.schemas(attach_opaque_data=attach_opaque_data, transaction_opaque_data=None)
            ],
            etag="same-provider-etag",
        )

    def table_option_definitions(self, **kwargs: Any) -> Any:
        return DEFINITIONS

    def _arrange(self, rows: list[dict[str, Any]], info: TableInfo) -> list[dict[str, Any]]:
        layout = info.layout or TableLayout()
        for expression in layout.partition_expressions() + [key.expression for key in layout.sort_keys()]:
            if expression.profile not in self.profiles.read:
                raise ValueError("expression profile is no longer supported")
        return arrange_rows(rows, info)

    def _change(self, args: dict[str, Any], action: str) -> None:
        attach, tx = args["attach_opaque_data"], args["transaction_opaque_data"]
        path, name = args["schema_path"], args["name"]
        key = _table_key(path, name)
        with closing(sqlite3.connect(self.database_path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            version, data = self._snapshot(db, attach, tx)
            if action == "create":
                if json.dumps(schema_path_key(path)) not in data:
                    raise ValueError("schema does not exist")
                if any(
                    args.get(c)
                    for c in (
                        "not_null_constraints",
                        "unique_constraints",
                        "check_constraints",
                        "primary_key_constraints",
                        "foreign_key_constraints",
                    )
                ):
                    raise ValueError("constraint enforcement is outside this reference")
                info = TableInfo(
                    name=name,
                    schema_path=path,
                    columns=args["columns"],
                    comment=args.get("comment"),
                    tags=args.get("tags") or {},
                    not_null_constraints=[],
                    unique_constraints=[],
                    check_constraints=[],
                    options=DEFINITIONS.create(args.get("options")),
                    layout=args.get("layout") or TableLayout(),
                )
                physical = self._arrange([], info)
                if key in data:
                    if args["on_conflict"] == OnConflict.IGNORE:
                        return
                    if args["on_conflict"] != OnConflict.REPLACE:
                        raise ValueError("table already exists")
            else:
                if key not in data:
                    raise ValueError("table does not exist")
                record = json.loads(data[key])
                info, physical = self._info(data[key]), record["partitions"]
                if action == "options":
                    assert info.options is not None
                    info = replace(info, options=DEFINITIONS.apply(info.options, args["changes"]))
                elif action == "layout":
                    change: TableLayoutChange = args["changes"]
                    info = replace(info, layout=change.apply_to(info.layout or TableLayout()))
                    self._arrange([], info)  # Bind and validate even a future-write change.
                    if change.mode == LayoutChangeMode.REWRITE:
                        physical = self._arrange([row for part in physical for row in part["rows"]], info)
                elif action == "seed":
                    physical += self._arrange(args["rows"], info)
                elif action == "drop":
                    del data[key]
            if action != "drop":
                data[key] = json.dumps(
                    {"info": base64.b64encode(info.serialize_to_bytes()).decode(), "partitions": physical}
                )
            if tx is None:
                db.execute("UPDATE catalog SET version=?, data=? WHERE id=1", (version + 1, json.dumps(data)))
            else:
                db.execute("UPDATE transactions SET version=?, data=? WHERE id=?", (version + 1, json.dumps(data), tx))

    def table_create(self, **kwargs: Any) -> None:
        self._change(kwargs, "create")

    def table_alter_options(self, **kwargs: Any) -> None:
        self._change(kwargs, "options")

    def table_alter_layout(self, **kwargs: Any) -> None:
        self._change(kwargs, "layout")

    def table_drop(self, **kwargs: Any) -> None:
        self._change(kwargs, "drop")

    def seed_rows(self, **kwargs: Any) -> None:
        """Populate physical data for layout tests without advertising data writes."""
        self._change(kwargs, "seed")

    def physical_rows(
        self, *, attach_opaque_data: Any, transaction_opaque_data: Any, schema_path: list[str], name: str
    ) -> list[dict[str, Any]]:
        """Read the durable physical partitions for a test assertion."""
        with closing(sqlite3.connect(self.database_path)) as db:
            _, data = self._snapshot(db, attach_opaque_data, transaction_opaque_data)
            return json.loads(data[_table_key(schema_path, name)])["partitions"]  # type: ignore[no-any-return]


class TableLayoutWorker(Worker):
    """Normal public protocol host for the table-layout reference."""

    catalog_interface = TableLayoutCatalog
