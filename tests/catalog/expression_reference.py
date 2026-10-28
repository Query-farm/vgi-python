# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Closed deterministic scalar proof on DuckDB 1.5, not a general SQL executor.

The local JSON parser output is only inspected here; no syntax tree is sent on
the wire. This reference qualifies the default context and primitive signed
integers, booleans, UTF-8 strings and NULL. Broader types/contexts fail closed.
"""

from __future__ import annotations

import json
from typing import Any

import duckdb
import pyarrow as pa

from vgi.catalog.capabilities import ROW_SCALAR_EXPRESSION
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.sql import SqlExpression
from vgi.schema_path import schema_path_key

_ROLES = {"default", "generated", "check", "using", "layout", "index"}
_TYPES = {"BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "VARCHAR", "SQLNULL", "NULL"}


def validate_expression(
    connection: Any, expr: SqlExpression, owner_path: list[str], *, role: str = "layout"
) -> frozenset[str]:
    """Validate one scalar tree and return its row-column dependencies."""
    expr.__post_init__()
    if role not in _ROLES:
        raise ValueError("unsupported expression evaluation role")
    if expr.profile != ROW_SCALAR_EXPRESSION or schema_path_key(expr.schema_path) != schema_path_key(owner_path):
        raise ValueError("unsupported expression profile or owner schema")
    if expr.evaluation_context != QueryEvaluationContext():
        raise ValueError("reference evaluator supports only the default explicit evaluation context")
    parsed = json.loads(
        connection.execute("SELECT json_serialize_sql(?)", [f"SELECT ({expr.sql}) AS v FROM input_data"]).fetchone()[0]
    )
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
    dependencies: set[str] = set()

    def check(expression: dict[str, Any]) -> None:
        kind = expression.get("class")
        if kind == "COLUMN_REF":
            names = expression.get("column_names", [])
            # The existing column-edit reference binds nested USING paths to
            # its old row schema. Other new roles retain a flat primitive scope.
            if role == "default" or not names or (len(names) != 1 and role != "using"):
                raise ValueError("column reference is not allowed in this expression scope")
            dependencies.add(names[0])
            return
        constant_types = _TYPES | ({"DECIMAL", "DOUBLE", "FLOAT"} if role == "using" else set())
        if kind == "CONSTANT" and expression.get("value", {}).get("type", {}).get("id") in constant_types:
            return
        if kind == "CAST" and expression.get("cast_type", {}).get("id") in _TYPES and not expression.get("try_cast"):
            check(expression["child"])
            return
        if kind == "COMPARISON" and expression.get("type") in {
            "COMPARE_EQUAL",
            "COMPARE_NOTEQUAL",
            "COMPARE_LESSTHAN",
            "COMPARE_GREATERTHAN",
            "COMPARE_LESSTHANOREQUALTO",
            "COMPARE_GREATERTHANOREQUALTO",
        }:
            check(expression["left"])
            check(expression["right"])
            return
        if kind == "CONJUNCTION" and expression.get("type") in {"CONJUNCTION_AND", "CONJUNCTION_OR"}:
            for child in expression["children"]:
                check(child)
            return
        functions = (
            {"+", "-", "*", "%", "abs", "lower", "upper"}
            | ({"/"} if role == "using" else set())
            | ({"||"} if role == "generated" else set())
        )
        if kind == "FUNCTION" and expression.get("function_name") in functions:
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
        if kind == "OPERATOR" and expression.get("type") in {
            "OPERATOR_COALESCE",
            "OPERATOR_IS_NULL",
            "OPERATOR_IS_NOT_NULL",
            "OPERATOR_NOT",
        }:
            for child in expression["children"]:
                check(child)
            return
        raise ValueError("expression is outside the deterministic row-scalar profile")

    check(node["select_list"][0])
    return frozenset(dependencies)


def _primitive(dtype: pa.DataType) -> bool:
    return bool(
        pa.types.is_signed_integer(dtype)
        or pa.types.is_boolean(dtype)
        or pa.types.is_string(dtype)
        or pa.types.is_null(dtype)
    )


def evaluate_expression(
    expr: SqlExpression, table: pa.Table, owner_path: list[str], *, role: str
) -> pa.ChunkedArray[Any]:
    """Evaluate a qualified row expression without erasing CHECK's NULL values."""
    table.validate(full=True)
    for field in table.schema:
        if not _primitive(field.type) or any(
            key.startswith((b"vgi.logical_type.", b"ARROW:extension:")) for key in (field.metadata or {})
        ):
            raise ValueError("reference evaluator requires primitive unannotated input types")
    with duckdb.connect(config={"enable_external_access": False}) as connection:
        connection.execute("SET TimeZone='UTC'")
        connection.execute("SET default_collation='binary'")
        connection.execute("SET default_order='asc'")
        connection.execute("SET default_null_order='nulls_last'")
        connection.execute("SET preserve_insertion_order=true")
        connection.register("input_data", table)
        validate_expression(connection, expr, owner_path, role=role)
        result = connection.execute(f"SELECT ({expr.sql}) AS v FROM input_data").to_arrow_table().column(0)
    if not _primitive(result.type) or (role == "check" and not pa.types.is_boolean(result.type)):
        raise ValueError("expression result has an unsupported type for its role")
    return result


def evaluate_generated(table: pa.Table, definitions: dict[str, SqlExpression], owner_path: list[str]) -> pa.Table:
    """Resolve a finite generated-column DAG before evaluating its candidate."""
    if set(definitions) & set(table.schema.names):
        raise ValueError("generated column names conflict with base columns")
    with duckdb.connect(config={"enable_external_access": False}) as connection:
        dependencies = {
            name: validate_expression(connection, expression, owner_path, role="generated")
            for name, expression in definitions.items()
        }
    known = set(table.schema.names) | set(definitions)
    if any(references - known for references in dependencies.values()):
        raise ValueError("generated column references an unknown column")
    ordered: list[str] = []
    pending = set(definitions)
    while pending:
        ready = [name for name in definitions if name in pending and not (dependencies[name] & pending)]
        if not ready:
            raise ValueError("generated column dependency cycle")
        ordered.extend(ready)
        pending.difference_update(ready)
    candidate = table
    for name in ordered:
        candidate = candidate.append_column(
            name, evaluate_expression(definitions[name], candidate, owner_path, role="generated")
        )
    return candidate
