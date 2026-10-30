# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The DuckDB extension's accepted CHECK and generated-column SQL grammars.

The extension parses canonical SQL with DuckDB's parser and accepts only a
small deterministic subset (``ParseNumericCheck`` in
``vgi_constraint_contract.cpp`` and ``Scalar`` in
``vgi_column_expression.cpp``). This module mirrors both walks over the same
parser's AST, obtained from ``json_serialize_sql`` on the locally installed
engine (haybarn or duckdb).

vgi does not hard-depend on an engine. Without one these checks are skipped
(the extension still rejects at ATTACH); with one, an author's own tests see
the extension's verdict before deployment.
"""

from __future__ import annotations

import json
import threading
from typing import Any

import pyarrow as pa

_NUMERIC_TYPES = frozenset(
    {
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
        "UHUGEINT",
        "FLOAT",
        "DOUBLE",
        "DECIMAL",
    }
)
_PRIMITIVE = frozenset({"NULL", "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE"})
_PRIMITIVE |= {"DECIMAL", "VARCHAR"}
_GENERATED_PRIMITIVE = frozenset({"NULL", "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "VARCHAR"})
_COMPARISONS = frozenset(
    {
        "COMPARE_EQUAL",
        "COMPARE_NOTEQUAL",
        "COMPARE_LESSTHAN",
        "COMPARE_GREATERTHAN",
        "COMPARE_LESSTHANOREQUALTO",
        "COMPARE_GREATERTHANOREQUALTO",
    }
)
_OPERATORS = frozenset({"OPERATOR_COALESCE", "OPERATOR_IS_NULL", "OPERATOR_IS_NOT_NULL", "OPERATOR_NOT"})
_FUNCTIONS = frozenset({"+", "-", "*", "%", "abs", "lower", "upper"})
_CHECK_COLUMN_TYPES = (
    pa.types.is_int8,
    pa.types.is_int16,
    pa.types.is_int32,
    pa.types.is_int64,
    pa.types.is_float32,
    pa.types.is_float64,
)

_lock = threading.Lock()
_connection: Any = None
_unavailable = False


def _parse(sql: str) -> dict[str, Any] | None:
    """Return ``json_serialize_sql`` output, or ``None`` when no engine is installed."""
    global _connection, _unavailable
    with _lock:
        if _unavailable:
            return None
        if _connection is None:
            try:
                from vgi._duckdb import connect

                _connection = connect()
            except ImportError:
                _unavailable = True
                return None
        row = _connection.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()
    assert row is not None
    result: dict[str, Any] = json.loads(row[0])
    return result


def _single_expression(sql: str, wrapped: str, error: str) -> dict[str, Any] | None:
    """Parse ``wrapped`` and return its only select-list expression (extension ``Parse``)."""
    if not sql or "\0" in sql or len(sql.encode()) > 1024 * 1024:
        raise ValueError("invalid SQL size or text")
    parsed = _parse(wrapped)
    if parsed is None:
        return None
    if parsed.get("error"):
        raise ValueError(f"{error}: {parsed.get('error_message', 'parse error')}")
    statements = parsed.get("statements", [])
    if len(statements) != 1 or statements[0]["node"].get("type") != "SELECT_NODE":
        raise ValueError(f"{error}: expected exactly one scalar expression")
    node = statements[0]["node"]
    if (
        len(node["select_list"]) != 1
        or (node.get("from_table") or {}).get("type") != "EMPTY"
        or node.get("modifiers")
        or (node.get("cte_map") or {}).get("map")
        or node.get("where_clause") is not None
        or node.get("group_expressions")
        or node.get("group_sets")
        or node.get("having") is not None
        or node.get("qualify") is not None
        or node.get("sample") is not None
    ):
        raise ValueError(f"{error}: query clauses are unsupported")
    expression: dict[str, Any] = node["select_list"][0]
    return expression


def _children(expression: dict[str, Any]) -> list[dict[str, Any]]:
    """Mirror ``ParsedExpressionIterator::EnumerateChildren`` for the accepted classes."""
    kind = expression["class"]
    if kind == "CAST":
        return [expression["child"]]
    if kind == "COMPARISON":
        return [expression["left"], expression["right"]]
    if kind in {"CONJUNCTION", "OPERATOR", "FUNCTION"}:
        return list(expression.get("children", []))
    return []


def validate_check_sql(sql: str, schema: pa.Schema, row_id_column: int | None = None) -> None:
    """Accept exactly what ``ParseNumericCheck`` accepts for this table.

    A CHECK must be one comparison between unqualified columns of a signed
    integer or floating-point type and non-NULL numeric constants.

    Args:
        sql: The CHECK expression text.
        schema: The table's Arrow schema.
        row_id_column: Index of the ``is_row_id`` column, if any.

    Raises:
        ValueError: The extension would reject this CHECK at ATTACH.
    """
    expression = _single_expression(sql, "SELECT " + sql, "CHECK requires exactly one scalar expression")
    if expression is None:
        return
    if expression.get("alias") or expression["class"] != "COMPARISON":
        raise ValueError("the DuckDB extension reads only Boolean numeric comparisons as CHECK constraints")
    if expression["type"] not in _COMPARISONS:
        raise ValueError(f"unsupported CHECK comparison {expression['type']}")
    for operand in (expression["left"], expression["right"]):
        if operand.get("alias"):
            raise ValueError("CHECK expressions cannot have aliases")
        if operand["class"] == "CONSTANT":
            value = operand["value"]
            if value.get("is_null") or value["type"]["id"] not in _NUMERIC_TYPES:
                raise ValueError("CHECK requires numeric operands")
            continue
        if operand["class"] != "COLUMN_REF":
            raise ValueError("CHECK supports only comparisons between numeric columns and constants")
        names = operand["column_names"]
        if len(names) != 1:
            raise ValueError("CHECK requires unqualified column references")
        matches = [index for index, field in enumerate(schema) if field.name.lower() == names[0].lower()]
        if len(matches) > 1:
            raise ValueError(f"ambiguous CHECK column reference {names[0]!r}")
        if not matches or matches[0] == row_id_column:
            raise ValueError(f"CHECK references a missing or synthetic column {names[0]!r}")
        field = schema.field(matches[0])
        if b"ARROW:extension:name" in (field.metadata or {}) or isinstance(field.type, pa.BaseExtensionType):
            raise ValueError(f"CHECK column {field.name!r} has an extension type, which is unsupported")
        if not any(predicate(field.type) for predicate in _CHECK_COLUMN_TYPES):
            raise ValueError(
                f"CHECK column {field.name!r} is {field.type}; CHECK requires int8/16/32/64, float32 or float64"
            )


def validate_generated_sql(sql: str) -> None:
    """Accept exactly what the extension's generated-column ``Scalar`` walk accepts.

    Args:
        sql: The generated expression text.

    Raises:
        ValueError: The extension would reject this generated expression.
    """
    expression = _single_expression(sql, "SELECT (" + sql + ")", "generated column expression")
    if expression is None:
        return
    nodes = 0

    def visit(node: dict[str, Any], depth: int) -> None:
        nonlocal nodes
        nodes += 1
        if depth > 64 or nodes > 4096:
            raise ValueError("generated expression is too complex")
        if node.get("alias"):
            raise ValueError("generated expression aliases are unsupported")
        kind, kind_type = node["class"], node["type"]
        if kind == "CONSTANT":
            if node["value"]["type"]["id"] not in _GENERATED_PRIMITIVE:
                raise ValueError(
                    f"unsupported generated literal type {node['value']['type']['id']}; "
                    "use Boolean, signed integer, VARCHAR or NULL literals"
                )
        elif kind == "COLUMN_REF":
            if len(node["column_names"]) != 1:
                raise ValueError("generated expressions require unqualified column references")
        elif kind == "CAST":
            if node.get("try_cast") or node["cast_type"]["id"] not in _GENERATED_PRIMITIVE:
                raise ValueError("generated expressions support only CAST to Boolean, signed integer or VARCHAR")
        elif kind == "COMPARISON":
            if kind_type not in _COMPARISONS:
                raise ValueError(f"unsupported comparison {kind_type}")
        elif kind == "CONJUNCTION":
            if kind_type not in {"CONJUNCTION_AND", "CONJUNCTION_OR"}:
                raise ValueError(f"unsupported conjunction {kind_type}")
        elif kind == "OPERATOR":
            if kind_type not in _OPERATORS:
                raise ValueError(f"unsupported operator {kind_type}")
        elif kind == "FUNCTION":
            name = node["function_name"]
            if (
                node.get("schema")
                or node.get("catalog")
                or node.get("distinct")
                or node.get("filter") is not None
                or node.get("export_state")
                or (node.get("order_bys") or {}).get("orders")
                or not (name.lower() in _FUNCTIONS or name == "||")
            ):
                raise ValueError(
                    f"unsupported function {name!r}; generated expressions allow + - * % || abs lower upper"
                )
        else:
            raise ValueError("expression is outside the supported deterministic generated-column grammar")
        for child in _children(node):
            visit(child, depth + 1)

    visit(expression, 0)
