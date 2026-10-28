# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Required contexts, portable column metadata and bounded deterministic role proof."""

from __future__ import annotations

import base64
from collections.abc import Callable
from dataclasses import replace
from typing import Any, cast

import pyarrow as pa
import pytest

from vgi.catalog import sql as sql_module
from vgi.catalog.capabilities import ROW_SCALAR_EXPRESSION
from vgi.catalog.evaluation_context import QueryEvaluationContext
from vgi.catalog.macros import MACRO_DEFAULT_PROFILE, MACRO_PROFILE, MacroOverload, MacroParameter
from vgi.catalog.query import QueryEvaluationContext as QueryContextCompatibilityImport
from vgi.catalog.sql import (
    DEFAULT_EXPRESSION_KEY,
    GENERATED_EXPRESSION_KEY,
    GENERATION_KEY,
    ColumnExpressions,
    SqlExpression,
    SqlStatement,
    column_expressions,
    with_column_expressions,
)

from .expression_reference import evaluate_expression, evaluate_generated

OWNER = ["a.b", "nested"]
CONTEXT = QueryEvaluationContext()


def expression(sql: str, **changes: Any) -> SqlExpression:
    return SqlExpression(
        sql=sql,
        profile=ROW_SCALAR_EXPRESSION,
        schema_path=OWNER,
        evaluation_context=CONTEXT,
        **changes,
    )


def test_context_is_required_and_the_query_identity_and_digest_are_unchanged() -> None:
    assert QueryContextCompatibilityImport is QueryEvaluationContext
    assert CONTEXT.digest("duckdb.sql.v1", "vgi.remote.readonly.v1").hex() == (
        "0d33dfb516d67c8096fb174f0ae08a04f028ad5669e4416cde2973a2dd352d34"
    )
    for record in [SqlExpression, SqlStatement]:
        with pytest.raises(TypeError, match="evaluation_context"):
            cast("Callable[..., SqlExpression | SqlStatement]", record)(
                sql="42", profile="duckdb.scalar.v1", schema_path=OWNER
            )
        definition = record(sql="42", profile="duckdb.scalar.v1", schema_path=OWNER, evaluation_context=CONTEXT)
        assert record.deserialize_from_bytes(definition.serialize_to_bytes()) == definition
        with pytest.raises(ValueError, match="trailing"):
            record.deserialize_from_bytes(definition.serialize_to_bytes() + b"trailing")
        with pa.ipc.open_stream(definition.serialize_to_bytes()) as reader:
            batch = reader.read_next_batch()
        with pytest.raises(ValueError, match="complete schema"):
            record.deserialize_from_batch(batch.select(["sql", "profile", "schema_path"]))


def test_macro_default_owner_and_context_must_equal_its_body_context() -> None:
    default = replace(expression("42"), profile=MACRO_DEFAULT_PROFILE)
    overload = MacroOverload(
        parameters=[MacroParameter(name="x", default_expression=default)],
        definition="x + 1",
        profile=MACRO_PROFILE,
        schema_path=OWNER,
        macro_type="scalar",
        evaluation_context=CONTEXT,
    )
    assert MacroOverload.deserialize_from_bytes(overload.serialize_to_bytes()) == overload
    with pytest.raises(ValueError, match="evaluation context"):
        replace(overload, evaluation_context=replace(CONTEXT, timezone="Europe/Paris"))
    with pytest.raises(ValueError, match="owner schema"):
        replace(overload, schema_path=["other"])


def test_column_expression_metadata_preserves_base64_ipc_and_unrelated_metadata() -> None:
    field = pa.field("a.b", pa.int64(), metadata={b"custom": "caf\u00e9".encode(), b"comment": b""})
    typed_null = ColumnExpressions(default=expression("CAST(NULL AS BIGINT)"))
    encoded = with_column_expressions(field, typed_null)
    restored = pa.ipc.read_schema(pa.py_buffer(pa.schema([encoded]).serialize())).field(0)
    assert restored.equals(encoded, check_metadata=True)
    assert column_expressions(restored) == typed_null
    assert restored.metadata is not None
    assert restored.metadata[b"custom"] == "caf\u00e9".encode()
    assert restored.metadata[DEFAULT_EXPRESSION_KEY].isascii()
    assert typed_null.default is not None
    assert base64.b64decode(restored.metadata[DEFAULT_EXPRESSION_KEY], validate=True) == (
        typed_null.default.serialize_to_bytes()
    )
    assert column_expressions(field).default is None
    assert with_column_expressions(encoded, ColumnExpressions()).equals(field, check_metadata=True)
    generated = ColumnExpressions(generated=expression('"a.b" * 2'), generation="virtual")
    assert column_expressions(with_column_expressions(field, generated)) == generated


def test_column_expression_metadata_rejects_collisions_unknown_versions_and_bad_unions() -> None:
    raw_payload = expression("42").serialize_to_bytes()
    payload = base64.b64encode(raw_payload)
    for metadata in [
        {DEFAULT_EXPRESSION_KEY: payload, b"default": b"42"},
        {GENERATED_EXPRESSION_KEY: payload},
        {GENERATION_KEY: b"virtual"},
        {GENERATED_EXPRESSION_KEY: payload, GENERATION_KEY: b"mystery"},
        {DEFAULT_EXPRESSION_KEY: payload, GENERATED_EXPRESSION_KEY: payload, GENERATION_KEY: b"virtual"},
        {b"vgi.column.default.v2": payload},
        {DEFAULT_EXPRESSION_KEY: base64.b64encode(raw_payload + b"extra")},
        cast("Callable[..., pa.KeyValueMetadata]", pa.KeyValueMetadata)(
            [(DEFAULT_EXPRESSION_KEY, payload), (DEFAULT_EXPRESSION_KEY, payload)]
        ),
    ]:
        with pytest.raises(ValueError):
            column_expressions(cast("Callable[..., pa.Field[Any]]", pa.field)("x", pa.int64(), metadata=metadata))


@pytest.mark.parametrize("payload", [b"AA", b"AA==\n", b"AAAA====", b"AB==", b"AA-_", b"!!!!", b"\xff"])
def test_column_expression_metadata_rejects_noncanonical_base64(payload: bytes) -> None:
    with pytest.raises(ValueError, match="canonical padded base64"):
        column_expressions(pa.field("x", pa.int64(), metadata={DEFAULT_EXPRESSION_KEY: payload}))


def test_column_expression_metadata_rejects_raw_ipc_and_bounds_both_encodings(monkeypatch: pytest.MonkeyPatch) -> None:
    raw_payload = expression("42").serialize_to_bytes()
    with pytest.raises(ValueError, match="canonical padded base64"):
        column_expressions(pa.field("x", pa.int64(), metadata={DEFAULT_EXPRESSION_KEY: raw_payload}))
    monkeypatch.setattr(sql_module, "MAX_SQL_RECORD_BYTES", 1)
    # The first exceeds the encoded bound; the second fits that bound but
    # decodes to two bytes, exceeding the decoded bound of one byte.
    for payload in [base64.b64encode(raw_payload), b"AAA="]:
        with pytest.raises(ValueError, match="metadata size"):
            column_expressions(pa.field("x", pa.int64(), metadata={DEFAULT_EXPRESSION_KEY: payload}))


def test_check_preserves_true_false_and_null_and_default_has_no_row_scope() -> None:
    table = pa.table({"n": pa.array([-1, 0, None], type=pa.int64())})
    assert evaluate_expression(expression("n >= 0"), table, OWNER, role="check").to_pylist() == [False, True, None]
    assert evaluate_expression(expression("40 + 2"), table, OWNER, role="default").to_pylist() == [42, 42, 42]
    with pytest.raises(ValueError, match="column reference"):
        evaluate_expression(expression("n + 1"), table, OWNER, role="default")
    assert evaluate_expression(expression("CAST(n AS INTEGER)"), table, OWNER, role="using").to_pylist() == [
        -1,
        0,
        None,
    ]


def test_generated_dependencies_bind_in_order_and_cycles_reject_before_publication() -> None:
    table = pa.table({"n": pa.array([2, None], type=pa.int64())})
    generated = evaluate_generated(table, {"last": expression("twice + 1"), "twice": expression("n * 2")}, OWNER)
    assert generated["last"].to_pylist() == [5, None]
    assert table.schema.names == ["n"]
    with pytest.raises(ValueError, match="cycle"):
        evaluate_generated(table, {"x": expression("y + 1"), "y": expression("x + 1")}, OWNER)


@pytest.mark.parametrize("sql", ["random()", "(SELECT 1)", "$value", "sum(n)", "n OVER ()", "main.abs(n)"])
def test_unqualified_expression_forms_reject(sql: str) -> None:
    with pytest.raises(ValueError):
        evaluate_expression(expression(sql), pa.table({"n": [1]}), OWNER, role="generated")


def test_reference_rejects_unqualified_context_and_type_without_implicit_fallback() -> None:
    with pytest.raises(ValueError, match="context"):
        evaluate_expression(
            replace(expression("n"), evaluation_context=replace(CONTEXT, collation="nocase")),
            pa.table({"n": [1]}),
            OWNER,
            role="using",
        )
    with pytest.raises(ValueError, match="primitive"):
        evaluate_expression(expression("n"), pa.table({"n": [1.5]}), OWNER, role="using")
