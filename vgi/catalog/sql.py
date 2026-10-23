# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Explicit SQL definition carriers, independent of engine-private syntax trees.

These codecs validate representation, not SQL semantics. The receiving owner
must bind the selected profile in the enclosing operation's declared scope.
"""

from __future__ import annotations

import base64
import binascii
import re
from dataclasses import dataclass
from typing import Any, Literal, cast

import pyarrow as pa

from vgi.catalog.evaluation_context import MAX_SQL_RECORD_BYTES, QueryEvaluationContext, _SqlRecord, _text
from vgi.schema_path import SchemaPath, schema_path_key

DEFAULT_EXPRESSION_KEY = b"vgi.column.default.v1"
GENERATED_EXPRESSION_KEY = b"vgi.column.generated.v1"
GENERATION_KEY = b"vgi.column.generation.v1"
_EXPRESSION_KEYS = {DEFAULT_EXPRESSION_KEY, GENERATED_EXPRESSION_KEY, GENERATION_KEY}


def _definition(sql: str, profile: str, schema_path: SchemaPath, context: QueryEvaluationContext) -> None:
    _text(sql, "SQL definition", maximum=1024 * 1024)
    if not sql.strip():
        raise ValueError("SQL definition must be nonempty")
    if not isinstance(profile, str) or not re.fullmatch(
        r"[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\.v[1-9][0-9]*", profile
    ):
        raise ValueError("SQL profile must be a versioned semantic name")
    _text(profile, "SQL profile", maximum=256)
    schema_path_key(schema_path)
    if len(schema_path) > 32:
        raise ValueError("SQL owner schema is too deep")
    for component in schema_path:
        _text(component, "SQL owner identifier")
    if not isinstance(context, QueryEvaluationContext):
        raise TypeError("SQL definition requires an explicit evaluation context")
    context.__post_init__()


@dataclass(frozen=True, kw_only=True)
class SqlExpression(_SqlRecord):
    """One scalar expression, interpreted only by a qualified owning binder."""

    sql: str
    profile: str
    schema_path: SchemaPath
    evaluation_context: QueryEvaluationContext

    def __post_init__(self) -> None:
        """Validate the bounded carrier and required evaluation context."""
        _definition(self.sql, self.profile, self.schema_path, self.evaluation_context)

    def validate_owner(self, schema_path: SchemaPath) -> None:
        """Require the expression's raw owner path to match its operation."""
        self.__post_init__()
        if schema_path_key(self.schema_path) != schema_path_key(schema_path):
            raise ValueError("SQL expression owner schema does not match the target schema")


@dataclass(frozen=True, kw_only=True)
class SqlStatement(_SqlRecord):
    """One profiled query or DML statement; the codec grants no execution rights."""

    sql: str
    profile: str
    schema_path: SchemaPath
    evaluation_context: QueryEvaluationContext

    def __post_init__(self) -> None:
        """Validate the carrier; a receiving binder must reject scripts/control SQL."""
        _definition(self.sql, self.profile, self.schema_path, self.evaluation_context)


@dataclass(frozen=True, kw_only=True)
class ColumnExpressions:
    """Decoded authoritative default or generated metadata for one Arrow field."""

    default: SqlExpression | None = None
    generated: SqlExpression | None = None
    generation: Literal["virtual", "stored"] | None = None

    def __post_init__(self) -> None:
        """Reject mixed defaults and inconsistent generated categories."""
        for expression in (self.default, self.generated):
            if expression is not None:
                if not isinstance(expression, SqlExpression):
                    raise TypeError("column expressions require SqlExpression records")
                expression.__post_init__()
        if self.default is not None and self.generated is not None:
            raise ValueError("a generated column cannot also have a default")
        if (self.generated is None) != (self.generation is None):
            raise ValueError("generated category and expression must occur together")
        if self.generation is not None and self.generation not in {"virtual", "stored"}:
            raise ValueError("unknown generated column category")


def _metadata(field: pa.Field[Any]) -> dict[bytes, bytes]:
    if not field.equals(field.with_metadata(cast(Any, field.metadata)), check_metadata=True):
        raise ValueError("column expression metadata keys must be unique")
    metadata = field.metadata or {}
    for key in metadata:
        if (
            key.startswith((b"vgi.column.default.", b"vgi.column.generated.", b"vgi.column.generation."))
            and key not in _EXPRESSION_KEYS
        ):
            raise ValueError("unknown column expression metadata version")
    return metadata


def _decode_expression_metadata(value: bytes) -> SqlExpression:
    # Arrow metadata is a UTF-8 string in the format and in several SDKs.
    # Check the encoded bound before allocating the decoded IPC envelope.
    maximum = 4 * ((MAX_SQL_RECORD_BYTES + 2) // 3)
    if not 0 < len(value) <= maximum:
        raise ValueError("invalid column expression metadata size")
    try:
        payload = base64.b64decode(value, validate=True)
    except binascii.Error as error:
        raise ValueError("column expression metadata requires canonical padded base64") from error
    if not 0 < len(payload) <= MAX_SQL_RECORD_BYTES:
        raise ValueError("invalid column expression metadata size")
    if base64.b64encode(payload) != value:
        raise ValueError("column expression metadata requires canonical padded base64")
    return SqlExpression.deserialize_from_bytes(payload)


def column_expressions(field: pa.Field[Any]) -> ColumnExpressions:
    """Decode canonical padded base64 IPC metadata; legacy text is not a record."""
    metadata = _metadata(field)
    if _EXPRESSION_KEYS & metadata.keys() and {b"default", b"generated_expression"} & metadata.keys():
        raise ValueError("canonical column expressions conflict with legacy metadata")
    default = metadata.get(DEFAULT_EXPRESSION_KEY)
    generated = metadata.get(GENERATED_EXPRESSION_KEY)
    generation = metadata.get(GENERATION_KEY)
    if generation not in {None, b"virtual", b"stored"}:
        raise ValueError("unknown generated column category")
    return ColumnExpressions(
        default=None if default is None else _decode_expression_metadata(default),
        generated=None if generated is None else _decode_expression_metadata(generated),
        generation=None if generation is None else cast(Literal["virtual", "stored"], generation.decode()),
    )


def with_column_expressions(field: pa.Field[Any], expressions: ColumnExpressions) -> pa.Field[Any]:
    """Replace canonical expression metadata while preserving unrelated bytes."""
    expressions.__post_init__()
    column_expressions(field)
    metadata = _metadata(field)
    if {b"default", b"generated_expression"} & metadata.keys():
        raise ValueError("legacy expression metadata requires explicit translation")
    for key in _EXPRESSION_KEYS:
        metadata.pop(key, None)
    if expressions.default is not None:
        metadata[DEFAULT_EXPRESSION_KEY] = base64.b64encode(expressions.default.serialize_to_bytes())
    if expressions.generated is not None:
        metadata[GENERATED_EXPRESSION_KEY] = base64.b64encode(expressions.generated.serialize_to_bytes())
        metadata[GENERATION_KEY] = str(expressions.generation).encode()
    return field.with_metadata(cast(Any, metadata))
