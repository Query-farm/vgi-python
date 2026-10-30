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

from vgi.catalog._legacy import reject_removed_arguments
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


@reject_removed_arguments(
    {},
    positional="SqlExpression is a record under vgi.v3, not a str NewType. Use "
    "SqlExpression.scalar(sql, schema_path) or SqlExpression(sql=..., profile=ROW_SCALAR_EXPRESSION, "
    "schema_path=[...], evaluation_context=QueryEvaluationContext())",
)
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

    @classmethod
    def scalar(cls, sql: str, schema_path: SchemaPath) -> SqlExpression:
        """Build a DuckDB scalar expression owned by ``schema_path`` in the default context.

        This is the form the DuckDB extension reads for column defaults,
        generated columns and CHECK constraints.

        Args:
            sql: DuckDB scalar expression text.
            schema_path: The owning schema path.

        Returns:
            The validated expression.
        """
        from vgi.catalog.capabilities import ROW_SCALAR_EXPRESSION

        return cls(
            sql=sql,
            profile=ROW_SCALAR_EXPRESSION,
            schema_path=list(schema_path),
            evaluation_context=QueryEvaluationContext(),
        )

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


_LEGACY_GENERATED_KEY = b"generated_expression"


def canonical_generated_columns(schema: pa.Schema, schema_path: SchemaPath) -> pa.Schema:
    """Translate legacy ``generated_expression`` text metadata to the canonical record.

    The legacy key held raw SQL for a VIRTUAL generated column. When a
    top-level field carries only that key (no canonical expression keys and
    no default), it becomes a canonical [`SqlExpression`][] owned by
    ``schema_path`` with the default evaluation context. Anything else is
    ambiguous and left for validation to reject.
    """
    from vgi.catalog.capabilities import ROW_SCALAR_EXPRESSION
    from vgi.catalog.evaluation_context import QueryEvaluationContext

    fields = []
    for column_field in schema:
        metadata = dict(column_field.metadata or {})
        legacy = metadata.get(_LEGACY_GENERATED_KEY)
        if legacy is not None and not (_EXPRESSION_KEYS | {b"default"}) & metadata.keys():
            del metadata[_LEGACY_GENERATED_KEY]
            expression = SqlExpression(
                sql=legacy.decode(),
                profile=ROW_SCALAR_EXPRESSION,
                schema_path=list(schema_path),
                evaluation_context=QueryEvaluationContext(),
            )
            column_field = with_column_expressions(
                column_field.with_metadata(cast(Any, metadata)),
                ColumnExpressions(generated=expression, generation="virtual"),
            )
        fields.append(column_field)
    return pa.schema(fields, metadata=cast(Any, schema.metadata))


def _generated_type_supported(dtype: pa.DataType) -> bool:
    return (
        pa.types.is_boolean(dtype)
        or pa.types.is_int8(dtype)
        or pa.types.is_int16(dtype)
        or pa.types.is_int32(dtype)
        or pa.types.is_int64(dtype)
        or pa.types.is_string(dtype)
    )


def validate_generated_columns(schema: pa.Schema, schema_path: SchemaPath, *, table: str | None = None) -> None:
    """Apply the DuckDB extension's generated-column rules to top-level fields.

    Mirrors ``ReadNativeColumnGenerated``: VIRTUAL only, no default, the field
    nullable, no extension type, Boolean/int8-64/utf8 storage, owned by the
    table's schema with the default evaluation context, and an expression in
    the extension's deterministic grammar (checked when a DuckDB engine is
    installed).

    Args:
        schema: The table's Arrow schema (after nullability is derived).
        schema_path: The table's schema path.
        table: Table name for error messages.

    Raises:
        ValueError: DuckDB would reject the table at ATTACH.
        NotImplementedError: The column is STORED or extension-typed.
    """
    from vgi.catalog._expression_grammar import validate_generated_sql
    from vgi.catalog.capabilities import ROW_SCALAR_EXPRESSION
    from vgi.catalog.evaluation_context import QueryEvaluationContext

    owner = f"Table '{table}': " if table is not None else ""
    for column_field in schema:
        metadata = column_field.metadata or {}
        label = f"{owner}generated column {column_field.name!r}"
        if _LEGACY_GENERATED_KEY in metadata:
            raise ValueError(
                f"{label}: legacy generated_expression metadata conflicts with canonical or default metadata; "
                "use Table(generated_columns=...) or with_column_expressions()"
            )
        expressions = column_expressions(column_field)
        if expressions.generated is None:
            continue
        if expressions.generation != "virtual":
            raise NotImplementedError(f"{label}: stored generated columns are not supported; DuckDB reads only VIRTUAL")
        if b"default" in metadata:
            raise ValueError(f"{label} cannot also have a default")
        if not column_field.nullable:
            raise ValueError(
                f"{label} must be nullable; DuckDB rejects NOT NULL/PRIMARY KEY on generated columns, "
                "so do not name it in a NOT_NULL or PRIMARY_KEY record"
            )
        if isinstance(column_field.type, pa.BaseExtensionType) or b"ARROW:extension:name" in metadata:
            raise NotImplementedError(f"{label}: generated columns cannot have an extension type")
        if not _generated_type_supported(column_field.type):
            raise ValueError(f"{label} is {column_field.type}; generated columns must be bool, int8-64 or string")
        generated = expressions.generated
        if generated.profile != ROW_SCALAR_EXPRESSION:
            raise ValueError(f"{label}: expression profile must be {ROW_SCALAR_EXPRESSION}")
        if schema_path_key(generated.schema_path) != schema_path_key(schema_path):
            raise ValueError(f"{label}: expression must be owned by the table's schema {list(schema_path)!r}")
        if generated.evaluation_context != QueryEvaluationContext():
            raise ValueError(f"{label}: expression requires the default evaluation context")
        try:
            validate_generated_sql(generated.sql)
        except ValueError as error:
            raise ValueError(f"{label}: {generated.sql!r}: {error}") from error
