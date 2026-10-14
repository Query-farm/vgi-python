# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Typed query parameters carried entirely by a one-row Arrow batch.

The schema fixes placeholder mapping and preserves recursive logical metadata.
Preparation binds this schema, not a set of values. Every execution must supply
the same schema, including metadata, with exactly one row of values.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Self, cast

import pyarrow as pa

from vgi.typed_values import TypedValueBatch

PARAMETER_MODE_KEY = b"vgi.query.parameters.v1"
ParameterMode = Literal["named", "positional"]
MAX_PARAMETER_BYTES = 8 * 1024 * 1024
MAX_PARAMETER_SCHEMA_BYTES = 1024 * 1024
MAX_PARAMETERS = 1024


def _validate_field_metadata(field: pa.Field[Any], depth: int = 0) -> None:
    if depth > 64:
        raise ValueError("query parameter type nesting is too deep")
    if not field.equals(field.with_metadata(cast(Any, field.metadata)), check_metadata=True):
        raise ValueError("query parameter field metadata keys must be unique")
    dtype = field.type
    for i in range(dtype.num_fields):
        _validate_field_metadata(dtype.field(i), depth + 1)
    if pa.types.is_dictionary(dtype):
        _validate_field_metadata(pa.field("value", dtype.value_type), depth + 1)
    elif isinstance(dtype, pa.BaseExtensionType):
        _validate_field_metadata(pa.field("storage", dtype.storage_type), depth + 1)


def validate_parameter_schema(schema: pa.Schema) -> ParameterMode:
    """Validate mapping without interpreting or changing logical-type metadata.

    Positional parameters use ordered field names ``1`` through ``N``. Named
    parameters use their exact unprefixed placeholder names. Dialect-specific
    resolution, including case equivalence, is checked by the query provider.
    Repeated occurrences of one placeholder refer to one field. A request may
    not mix named and positional mapping modes.
    """
    if not isinstance(schema, pa.Schema):
        raise TypeError("query parameter schema must be an Arrow Schema")
    if not schema.equals(schema.with_metadata(schema.metadata), check_metadata=True):
        raise ValueError("query parameter schema metadata keys must be unique")
    for field in schema:
        _validate_field_metadata(field)
    mode = (schema.metadata or {}).get(PARAMETER_MODE_KEY)
    if mode not in {b"named", b"positional"}:
        raise ValueError("query parameter schema requires vgi.query.parameters.v1 named or positional metadata")
    if len(schema) > MAX_PARAMETERS:
        raise ValueError("too many query parameters")
    if len(set(schema.names)) != len(schema):
        raise ValueError("query parameter names must be unique")
    if any(not name or "\0" in name for name in schema.names):
        raise ValueError("query parameter names must be nonempty and contain no NUL")
    if mode == b"positional":
        if schema.names != [str(i + 1) for i in range(len(schema))]:
            raise ValueError("positional query parameter fields must be ordered 1 through N")
        return "positional"
    if any(name[0] in "$:@?" or name.isdecimal() for name in schema.names):
        raise ValueError("named query parameter fields require unprefixed nonnumeric names")
    return "named"


def read_parameter_schema(payload: bytes) -> pa.Schema:
    """Decode one bounded schema message; reject trailing bytes or batches."""
    if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_PARAMETER_SCHEMA_BYTES:
        raise ValueError("invalid query parameter schema size")
    source = pa.BufferReader(payload)
    schema = pa.ipc.read_schema(cast(Any, source))
    if source.tell() != len(payload):
        raise ValueError("query parameter schema has trailing data")
    validate_parameter_schema(schema)
    return schema


@dataclass(frozen=True, slots=True)
class QueryParameters:
    """One execution's typed values, with a self-describing placeholder map.

    Typed NULL is a supplied parameter. Missing fields, NULL IPC and zero rows
    are not parameter sets. Zero parameters are an explicit one-row, zero-column
    batch with a mapping mode. No inference, casts, key folding or SQL text
    substitution occurs in this carrier.
    """

    values: TypedValueBatch

    def __post_init__(self) -> None:
        """Require a fully validated batch and the versioned mapping metadata."""
        if not isinstance(self.values, TypedValueBatch):
            raise TypeError("query parameters require a TypedValueBatch")
        self.values.__post_init__()
        validate_parameter_schema(self.schema)

    @property
    def schema(self) -> pa.Schema:
        """Return all schema and recursive field metadata unchanged."""
        return self.values.schema

    @property
    def batch(self) -> pa.RecordBatch:
        """Return the one-row execution batch."""
        return self.values.batch

    @property
    def mode(self) -> ParameterMode:
        """Return the placeholder mapping carried by the schema."""
        return validate_parameter_schema(self.schema)

    def schema_bytes(self) -> bytes:
        """Serialize the schema alone for preparation, without parameter values."""
        payload = self.schema.serialize().to_pybytes()
        if len(payload) > MAX_PARAMETER_SCHEMA_BYTES:
            raise ValueError("query parameter schema is too large")
        return payload

    def validate_prepared_schema(self, expected: pa.Schema | bytes) -> None:
        """Require a new preparation for any schema or metadata change."""
        if isinstance(expected, bytes):
            expected = read_parameter_schema(expected)
        validate_parameter_schema(expected)
        self.values.validate_schema(expected)

    def serialize_to_bytes(self) -> bytes:
        """Write a bounded, complete one-batch IPC stream."""
        payload = self.values.serialize_to_bytes()
        if len(payload) > MAX_PARAMETER_BYTES:
            raise ValueError("query parameter batch is too large")
        self.schema_bytes()
        return payload

    @classmethod
    def deserialize_from_bytes(cls, payload: bytes, ipc_validation: Any = None) -> Self:
        """Validate size before decoding and preserve typed NULLs and metadata."""
        if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_PARAMETER_BYTES:
            raise ValueError("invalid query parameter batch size")
        result = cls(TypedValueBatch.deserialize_from_bytes(payload))
        result.schema_bytes()
        return result

    @classmethod
    def from_batch(cls, batch: pa.RecordBatch, *, mode: ParameterMode) -> Self:
        """Add a mapping mode without replacing other metadata or field types.

        An existing conflicting mode is an error, not an implicit rebinding.
        The caller supplies positional field names explicitly.
        """
        if mode not in {"named", "positional"}:
            raise ValueError("unknown query parameter mode")
        if not batch.schema.equals(batch.schema.with_metadata(batch.schema.metadata), check_metadata=True):
            raise ValueError("query parameter schema metadata keys must be unique")
        metadata = dict(batch.schema.metadata or {})
        existing = metadata.get(PARAMETER_MODE_KEY)
        if existing is not None and existing != mode.encode():
            raise ValueError("query parameter mode conflicts with schema metadata")
        metadata[PARAMETER_MODE_KEY] = mode.encode()
        return cls(TypedValueBatch(batch.replace_schema_metadata(metadata)))

    @classmethod
    def empty(cls, *, mode: ParameterMode = "positional") -> Self:
        """Represent one execution with no parameters, retaining mapping mode."""
        if mode not in {"named", "positional"}:
            raise ValueError("unknown query parameter mode")
        return cls(TypedValueBatch.empty(metadata={PARAMETER_MODE_KEY: mode.encode()}))
