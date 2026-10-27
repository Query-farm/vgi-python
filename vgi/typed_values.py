# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""One-row Arrow values that retain field and schema metadata.

Use this carrier when each column is one typed argument or option. A typed NULL
is a present value; an absent field is a different operation. Interpretation of
logical-type metadata belongs to the consuming protocol and its capabilities.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Self, cast

import pyarrow as pa


def _validate_scalar(value: pa.Scalar[Any] | None, definition: pa.Field[Any]) -> None:
    """Check declared nullability, including children of present nested values."""
    if isinstance(value, pa.UnionScalar):
        # PyArrow's stub declares type_code as str; the runtime returns int.
        child = value.type[value.type.type_codes.index(cast(int, value.type_code))]
        _validate_scalar(value.value, child)
        return
    if isinstance(value, (pa.DictionaryScalar, pa.ExtensionScalar, pa.RunEndEncodedScalar)):
        decoded = cast("pa.Scalar[Any] | None", value.value)
        _validate_scalar(decoded, definition.with_type(decoded.type) if decoded is not None else definition)
        return
    if value is None or not value.is_valid:
        if not definition.nullable:
            raise ValueError(f"NULL supplied for non-nullable field {definition.name!r}")
        return
    if isinstance(value, pa.StructScalar):
        for index, child in enumerate(value.type):
            _validate_scalar(cast(Any, value)[index], child)
    elif isinstance(value, pa.MapScalar):
        # MapScalar inherits ListScalar, but has key/item fields rather than a
        # list's value_field, so it must be handled first.
        assert value.values is not None
        for pair in value.values:
            _validate_scalar(pair[0], value.type.key_field)
            _validate_scalar(pair[1], value.type.item_field)
    elif isinstance(
        value, (pa.ListScalar, pa.LargeListScalar, pa.FixedSizeListScalar, pa.ListViewScalar, pa.LargeListViewScalar)
    ):
        assert value.values is not None
        for item in value.values:
            _validate_scalar(item, value.type.value_field)


@dataclass(frozen=True, slots=True)
class TypedValueBatch:
    """Exactly one row of typed values, with the original Arrow schema intact.

    Field names must be unique, nonempty, and contain no NUL. Zero columns are
    valid and still represent one set of values. This class preserves metadata;
    it does not infer or claim support for the logical types described by it.

    Attributes:
        batch: The single record batch containing the values.
    """

    batch: pa.RecordBatch

    def __post_init__(self) -> None:
        """Validate shape, names, Arrow buffers, and declared nullability."""
        if not isinstance(self.batch, pa.RecordBatch):
            raise TypeError("typed values require an Arrow RecordBatch")
        if self.batch.num_rows != 1:
            raise ValueError("typed values must contain exactly one row")
        if len(set(self.schema.names)) != self.batch.num_columns:
            raise ValueError("typed value field names must be unique")
        if any(not name or "\x00" in name for name in self.schema.names):
            raise ValueError("typed value field names must be nonempty and contain no NUL")
        self.batch.validate(full=True)
        for index, definition in enumerate(self.schema):
            _validate_scalar(self.batch.column(index)[0], definition)

    @property
    def schema(self) -> pa.Schema:
        """Return the original schema, including recursive field metadata."""
        return self.batch.schema

    def validate_schema(self, expected: pa.Schema) -> None:
        """Require identical names, order, types, nullability, and metadata.

        Values can change between executions of a prepared operation. A schema
        change requires an explicit new binding; this helper performs no casts.
        """
        if not self.schema.equals(expected, check_metadata=True):
            raise ValueError("typed values do not match the expected schema and metadata")

    def serialize_to_bytes(self) -> bytes:
        """Encode one batch as an IPC stream, preserving its complete schema."""
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, self.schema) as writer:
            writer.write_batch(self.batch)
        return sink.getvalue().to_pybytes()

    @classmethod
    def deserialize_from_bytes(cls, payload: bytes, ipc_validation: Any = None) -> Self:
        """Decode exactly one batch, rejecting extra batches and trailing bytes.

        ``ipc_validation`` is accepted for vgi-rpc's custom-codec interface.
        Typed values always receive full Arrow and nullability validation.
        Transport request-size budgets remain the caller's responsibility.
        """
        source = pa.BufferReader(payload)
        try:
            with pa.ipc.open_stream(source) as reader:
                try:
                    batch = reader.read_next_batch()
                except StopIteration as exc:
                    raise ValueError("typed value IPC contains no batch") from exc
                try:
                    reader.read_next_batch()
                except StopIteration:
                    pass
                else:
                    raise ValueError("typed value IPC must contain exactly one batch")
                if source.tell() != len(payload):
                    raise ValueError("typed value IPC has trailing data")
        except OSError as exc:
            # PyArrow can report a truncated in-memory IPC body as OSError.
            # This is malformed caller data, not a filesystem/server failure.
            raise ValueError("typed value IPC is malformed or truncated") from exc
        return cls(batch)

    @classmethod
    def empty(cls, *, metadata: Mapping[bytes, bytes] | None = None) -> Self:
        """Create a zero-column batch that still contains one set of values."""
        array = cast(pa.StructArray, pa.array([{}], type=pa.struct([])))
        batch = pa.RecordBatch.from_struct_array(array)
        return cls(batch.replace_schema_metadata(dict(metadata) if metadata is not None else None))
