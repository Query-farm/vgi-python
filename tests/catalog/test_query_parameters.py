# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103
"""Parameter mapping and exact rebind boundaries in the Arrow batch carrier."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import Any, Literal, cast

import pyarrow as pa
import pytest

from vgi.query_parameters import (
    PARAMETER_MODE_KEY,
    QueryParameters,
    read_parameter_schema,
    validate_parameter_schema,
)
from vgi.typed_values import TypedValueBatch


def parameters(value: int | None = None) -> QueryParameters:
    fields = [
        pa.field("limit", pa.int32(), metadata={b"logical": b"integer"}),
        pa.field(
            "record",
            pa.struct([pa.field("a.b", pa.string(), nullable=False, metadata={b"logical": b"literal-name"})]),
            nullable=False,
        ),
    ]
    batch = pa.RecordBatch.from_pylist(
        [{"limit": value, "record": {"a.b": "kept"}}], schema=pa.schema(fields, metadata={b"provider": b"query"})
    )
    return QueryParameters.from_batch(batch, mode="named")


def test_typed_null_values_and_recursive_metadata_survive_rebinding() -> None:
    prepared = parameters().schema_bytes()
    expected = read_parameter_schema(prepared)
    for value in [None, 7, -4]:
        source = parameters(value)
        result = QueryParameters.deserialize_from_bytes(source.serialize_to_bytes())
        result.validate_prepared_schema(prepared)
        assert result.schema.equals(expected, check_metadata=True)
        assert result.batch.column("limit")[0].as_py() == value
        assert result.batch.column("record")[0].as_py() == {"a.b": "kept"}
        assert result.mode == "named"


@pytest.mark.parametrize("mode", ["named", "positional"])
def test_zero_parameters_are_one_row_and_mode_is_retained(mode: Literal["named", "positional"]) -> None:
    value = QueryParameters.empty(mode=mode)
    copy = QueryParameters.deserialize_from_bytes(value.serialize_to_bytes())
    assert copy.batch.num_rows == 1
    assert copy.batch.num_columns == 0
    assert copy.mode == mode
    copy.validate_prepared_schema(value.schema_bytes())


def test_positional_fields_map_explicitly_without_casts() -> None:
    value = QueryParameters.from_batch(
        pa.record_batch([pa.array([None], type=pa.int16()), pa.array([b"value"], type=pa.binary())], names=["1", "2"]),
        mode="positional",
    )
    assert value.mode == "positional"
    assert value.schema.field(0).type == pa.int16()
    assert value.batch.column(0)[0].as_py() is None
    assert value.batch.column(1)[0].as_py() == b"value"


@pytest.mark.parametrize(
    ("mode", "names"),
    [
        ("positional", ["0"]),
        ("positional", ["1", "3"]),
        ("positional", ["2", "1"]),
        ("positional", ["01"]),
        ("positional", ["name"]),
        ("named", ["1"]),
        ("named", ["$name"]),
        ("named", [":name"]),
        ("named", ["?name"]),
        ("named", ["@name"]),
        ("named", ["name", "name"]),
        ("named", [""]),
        ("named", ["bad\0name"]),
    ],
)
def test_invalid_placeholder_mapping_rejects(mode: str, names: list[str]) -> None:
    schema = pa.schema([pa.field(n, pa.int32()) for n in names], metadata={PARAMETER_MODE_KEY: mode.encode()})
    with pytest.raises(ValueError):
        validate_parameter_schema(schema)


@pytest.mark.parametrize("value", [None, b"", b"NAMED", b"unknown"])
def test_absent_or_unknown_mapping_is_not_guessed(value: bytes | None) -> None:
    schema = pa.schema([pa.field("n", pa.int64())], metadata=None if value is None else {PARAMETER_MODE_KEY: value})
    with pytest.raises(ValueError, match="requires"):
        validate_parameter_schema(schema)


def test_named_spelling_is_preserved_for_provider_resolution() -> None:
    # Whether these names are equivalent depends on the selected SQL dialect.
    schema = pa.schema([pa.field(n, pa.int32()) for n in ["Name", "name", "naïve"]])
    value = QueryParameters.from_batch(pa.record_batch([[1], [2], [3]], schema=schema), mode="named")
    assert value.schema.names == ["Name", "name", "naïve"]


@pytest.mark.parametrize("change", ["type", "name", "order", "nullability", "schema_metadata", "nested_metadata"])
def test_changed_schema_requires_new_preparation(change: str) -> None:
    source = parameters(7)
    expected = source.schema
    if change == "type":
        expected = expected.set(0, expected.field(0).with_type(pa.int64()))
    elif change == "name":
        expected = expected.set(0, expected.field(0).with_name("LIMIT"))
    elif change == "order":
        expected = pa.schema(
            list(reversed(list(expected))), metadata={k: v for k, v in (expected.metadata or {}).items()}
        )
    elif change == "nullability":
        expected = expected.set(0, expected.field(0).with_nullable(False))
    elif change == "schema_metadata":
        expected = expected.with_metadata({**expected.metadata, b"provider": b"changed"})
    else:
        nested = expected.field(1)
        expected = expected.set(
            1, nested.with_type(pa.struct([nested.type.field(0).with_metadata({b"logical": b"other"})]))
        )
    with pytest.raises(ValueError, match="expected schema"):
        source.validate_prepared_schema(expected)


def test_conflicting_mapping_and_missing_typed_values_reject() -> None:
    source = parameters()
    with pytest.raises(ValueError, match="conflicts"):
        QueryParameters.from_batch(source.batch, mode="positional")
    with pytest.raises(TypeError, match="TypedValueBatch"):
        replace(source, values=cast(TypedValueBatch, source.batch))
    with pytest.raises(ValueError, match="exactly one row"):
        QueryParameters.from_batch(source.batch.slice(0, 0), mode="named")


@pytest.mark.parametrize("location", ["schema", "field", "nested", "dictionary"])
def test_duplicate_metadata_cannot_ambiguously_change_interpretation(location: str) -> None:
    metadata = cast("Callable[..., pa.KeyValueMetadata]", pa.KeyValueMetadata)([("logical", "one"), ("logical", "two")])
    field = cast("Callable[..., pa.Field[Any]]", pa.field)("n", pa.int32(), metadata=metadata)
    if location == "nested":
        field = pa.field("n", pa.list_(field))
    elif location == "dictionary":
        field = pa.field("n", cast("Callable[..., pa.DataType]", pa.dictionary)(pa.int8(), pa.struct([field])))
    if location == "schema":
        schema = cast("Callable[..., pa.Schema]", pa.schema)(
            [],
            metadata=cast("Callable[..., pa.KeyValueMetadata]", pa.KeyValueMetadata)(
                [(PARAMETER_MODE_KEY, b"named"), (PARAMETER_MODE_KEY, b"positional")]
            ),
        )
    else:
        schema = pa.schema([field], metadata={PARAMETER_MODE_KEY: b"named"})
    with pytest.raises(ValueError, match="metadata keys must be unique"):
        validate_parameter_schema(schema)


def test_schema_ipc_is_not_a_value_batch_and_both_reject_trailing_data() -> None:
    source = parameters()
    for suffix in [b"extra", source.schema_bytes()]:
        with pytest.raises(ValueError, match="trailing"):
            read_parameter_schema(source.schema_bytes() + suffix)
        with pytest.raises(ValueError, match="trailing"):
            QueryParameters.deserialize_from_bytes(source.serialize_to_bytes() + suffix)
    with pytest.raises(ValueError, match="trailing"):
        read_parameter_schema(source.serialize_to_bytes())
    with pytest.raises(ValueError, match="no batch"):
        QueryParameters.deserialize_from_bytes(source.schema_bytes())


def test_builder_rejects_duplicate_mapping_keys_before_copying_metadata() -> None:
    schema = cast("Callable[..., pa.Schema]", pa.schema)(
        [pa.field("n", pa.int32())],
        metadata=cast("Callable[..., pa.KeyValueMetadata]", pa.KeyValueMetadata)(
            [(PARAMETER_MODE_KEY, b"named"), (PARAMETER_MODE_KEY, b"positional")]
        ),
    )
    with pytest.raises(ValueError, match="metadata keys must be unique"):
        QueryParameters.from_batch(pa.record_batch([[1]], schema=schema), mode="named")


def test_size_and_parameter_count_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    source = parameters()
    monkeypatch.setattr("vgi.query_parameters.MAX_PARAMETERS", 1)
    with pytest.raises(ValueError, match="too many"):
        validate_parameter_schema(source.schema)
    monkeypatch.setattr("vgi.query_parameters.MAX_PARAMETERS", 1024)
    monkeypatch.setattr("vgi.query_parameters.MAX_PARAMETER_SCHEMA_BYTES", 1)
    with pytest.raises(ValueError, match="size"):
        read_parameter_schema(source.schema.serialize().to_pybytes())
    with pytest.raises(ValueError, match="too large"):
        source.schema_bytes()
    monkeypatch.setattr("vgi.query_parameters.MAX_PARAMETER_BYTES", 1)
    with pytest.raises(ValueError, match="size"):
        QueryParameters.deserialize_from_bytes(b"too large")
    with pytest.raises(ValueError, match="too large"):
        source.serialize_to_bytes()
