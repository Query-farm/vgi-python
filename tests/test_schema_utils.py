# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for vgi.schema_utils module."""

from __future__ import annotations

import pyarrow as pa
import pytest

from tests.conftest import make_schema
from vgi import schema, schema_like


class TestSchema:
    """Tests for the schema() helper function."""

    def test_empty_schema(self) -> None:
        """schema() with no arguments returns empty schema."""
        s = schema()
        assert s == pa.schema([])

    def test_single_field(self) -> None:
        """schema() with one field."""
        s = schema(x=pa.int64())
        assert s == make_schema([pa.field("x", pa.int64())])

    def test_multiple_fields(self) -> None:
        """schema() with multiple fields preserves order."""
        s = schema(a=pa.int64(), b=pa.string(), c=pa.float64())
        expected = make_schema(
            [
                pa.field("a", pa.int64()),
                pa.field("b", pa.string()),
                pa.field("c", pa.float64()),
            ]
        )
        assert s == expected

    def test_from_dict(self) -> None:
        """schema() accepts a dict as first positional argument."""
        fields: dict[str, pa.DataType] = {"x": pa.int64(), "y": pa.string()}
        s = schema(fields)
        expected = make_schema(
            [
                pa.field("x", pa.int64()),
                pa.field("y", pa.string()),
            ]
        )
        assert s == expected

    def test_dict_plus_kwargs(self) -> None:
        """schema() combines dict and kwargs."""
        s = schema({"a": pa.int64()}, b=pa.string())
        expected = make_schema(
            [
                pa.field("a", pa.int64()),
                pa.field("b", pa.string()),
            ]
        )
        assert s == expected

    def test_kwargs_override_dict(self) -> None:
        """Keyword args override dict values for same key."""
        s = schema({"x": pa.int64()}, x=pa.string())
        assert s == make_schema([pa.field("x", pa.string())])

    def test_common_types(self) -> None:
        """schema() works with common Arrow types."""
        s = schema(
            int_col=pa.int64(),
            float_col=pa.float64(),
            str_col=pa.string(),
            bool_col=pa.bool_(),
            binary_col=pa.binary(),
            ts_col=pa.timestamp("us"),
            list_col=pa.list_(pa.int64()),
        )
        assert len(s) == 7
        assert s.field("int_col").type == pa.int64()
        assert s.field("float_col").type == pa.float64()
        assert s.field("str_col").type == pa.string()
        assert s.field("bool_col").type == pa.bool_()
        assert s.field("binary_col").type == pa.binary()
        assert s.field("ts_col").type == pa.timestamp("us")
        assert s.field("list_col").type == pa.list_(pa.int64())

    def test_invalid_type_raises(self) -> None:
        """schema() raises TypeError for non-DataType values."""
        with pytest.raises(TypeError) as exc_info:
            schema(x="int64")  # type: ignore[arg-type]  # Intentionally invalid
        assert "Field 'x'" in str(exc_info.value)
        assert "expected pa.DataType" in str(exc_info.value)
        assert "got str" in str(exc_info.value)

    def test_invalid_type_in_dict_raises(self) -> None:
        """schema() raises TypeError for invalid types in dict."""
        with pytest.raises(TypeError) as exc_info:
            schema({"x": 123})  # type: ignore[dict-item]  # Intentionally invalid
        assert "Field 'x'" in str(exc_info.value)

    def test_field_with_metadata(self) -> None:
        """schema() accepts (DataType, metadata) tuples."""
        s = schema(
            row_id=(pa.int64(), {b"is_row_id": b""}),
            name=pa.string(),
        )
        assert s.names == ["row_id", "name"]
        assert s.field("row_id").type == pa.int64()
        assert s.field("row_id").metadata == {b"is_row_id": b""}
        assert s.field("name").metadata is None

    def test_field_metadata_mixed_with_plain(self) -> None:
        """schema() mixes metadata and plain fields."""
        s = schema(
            a=pa.int64(),
            b=(pa.string(), {b"key": b"val"}),
            c=pa.float64(),
        )
        assert len(s) == 3
        assert s.field("a").metadata is None
        assert s.field("b").metadata == {b"key": b"val"}
        assert s.field("c").metadata is None

    def test_field_metadata_invalid_type_in_tuple(self) -> None:
        """schema() raises TypeError for invalid type in tuple."""
        with pytest.raises(TypeError) as exc_info:
            schema(x=("not_a_type", {}))  # type: ignore[arg-type]  # Intentionally invalid
        assert "Field 'x'" in str(exc_info.value)
        assert "first tuple element" in str(exc_info.value)


class TestSchemaLike:
    """Tests for the schema_like() helper function."""

    @pytest.fixture
    def base_schema(self) -> pa.Schema:
        """Fixture providing a base schema for tests."""
        return make_schema(
            [
                pa.field("a", pa.int64()),
                pa.field("b", pa.string()),
                pa.field("c", pa.float64()),
            ]
        )

    def test_passthrough(self, base_schema: pa.Schema) -> None:
        """schema_like() with no modifications returns equivalent schema."""
        result = schema_like(base_schema)
        assert result == base_schema

    def test_add_single_field(self, base_schema: pa.Schema) -> None:
        """schema_like() adds field at the end."""
        result = schema_like(base_schema, add={"d": pa.bool_()})
        expected = make_schema(
            [
                pa.field("a", pa.int64()),
                pa.field("b", pa.string()),
                pa.field("c", pa.float64()),
                pa.field("d", pa.bool_()),
            ]
        )
        assert result == expected

    def test_add_multiple_fields(self, base_schema: pa.Schema) -> None:
        """schema_like() adds multiple fields."""
        result = schema_like(base_schema, add={"d": pa.bool_(), "e": pa.int32()})
        assert len(result) == 5
        assert result.field("d").type == pa.bool_()
        assert result.field("e").type == pa.int32()

    def test_remove_single_field(self, base_schema: pa.Schema) -> None:
        """schema_like() removes a field."""
        result = schema_like(base_schema, remove=["b"])
        expected = make_schema(
            [
                pa.field("a", pa.int64()),
                pa.field("c", pa.float64()),
            ]
        )
        assert result == expected

    def test_remove_multiple_fields(self, base_schema: pa.Schema) -> None:
        """schema_like() removes multiple fields."""
        result = schema_like(base_schema, remove=["a", "c"])
        expected = make_schema([pa.field("b", pa.string())])
        assert result == expected

    def test_rename_single_field(self, base_schema: pa.Schema) -> None:
        """schema_like() renames a field."""
        result = schema_like(base_schema, rename={"a": "alpha"})
        expected = make_schema(
            [
                pa.field("alpha", pa.int64()),
                pa.field("b", pa.string()),
                pa.field("c", pa.float64()),
            ]
        )
        assert result == expected

    def test_rename_multiple_fields(self, base_schema: pa.Schema) -> None:
        """schema_like() renames multiple fields."""
        result = schema_like(base_schema, rename={"a": "x", "b": "y"})
        assert result.names == ["x", "y", "c"]

    def test_replace_type(self, base_schema: pa.Schema) -> None:
        """schema_like() replaces a field's type."""
        result = schema_like(base_schema, replace={"a": pa.int32()})
        expected = make_schema(
            [
                pa.field("a", pa.int32()),
                pa.field("b", pa.string()),
                pa.field("c", pa.float64()),
            ]
        )
        assert result == expected

    def test_replace_preserves_position(self, base_schema: pa.Schema) -> None:
        """schema_like() replace keeps field in original position."""
        result = schema_like(base_schema, replace={"b": pa.binary()})
        assert result.names == ["a", "b", "c"]
        assert result.field("b").type == pa.binary()

    def test_combined_operations(self, base_schema: pa.Schema) -> None:
        """schema_like() applies all operations correctly."""
        result = schema_like(
            base_schema,
            remove=["c"],
            rename={"a": "id"},
            replace={"b": pa.large_string()},
            add={"new_col": pa.bool_()},
        )
        expected = make_schema(
            [
                pa.field("id", pa.int64()),
                pa.field("b", pa.large_string()),
                pa.field("new_col", pa.bool_()),
            ]
        )
        assert result == expected

    def test_operation_order(self, base_schema: pa.Schema) -> None:
        """Operations apply in order: remove -> rename -> replace -> add."""
        # Remove 'a', rename 'b' to 'a', then add a new 'b'
        result = schema_like(
            base_schema,
            remove=["a"],
            rename={"b": "a"},
            add={"b": pa.bool_()},
        )
        expected = make_schema(
            [
                pa.field("a", pa.string()),  # was 'b', renamed to 'a'
                pa.field("c", pa.float64()),
                pa.field("b", pa.bool_()),  # new field
            ]
        )
        assert result == expected

    def test_remove_nonexistent_raises(self, base_schema: pa.Schema) -> None:
        """schema_like() raises KeyError for removing nonexistent field."""
        with pytest.raises(KeyError) as exc_info:
            schema_like(base_schema, remove=["nonexistent"])
        assert "Cannot remove field 'nonexistent'" in str(exc_info.value)
        assert "not found in schema" in str(exc_info.value)

    def test_rename_nonexistent_raises(self, base_schema: pa.Schema) -> None:
        """schema_like() raises KeyError for renaming nonexistent field."""
        with pytest.raises(KeyError) as exc_info:
            schema_like(base_schema, rename={"nonexistent": "new"})
        assert "Cannot rename field 'nonexistent'" in str(exc_info.value)

    def test_replace_nonexistent_raises(self, base_schema: pa.Schema) -> None:
        """schema_like() raises KeyError for replacing nonexistent field."""
        with pytest.raises(KeyError) as exc_info:
            schema_like(base_schema, replace={"nonexistent": pa.int64()})
        assert "Cannot replace field 'nonexistent'" in str(exc_info.value)

    def test_add_existing_raises(self, base_schema: pa.Schema) -> None:
        """schema_like() raises ValueError for adding existing field."""
        with pytest.raises(ValueError) as exc_info:
            schema_like(base_schema, add={"a": pa.int64()})
        assert "Cannot add field 'a'" in str(exc_info.value)
        assert "already exists" in str(exc_info.value)

    def test_add_invalid_type_raises(self, base_schema: pa.Schema) -> None:
        """schema_like() raises TypeError for invalid add type."""
        with pytest.raises(TypeError) as exc_info:
            schema_like(base_schema, add={"d": "int64"})  # type: ignore[dict-item]  # Intentionally invalid
        assert "Field 'd'" in str(exc_info.value)
        assert "expected pa.DataType" in str(exc_info.value)
