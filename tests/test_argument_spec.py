"""Tests for Arrow-based argument specification serialization."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from vgi.argument_spec import (
    VGI_ARG_KEY,
    VGI_ARG_NAMED,
    VGI_TYPE_ANY,
    VGI_TYPE_KEY,
    VGI_TYPE_TABLE,
    VGI_VARARGS_KEY,
    VGI_VARARGS_TRUE,
    ArgumentSpec,
    argument_specs_to_schema,
    extract_argument_specs,
    schema_to_argument_specs,
)
from vgi.arguments import AnyArrow, Arg, TableInput
from vgi.table_in_out_function import TableInOutFunction


class TestArgumentSpecToSchema:
    """Test converting ArgumentSpec objects to Arrow schema."""

    def test_positional_arguments_preserve_order(self) -> None:
        """Positional arguments should maintain their order in schema."""
        specs = [
            ArgumentSpec(name="third", position=2, arrow_type=pa.float64()),
            ArgumentSpec(name="first", position=0, arrow_type=pa.int64()),
            ArgumentSpec(name="second", position=1, arrow_type=pa.utf8()),
        ]
        schema = argument_specs_to_schema(specs)

        assert len(schema) == 3
        assert schema.field(0).name == "first"
        assert schema.field(1).name == "second"
        assert schema.field(2).name == "third"
        assert schema.field(0).type == pa.int64()
        assert schema.field(1).type == pa.utf8()
        assert schema.field(2).type == pa.float64()

    def test_named_arguments_have_metadata(self) -> None:
        """Named arguments should have vgi_arg=named metadata."""
        specs = [
            ArgumentSpec(name="format", position="format", arrow_type=pa.utf8()),
        ]
        schema = argument_specs_to_schema(specs)

        assert len(schema) == 1
        field = schema.field(0)
        assert field.name == "format"
        assert field.metadata is not None
        assert field.metadata.get(VGI_ARG_KEY) == VGI_ARG_NAMED

    def test_mixed_positional_and_named(self) -> None:
        """Mixed args should have positional first, then named."""
        specs = [
            ArgumentSpec(name="verbose", position="verbose", arrow_type=pa.bool_()),
            ArgumentSpec(name="count", position=0, arrow_type=pa.int64()),
            ArgumentSpec(name="format", position="format", arrow_type=pa.utf8()),
            ArgumentSpec(name="name", position=1, arrow_type=pa.utf8()),
        ]
        schema = argument_specs_to_schema(specs)

        # Positional come first (sorted by index)
        assert schema.field(0).name == "count"
        assert schema.field(1).name == "name"
        # Named come after (sorted alphabetically)
        assert schema.field(2).name == "format"
        assert schema.field(3).name == "verbose"

        # Named args have metadata
        assert schema.field(0).metadata is None
        assert schema.field(1).metadata is None
        field2_meta = schema.field(2).metadata
        field3_meta = schema.field(3).metadata
        assert field2_meta is not None
        assert field3_meta is not None
        assert field2_meta.get(VGI_ARG_KEY) == VGI_ARG_NAMED
        assert field3_meta.get(VGI_ARG_KEY) == VGI_ARG_NAMED

    def test_table_input_uses_null_type_and_metadata(self) -> None:
        """TableInput args should use pa.null() with vgi_type=table."""
        specs = [
            ArgumentSpec(
                name="data",
                position=0,
                arrow_type=pa.null(),
                is_table_input=True,
            ),
        ]
        schema = argument_specs_to_schema(specs)

        field = schema.field(0)
        assert field.type == pa.null()
        assert field.metadata is not None
        assert field.metadata.get(VGI_TYPE_KEY) == VGI_TYPE_TABLE

    def test_any_type_uses_null_type_and_metadata(self) -> None:
        """AnyArrow args should use pa.null() with vgi_type=any."""
        specs = [
            ArgumentSpec(
                name="value",
                position=0,
                arrow_type=pa.null(),
                is_any_type=True,
            ),
        ]
        schema = argument_specs_to_schema(specs)

        field = schema.field(0)
        assert field.type == pa.null()
        assert field.metadata is not None
        assert field.metadata.get(VGI_TYPE_KEY) == VGI_TYPE_ANY

    def test_varargs_has_metadata(self) -> None:
        """Varargs should preserve element type and have vgi_varargs=true."""
        specs = [
            ArgumentSpec(
                name="columns",
                position=0,
                arrow_type=pa.utf8(),
                is_varargs=True,
            ),
        ]
        schema = argument_specs_to_schema(specs)

        field = schema.field(0)
        assert field.type == pa.utf8()  # Element type preserved
        assert field.metadata is not None
        assert field.metadata.get(VGI_VARARGS_KEY) == VGI_VARARGS_TRUE

    def test_empty_specs(self) -> None:
        """Empty specs should produce empty schema."""
        schema = argument_specs_to_schema([])
        assert len(schema) == 0


class TestSchemaToArgumentSpecs:
    """Test converting Arrow schema back to ArgumentSpec objects."""

    def test_positional_arguments_from_schema(self) -> None:
        """Fields without named metadata should be positional."""
        fields: list[pa.Field[Any]] = [
            pa.field("a", pa.int64()),
            pa.field("b", pa.utf8()),
            pa.field("c", pa.float64()),
        ]
        schema = pa.schema(fields)
        specs = schema_to_argument_specs(schema)

        assert len(specs) == 3
        assert specs[0].position == 0
        assert specs[1].position == 1
        assert specs[2].position == 2
        assert specs[0].name == "a"
        assert specs[1].name == "b"
        assert specs[2].name == "c"

    def test_named_arguments_from_metadata(self) -> None:
        """Fields with vgi_arg=named should have string position."""
        schema = pa.schema(
            [
                pa.field("format", pa.utf8(), metadata={VGI_ARG_KEY: VGI_ARG_NAMED}),
            ]
        )
        specs = schema_to_argument_specs(schema)

        assert len(specs) == 1
        assert specs[0].position == "format"
        assert specs[0].name == "format"

    def test_table_input_detected(self) -> None:
        """vgi_type=table metadata should set is_table_input."""
        schema = pa.schema(
            [
                pa.field("data", pa.null(), metadata={VGI_TYPE_KEY: VGI_TYPE_TABLE}),
            ]
        )
        specs = schema_to_argument_specs(schema)

        assert specs[0].is_table_input is True
        assert specs[0].is_any_type is False

    def test_any_type_detected(self) -> None:
        """vgi_type=any metadata should set is_any_type."""
        schema = pa.schema(
            [
                pa.field("value", pa.null(), metadata={VGI_TYPE_KEY: VGI_TYPE_ANY}),
            ]
        )
        specs = schema_to_argument_specs(schema)

        assert specs[0].is_any_type is True
        assert specs[0].is_table_input is False

    def test_varargs_detected(self) -> None:
        """vgi_varargs=true metadata should set is_varargs."""
        schema = pa.schema(
            [
                pa.field(
                    "cols", pa.utf8(), metadata={VGI_VARARGS_KEY: VGI_VARARGS_TRUE}
                ),
            ]
        )
        specs = schema_to_argument_specs(schema)

        assert specs[0].is_varargs is True

    def test_mixed_positional_and_named_positions(self) -> None:
        """Position index should only increment for positional args."""
        fields: list[pa.Field[Any]] = [
            pa.field("a", pa.int64()),  # positional 0
            pa.field("b", pa.utf8()),  # positional 1
            pa.field("key", pa.bool_(), metadata={VGI_ARG_KEY: VGI_ARG_NAMED}),
        ]
        schema = pa.schema(fields)
        specs = schema_to_argument_specs(schema)

        assert specs[0].position == 0
        assert specs[1].position == 1
        assert specs[2].position == "key"  # named, not 2


class TestRoundTrip:
    """Test that specs survive serialization round-trip."""

    def test_simple_round_trip(self) -> None:
        """Basic specs should round-trip correctly."""
        original = [
            ArgumentSpec(name="count", position=0, arrow_type=pa.int64()),
            ArgumentSpec(name="name", position=1, arrow_type=pa.utf8()),
        ]
        schema = argument_specs_to_schema(original)
        restored = schema_to_argument_specs(schema)

        assert len(restored) == 2
        assert restored[0].name == "count"
        assert restored[0].position == 0
        assert restored[0].arrow_type == pa.int64()
        assert restored[1].name == "name"
        assert restored[1].position == 1
        assert restored[1].arrow_type == pa.utf8()

    @pytest.mark.parametrize(
        "arrow_type",
        [
            pa.int64(),
            pa.int32(),
            pa.float32(),
            pa.float64(),
            pa.utf8(),
            pa.bool_(),
            pa.binary(),
            pa.list_(pa.float64()),
            pa.struct([pa.field("a", pa.int32()), pa.field("b", pa.string())]),
            pa.map_(pa.string(), pa.int64()),
            pa.decimal128(10, 2),
            pa.timestamp("us", tz="UTC"),
            pa.date32(),
            pa.time64("us"),
            pa.duration("ms"),
        ],
    )
    def test_complex_arrow_types_preserved(self, arrow_type: pa.DataType) -> None:
        """Complex Arrow types should survive round-trip."""
        original = [ArgumentSpec(name="arg", position=0, arrow_type=arrow_type)]
        schema = argument_specs_to_schema(original)

        # Serialize to bytes and back
        schema_bytes = schema.serialize().to_pybytes()
        restored_schema = pa.ipc.read_schema(pa.py_buffer(schema_bytes))

        restored = schema_to_argument_specs(restored_schema)
        assert restored[0].arrow_type == arrow_type

    def test_full_function_signature_roundtrip(self) -> None:
        """Complete function signature should round-trip."""
        original = [
            ArgumentSpec(name="count", position=0, arrow_type=pa.int64()),
            ArgumentSpec(
                name="data", position=1, arrow_type=pa.null(), is_table_input=True
            ),
            ArgumentSpec(
                name="extra", position=2, arrow_type=pa.float64(), is_varargs=True
            ),
            ArgumentSpec(name="format", position="format", arrow_type=pa.utf8()),
            ArgumentSpec(
                name="threshold",
                position="threshold",
                arrow_type=pa.null(),
                is_any_type=True,
            ),
        ]

        schema = argument_specs_to_schema(original)

        # Full serialization round-trip
        schema_bytes = schema.serialize().to_pybytes()
        restored_schema = pa.ipc.read_schema(pa.py_buffer(schema_bytes))
        restored = schema_to_argument_specs(restored_schema)

        assert len(restored) == 5

        # Positional args
        assert restored[0].name == "count"
        assert restored[0].position == 0
        assert restored[0].arrow_type == pa.int64()

        assert restored[1].name == "data"
        assert restored[1].position == 1
        assert restored[1].is_table_input is True

        assert restored[2].name == "extra"
        assert restored[2].position == 2
        assert restored[2].is_varargs is True

        # Named args
        assert restored[3].name == "format"
        assert restored[3].position == "format"

        assert restored[4].name == "threshold"
        assert restored[4].position == "threshold"
        assert restored[4].is_any_type is True


class TestExtractArgumentSpecs:
    """Test extracting specs from function classes."""

    def test_extract_from_simple_function(self) -> None:
        """Extract specs from function with basic Arg descriptors."""

        class SimpleFunction(TableInOutFunction):
            count = Arg[int](0)
            name = Arg[str](1)

        arg_types: dict[str, pa.DataType] = {"count": pa.int64(), "name": pa.utf8()}
        specs = extract_argument_specs(SimpleFunction, arg_types)

        assert len(specs) == 2
        assert specs[0].name == "count"
        assert specs[0].position == 0
        assert specs[0].arrow_type == pa.int64()
        assert specs[1].name == "name"
        assert specs[1].position == 1
        assert specs[1].arrow_type == pa.utf8()

    def test_extract_table_input(self) -> None:
        """Extract specs should detect Arg[TableInput]."""

        class FunctionWithTable(TableInOutFunction):
            multiplier = Arg[float](0)
            data: TableInput = Arg[TableInput](1)  # type: ignore[assignment]

        arg_types: dict[str, pa.DataType] = {
            "multiplier": pa.float64(),
            "data": pa.null(),
        }
        specs = extract_argument_specs(FunctionWithTable, arg_types)

        assert len(specs) == 2
        assert specs[1].name == "data"
        assert specs[1].is_table_input is True

    def test_extract_any_arrow(self) -> None:
        """Extract specs should detect Arg[AnyArrow]."""

        class FunctionWithAny(TableInOutFunction):
            value: AnyArrow = Arg[AnyArrow](0)  # type: ignore[assignment]

        arg_types: dict[str, pa.DataType] = {"value": pa.null()}
        specs = extract_argument_specs(FunctionWithAny, arg_types)

        assert len(specs) == 1
        assert specs[0].is_any_type is True

    def test_extract_varargs(self) -> None:
        """Extract specs should detect varargs=True."""

        class FunctionWithVarargs(TableInOutFunction):
            columns = Arg[str](0, varargs=True)

        arg_types: dict[str, pa.DataType] = {"columns": pa.utf8()}
        specs = extract_argument_specs(FunctionWithVarargs, arg_types)

        assert len(specs) == 1
        assert specs[0].is_varargs is True

    def test_extract_named_arguments(self) -> None:
        """Extract specs should handle named arguments."""

        class FunctionWithNamed(TableInOutFunction):
            count = Arg[int](0)
            format = Arg[str]("format")

        arg_types: dict[str, pa.DataType] = {"count": pa.int64(), "format": pa.utf8()}
        specs = extract_argument_specs(FunctionWithNamed, arg_types)

        assert len(specs) == 2
        assert specs[0].position == 0
        assert specs[1].position == "format"

    def test_extract_mixed_arguments(self) -> None:
        """Extract specs should handle mixed positional and named args."""

        class ComplexFunction(TableInOutFunction):
            count = Arg[int](0)
            data: TableInput = Arg[TableInput](1)  # type: ignore[assignment]
            extra = Arg[float](2, varargs=True)
            format = Arg[str]("format")
            threshold: AnyArrow = Arg[AnyArrow]("threshold")  # type: ignore[assignment]

        arg_types: dict[str, pa.DataType] = {
            "count": pa.int64(),
            "data": pa.null(),
            "extra": pa.float64(),
            "format": pa.utf8(),
            "threshold": pa.null(),
        }
        specs = extract_argument_specs(ComplexFunction, arg_types)

        assert len(specs) == 5

        # Positional first
        assert specs[0].name == "count"
        assert specs[0].position == 0

        assert specs[1].name == "data"
        assert specs[1].position == 1
        assert specs[1].is_table_input is True

        assert specs[2].name == "extra"
        assert specs[2].position == 2
        assert specs[2].is_varargs is True

        # Named after
        assert specs[3].name == "format"
        assert specs[3].position == "format"

        assert specs[4].name == "threshold"
        assert specs[4].position == "threshold"
        assert specs[4].is_any_type is True


class TestArgumentSpecToSchemaValidation:
    """Test validation in argument_specs_to_schema."""

    def test_non_contiguous_indices_warns(self) -> None:
        """Non-contiguous positional indices should issue a warning."""
        specs = [
            ArgumentSpec(name="first", position=0, arrow_type=pa.int64()),
            ArgumentSpec(name="third", position=2, arrow_type=pa.int64()),  # Gap: no 1
        ]
        with pytest.warns(UserWarning, match="not contiguous"):
            argument_specs_to_schema(specs)

    def test_indices_not_starting_at_zero_warns(self) -> None:
        """Positional indices not starting at 0 should warn."""
        specs = [
            ArgumentSpec(name="second", position=1, arrow_type=pa.int64()),
            ArgumentSpec(name="third", position=2, arrow_type=pa.int64()),
        ]
        with pytest.warns(UserWarning, match="not contiguous"):
            argument_specs_to_schema(specs)

    def test_contiguous_indices_no_warning(self) -> None:
        """Contiguous positional indices should not warn."""
        specs = [
            ArgumentSpec(name="first", position=0, arrow_type=pa.int64()),
            ArgumentSpec(name="second", position=1, arrow_type=pa.int64()),
            ArgumentSpec(name="third", position=2, arrow_type=pa.int64()),
        ]
        # Should not raise any warnings
        import warnings as w

        with w.catch_warnings(record=True) as caught:
            w.simplefilter("always")
            argument_specs_to_schema(specs)
            # Filter for our specific warning
            contiguity_warnings = [
                x for x in caught if "not contiguous" in str(x.message)
            ]
            assert len(contiguity_warnings) == 0


class TestExtractArgumentSpecsValidation:
    """Test validation in extract_argument_specs."""

    def test_missing_arg_type_warns(self) -> None:
        """Missing arg_types entry should issue a warning."""

        class FunctionWithArg(TableInOutFunction):
            count = Arg[int](0)

        # Provide empty arg_types - should warn
        with pytest.warns(UserWarning, match="Missing type for argument 'count'"):
            specs = extract_argument_specs(FunctionWithArg, {})

        assert len(specs) == 1
        assert specs[0].arrow_type == pa.null()

    def test_partial_arg_types_warns_for_missing(self) -> None:
        """Only missing args should trigger warnings."""

        class FunctionWithTwoArgs(TableInOutFunction):
            count = Arg[int](0)
            name = Arg[str](1)

        # Provide type for only 'count'
        with pytest.warns(UserWarning, match="Missing type for argument 'name'"):
            specs = extract_argument_specs(FunctionWithTwoArgs, {"count": pa.int64()})

        assert specs[0].arrow_type == pa.int64()
        assert specs[1].arrow_type == pa.null()


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_schema_roundtrip(self) -> None:
        """Empty specs/schema should round-trip."""
        schema = argument_specs_to_schema([])
        assert len(schema) == 0

        specs = schema_to_argument_specs(schema)
        assert specs == []

    def test_only_named_arguments(self) -> None:
        """Function with only named arguments should work."""
        specs = [
            ArgumentSpec(name="format", position="format", arrow_type=pa.utf8()),
            ArgumentSpec(name="verbose", position="verbose", arrow_type=pa.bool_()),
        ]
        schema = argument_specs_to_schema(specs)
        restored = schema_to_argument_specs(schema)

        assert len(restored) == 2
        assert all(isinstance(s.position, str) for s in restored)

    def test_only_positional_arguments(self) -> None:
        """Function with only positional arguments should work."""
        specs = [
            ArgumentSpec(name="a", position=0, arrow_type=pa.int64()),
            ArgumentSpec(name="b", position=1, arrow_type=pa.utf8()),
        ]
        schema = argument_specs_to_schema(specs)
        restored = schema_to_argument_specs(schema)

        assert len(restored) == 2
        assert all(isinstance(s.position, int) for s in restored)

    def test_combined_metadata(self) -> None:
        """Named argument with special type should have both metadata keys."""
        specs = [
            ArgumentSpec(
                name="threshold",
                position="threshold",
                arrow_type=pa.null(),
                is_any_type=True,
            ),
        ]
        schema = argument_specs_to_schema(specs)

        field = schema.field(0)
        assert field.metadata is not None
        assert field.metadata.get(VGI_ARG_KEY) == VGI_ARG_NAMED
        assert field.metadata.get(VGI_TYPE_KEY) == VGI_TYPE_ANY


class TestArgumentSpecRepr:
    """Test ArgumentSpec __repr__ method."""

    def test_positional_argument_repr(self) -> None:
        """Positional argument should show integer position."""
        spec = ArgumentSpec(name="count", position=0, arrow_type=pa.int64())
        result = repr(spec)
        assert 'name="count"' in result
        assert "pos=0" in result
        assert "int64" in result
        assert "flags=" not in result  # No flags when all False

    def test_named_argument_repr(self) -> None:
        """Named argument should show quoted string position."""
        spec = ArgumentSpec(name="format", position="format", arrow_type=pa.utf8())
        result = repr(spec)
        assert 'name="format"' in result
        assert 'pos="format"' in result
        assert "string" in result or "utf8" in result

    def test_flags_shown_when_true(self) -> None:
        """Flags should only appear when True."""
        spec = ArgumentSpec(
            name="data",
            position=0,
            arrow_type=pa.null(),
            is_table_input=True,
        )
        result = repr(spec)
        assert "flags=[table_input]" in result

    def test_multiple_flags(self) -> None:
        """Multiple flags should all be shown."""
        spec = ArgumentSpec(
            name="cols",
            position=0,
            arrow_type=pa.utf8(),
            is_varargs=True,
        )
        result = repr(spec)
        assert "varargs" in result

    def test_any_type_flag(self) -> None:
        """any_type flag should be shown."""
        spec = ArgumentSpec(
            name="value",
            position=0,
            arrow_type=pa.null(),
            is_any_type=True,
        )
        result = repr(spec)
        assert "any_type" in result
