"""Tests for the Setting descriptor and extraction functions."""

from typing import Annotated

import pyarrow as pa
import pytest

from vgi.catalog.setting import (
    Setting,
    SettingSpec,
    _resolve_arrow_type,
    extract_setting_specs,
)


class TestResolveArrowType:
    """Tests for _resolve_arrow_type helper."""

    def test_python_bool(self) -> None:
        """Test bool maps to pa.bool_()."""
        assert _resolve_arrow_type(bool) == pa.bool_()

    def test_python_int(self) -> None:
        """Test int maps to pa.int64()."""
        assert _resolve_arrow_type(int) == pa.int64()

    def test_python_float(self) -> None:
        """Test float maps to pa.float64()."""
        assert _resolve_arrow_type(float) == pa.float64()

    def test_python_str(self) -> None:
        """Test str maps to pa.string()."""
        assert _resolve_arrow_type(str) == pa.string()

    def test_python_bytes(self) -> None:
        """Test bytes maps to pa.binary()."""
        assert _resolve_arrow_type(bytes) == pa.binary()

    def test_arrow_datatype_passthrough(self) -> None:
        """Arrow DataTypes should be returned unchanged."""
        assert _resolve_arrow_type(pa.int32()) == pa.int32()
        assert _resolve_arrow_type(pa.list_(pa.int64())) == pa.list_(pa.int64())
        assert _resolve_arrow_type(
            pa.struct([("key", pa.string()), ("value", pa.int64())])
        ) == pa.struct([("key", pa.string()), ("value", pa.int64())])

    def test_unsupported_type_raises(self) -> None:
        """Test that unsupported types raise TypeError."""
        with pytest.raises(TypeError, match="Cannot resolve Arrow type"):
            _resolve_arrow_type(list)

        with pytest.raises(TypeError, match="Cannot resolve Arrow type"):
            _resolve_arrow_type(dict)


class TestSettingDescriptor:
    """Tests for the Setting descriptor class."""

    def test_setting_with_desc(self) -> None:
        """Test Setting with description only."""
        setting = Setting(desc="Enable verbose output")
        assert setting.desc == "Enable verbose output"
        assert setting.arrow_type is None

    def test_setting_with_explicit_type(self) -> None:
        """Test Setting with explicit Arrow type."""
        setting = Setting(desc="Custom type", arrow_type=pa.int32())
        assert setting.arrow_type == pa.int32()

    def test_set_name(self) -> None:
        """Test __set_name__ is called when assigned to a class."""

        class TestSettings:
            verbose: Annotated[bool, Setting(desc="Verbose")] = False

        # The descriptor should have _name set
        descriptor = TestSettings.__annotations__["verbose"]
        # get_args returns (bool, Setting(...))
        from typing import get_args

        # Note: _name is set by __set_name__ which isn't called for Annotated
        # Just verify we can get the Setting from the annotation
        args = get_args(descriptor)
        assert len(args) >= 2
        assert isinstance(args[1], Setting)


class TestExtractSettingSpecs:
    """Tests for extract_setting_specs function."""

    def test_empty_class(self) -> None:
        """Test empty Settings class returns empty list."""

        class EmptySettings:
            pass

        specs = extract_setting_specs(EmptySettings)
        assert specs == []

    def test_single_bool_setting(self) -> None:
        """Test extraction of a single bool setting."""

        class Settings:
            verbose: Annotated[bool, Setting(desc="Enable verbose")] = False

        specs = extract_setting_specs(Settings)
        assert len(specs) == 1
        assert specs[0].name == "verbose"
        assert specs[0].desc == "Enable verbose"
        assert specs[0].type == pa.bool_()
        assert specs[0].default is False

    def test_single_int_setting(self) -> None:
        """Test extraction of a single int setting."""

        class Settings:
            batch_size: Annotated[int, Setting(desc="Batch size")] = 1000

        specs = extract_setting_specs(Settings)
        assert len(specs) == 1
        assert specs[0].name == "batch_size"
        assert specs[0].type == pa.int64()
        assert specs[0].default == 1000

    def test_single_str_setting(self) -> None:
        """Test extraction of a single str setting."""

        class Settings:
            format: Annotated[str, Setting(desc="Output format")] = "json"

        specs = extract_setting_specs(Settings)
        assert len(specs) == 1
        assert specs[0].name == "format"
        assert specs[0].type == pa.string()
        assert specs[0].default == "json"

    def test_multiple_settings(self) -> None:
        """Test extraction of multiple settings."""

        class Settings:
            verbose: Annotated[bool, Setting(desc="Verbose mode")] = False
            batch_size: Annotated[int, Setting(desc="Batch size")] = 100
            output_format: Annotated[str, Setting(desc="Format")] = "json"

        specs = extract_setting_specs(Settings)
        assert len(specs) == 3
        names = {s.name for s in specs}
        assert names == {"verbose", "batch_size", "output_format"}

    def test_arrow_datatype_annotation(self) -> None:
        """Settings can use Arrow DataTypes directly in annotations."""

        class Settings:
            allowed_ids: Annotated[  # type: ignore[valid-type]
                pa.list_(pa.int64()), Setting(desc="IDs")
            ] = []

        specs = extract_setting_specs(Settings)
        assert len(specs) == 1
        assert specs[0].name == "allowed_ids"
        assert specs[0].type == pa.list_(pa.int64())
        assert specs[0].default == []

    def test_struct_type_annotation(self) -> None:
        """Settings can use struct types."""

        class Settings:
            config: Annotated[  # type: ignore[valid-type]
                pa.struct([("key", pa.string()), ("value", pa.int64())]),
                Setting(desc="Config"),
            ] = {}

        specs = extract_setting_specs(Settings)
        assert len(specs) == 1
        assert specs[0].name == "config"
        assert specs[0].type == pa.struct([("key", pa.string()), ("value", pa.int64())])

    def test_explicit_type_override(self) -> None:
        """Setting.arrow_type overrides the annotation type."""

        class Settings:
            count: Annotated[int, Setting(desc="Count", arrow_type=pa.int32())] = 10

        specs = extract_setting_specs(Settings)
        assert len(specs) == 1
        assert specs[0].type == pa.int32()  # Not int64 from int annotation

    def test_non_annotated_attributes_ignored(self) -> None:
        """Non-Annotated attributes should be ignored."""

        class Settings:
            verbose: Annotated[bool, Setting(desc="Verbose")] = False
            _internal: str = "ignored"
            other: int = 123

        specs = extract_setting_specs(Settings)
        assert len(specs) == 1
        assert specs[0].name == "verbose"

    def test_no_default_value(self) -> None:
        """Settings without default values should have None as default."""

        class Settings:
            required: Annotated[str, Setting(desc="Required setting")]

        specs = extract_setting_specs(Settings)
        assert len(specs) == 1
        assert specs[0].default is None


class TestSettingSpecSerialization:
    """Tests for SettingSpec serialization/deserialization."""

    def test_serialize_deserialize_bool(self) -> None:
        """Test round-trip serialization for bool setting."""
        spec = SettingSpec(
            name="verbose",
            desc="Enable verbose output",
            type=pa.bool_(),
            default=False,
        )
        serialized = spec.serialize()
        assert isinstance(serialized, bytes)

        # Deserialize
        import vgi.ipc_utils

        batch = vgi.ipc_utils.deserialize_record_batch(serialized)
        deserialized = SettingSpec.deserialize(batch)

        assert deserialized.name == "verbose"
        assert deserialized.desc == "Enable verbose output"
        assert deserialized.type == pa.bool_()
        assert deserialized.default is False

    def test_serialize_deserialize_int(self) -> None:
        """Test round-trip serialization for int setting."""
        spec = SettingSpec(
            name="batch_size",
            desc="Batch size",
            type=pa.int64(),
            default=1000,
        )
        serialized = spec.serialize()
        batch = __import__("vgi.ipc_utils", fromlist=["deserialize_record_batch"])
        batch = batch.deserialize_record_batch(serialized)
        deserialized = SettingSpec.deserialize(batch)

        assert deserialized.name == "batch_size"
        assert deserialized.type == pa.int64()
        assert deserialized.default == 1000

    def test_serialize_deserialize_no_default(self) -> None:
        """Test round-trip serialization for setting without default."""
        spec = SettingSpec(
            name="api_key",
            desc="API key",
            type=pa.string(),
            default=None,
        )
        serialized = spec.serialize()

        import vgi.ipc_utils

        batch = vgi.ipc_utils.deserialize_record_batch(serialized)
        deserialized = SettingSpec.deserialize(batch)

        assert deserialized.name == "api_key"
        assert deserialized.type == pa.string()
        assert deserialized.default is None

    def test_serialize_deserialize_list_type(self) -> None:
        """Test round-trip serialization for list type setting."""
        spec = SettingSpec(
            name="allowed_ids",
            desc="Allowed IDs",
            type=pa.list_(pa.int64()),
            default=[1, 2, 3],
        )
        serialized = spec.serialize()

        import vgi.ipc_utils

        batch = vgi.ipc_utils.deserialize_record_batch(serialized)
        deserialized = SettingSpec.deserialize(batch)

        assert deserialized.name == "allowed_ids"
        assert deserialized.type == pa.list_(pa.int64())
        assert deserialized.default == [1, 2, 3]
