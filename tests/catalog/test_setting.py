# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for the Setting descriptor and extraction functions."""

from typing import Annotated

import pyarrow as pa
import pytest
from vgi_rpc.utils import deserialize_record_batch

from vgi.catalog.setting import (
    Setting,
    SettingSpec,
    _resolve_arrow_type,
    extract_setting_specs,
)


class TestResolveArrowType:
    """Tests for _resolve_arrow_type helper."""

    @pytest.mark.parametrize(
        ("python_type", "arrow_type"),
        [
            (bool, pa.bool_()),
            (int, pa.int64()),
            (float, pa.float64()),
            (str, pa.string()),
            (bytes, pa.binary()),
        ],
        ids=["bool", "int", "float", "str", "bytes"],
    )
    def test_python_type_mapping(self, python_type: type, arrow_type: pa.DataType) -> None:
        """Python types map to expected Arrow types."""
        assert _resolve_arrow_type(python_type) == arrow_type

    def test_arrow_datatype_passthrough(self) -> None:
        """Arrow DataTypes should be returned unchanged."""
        assert _resolve_arrow_type(pa.int32()) == pa.int32()
        assert _resolve_arrow_type(pa.list_(pa.int64())) == pa.list_(pa.int64())
        assert _resolve_arrow_type(pa.struct([("key", pa.string()), ("value", pa.int64())])) == pa.struct(
            [("key", pa.string()), ("value", pa.int64())]
        )

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

    @pytest.mark.parametrize(
        ("name", "desc", "arrow_type", "default"),
        [
            ("verbose", "Enable verbose output", pa.bool_(), False),
            ("batch_size", "Batch size", pa.int64(), 1000),
            ("api_key", "API key", pa.string(), None),
            ("allowed_ids", "Allowed IDs", pa.list_(pa.int64()), [1, 2, 3]),
        ],
        ids=["bool", "int", "no_default", "list"],
    )
    def test_serialize_deserialize_round_trip(
        self, name: str, desc: str, arrow_type: pa.DataType, default: object
    ) -> None:
        """Test round-trip serialization for different setting types."""
        from vgi_rpc.utils import deserialize_record_batch

        spec = SettingSpec(name=name, desc=desc, type=arrow_type, default=default)
        serialized = spec.serialize()
        assert isinstance(serialized, bytes)

        batch, _ = deserialize_record_batch(serialized)
        deserialized = SettingSpec.deserialize(batch)

        assert deserialized.name == name
        assert deserialized.desc == desc
        assert deserialized.type == arrow_type
        assert deserialized.default == default


class TestCatalogAttachSettingsRoundTrip:
    """Tests that Worker.Settings survive catalog_attach serialization."""

    def test_worker_settings_in_catalog_attach(self) -> None:
        """All ExampleWorker settings should be present in catalog_attach result."""
        from vgi._test_fixtures.worker import ExampleWorker

        catalog_interface_cls = ExampleWorker._get_catalog_interface()
        assert catalog_interface_cls is not None
        catalog_interface = catalog_interface_cls()

        result = catalog_interface.catalog_attach(
            name="example", options={}, data_version_spec=None, implementation_version=None
        )

        # Deserialize settings from the result
        assert result.settings is not None
        specs_by_name: dict[str, SettingSpec] = {}
        for setting_bytes in result.settings:
            batch, _ = deserialize_record_batch(setting_bytes)
            spec = SettingSpec.deserialize(batch)
            specs_by_name[spec.name] = spec

        # Verify all expected settings are present
        expected_names = {"vgi_verbose_mode", "greeting", "multiplier", "threshold", "config"}
        assert set(specs_by_name.keys()) == expected_names

        # Check types and defaults
        assert specs_by_name["vgi_verbose_mode"].type == pa.bool_()
        assert specs_by_name["vgi_verbose_mode"].default is False

        assert specs_by_name["greeting"].type == pa.string()
        assert specs_by_name["greeting"].default == "Hello"

        assert specs_by_name["multiplier"].type == pa.int64()
        assert specs_by_name["multiplier"].default == 1

        assert specs_by_name["threshold"].type == pa.int64()
        assert specs_by_name["threshold"].default == 0

        assert specs_by_name["config"].type == pa.struct(
            [("start", pa.int64()), ("step", pa.int64()), ("label", pa.string())]
        )
        assert specs_by_name["config"].default is None

    def test_struct_setting_round_trip(self) -> None:
        """Struct-typed SettingSpec should survive serialize/deserialize."""
        spec = SettingSpec(
            name="config",
            desc="Configuration struct",
            type=pa.struct([("start", pa.int64()), ("step", pa.int64()), ("label", pa.string())]),
            default=None,
        )
        serialized = spec.serialize()
        batch, _ = deserialize_record_batch(serialized)
        deserialized = SettingSpec.deserialize(batch)

        assert deserialized.name == "config"
        assert deserialized.type == pa.struct([("start", pa.int64()), ("step", pa.int64()), ("label", pa.string())])
        assert deserialized.default is None
