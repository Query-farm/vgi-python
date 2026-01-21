"""Tests for catalog dataclass serialization/deserialization."""

import pyarrow as pa

from vgi import schema
from vgi.catalog import (
    AttachId,
    CatalogAttachResult,
    FunctionInfo,
    FunctionType,
    ScanFunctionResult,
    SchemaInfo,
    SerializedSchema,
    SettingSpec,
    TableInfo,
    ViewInfo,
)
from vgi.ipc_utils import deserialize_record_batch


class TestCatalogAttachResultSerialization:
    """Test CatalogAttachResult serialization round-trip."""

    def test_basic_round_trip(self) -> None:
        """Test basic serialization and deserialization."""
        original = CatalogAttachResult(
            attach_id=AttachId(b"\x01\x02\x03\x04"),
            supports_transactions=True,
            supports_time_travel=False,
            catalog_version_frozen=False,
            catalog_version=42,
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = CatalogAttachResult.deserialize(batch)

        assert restored.attach_id == original.attach_id
        assert restored.supports_transactions == original.supports_transactions
        assert restored.supports_time_travel == original.supports_time_travel
        assert restored.catalog_version_frozen == original.catalog_version_frozen
        assert restored.catalog_version == original.catalog_version

    def test_empty_attach_id(self) -> None:
        """Test with empty attach_id bytes."""
        original = CatalogAttachResult(
            attach_id=AttachId(b""),
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=True,
            catalog_version=0,
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = CatalogAttachResult.deserialize(batch)

        assert restored.attach_id == b""

    def test_all_flags_true(self) -> None:
        """Test with all boolean flags set to true."""
        original = CatalogAttachResult(
            attach_id=AttachId(b"test"),
            supports_transactions=True,
            supports_time_travel=True,
            catalog_version_frozen=True,
            catalog_version=999,
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = CatalogAttachResult.deserialize(batch)

        assert restored.supports_transactions is True
        assert restored.supports_time_travel is True
        assert restored.catalog_version_frozen is True

    def test_with_settings(self) -> None:
        """Test with settings list."""
        # Create some serialized ExtensionOption bytes
        option_bytes_1 = b"serialized_option_1"
        option_bytes_2 = b"serialized_option_2"

        original = CatalogAttachResult(
            attach_id=AttachId(b"\x01\x02"),
            supports_transactions=True,
            supports_time_travel=False,
            catalog_version_frozen=False,
            catalog_version=1,
            settings=[option_bytes_1, option_bytes_2],
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = CatalogAttachResult.deserialize(batch)

        assert len(restored.settings) == 2
        assert restored.settings[0] == option_bytes_1
        assert restored.settings[1] == option_bytes_2

    def test_with_empty_settings(self) -> None:
        """Test with empty settings list."""
        original = CatalogAttachResult(
            attach_id=AttachId(b"\x01\x02"),
            supports_transactions=True,
            supports_time_travel=False,
            catalog_version_frozen=False,
            catalog_version=1,
            settings=[],
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = CatalogAttachResult.deserialize(batch)

        assert restored.settings == []


class TestSchemaInfoSerialization:
    """Test SchemaInfo serialization round-trip."""

    def test_basic_round_trip(self) -> None:
        """Test basic serialization and deserialization."""
        original = SchemaInfo(
            attach_id=AttachId(b"\x01\x02\x03\x04"),
            name="main",
            comment="Test schema",
            tags={"env": "test", "owner": "alice"},
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = SchemaInfo.deserialize(batch)

        assert restored.attach_id == original.attach_id
        assert restored.name == original.name
        assert restored.comment == original.comment
        assert restored.tags == original.tags

    def test_none_comment(self) -> None:
        """Test with None comment."""
        original = SchemaInfo(
            attach_id=AttachId(b"test"),
            name="schema1",
            comment=None,
            tags={},
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = SchemaInfo.deserialize(batch)

        assert restored.comment is None

    def test_empty_tags(self) -> None:
        """Test with empty tags dictionary."""
        original = SchemaInfo(
            attach_id=AttachId(b"test"),
            name="schema1",
            comment="Comment",
            tags={},
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = SchemaInfo.deserialize(batch)

        assert restored.tags == {}

    def test_empty_string_name(self) -> None:
        """Test with empty string name."""
        original = SchemaInfo(
            attach_id=AttachId(b"test"),
            name="",
            comment=None,
            tags={},
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = SchemaInfo.deserialize(batch)

        assert restored.name == ""


class TestTableInfoSerialization:
    """Test TableInfo serialization round-trip."""

    def test_basic_round_trip(self) -> None:
        """Test basic serialization and deserialization."""
        columns_schema = schema(id=pa.int64(), name=pa.string())
        columns_bytes = SerializedSchema(columns_schema.serialize().to_pybytes())

        original = TableInfo(
            name="users",
            schema_name="main",
            columns=columns_bytes,
            not_null_constraints=[0],
            unique_constraints=[[0]],
            check_constraints=["id > 0"],
            comment="Users table",
            tags={"category": "core"},
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = TableInfo.deserialize(batch)

        assert restored.name == original.name
        assert restored.schema_name == original.schema_name
        assert restored.columns == original.columns
        assert restored.not_null_constraints == original.not_null_constraints
        assert restored.unique_constraints == original.unique_constraints
        assert restored.check_constraints == original.check_constraints
        assert restored.comment == original.comment
        assert restored.tags == original.tags

    def test_empty_constraints(self) -> None:
        """Test with empty constraint lists."""
        columns_schema = schema(x=pa.int32())
        columns_bytes = SerializedSchema(columns_schema.serialize().to_pybytes())

        original = TableInfo(
            name="simple",
            schema_name="main",
            columns=columns_bytes,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
            comment=None,
            tags={},
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = TableInfo.deserialize(batch)

        assert restored.not_null_constraints == []
        assert restored.unique_constraints == []
        assert restored.check_constraints == []

    def test_multiple_unique_constraints(self) -> None:
        """Test with multiple unique constraints on multiple columns."""
        columns_schema = schema(a=pa.int32(), b=pa.int32(), c=pa.int32())
        columns_bytes = SerializedSchema(columns_schema.serialize().to_pybytes())

        original = TableInfo(
            name="multi",
            schema_name="main",
            columns=columns_bytes,
            not_null_constraints=[0, 1],
            unique_constraints=[[0], [1, 2]],
            check_constraints=["a > 0", "b < 100"],
            comment=None,
            tags={},
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = TableInfo.deserialize(batch)

        assert restored.unique_constraints == [[0], [1, 2]]


class TestViewInfoSerialization:
    """Test ViewInfo serialization round-trip."""

    def test_basic_round_trip(self) -> None:
        """Test basic serialization and deserialization."""
        original = ViewInfo(
            name="user_summary",
            schema_name="main",
            definition="SELECT id, name FROM users",
            comment="Summary view",
            tags={"type": "summary"},
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = ViewInfo.deserialize(batch)

        assert restored.name == original.name
        assert restored.schema_name == original.schema_name
        assert restored.definition == original.definition
        assert restored.comment == original.comment
        assert restored.tags == original.tags

    def test_complex_definition(self) -> None:
        """Test with complex SQL definition."""
        original = ViewInfo(
            name="complex",
            schema_name="analytics",
            definition="""
                SELECT u.id, u.name, COUNT(o.id) as order_count
                FROM users u
                LEFT JOIN orders o ON u.id = o.user_id
                WHERE u.active = true
                GROUP BY u.id, u.name
                HAVING COUNT(o.id) > 0
            """,
            comment=None,
            tags={},
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = ViewInfo.deserialize(batch)

        assert restored.definition == original.definition


class TestFunctionInfoSerialization:
    """Test FunctionInfo serialization round-trip."""

    def test_scalar_function(self) -> None:
        """Test with scalar function type."""
        args_schema = schema(value=pa.int64())
        output_schema = schema(result=pa.int64())

        original = FunctionInfo(
            name="double",
            schema_name="main",
            function_type=FunctionType.SCALAR,
            arguments=SerializedSchema(args_schema.serialize().to_pybytes()),
            output_schema=SerializedSchema(output_schema.serialize().to_pybytes()),
            comment="Double the input",
            tags={"category": "math"},
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize(batch)

        assert restored.name == original.name
        assert restored.function_type == FunctionType.SCALAR
        assert restored.arguments == original.arguments
        assert restored.output_schema == original.output_schema

    def test_table_function(self) -> None:
        """Test with table function type."""
        args_schema = schema(count=pa.int32())
        output_schema = schema(n=pa.int64())

        original = FunctionInfo(
            name="sequence",
            schema_name="main",
            function_type=FunctionType.TABLE,
            arguments=SerializedSchema(args_schema.serialize().to_pybytes()),
            output_schema=SerializedSchema(output_schema.serialize().to_pybytes()),
            comment=None,
            tags={},
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize(batch)

        assert restored.function_type == FunctionType.TABLE

    def test_examples_with_descriptions(self) -> None:
        """Test structured examples with descriptions."""
        from vgi.catalog import CatalogExample

        args_schema = schema(value=pa.int64())
        output_schema = schema(result=pa.int64())

        original = FunctionInfo(
            name="double",
            schema_name="main",
            function_type=FunctionType.SCALAR,
            arguments=SerializedSchema(args_schema.serialize().to_pybytes()),
            output_schema=SerializedSchema(output_schema.serialize().to_pybytes()),
            comment="Double the input",
            tags={},
            examples=[
                CatalogExample(
                    sql="SELECT double(5)",
                    description="Double the number 5",
                    expected_output="10",
                ),
                CatalogExample(
                    sql="SELECT double(-3)",
                    description="Double a negative number",
                    expected_output=None,
                ),
            ],
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize(batch)

        assert len(restored.examples) == 2
        ex0 = restored.examples[0]
        ex1 = restored.examples[1]
        assert isinstance(ex0, CatalogExample)
        assert isinstance(ex1, CatalogExample)
        assert ex0.sql == "SELECT double(5)"
        assert ex0.description == "Double the number 5"
        assert ex0.expected_output == "10"
        assert ex1.sql == "SELECT double(-3)"
        assert ex1.description == "Double a negative number"
        assert ex1.expected_output is None

    def test_examples_empty_descriptions(self) -> None:
        """Test examples with empty descriptions."""
        from vgi.catalog import CatalogExample

        args_schema = schema(value=pa.int64())
        output_schema = schema(result=pa.int64())

        original = FunctionInfo(
            name="echo",
            schema_name="main",
            function_type=FunctionType.SCALAR,
            arguments=SerializedSchema(args_schema.serialize().to_pybytes()),
            output_schema=SerializedSchema(output_schema.serialize().to_pybytes()),
            comment=None,
            tags={},
            examples=[
                CatalogExample(sql="SELECT echo('hi')", description=""),
            ],
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize(batch)

        assert len(restored.examples) == 1
        ex = restored.examples[0]
        assert isinstance(ex, CatalogExample)
        assert ex.sql == "SELECT echo('hi')"
        assert ex.description == ""

    def test_examples_empty_list(self) -> None:
        """Test with no examples."""
        args_schema = schema(value=pa.int64())
        output_schema = schema(result=pa.int64())

        original = FunctionInfo(
            name="test_func",
            schema_name="main",
            function_type=FunctionType.SCALAR,
            arguments=SerializedSchema(args_schema.serialize().to_pybytes()),
            output_schema=SerializedSchema(output_schema.serialize().to_pybytes()),
            comment=None,
            tags={},
            examples=[],
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize(batch)

        assert restored.examples == []

    def test_tags_dict_serialization(self) -> None:
        """Test tags dict serialization and deserialization."""
        args_schema = schema(value=pa.int64())
        output_schema = schema(result=pa.int64())

        original = FunctionInfo(
            name="tagged_func",
            schema_name="main",
            function_type=FunctionType.SCALAR,
            arguments=SerializedSchema(args_schema.serialize().to_pybytes()),
            output_schema=SerializedSchema(output_schema.serialize().to_pybytes()),
            comment=None,
            tags={"category": "math", "type": "scalar", "version": "1.0"},
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize(batch)

        assert restored.tags == {"category": "math", "type": "scalar", "version": "1.0"}

    def test_empty_tags_dict(self) -> None:
        """Test with empty tags dict."""
        args_schema = schema(value=pa.int64())
        output_schema = schema(result=pa.int64())

        original = FunctionInfo(
            name="no_tags_func",
            schema_name="main",
            function_type=FunctionType.SCALAR,
            arguments=SerializedSchema(args_schema.serialize().to_pybytes()),
            output_schema=SerializedSchema(output_schema.serialize().to_pybytes()),
            comment=None,
            tags={},
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize(batch)

        assert restored.tags == {}


class TestScanFunctionResultSerialization:
    """Test ScanFunctionResult serialization round-trip.

    ScanFunctionResult allows the VGI DuckDB extension to call any DuckDB
    function with specified arguments and load required extensions.
    """

    def test_basic_round_trip(self) -> None:
        """Test basic serialization and deserialization."""
        original = ScanFunctionResult(
            function_name="read_parquet",
            positional_arguments=[pa.scalar("data.parquet")],
            named_arguments={"hive_partitioning": pa.scalar(True)},
            required_extensions=["parquet"],
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = ScanFunctionResult.deserialize(batch)

        assert restored.function_name == original.function_name
        assert len(restored.positional_arguments) == 1
        assert restored.positional_arguments[0].as_py() == "data.parquet"
        assert "hive_partitioning" in restored.named_arguments
        assert restored.named_arguments["hive_partitioning"].as_py() is True
        assert restored.required_extensions == ["parquet"]

    def test_multiple_positional_args(self) -> None:
        """Test with multiple positional arguments of different types."""
        original = ScanFunctionResult(
            function_name="my_function",
            positional_arguments=[
                pa.scalar(42),
                pa.scalar("hello"),
                pa.scalar(3.14),
            ],
            named_arguments={},
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = ScanFunctionResult.deserialize(batch)

        assert restored.function_name == original.function_name
        assert len(restored.positional_arguments) == 3
        assert restored.positional_arguments[0].as_py() == 42
        assert restored.positional_arguments[1].as_py() == "hello"
        assert restored.positional_arguments[2].as_py() == 3.14

    def test_empty_args(self) -> None:
        """Test with no arguments."""
        original = ScanFunctionResult(
            function_name="simple_function",
            positional_arguments=[],
            named_arguments={},
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = ScanFunctionResult.deserialize(batch)

        assert restored.function_name == original.function_name
        assert len(restored.positional_arguments) == 0
        assert len(restored.named_arguments) == 0
        assert restored.required_extensions == []

    def test_multiple_extensions(self) -> None:
        """Test with multiple required extensions."""
        original = ScanFunctionResult(
            function_name="complex_scan",
            positional_arguments=[pa.scalar("path")],
            named_arguments={},
            required_extensions=["iceberg", "parquet", "httpfs"],
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = ScanFunctionResult.deserialize(batch)

        assert restored.required_extensions == ["iceberg", "parquet", "httpfs"]


class TestSettingSpecSerialization:
    """Test SettingSpec serialization round-trip."""

    def test_basic_round_trip_explicit_type(self) -> None:
        """Test with explicit type and no default (required setting)."""
        original = SettingSpec(
            name="vgi_api_key",
            desc="API key for auth",
            type=pa.string(),
            default=None,
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = SettingSpec.deserialize(batch)

        assert restored.name == original.name
        assert restored.desc == original.desc
        assert restored.type == pa.string()
        assert restored.default is None

    def test_string_default(self) -> None:
        """Test with string type and default."""
        original = SettingSpec(
            name="vgi_log_level",
            desc="Logging level",
            type=pa.string(),
            default="info",
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = SettingSpec.deserialize(batch)

        assert restored.name == original.name
        assert restored.type == pa.string()
        assert restored.default == "info"

    def test_int_default(self) -> None:
        """Test with int type and default."""
        original = SettingSpec(
            name="vgi_max_workers",
            desc="Maximum worker count",
            type=pa.int64(),
            default=4,
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = SettingSpec.deserialize(batch)

        assert restored.type == pa.int64()
        assert restored.default == 4

    def test_bool_default(self) -> None:
        """Test with bool type and default."""
        original = SettingSpec(
            name="vgi_debug",
            desc="Enable debug mode",
            type=pa.bool_(),
            default=False,
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = SettingSpec.deserialize(batch)

        assert restored.type == pa.bool_()
        assert restored.default is False

    def test_float_default(self) -> None:
        """Test with float type and default."""
        original = SettingSpec(
            name="vgi_timeout",
            desc="Timeout in seconds",
            type=pa.float64(),
            default=30.5,
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = SettingSpec.deserialize(batch)

        assert restored.type == pa.float64()
        assert restored.default == 30.5

    def test_int32_type(self) -> None:
        """Test with int32 type."""
        original = SettingSpec(
            name="vgi_port",
            desc="Port number",
            type=pa.int32(),
            default=8080,
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = SettingSpec.deserialize(batch)

        assert restored.type == pa.int32()
        assert restored.default == 8080


class TestFunctionInfoRequiredSettings:
    """Test FunctionInfo with required_settings field."""

    def test_empty_required_settings(self) -> None:
        """Test with no required settings."""
        args_schema = schema(value=pa.int64())
        output_schema = schema(result=pa.int64())

        original = FunctionInfo(
            name="echo",
            schema_name="main",
            function_type=FunctionType.SCALAR,
            arguments=SerializedSchema(args_schema.serialize().to_pybytes()),
            output_schema=SerializedSchema(output_schema.serialize().to_pybytes()),
            comment=None,
            tags={},
            required_settings=[],
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize(batch)

        assert restored.required_settings == []

    def test_single_required_setting(self) -> None:
        """Test with a single required setting."""
        args_schema = schema(count=pa.int64())
        output_schema = schema(result=pa.string())

        original = FunctionInfo(
            name="settings_aware",
            schema_name="main",
            function_type=FunctionType.TABLE,
            arguments=SerializedSchema(args_schema.serialize().to_pybytes()),
            output_schema=SerializedSchema(output_schema.serialize().to_pybytes()),
            comment=None,
            tags={},
            required_settings=["vgi_verbose_mode"],
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize(batch)

        assert restored.required_settings == ["vgi_verbose_mode"]

    def test_multiple_required_settings(self) -> None:
        """Test with multiple required settings."""
        args_schema = schema(value=pa.int64())
        output_schema = schema(result=pa.int64())

        original = FunctionInfo(
            name="logging_function",
            schema_name="main",
            function_type=FunctionType.TABLE,
            arguments=SerializedSchema(args_schema.serialize().to_pybytes()),
            output_schema=SerializedSchema(output_schema.serialize().to_pybytes()),
            comment=None,
            tags={},
            required_settings=["vgi_log_level", "vgi_log_format"],
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize(batch)

        assert restored.required_settings == ["vgi_log_level", "vgi_log_format"]


class TestArrowSchemaCorrectness:
    """Test that Arrow schemas are correct for each type."""

    def test_catalog_attach_result_schema(self) -> None:
        """Verify CatalogAttachResult Arrow schema."""
        schema = CatalogAttachResult.ARROW_SCHEMA
        assert len(schema) == 8
        assert schema.field("attach_id").type == pa.binary()
        assert schema.field("supports_transactions").type == pa.bool_()
        assert schema.field("supports_time_travel").type == pa.bool_()
        assert schema.field("catalog_version_frozen").type == pa.bool_()
        assert schema.field("catalog_version").type == pa.int64()
        assert schema.field("attach_id_required").type == pa.bool_()
        assert schema.field("default_schema").type == pa.string()
        assert schema.field("settings").type == pa.list_(pa.binary())

    def test_schema_info_schema(self) -> None:
        """Verify SchemaInfo Arrow schema."""
        schema = SchemaInfo.ARROW_SCHEMA
        assert len(schema) == 4
        assert schema.field("attach_id").type == pa.binary()
        assert schema.field("name").type == pa.string()
        assert schema.field("comment").type == pa.string()
        assert schema.field("comment").nullable is True
        assert schema.field("tags").type == pa.map_(pa.string(), pa.string())

    def test_table_info_schema(self) -> None:
        """Verify TableInfo Arrow schema."""
        schema = TableInfo.ARROW_SCHEMA
        assert schema.field("name").type == pa.string()
        assert schema.field("schema_name").type == pa.string()
        assert schema.field("columns").type == pa.binary()
        assert schema.field("not_null_constraints").type == pa.list_(pa.int32())
        assert schema.field("unique_constraints").type == pa.list_(pa.list_(pa.int32()))
        assert schema.field("check_constraints").type == pa.list_(pa.string())

    def test_view_info_schema(self) -> None:
        """Verify ViewInfo Arrow schema."""
        schema = ViewInfo.ARROW_SCHEMA
        assert schema.field("name").type == pa.string()
        assert schema.field("schema_name").type == pa.string()
        assert schema.field("definition").type == pa.string()

    def test_function_info_schema(self) -> None:
        """Verify FunctionInfo Arrow schema."""
        schema = FunctionInfo.ARROW_SCHEMA
        assert schema.field("name").type == pa.string()
        assert schema.field("arguments").type == pa.binary()
        assert schema.field("output_schema").type == pa.binary()

    def test_scan_function_result_schema(self) -> None:
        """Verify ScanFunctionResult Arrow schema."""
        schema = ScanFunctionResult.ARROW_SCHEMA
        assert schema.field("function_name").type == pa.string()
        assert schema.field("arguments").type == pa.binary()
        assert schema.field("required_extensions").type == pa.list_(pa.string())
