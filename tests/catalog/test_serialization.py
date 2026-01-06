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
        batch = deserialize_record_batch(serialized)
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
        batch = deserialize_record_batch(serialized)
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
        batch = deserialize_record_batch(serialized)
        restored = CatalogAttachResult.deserialize(batch)

        assert restored.supports_transactions is True
        assert restored.supports_time_travel is True
        assert restored.catalog_version_frozen is True


class TestSchemaInfoSerialization:
    """Test SchemaInfo serialization round-trip."""

    def test_basic_round_trip(self) -> None:
        """Test basic serialization and deserialization."""
        original = SchemaInfo(
            attach_id=AttachId(b"\x01\x02\x03\x04"),
            name="main",
            is_default=True,
            comment="Test schema",
            tags={"env": "test", "owner": "alice"},
        )
        serialized = original.serialize()
        batch = deserialize_record_batch(serialized)
        restored = SchemaInfo.deserialize(batch)

        assert restored.attach_id == original.attach_id
        assert restored.name == original.name
        assert restored.is_default == original.is_default
        assert restored.comment == original.comment
        assert restored.tags == original.tags

    def test_none_comment(self) -> None:
        """Test with None comment."""
        original = SchemaInfo(
            attach_id=AttachId(b"test"),
            name="schema1",
            is_default=False,
            comment=None,
            tags={},
        )
        serialized = original.serialize()
        batch = deserialize_record_batch(serialized)
        restored = SchemaInfo.deserialize(batch)

        assert restored.comment is None

    def test_empty_tags(self) -> None:
        """Test with empty tags dictionary."""
        original = SchemaInfo(
            attach_id=AttachId(b"test"),
            name="schema1",
            is_default=False,
            comment="Comment",
            tags={},
        )
        serialized = original.serialize()
        batch = deserialize_record_batch(serialized)
        restored = SchemaInfo.deserialize(batch)

        assert restored.tags == {}

    def test_empty_string_name(self) -> None:
        """Test with empty string name."""
        original = SchemaInfo(
            attach_id=AttachId(b"test"),
            name="",
            is_default=False,
            comment=None,
            tags={},
        )
        serialized = original.serialize()
        batch = deserialize_record_batch(serialized)
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
        batch = deserialize_record_batch(serialized)
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
        batch = deserialize_record_batch(serialized)
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
        batch = deserialize_record_batch(serialized)
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
        batch = deserialize_record_batch(serialized)
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
        batch = deserialize_record_batch(serialized)
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
        batch = deserialize_record_batch(serialized)
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
        batch = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize(batch)

        assert restored.function_type == FunctionType.TABLE


class TestScanFunctionResultSerialization:
    """Test ScanFunctionResult serialization round-trip."""

    def test_basic_round_trip(self) -> None:
        """Test basic serialization and deserialization."""
        original = ScanFunctionResult(
            function_name="scan_table",
            max_processes=4,
            invocation_id=b"\x01\x02\x03\x04",
        )
        serialized = original.serialize()
        batch = deserialize_record_batch(serialized)
        restored = ScanFunctionResult.deserialize(batch)

        assert restored.function_name == original.function_name
        assert restored.max_processes == original.max_processes
        assert restored.invocation_id == original.invocation_id

    def test_none_invocation_id(self) -> None:
        """Test with None invocation_id."""
        original = ScanFunctionResult(
            function_name="scan",
            max_processes=1,
            invocation_id=None,
        )
        serialized = original.serialize()
        batch = deserialize_record_batch(serialized)
        restored = ScanFunctionResult.deserialize(batch)

        assert restored.invocation_id is None


class TestArrowSchemaCorrectness:
    """Test that Arrow schemas are correct for each type."""

    def test_catalog_attach_result_schema(self) -> None:
        """Verify CatalogAttachResult Arrow schema."""
        schema = CatalogAttachResult.ARROW_SCHEMA
        assert len(schema) == 6
        assert schema.field("attach_id").type == pa.binary()
        assert schema.field("supports_transactions").type == pa.bool_()
        assert schema.field("supports_time_travel").type == pa.bool_()
        assert schema.field("catalog_version_frozen").type == pa.bool_()
        assert schema.field("catalog_version").type == pa.int64()
        assert schema.field("attach_id_required").type == pa.bool_()

    def test_schema_info_schema(self) -> None:
        """Verify SchemaInfo Arrow schema."""
        schema = SchemaInfo.ARROW_SCHEMA
        assert len(schema) == 5
        assert schema.field("attach_id").type == pa.binary()
        assert schema.field("name").type == pa.string()
        assert schema.field("is_default").type == pa.bool_()
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
        assert schema.field("function_type").type == pa.string()
        assert schema.field("arguments").type == pa.binary()
        assert schema.field("output_schema").type == pa.binary()

    def test_scan_function_result_schema(self) -> None:
        """Verify ScanFunctionResult Arrow schema."""
        schema = ScanFunctionResult.ARROW_SCHEMA
        assert schema.field("function_name").type == pa.string()
        assert schema.field("max_processes").type == pa.int32()
        assert schema.field("invocation_id").type == pa.binary()
        assert schema.field("invocation_id").nullable is True
