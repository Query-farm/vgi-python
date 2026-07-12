# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for catalog dataclass serialization/deserialization."""

import pyarrow as pa
import pytest
from vgi_rpc.utils import deserialize_record_batch

from vgi import schema
from vgi.catalog import (
    AttachOpaqueData,
    CatalogAttachResult,
    FunctionInfo,
    FunctionType,
    MacroInfo,
    MacroType,
    ScanFunctionResult,
    SchemaInfo,
    SerializedSchema,
    SettingSpec,
    TableInfo,
    ViewInfo,
)


class TestCatalogAttachResultSerialization:
    """Test CatalogAttachResult serialization round-trip."""

    def test_basic_round_trip(self) -> None:
        """Test basic serialization and deserialization."""
        original = CatalogAttachResult(
            attach_opaque_data=AttachOpaqueData(b"\x01\x02\x03\x04"),
            supports_transactions=True,
            supports_time_travel=False,
            catalog_version_frozen=False,
            catalog_version=42,
            resolved_data_version=None,
            resolved_implementation_version=None,
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = CatalogAttachResult.deserialize_from_batch(batch)

        assert restored.attach_opaque_data == original.attach_opaque_data
        assert restored.supports_transactions == original.supports_transactions
        assert restored.supports_time_travel == original.supports_time_travel
        assert restored.catalog_version_frozen == original.catalog_version_frozen
        assert restored.catalog_version == original.catalog_version

    def test_empty_attach_opaque_data(self) -> None:
        """Test with empty attach_opaque_data bytes."""
        original = CatalogAttachResult(
            attach_opaque_data=AttachOpaqueData(b""),
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=True,
            catalog_version=0,
            resolved_data_version=None,
            resolved_implementation_version=None,
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = CatalogAttachResult.deserialize_from_batch(batch)

        assert restored.attach_opaque_data == b""

    def test_all_flags_true(self) -> None:
        """Test with all boolean flags set to true."""
        original = CatalogAttachResult(
            attach_opaque_data=AttachOpaqueData(b"test"),
            supports_transactions=True,
            supports_time_travel=True,
            catalog_version_frozen=True,
            catalog_version=999,
            resolved_data_version=None,
            resolved_implementation_version=None,
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = CatalogAttachResult.deserialize_from_batch(batch)

        assert restored.supports_transactions is True
        assert restored.supports_time_travel is True
        assert restored.catalog_version_frozen is True

    def test_with_settings(self) -> None:
        """Test with settings list."""
        # Create some serialized ExtensionOption bytes
        option_bytes_1 = b"serialized_option_1"
        option_bytes_2 = b"serialized_option_2"

        original = CatalogAttachResult(
            attach_opaque_data=AttachOpaqueData(b"\x01\x02"),
            supports_transactions=True,
            supports_time_travel=False,
            catalog_version_frozen=False,
            catalog_version=1,
            settings=[option_bytes_1, option_bytes_2],
            resolved_data_version=None,
            resolved_implementation_version=None,
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = CatalogAttachResult.deserialize_from_batch(batch)

        assert len(restored.settings) == 2
        assert restored.settings[0] == option_bytes_1
        assert restored.settings[1] == option_bytes_2

    def test_with_empty_settings(self) -> None:
        """Test with empty settings list."""
        original = CatalogAttachResult(
            attach_opaque_data=AttachOpaqueData(b"\x01\x02"),
            supports_transactions=True,
            supports_time_travel=False,
            catalog_version_frozen=False,
            catalog_version=1,
            settings=[],
            resolved_data_version=None,
            resolved_implementation_version=None,
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = CatalogAttachResult.deserialize_from_batch(batch)

        assert restored.settings == []


class TestSchemaInfoSerialization:
    """Test SchemaInfo serialization round-trip."""

    def test_basic_round_trip(self) -> None:
        """Test basic serialization and deserialization."""
        original = SchemaInfo(
            attach_opaque_data=AttachOpaqueData(b"\x01\x02\x03\x04"),
            name="main",
            comment="Test schema",
            tags={"env": "test", "owner": "alice"},
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = SchemaInfo.deserialize_from_batch(batch)

        assert restored.attach_opaque_data == original.attach_opaque_data
        assert restored.name == original.name
        assert restored.comment == original.comment
        assert restored.tags == original.tags

    def test_none_comment(self) -> None:
        """Test with None comment."""
        original = SchemaInfo(
            attach_opaque_data=AttachOpaqueData(b"test"),
            name="schema1",
            comment=None,
            tags={},
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = SchemaInfo.deserialize_from_batch(batch)

        assert restored.comment is None

    def test_empty_tags(self) -> None:
        """Test with empty tags dictionary."""
        original = SchemaInfo(
            attach_opaque_data=AttachOpaqueData(b"test"),
            name="schema1",
            comment="Comment",
            tags={},
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = SchemaInfo.deserialize_from_batch(batch)

        assert restored.tags == {}

    def test_empty_string_name(self) -> None:
        """Test with empty string name."""
        original = SchemaInfo(
            attach_opaque_data=AttachOpaqueData(b"test"),
            name="",
            comment=None,
            tags={},
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = SchemaInfo.deserialize_from_batch(batch)

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
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = TableInfo.deserialize_from_batch(batch)

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
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = TableInfo.deserialize_from_batch(batch)

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
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = TableInfo.deserialize_from_batch(batch)

        assert restored.unique_constraints == [[0], [1, 2]]

    def test_inlined_scan_function_round_trip(self) -> None:
        """``scan_function`` IPC bytes round-trip through TableInfo serialize/deserialize.

        The C++ extension reads this field (when populated) and skips the
        per-bind ``catalog_table_scan_function_get`` RPC.
        """
        columns_schema = schema(id=pa.int64())
        columns_bytes = SerializedSchema(columns_schema.serialize().to_pybytes())
        sfr = ScanFunctionResult(
            function_name="read_parquet",
            positional_arguments=[pa.scalar("s3://bucket/x.parquet", pa.string())],
            named_arguments={"hive_partitioning": pa.scalar(True, pa.bool_())},
            required_extensions=["parquet"],
        )
        original = TableInfo(
            name="t",
            schema_name="s",
            columns=columns_bytes,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
            comment=None,
            tags={},
            scan_function=sfr.serialize(),
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = TableInfo.deserialize_from_batch(batch)

        assert restored.scan_function is not None
        nested, _ = deserialize_record_batch(restored.scan_function)
        restored_sfr = ScanFunctionResult.deserialize(nested)
        assert restored_sfr.function_name == "read_parquet"
        assert [s.as_py() for s in restored_sfr.positional_arguments] == ["s3://bucket/x.parquet"]
        assert restored_sfr.named_arguments["hive_partitioning"].as_py() is True
        assert restored_sfr.required_extensions == ["parquet"]
        # Other inline fields default to None when not populated.
        assert restored.insert_function is None
        assert restored.update_function is None
        assert restored.delete_function is None

    def test_inlined_cardinality_round_trip(self) -> None:
        """``cardinality_estimate`` and ``cardinality_max`` round-trip on TableInfo.

        The C++ extension reads these (when populated) and skips the per-bind
        ``table_function_cardinality`` RPC.
        """
        columns_schema = schema(id=pa.int64())
        columns_bytes = SerializedSchema(columns_schema.serialize().to_pybytes())
        original = TableInfo(
            name="t",
            schema_name="s",
            columns=columns_bytes,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
            comment=None,
            tags={},
            cardinality_estimate=12345,
            cardinality_max=99999,
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = TableInfo.deserialize_from_batch(batch)
        assert restored.cardinality_estimate == 12345
        assert restored.cardinality_max == 99999

    def test_inlined_cardinality_partial(self) -> None:
        """Partial population (estimate-only or max-only) round-trips correctly."""
        columns_schema = schema(id=pa.int64())
        columns_bytes = SerializedSchema(columns_schema.serialize().to_pybytes())
        for estimate, max_ in [(1000, None), (None, 5000)]:
            original = TableInfo(
                name="t",
                schema_name="s",
                columns=columns_bytes,
                not_null_constraints=[],
                unique_constraints=[],
                check_constraints=[],
                comment=None,
                tags={},
                cardinality_estimate=estimate,
                cardinality_max=max_,
            )
            data = original.serialize_to_bytes()
            batch, _ = deserialize_record_batch(data)
            restored = TableInfo.deserialize_from_batch(batch)
            assert restored.cardinality_estimate == estimate
            assert restored.cardinality_max == max_

    def test_omitted_inline_fields_default_to_none(self) -> None:
        """A TableInfo without inline fields round-trips with None values.

        Models the backward-compat path: an old worker omits these fields,
        the C++ extension sees None and falls back to the per-bind RPC.
        """
        columns_schema = schema(id=pa.int64())
        original = TableInfo(
            name="t",
            schema_name="s",
            columns=SerializedSchema(columns_schema.serialize().to_pybytes()),
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
            comment=None,
            tags={},
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = TableInfo.deserialize_from_batch(batch)
        assert restored.scan_function is None
        assert restored.insert_function is None
        assert restored.update_function is None
        assert restored.delete_function is None
        assert restored.cardinality_estimate is None
        assert restored.cardinality_max is None
        assert restored.column_statistics is None
        assert restored.bind_result is None

    def test_inlined_bind_result_round_trip(self) -> None:
        """``bind_result`` IPC bytes round-trip through TableInfo.

        The C++ extension reads this field (when populated) and skips the
        per-scan ``bind`` RPC.
        """
        from vgi.invocation import BindResponse

        columns_schema = schema(id=pa.int64())
        columns_bytes = SerializedSchema(columns_schema.serialize().to_pybytes())
        # The inlined bytes are an actual BindResponse — not a synthetic blob —
        # so we deserialize it back at the end to confirm shape.
        output_schema = pa.schema([pa.field("x", pa.int32(), nullable=False)])
        bind_blob = BindResponse(output_schema=output_schema, opaque_data=None).serialize_to_bytes()
        original = TableInfo(
            name="t",
            schema_name="s",
            columns=columns_bytes,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
            comment=None,
            tags={},
            bind_result=bind_blob,
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = TableInfo.deserialize_from_batch(batch)
        assert restored.bind_result is not None
        assert restored.bind_result == bind_blob
        # Round-trip the inner BindResponse so we know the bytes are valid.
        restored_bind = BindResponse.deserialize_from_bytes(restored.bind_result)
        assert restored_bind.output_schema.equals(output_schema)
        assert restored_bind.opaque_data is None

    def test_inlined_column_statistics_round_trip(self) -> None:
        """``column_statistics`` IPC bytes round-trip through TableInfo.

        The C++ extension reads this field (when populated) and skips the
        per-scan ``table_function_statistics`` and per-table
        ``catalog_table_column_statistics_get`` RPCs.
        """
        from vgi.catalog.catalog_interface import ColumnStatistics, serialize_column_statistics

        columns_schema = schema(id=pa.int64())
        columns_bytes = SerializedSchema(columns_schema.serialize().to_pybytes())
        stats_blob = serialize_column_statistics(
            [
                ColumnStatistics(
                    column_name="id",
                    min=pa.scalar(0, pa.int64()),
                    max=pa.scalar(999, pa.int64()),
                    has_null=False,
                    distinct_count=1000,
                ),
            ],
            cache_max_age_seconds=3600,
        )
        original = TableInfo(
            name="t",
            schema_name="s",
            columns=columns_bytes,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
            comment=None,
            tags={},
            supports_column_statistics=True,
            column_statistics=stats_blob,
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = TableInfo.deserialize_from_batch(batch)
        assert restored.column_statistics is not None
        assert restored.column_statistics == stats_blob
        # Other inline fields default to None.
        assert restored.scan_function is None
        assert restored.cardinality_estimate is None


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
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = ViewInfo.deserialize_from_batch(batch)

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
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = ViewInfo.deserialize_from_batch(batch)

        assert restored.definition == original.definition


class TestMacroInfoSerialization:
    """Test MacroInfo serialization round-trip."""

    def test_scalar_macro_round_trip(self) -> None:
        """Test round-trip with scalar macro and all fields."""
        original = MacroInfo(
            name="multiply",
            schema_name="main",
            macro_type=MacroType.SCALAR,
            parameters=["x", "y"],
            definition="x * y",
            comment="Multiply two values",
            tags={"category": "math"},
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = MacroInfo.deserialize_from_batch(batch)

        assert restored.name == original.name
        assert restored.schema_name == original.schema_name
        assert restored.macro_type == MacroType.SCALAR
        assert restored.parameters == ["x", "y"]
        assert restored.definition == "x * y"
        assert restored.comment == "Multiply two values"
        assert restored.tags == {"category": "math"}

    def test_table_macro_round_trip(self) -> None:
        """Test round-trip with table macro."""
        original = MacroInfo(
            name="my_range",
            schema_name="main",
            macro_type=MacroType.TABLE,
            parameters=["n"],
            definition="SELECT * FROM range(n)",
            comment=None,
            tags={},
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = MacroInfo.deserialize_from_batch(batch)

        assert restored.macro_type == MacroType.TABLE
        assert restored.parameters == ["n"]
        assert restored.definition == "SELECT * FROM range(n)"

    def test_parameter_default_values_round_trip(self) -> None:
        """Test parameter_default_values RecordBatch survives round-trip with types."""
        defaults = pa.RecordBatch.from_pydict(
            {"lo": pa.array([0], type=pa.int64()), "hi": pa.array([100], type=pa.int64())}
        )
        original = MacroInfo(
            name="clamp",
            schema_name="main",
            macro_type=MacroType.SCALAR,
            parameters=["val", "lo", "hi"],
            parameter_default_values=defaults,
            definition="GREATEST(lo, LEAST(hi, val))",
            comment=None,
            tags={},
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = MacroInfo.deserialize_from_batch(batch)

        assert restored.parameter_default_values is not None
        assert restored.parameter_default_values.num_rows == 1
        assert restored.parameter_default_values.schema.names == ["lo", "hi"]
        assert restored.parameter_default_values.column("lo").type == pa.int64()
        assert restored.parameter_default_values.column("hi").type == pa.int64()
        assert restored.parameter_default_values.column("lo")[0].as_py() == 0
        assert restored.parameter_default_values.column("hi")[0].as_py() == 100

    def test_none_parameter_default_values(self) -> None:
        """Test with None parameter_default_values."""
        original = MacroInfo(
            name="simple",
            schema_name="main",
            macro_type=MacroType.SCALAR,
            parameters=["x"],
            parameter_default_values=None,
            definition="x",
            comment=None,
            tags={},
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = MacroInfo.deserialize_from_batch(batch)

        assert restored.parameter_default_values is None

    def test_macro_type_enum_survival(self) -> None:
        """MacroType enum survives serialization round-trip."""
        for macro_type in MacroType:
            original = MacroInfo(
                name="test",
                schema_name="main",
                macro_type=macro_type,
                parameters=[],
                definition="1",
                comment=None,
                tags={},
            )
            serialized = original.serialize_to_bytes()
            batch, _ = deserialize_record_batch(serialized)
            restored = MacroInfo.deserialize_from_batch(batch)
            assert restored.macro_type == macro_type

    def test_arguments_schema_round_trip(self) -> None:
        """arguments_schema carries vgi_doc per documented param; absent doc -> no key."""
        from vgi.argument_spec import VGI_DOC_KEY, macro_arguments_schema, macro_parameter_docs_from_schema

        defaults = pa.RecordBatch.from_pydict({"lo": pa.array([0], type=pa.int64())})
        args = macro_arguments_schema(
            parameters=["val", "lo", "hi"],
            parameter_default_values=defaults,
            parameter_docs={"val": "value to clamp", "hi": "upper bound"},
        )
        original = MacroInfo(
            name="clamp",
            schema_name="main",
            macro_type=MacroType.SCALAR,
            parameters=["val", "lo", "hi"],
            parameter_default_values=defaults,
            definition="GREATEST(lo, LEAST(hi, val))",
            comment=None,
            tags={},
            arguments_schema=args,
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = MacroInfo.deserialize_from_batch(batch)

        assert restored.arguments_schema is not None
        rs = restored.arguments_schema
        # One field per parameter, in order.
        assert rs.names == ["val", "lo", "hi"]
        # Field type tracks the default value type when known, else null.
        assert rs.field("lo").type == pa.int64()
        assert rs.field("val").type == pa.null()
        assert rs.field("hi").type == pa.null()
        # Documented params carry vgi_doc; undocumented (lo) has no key.
        assert (rs.field("val").metadata or {}).get(VGI_DOC_KEY) == b"value to clamp"
        assert (rs.field("hi").metadata or {}).get(VGI_DOC_KEY) == b"upper bound"
        assert VGI_DOC_KEY not in (rs.field("lo").metadata or {})
        # Convenience extractor returns only documented params.
        assert macro_parameter_docs_from_schema(rs) == {"val": "value to clamp", "hi": "upper bound"}

    def test_none_arguments_schema(self) -> None:
        """arguments_schema defaults to None (older workers) and survives round-trip."""
        original = MacroInfo(
            name="simple",
            schema_name="main",
            macro_type=MacroType.SCALAR,
            parameters=["x"],
            definition="x",
            comment=None,
            tags={},
        )
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = MacroInfo.deserialize_from_batch(batch)
        assert restored.arguments_schema is None


class TestMacroArgumentsSchemaWire:
    """Macro per-parameter docs flow over create/list wire types."""

    def test_declarative_macro_to_info_carries_docs(self) -> None:
        """Declarative Macro.parameter_docs -> MacroInfo.arguments_schema vgi_doc."""
        from vgi.argument_spec import macro_parameter_docs_from_schema
        from vgi.catalog.descriptors import Macro

        m = Macro(
            name="clamp",
            macro_type=MacroType.SCALAR,
            parameters=["x", "lo", "hi"],
            parameter_default_values=pa.RecordBatch.from_pydict(
                {"lo": pa.array([0], type=pa.int64()), "hi": pa.array([100], type=pa.int64())}
            ),
            parameter_docs={"x": "value to clamp"},
            definition="GREATEST(lo, LEAST(hi, x))",
        )
        info = m.to_macro_info("main")
        assert info.arguments_schema is not None
        assert info.arguments_schema.names == ["x", "lo", "hi"]
        assert macro_parameter_docs_from_schema(info.arguments_schema) == {"x": "value to clamp"}

    def test_declarative_macro_rejects_unknown_doc_param(self) -> None:
        """parameter_docs keys must be in parameters (validated like defaults)."""
        from vgi.catalog.descriptors import Macro

        with pytest.raises(ValueError, match="documented parameter 'bogus' not found"):
            Macro(
                name="bad",
                macro_type=MacroType.SCALAR,
                parameters=["x"],
                parameter_docs={"bogus": "nope"},
                definition="x",
            )

    def test_macro_create_request_round_trip(self) -> None:
        """MacroCreateRequest carries arguments_schema over the wire."""
        from vgi.argument_spec import macro_arguments_schema, macro_parameter_docs_from_schema
        from vgi.catalog import OnConflict
        from vgi.protocol import MacroCreateRequest

        args = macro_arguments_schema(
            parameters=["x", "y"],
            parameter_docs={"x": "first", "y": "second"},
        )
        req = MacroCreateRequest(
            attach_opaque_data=b"attach",
            schema_name="main",
            name="add",
            macro_type=MacroType.SCALAR,
            parameters=["x", "y"],
            definition="x + y",
            on_conflict=OnConflict.ERROR,
            arguments_schema=args,
        )
        restored = MacroCreateRequest.deserialize_from_bytes(req.serialize_to_bytes())
        assert restored.arguments_schema is not None
        assert macro_parameter_docs_from_schema(restored.arguments_schema) == {"x": "first", "y": "second"}

    def test_macro_create_request_none_arguments_schema(self) -> None:
        """MacroCreateRequest.arguments_schema defaults to None and round-trips."""
        from vgi.catalog import OnConflict
        from vgi.protocol import MacroCreateRequest

        req = MacroCreateRequest(
            attach_opaque_data=b"attach",
            schema_name="main",
            name="add",
            macro_type=MacroType.SCALAR,
            parameters=["x", "y"],
            definition="x + y",
            on_conflict=OnConflict.ERROR,
        )
        restored = MacroCreateRequest.deserialize_from_bytes(req.serialize_to_bytes())
        assert restored.arguments_schema is None


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
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize_from_batch(batch)

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
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize_from_batch(batch)

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
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize_from_batch(batch)

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
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize_from_batch(batch)

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
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize_from_batch(batch)

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
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize_from_batch(batch)

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
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize_from_batch(batch)

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

    @pytest.mark.parametrize(
        ("name", "desc", "arrow_type", "default"),
        [
            ("vgi_api_key", "API key for auth", pa.string(), None),
            ("vgi_log_level", "Logging level", pa.string(), "info"),
            ("vgi_max_workers", "Maximum worker count", pa.int64(), 4),
            ("vgi_debug", "Enable debug mode", pa.bool_(), False),
            ("vgi_timeout", "Timeout in seconds", pa.float64(), 30.5),
            ("vgi_port", "Port number", pa.int32(), 8080),
        ],
        ids=["string_no_default", "string_default", "int64", "bool", "float64", "int32"],
    )
    def test_round_trip(self, name: str, desc: str, arrow_type: pa.DataType, default: object) -> None:
        """Test serialization round-trip for different setting types."""
        original = SettingSpec(name=name, desc=desc, type=arrow_type, default=default)
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = SettingSpec.deserialize(batch)

        assert restored.name == original.name
        assert restored.desc == original.desc
        assert restored.type == arrow_type
        assert restored.default == default


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
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize_from_batch(batch)

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
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize_from_batch(batch)

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
        serialized = original.serialize_to_bytes()
        batch, _ = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize_from_batch(batch)

        assert restored.required_settings == ["vgi_log_level", "vgi_log_format"]


class TestArrowSchemaCorrectness:
    """Test that Arrow schemas are correct for each type."""

    def test_catalog_attach_result_schema(self) -> None:
        """Verify CatalogAttachResult Arrow schema."""
        schema = CatalogAttachResult.ARROW_SCHEMA
        assert len(schema) == 15
        assert schema.field("attach_opaque_data").type == pa.binary()
        assert schema.field("supports_transactions").type == pa.bool_()
        assert schema.field("supports_time_travel").type == pa.bool_()
        assert schema.field("catalog_version_frozen").type == pa.bool_()
        assert schema.field("catalog_version").type == pa.int64()
        assert schema.field("attach_opaque_data_required").type == pa.bool_()
        assert schema.field("default_schema").type == pa.string()
        assert schema.field("settings").type == pa.list_(pa.binary())
        assert schema.field("secret_types").type == pa.list_(pa.binary())
        assert schema.field("attach_catalogs").type == pa.list_(pa.binary())
        assert schema.field("resolved_data_version").type == pa.string()
        assert schema.field("resolved_implementation_version").type == pa.string()

    def test_schema_info_schema(self) -> None:
        """Verify SchemaInfo Arrow schema."""
        schema = SchemaInfo.ARROW_SCHEMA
        assert len(schema) == 5
        assert schema.field("attach_opaque_data").type == pa.binary()
        assert schema.field("name").type == pa.string()
        assert schema.field("comment").type == pa.string()
        assert schema.field("comment").nullable is True
        assert schema.field("tags").type == pa.map_(pa.string(), pa.string())
        assert schema.field("estimated_object_count").type == pa.map_(pa.string(), pa.int64())

    def test_table_info_schema(self) -> None:
        """Verify TableInfo Arrow schema."""
        schema = TableInfo.ARROW_SCHEMA
        assert schema.field("name").type == pa.string()
        assert schema.field("schema_name").type == pa.string()
        assert schema.field("columns").type == pa.binary()
        assert schema.field("not_null_constraints").type == pa.list_(pa.int32())
        assert schema.field("unique_constraints").type == pa.list_(pa.list_(pa.int32()))
        assert schema.field("check_constraints").type == pa.list_(pa.string())
        # Inlined optional fields — populated to skip per-bind RPCs.
        assert schema.field("scan_function").type == pa.binary()
        assert schema.field("cardinality_estimate").type == pa.int64()
        assert schema.field("column_statistics").type == pa.binary()
        assert schema.field("bind_result").type == pa.binary()
        assert schema.field("required_filters").type == pa.list_(pa.list_(pa.string()))
        # required_filters is the LAST field (positional schema compatibility:
        # old C++ extensions ignore-by-position trailing fields).
        assert schema.field(len(schema) - 1).name == "required_filters"

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


class TestScanBranchSerialization:
    """ScanBranch and ScanBranchesResult round-trip and schema checks."""

    def test_scan_branch_schema(self) -> None:
        """ScanBranch Arrow schema mirrors ScanFunctionResult sans required_extensions, plus branch_filter."""
        from vgi.catalog import ScanBranch

        schema = ScanBranch.ARROW_SCHEMA
        assert schema.field("function_name").type == pa.string()
        assert schema.field("arguments").type == pa.binary()
        assert schema.field("branch_filter").type == pa.string()
        assert schema.field("branch_filter").nullable is True
        assert schema.field("writable").type == pa.bool_()
        assert schema.field("writable").nullable is False

    def test_scan_branches_result_schema(self) -> None:
        """ScanBranchesResult Arrow schema: branches as list<binary>, required_extensions hoisted to top-level."""
        from vgi.catalog import ScanBranchesResult

        schema = ScanBranchesResult.ARROW_SCHEMA
        assert schema.field("branches").type == pa.list_(pa.binary())
        assert schema.field("required_extensions").type == pa.list_(pa.string())

    def test_scan_branch_round_trip_no_filter(self) -> None:
        """A ScanBranch with no branch_filter round-trips losslessly."""
        from vgi.catalog import ScanBranch

        original = ScanBranch(
            function_name="read_parquet",
            positional_arguments=[pa.scalar("s3://bucket/orders.parquet", pa.string())],
            named_arguments={"hive_partitioning": pa.scalar(True, pa.bool_())},
        )
        batch, _ = deserialize_record_batch(original.serialize())
        restored = ScanBranch.deserialize(batch)
        assert restored.function_name == "read_parquet"
        assert len(restored.positional_arguments) == 1
        assert restored.positional_arguments[0].as_py() == "s3://bucket/orders.parquet"
        assert restored.named_arguments["hive_partitioning"].as_py() is True
        assert restored.branch_filter is None
        assert restored.writable is False  # default

    def test_scan_branch_round_trip_with_writable(self) -> None:
        """A ScanBranch with writable=True round-trips losslessly."""
        from vgi.catalog import ScanBranch

        original = ScanBranch(
            function_name="kafka_writable_scan",
            positional_arguments=[pa.scalar("topic1", pa.string())],
            named_arguments={},
            writable=True,
        )
        batch, _ = deserialize_record_batch(original.serialize())
        restored = ScanBranch.deserialize(batch)
        assert restored.writable is True
        assert restored.function_name == "kafka_writable_scan"

    def test_scan_branch_round_trip_with_filter(self) -> None:
        """A ScanBranch with a branch_filter preserves the raw SQL text."""
        from vgi.catalog import ScanBranch

        original = ScanBranch(
            function_name="vgi_table_function",
            positional_arguments=[pa.scalar("kafka_worker", pa.string()), pa.scalar("kafka_scan", pa.string())],
            named_arguments={},
            branch_filter="ts >= TIMESTAMP '2026-05-15 00:00:00'",
        )
        batch, _ = deserialize_record_batch(original.serialize())
        restored = ScanBranch.deserialize(batch)
        assert restored.branch_filter == "ts >= TIMESTAMP '2026-05-15 00:00:00'"
        assert len(restored.positional_arguments) == 2

    def test_scan_branches_result_round_trip(self) -> None:
        """ScanBranchesResult round-trips two heterogeneous branches + required_extensions."""
        from vgi.catalog import ScanBranch, ScanBranchesResult

        original = ScanBranchesResult(
            branches=[
                ScanBranch(
                    function_name="vgi_table_function",
                    positional_arguments=[pa.scalar("kafka_worker", pa.string())],
                    named_arguments={},
                    branch_filter="ts >= TIMESTAMP '2026-05-15'",
                ),
                ScanBranch(
                    function_name="iceberg_scan",
                    positional_arguments=[pa.scalar("s3://archive/orders", pa.string())],
                    named_arguments={},
                    branch_filter="ts < TIMESTAMP '2026-05-15'",
                ),
            ],
            required_extensions=["iceberg", "httpfs"],
        )
        batch, _ = deserialize_record_batch(original.serialize())
        restored = ScanBranchesResult.deserialize(batch)
        assert len(restored.branches) == 2
        assert restored.branches[0].function_name == "vgi_table_function"
        assert restored.branches[1].function_name == "iceberg_scan"
        assert restored.required_extensions == ["iceberg", "httpfs"]

    def test_scan_branches_result_empty_branches_rejected(self) -> None:
        """Empty branches list is rejected at deserialize — workers must return >=1 branch."""
        import pytest

        from vgi.catalog import ScanBranchesResult

        empty = ScanBranchesResult(branches=[], required_extensions=[])
        batch, _ = deserialize_record_batch(empty.serialize())
        with pytest.raises(ValueError, match="branches list must not be empty"):
            ScanBranchesResult.deserialize(batch)


class TestSecretTypeSpecSerialization:
    """Test SecretTypeSpec serialization round-trip."""

    def test_round_trip(self) -> None:
        """Test basic serialization and deserialization."""
        from vgi.catalog.secret_type import SecretTypeSpec

        original = SecretTypeSpec(
            name="vgi_example",
            description="Example VGI secret for testing",
            schema=pa.schema(
                [
                    pa.field("secret_string", pa.string(), metadata={"redact": "true"}),
                    pa.field("api_key", pa.string(), metadata={"redact": "true"}),
                    pa.field("port", pa.int32()),
                ]  # type: ignore[arg-type]  # PyArrow field metadata typing limitation
            ),
        )
        serialized = original.serialize()
        batch, _ = deserialize_record_batch(serialized)
        restored = SecretTypeSpec.deserialize(batch)

        assert restored.name == original.name
        assert restored.description == original.description
        assert restored.schema.names == original.schema.names
        # Verify field metadata survives round-trip
        assert restored.schema.field("secret_string").metadata == {b"redact": b"true"}
        assert restored.schema.field("port").metadata is None
