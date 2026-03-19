"""Tests for CatalogInterface ABC and default implementations."""

from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.utils import deserialize_record_batch

from vgi.catalog import (
    AttachId,
    CatalogAttachResult,
    CatalogExample,
    CatalogInterface,
    FunctionInfo,
    FunctionType,
    MacroInfo,
    OnConflict,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    TableInfo,
    TransactionId,
    ViewInfo,
)
from vgi.catalog.catalog_interface import (
    DistinctDependence,
    FunctionStability,
    MacroType,
    NullHandling,
    OrderDependence,
    OrderPreservation,
    ReadOnlyCatalogInterface,
)
from vgi.exceptions import CatalogReadOnlyError

# Common test data
TEST_ATTACH_ID = AttachId(b"test")
TEST_TRANSACTION_ID = TransactionId(b"tx")


def empty_schema_bytes() -> SerializedSchema:
    """Create empty serialized schema for tests."""
    return SerializedSchema(pa.schema([]).serialize().to_pybytes())


def function_info_round_trip(info: FunctionInfo) -> FunctionInfo:
    """Serialize and deserialize FunctionInfo."""
    batch, _ = deserialize_record_batch(info.serialize_to_bytes())
    return FunctionInfo.deserialize_from_batch(batch)


@pytest.fixture
def catalog() -> "MinimalCatalog":
    """Create a MinimalCatalog instance."""
    return MinimalCatalog()


@pytest.fixture
def readonly_catalog() -> "MinimalReadOnlyCatalog":
    """Create a MinimalReadOnlyCatalog instance."""
    return MinimalReadOnlyCatalog()


class MinimalCatalog(CatalogInterface):
    """Minimal implementation for testing abstract method requirements."""

    def catalogs(self) -> list[str]:
        """Return list of catalogs."""
        return ["test"]

    def catalog_attach(self, *, name: str, options: dict[str, Any]) -> CatalogAttachResult:
        """Attach to catalog."""
        return CatalogAttachResult(
            attach_id=AttachId(b"test"),
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=False,
            catalog_version=1,
        )

    def schema_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
    ) -> SchemaInfo | None:
        """Get schema info."""
        if name == "main":
            return SchemaInfo(
                attach_id=attach_id,
                name="main",
                comment=None,
                tags={},
            )
        return None

    def table_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        """Get table info."""
        return None

    def view_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ViewInfo | None:
        """Get view info."""
        return None

    def macro_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> MacroInfo | None:
        """Get macro info."""
        return None


class TestCatalogInterfaceAbstract:
    """Test abstract method enforcement."""

    def test_cannot_instantiate_abstract_class(self) -> None:
        """CatalogInterface cannot be instantiated directly."""
        with pytest.raises(TypeError):
            CatalogInterface()  # type: ignore[abstract]

    def test_minimal_implementation_works(self) -> None:
        """A minimal implementation can be instantiated."""
        catalog = MinimalCatalog()
        assert list(catalog.catalogs()) == ["test"]


class TestCatalogInterfaceDefaults:
    """Test default method implementations."""

    def test_schemas_returns_main(self) -> None:
        """Default schemas() returns single 'main' schema."""
        catalog = MinimalCatalog()
        attach_id = AttachId(b"test")
        schemas = list(catalog.schemas(attach_id=attach_id, transaction_id=None))

        assert len(schemas) == 1
        assert schemas[0].name == "main"
        assert schemas[0].comment is None
        assert schemas[0].tags == {}

    def test_catalog_version_returns_zero(self) -> None:
        """Default catalog_version() returns 0."""
        catalog = MinimalCatalog()
        version = catalog.catalog_version(attach_id=AttachId(b"test"), transaction_id=None)
        assert version == 0

    def test_catalog_detach_does_nothing(self) -> None:
        """Default catalog_detach() does nothing (no exception)."""
        catalog = MinimalCatalog()
        # Should not raise
        catalog.catalog_detach(attach_id=AttachId(b"test"))

    def test_interface_feature_flags_empty(self) -> None:
        """Default interface_feature_flags returns empty set."""
        catalog = MinimalCatalog()
        assert catalog.interface_feature_flags == set()


def _not_implemented_test_cases() -> list[tuple[str, str, Callable[[MinimalCatalog], Any]]]:
    """Return test cases for NotImplementedError tests."""
    return [
        (
            "catalog_create",
            "Catalog create not implemented",
            lambda c: c.catalog_create(name="test", on_conflict=OnConflict.ERROR, options={}),
        ),
        (
            "catalog_drop",
            "Catalog drop not implemented",
            lambda c: c.catalog_drop(name="test"),
        ),
        (
            "transaction_begin",
            "Catalog transactions not implemented",
            lambda c: c.catalog_transaction_begin(attach_id=TEST_ATTACH_ID),
        ),
        (
            "transaction_commit",
            "Catalog transactions not implemented",
            lambda c: c.catalog_transaction_commit(attach_id=TEST_ATTACH_ID, transaction_id=TEST_TRANSACTION_ID),
        ),
        (
            "transaction_rollback",
            "Catalog transactions not implemented",
            lambda c: c.catalog_transaction_rollback(attach_id=TEST_ATTACH_ID, transaction_id=TEST_TRANSACTION_ID),
        ),
        (
            "schema_create",
            "Schema create not implemented",
            lambda c: c.schema_create(
                attach_id=TEST_ATTACH_ID,
                transaction_id=None,
                name="new_schema",
                comment=None,
                tags={},
            ),
        ),
        (
            "schema_drop",
            "Schema drop not implemented",
            lambda c: c.schema_drop(
                attach_id=TEST_ATTACH_ID,
                transaction_id=None,
                name="schema",
                ignore_not_found=False,
                cascade=False,
            ),
        ),
        (
            "schema_contents",
            "Schema contents not implemented",
            lambda c: c.schema_contents(
                attach_id=TEST_ATTACH_ID,
                transaction_id=None,
                name="main",
                type=SchemaObjectType.TABLE,
            ),
        ),
        (
            "table_create",
            "Table create not implemented",
            lambda c: c.table_create(
                attach_id=TEST_ATTACH_ID,
                transaction_id=None,
                schema_name="main",
                name="table",
                columns=SerializedSchema(b""),
                on_conflict=OnConflict.ERROR,
                not_null_constraints=[],
                unique_constraints=[],
                check_constraints=[],
            ),
        ),
        (
            "view_create",
            "View create not implemented",
            lambda c: c.view_create(
                attach_id=TEST_ATTACH_ID,
                transaction_id=None,
                schema_name="main",
                name="view",
                definition="SELECT 1",
                on_conflict=OnConflict.ERROR,
            ),
        ),
        (
            "macro_create",
            "Macro create not implemented",
            lambda c: c.macro_create(
                attach_id=TEST_ATTACH_ID,
                transaction_id=None,
                schema_name="main",
                name="my_macro",
                macro_type=MacroType.SCALAR,
                parameters=["x"],
                definition="x * 2",
                on_conflict=OnConflict.ERROR,
            ),
        ),
        (
            "macro_drop",
            "Macro drop not implemented",
            lambda c: c.macro_drop(
                attach_id=TEST_ATTACH_ID,
                transaction_id=None,
                schema_name="main",
                name="my_macro",
                ignore_not_found=False,
            ),
        ),
    ]


class TestCatalogInterfaceNotImplemented:
    """Test that optional methods raise NotImplementedError by default."""

    @pytest.mark.parametrize(
        ("method", "error_match", "call_fn"),
        _not_implemented_test_cases(),
        ids=[t[0] for t in _not_implemented_test_cases()],
    )
    def test_not_implemented(
        self,
        catalog: MinimalCatalog,
        method: str,
        error_match: str,
        call_fn: Callable[[MinimalCatalog], Any],
    ) -> None:
        """Optional methods raise NotImplementedError by default."""
        with pytest.raises(NotImplementedError, match=error_match):
            call_fn(catalog)


class MinimalReadOnlyCatalog(ReadOnlyCatalogInterface):
    """Minimal read-only implementation for testing."""

    def catalogs(self) -> list[str]:
        """Return list of catalogs."""
        return ["readonly"]

    def catalog_attach(self, *, name: str, options: dict[str, Any]) -> CatalogAttachResult:
        """Attach to catalog."""
        return CatalogAttachResult(
            attach_id=AttachId(b"readonly"),
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=True,
            catalog_version=1,
        )

    def schema_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
    ) -> SchemaInfo | None:
        """Get schema info."""
        return None

    def table_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        """Get table info."""
        return None

    def view_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ViewInfo | None:
        """Get view info."""
        return None

    def macro_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> MacroInfo | None:
        """Get macro info."""
        return None


def _readonly_test_cases() -> list[tuple[str, Callable[[MinimalReadOnlyCatalog], Any]]]:
    """Return test cases for ReadOnly tests."""
    return [
        (
            "catalog_create",
            lambda c: c.catalog_create(name="test", on_conflict=OnConflict.ERROR, options={}),
        ),
        ("catalog_drop", lambda c: c.catalog_drop(name="test")),
        (
            "transaction_begin",
            lambda c: c.catalog_transaction_begin(attach_id=TEST_ATTACH_ID),
        ),
        (
            "transaction_commit",
            lambda c: c.catalog_transaction_commit(attach_id=TEST_ATTACH_ID, transaction_id=TEST_TRANSACTION_ID),
        ),
        (
            "transaction_rollback",
            lambda c: c.catalog_transaction_rollback(attach_id=TEST_ATTACH_ID, transaction_id=TEST_TRANSACTION_ID),
        ),
        (
            "schema_create",
            lambda c: c.schema_create(
                attach_id=TEST_ATTACH_ID,
                transaction_id=None,
                name="new",
                comment=None,
                tags={},
            ),
        ),
        (
            "schema_drop",
            lambda c: c.schema_drop(
                attach_id=TEST_ATTACH_ID,
                transaction_id=None,
                name="main",
                ignore_not_found=False,
                cascade=False,
            ),
        ),
        (
            "table_create",
            lambda c: c.table_create(
                attach_id=TEST_ATTACH_ID,
                transaction_id=None,
                schema_name="main",
                name="table",
                columns=SerializedSchema(b""),
                on_conflict=OnConflict.ERROR,
                not_null_constraints=[],
                unique_constraints=[],
                check_constraints=[],
            ),
        ),
        (
            "table_drop",
            lambda c: c.table_drop(
                attach_id=TEST_ATTACH_ID,
                transaction_id=None,
                schema_name="main",
                name="table",
                ignore_not_found=False,
            ),
        ),
        (
            "table_rename",
            lambda c: c.table_rename(
                attach_id=TEST_ATTACH_ID,
                transaction_id=None,
                schema_name="main",
                name="old",
                new_name="new",
                ignore_not_found=False,
            ),
        ),
        (
            "view_create",
            lambda c: c.view_create(
                attach_id=TEST_ATTACH_ID,
                transaction_id=None,
                schema_name="main",
                name="view",
                definition="SELECT 1",
                on_conflict=OnConflict.ERROR,
            ),
        ),
        (
            "view_drop",
            lambda c: c.view_drop(
                attach_id=TEST_ATTACH_ID,
                transaction_id=None,
                schema_name="main",
                name="view",
                ignore_not_found=False,
            ),
        ),
        (
            "macro_create",
            lambda c: c.macro_create(
                attach_id=TEST_ATTACH_ID,
                transaction_id=None,
                schema_name="main",
                name="my_macro",
                macro_type=MacroType.SCALAR,
                parameters=["x"],
                definition="x * 2",
                on_conflict=OnConflict.ERROR,
            ),
        ),
        (
            "macro_drop",
            lambda c: c.macro_drop(
                attach_id=TEST_ATTACH_ID,
                transaction_id=None,
                schema_name="main",
                name="my_macro",
                ignore_not_found=False,
            ),
        ),
    ]


class TestReadOnlyCatalogInterface:
    """Test ReadOnlyCatalogInterface DDL rejection."""

    @pytest.mark.parametrize(
        ("method", "call_fn"),
        _readonly_test_cases(),
        ids=[t[0] for t in _readonly_test_cases()],
    )
    def test_readonly_raises_error(
        self,
        readonly_catalog: MinimalReadOnlyCatalog,
        method: str,
        call_fn: Callable[[MinimalReadOnlyCatalog], Any],
    ) -> None:
        """DDL methods raise CatalogReadOnlyError."""
        with pytest.raises(CatalogReadOnlyError, match="read-only"):
            call_fn(readonly_catalog)

    def test_class_attributes(self) -> None:
        """ReadOnlyCatalogInterface has correct class attributes."""
        assert ReadOnlyCatalogInterface.supports_transactions is False
        assert ReadOnlyCatalogInterface.catalog_version_frozen is True


class TestFunctionInfoNewFields:
    """Test FunctionInfo new metadata fields and serialization."""

    def test_default_values(self) -> None:
        """Create FunctionInfo with only required fields, verify defaults."""
        schema_bytes = empty_schema_bytes()
        info = FunctionInfo(
            name="test_func",
            schema_name="main",
            function_type=FunctionType.SCALAR,
            arguments=schema_bytes,
            output_schema=schema_bytes,
            comment=None,
            tags={},
        )

        # Scalar behavior fields default to None (set by _function_to_info)
        assert info.stability is None
        assert info.null_handling is None

        # Documentation fields
        assert info.examples == []
        assert info.categories == []

        # Table function capabilities default to None (set by _function_to_info)
        assert info.projection_pushdown is None
        assert info.filter_pushdown is None
        assert info.order_preservation is None
        assert info.max_workers is None

        # Aggregate function fields
        assert info.order_dependent == OrderDependence.NOT_ORDER_DEPENDENT
        assert info.distinct_dependent == DistinctDependence.NOT_DISTINCT_DEPENDENT

        # Settings
        assert info.required_settings == []

    def test_serialization_roundtrip_with_all_fields(self) -> None:
        """Serialize and deserialize FunctionInfo with all new fields set."""
        schema_bytes = empty_schema_bytes()
        info = FunctionInfo(
            name="test_func",
            schema_name="main",
            function_type=FunctionType.SCALAR,
            arguments=schema_bytes,
            output_schema=schema_bytes,
            comment=None,
            tags={},
            stability=FunctionStability.VOLATILE,
            null_handling=NullHandling.SPECIAL,
            examples=[
                CatalogExample(sql="SELECT test_func(1)"),
                CatalogExample(sql="SELECT test_func(2)"),
            ],
            categories=["math", "utility"],
            projection_pushdown=False,
            filter_pushdown=True,
            order_preservation=OrderPreservation.NO_ORDER_GUARANTEE,
            max_workers=4,
            order_dependent=OrderDependence.ORDER_DEPENDENT,
            distinct_dependent=DistinctDependence.DISTINCT_DEPENDENT,
            required_settings=["vgi_debug", "vgi_verbose"],
        )

        restored = function_info_round_trip(info)

        # Verify all fields match
        assert restored.name == info.name
        assert restored.schema_name == info.schema_name
        assert restored.function_type == info.function_type
        assert restored.arguments == info.arguments
        assert restored.output_schema == info.output_schema
        assert restored.comment == info.comment
        assert restored.tags == info.tags

        # New fields
        assert restored.stability == info.stability
        assert restored.null_handling == info.null_handling
        # Examples are deserialized to CatalogExample objects
        assert len(restored.examples) == len(info.examples)
        for restored_ex, orig_ex in zip(restored.examples, info.examples, strict=True):
            assert restored_ex.sql == orig_ex.sql
        assert restored.categories == info.categories
        assert restored.projection_pushdown == info.projection_pushdown
        assert restored.filter_pushdown == info.filter_pushdown
        assert restored.order_preservation == info.order_preservation
        assert restored.max_workers == info.max_workers
        assert restored.order_dependent == info.order_dependent
        assert restored.distinct_dependent == info.distinct_dependent
        assert restored.required_settings == info.required_settings

    def test_enum_serialization(self) -> None:
        """Verify enums serialize to strings and deserialize back correctly."""
        schema_bytes = empty_schema_bytes()
        info = FunctionInfo(
            name="test_func",
            schema_name="main",
            function_type=FunctionType.SCALAR,
            arguments=schema_bytes,
            output_schema=schema_bytes,
            comment=None,
            tags={},
            stability=FunctionStability.CONSISTENT_WITHIN_QUERY,
            null_handling=NullHandling.SPECIAL,
            order_preservation=OrderPreservation.NO_ORDER_GUARANTEE,
            order_dependent=OrderDependence.ORDER_DEPENDENT,
            distinct_dependent=DistinctDependence.DISTINCT_DEPENDENT,
        )

        # Serialize and inspect the Arrow data
        batch, _ = deserialize_record_batch(info.serialize_to_bytes())

        # Verify enums were serialized as strings
        row = batch.to_pydict()
        assert row["stability"][0] == "CONSISTENT_WITHIN_QUERY"
        assert row["null_handling"][0] == "SPECIAL"
        assert row["order_preservation"][0] == "NO_ORDER_GUARANTEE"
        assert row["order_dependent"][0] == "ORDER_DEPENDENT"
        assert row["distinct_dependent"][0] == "DISTINCT_DEPENDENT"

        # Verify deserialization produces correct enum values
        restored = FunctionInfo.deserialize_from_batch(batch)
        assert restored.stability == FunctionStability.CONSISTENT_WITHIN_QUERY
        assert restored.null_handling == NullHandling.SPECIAL
        assert restored.order_preservation == OrderPreservation.NO_ORDER_GUARANTEE
        assert restored.order_dependent == OrderDependence.ORDER_DEPENDENT
        assert restored.distinct_dependent == DistinctDependence.DISTINCT_DEPENDENT

    def test_backward_compatibility_without_new_fields(self) -> None:
        """Deserialize data that was serialized without new fields (legacy data)."""
        # Create legacy schema without new fields
        legacy_schema_bytes = pa.schema([]).serialize().to_pybytes()

        legacy_fields: list[pa.Field[pa.DataType]] = [
            pa.field("name", pa.string(), nullable=False),
            pa.field("schema_name", pa.string(), nullable=False),
            pa.field("function_type", pa.string(), nullable=False),
            pa.field("arguments", pa.binary(), nullable=False),
            pa.field("output_schema", pa.binary(), nullable=False),
            pa.field("comment", pa.string(), nullable=True),
            pa.field("tags", pa.map_(pa.string(), pa.string()), nullable=False),
        ]
        legacy_schema = pa.schema(legacy_fields)

        # Create legacy batch (without new fields)
        legacy_batch = pa.RecordBatch.from_pylist(
            [
                {
                    "name": "legacy_func",
                    "schema_name": "main",
                    "function_type": "scalar",
                    "arguments": legacy_schema_bytes,
                    "output_schema": legacy_schema_bytes,
                    "comment": "A legacy function",
                    "tags": [("version", "1.0")],
                }
            ],
            schema=legacy_schema,
        )

        # Deserialize - should use defaults for missing fields
        restored = FunctionInfo.deserialize_from_batch(legacy_batch)

        # Core fields should be preserved
        assert restored.name == "legacy_func"
        assert restored.schema_name == "main"
        assert restored.function_type == FunctionType.SCALAR
        assert restored.comment == "A legacy function"  # Comment is preserved
        assert restored.tags == {"version": "1.0"}

        # Optional fields should be None/default when not in legacy data
        assert restored.stability is None
        assert restored.null_handling is None
        assert restored.description == ""  # Default for missing description
        assert restored.examples == []
        assert restored.categories == []
        assert restored.projection_pushdown is None
        assert restored.filter_pushdown is None
        assert restored.order_preservation is None
        assert restored.max_workers is None
        assert restored.order_dependent == OrderDependence.NOT_ORDER_DEPENDENT
        assert restored.distinct_dependent == DistinctDependence.NOT_DISTINCT_DEPENDENT
        assert restored.required_settings == []

    @pytest.mark.parametrize("max_workers", [None, 8])
    def test_max_workers_nullable(self, max_workers: int | None) -> None:
        """Verify max_workers can be None or an integer."""
        schema_bytes = empty_schema_bytes()
        info = FunctionInfo(
            name="test_func",
            schema_name="main",
            function_type=FunctionType.SCALAR,
            arguments=schema_bytes,
            output_schema=schema_bytes,
            comment=None,
            tags={},
            max_workers=max_workers,
        )
        assert info.max_workers == max_workers

        restored = function_info_round_trip(info)
        assert restored.max_workers == max_workers

    def test_list_fields_serialization(self) -> None:
        """Verify list fields serialize and deserialize correctly."""
        schema_bytes = empty_schema_bytes()
        info = FunctionInfo(
            name="test_func",
            schema_name="main",
            function_type=FunctionType.SCALAR,
            arguments=schema_bytes,
            output_schema=schema_bytes,
            comment=None,
            tags={},
            examples=[
                CatalogExample(sql="SELECT f(1)"),
                CatalogExample(sql="SELECT f(2)"),
                CatalogExample(sql="SELECT f(3)"),
            ],
            categories=["a", "b"],
            required_settings=["setting1"],
        )

        restored = function_info_round_trip(info)

        # Examples are deserialized to CatalogExample objects
        assert [ex.sql for ex in restored.examples if hasattr(ex, "sql")] == [
            "SELECT f(1)",
            "SELECT f(2)",
            "SELECT f(3)",
        ]
        assert restored.categories == ["a", "b"]
        assert restored.required_settings == ["setting1"]

    def test_empty_list_fields(self) -> None:
        """Verify empty list fields serialize and deserialize correctly."""
        schema_bytes = empty_schema_bytes()
        info = FunctionInfo(
            name="test_func",
            schema_name="main",
            function_type=FunctionType.SCALAR,
            arguments=schema_bytes,
            output_schema=schema_bytes,
            comment=None,
            tags={},
            examples=[],
            categories=[],
            required_settings=[],
        )

        restored = function_info_round_trip(info)

        assert restored.examples == []
        assert restored.categories == []
        assert restored.required_settings == []


class TestSchemaObjectType:
    """Test SchemaObjectType enum."""

    def test_enum_values(self) -> None:
        """Verify all enum values are accessible."""
        assert SchemaObjectType.TABLE.value == "table"
        assert SchemaObjectType.VIEW.value == "view"
        assert SchemaObjectType.SCALAR_FUNCTION.value == "scalar_function"
        assert SchemaObjectType.TABLE_FUNCTION.value == "table_function"
        assert SchemaObjectType.SCALAR_MACRO.value == "scalar_macro"
        assert SchemaObjectType.TABLE_MACRO.value == "table_macro"

    def test_enum_from_string(self) -> None:
        """Verify enum can be created from string values."""
        assert SchemaObjectType("table") == SchemaObjectType.TABLE
        assert SchemaObjectType("view") == SchemaObjectType.VIEW
        assert SchemaObjectType("scalar_function") == SchemaObjectType.SCALAR_FUNCTION
        assert SchemaObjectType("table_function") == SchemaObjectType.TABLE_FUNCTION
        assert SchemaObjectType("scalar_macro") == SchemaObjectType.SCALAR_MACRO
        assert SchemaObjectType("table_macro") == SchemaObjectType.TABLE_MACRO


class TestSchemaContentsTypeFilter:
    """Test schema_contents type filtering in ReadOnlyCatalogInterface."""

    @pytest.fixture
    def catalog_with_functions(self) -> ReadOnlyCatalogInterface:
        """Create a catalog with scalar and table functions."""
        from dataclasses import dataclass
        from typing import ClassVar

        from vgi_rpc.rpc import OutputCollector

        from vgi import ScalarFunction
        from vgi.table_function import (
            ProcessParams,
            TableFunctionGenerator,
            bind_fixed_schema,
            init_single_worker,
        )

        class MyScalarFunction(ScalarFunction):
            """A test scalar function."""

            class Meta:
                output_type = pa.int64()

            def compute(self, batch: pa.RecordBatch) -> "pa.Array[pa.Int64Scalar]":
                return pa.array([1] * batch.num_rows, type=pa.int64())

        @dataclass(slots=True, frozen=True)
        class _EmptyArgs:
            """No arguments."""

        @init_single_worker
        @bind_fixed_schema
        class MyTableFunction(TableFunctionGenerator[_EmptyArgs]):
            """A test table function."""

            class Meta:
                name = "my_table"
                description = "A test table function"

            FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([("value", pa.int64())])

            @classmethod
            def process(cls, params: ProcessParams[_EmptyArgs], state: None, out: OutputCollector) -> None:
                out.emit(pa.RecordBatch.from_pydict({"value": [1]}, schema=params.output_schema))
                out.finish()

        class TestCatalog(ReadOnlyCatalogInterface):
            catalog_name = "test"
            functions = [MyScalarFunction, MyTableFunction]

        return TestCatalog()

    def test_fetch_all_function_types(self, catalog_with_functions: ReadOnlyCatalogInterface) -> None:
        """Can fetch both scalar and table functions with separate calls."""
        attach_result = catalog_with_functions.catalog_attach(name="test", options={})

        # Get scalar functions
        scalar_contents = list(
            catalog_with_functions.schema_contents(
                attach_id=attach_result.attach_id,
                transaction_id=None,
                name="main",
                type=SchemaObjectType.SCALAR_FUNCTION,
            )
        )

        # Get table functions
        table_contents = list(
            catalog_with_functions.schema_contents(
                attach_id=attach_result.attach_id,
                transaction_id=None,
                name="main",
                type=SchemaObjectType.TABLE_FUNCTION,
            )
        )

        # Combined should have both functions
        all_contents = scalar_contents + table_contents
        assert len(all_contents) == 2
        names = {obj.name for obj in all_contents}
        # Names are derived from class names: MyScalarFunction -> my_scalar
        assert "my_scalar" in names
        assert "my_table" in names

    def test_filter_scalar_function(self, catalog_with_functions: ReadOnlyCatalogInterface) -> None:
        """schema_contents with SCALAR_FUNCTION filter returns only scalar functions."""
        attach_result = catalog_with_functions.catalog_attach(name="test", options={})
        contents = list(
            catalog_with_functions.schema_contents(
                attach_id=attach_result.attach_id,
                transaction_id=None,
                name="main",
                type=SchemaObjectType.SCALAR_FUNCTION,
            )
        )
        assert len(contents) == 1
        func_info = contents[0]
        assert isinstance(func_info, FunctionInfo)
        assert func_info.name == "my_scalar"
        assert func_info.function_type == FunctionType.SCALAR

    def test_filter_table_function(self, catalog_with_functions: ReadOnlyCatalogInterface) -> None:
        """schema_contents with TABLE_FUNCTION filter returns only table functions."""
        attach_result = catalog_with_functions.catalog_attach(name="test", options={})
        contents = list(
            catalog_with_functions.schema_contents(
                attach_id=attach_result.attach_id,
                transaction_id=None,
                name="main",
                type=SchemaObjectType.TABLE_FUNCTION,
            )
        )
        assert len(contents) == 1
        func_info = contents[0]
        assert isinstance(func_info, FunctionInfo)
        assert func_info.name == "my_table"
        assert func_info.function_type == FunctionType.TABLE

    def test_filter_table_returns_empty(self, catalog_with_functions: ReadOnlyCatalogInterface) -> None:
        """schema_contents with TABLE filter returns empty (no tables in catalog)."""
        attach_result = catalog_with_functions.catalog_attach(name="test", options={})
        contents = list(
            catalog_with_functions.schema_contents(
                attach_id=attach_result.attach_id,
                transaction_id=None,
                name="main",
                type=SchemaObjectType.TABLE,
            )
        )
        assert len(contents) == 0

    def test_filter_view_returns_empty(self, catalog_with_functions: ReadOnlyCatalogInterface) -> None:
        """schema_contents with VIEW filter returns empty (no views in catalog)."""
        attach_result = catalog_with_functions.catalog_attach(name="test", options={})
        contents = list(
            catalog_with_functions.schema_contents(
                attach_id=attach_result.attach_id,
                transaction_id=None,
                name="main",
                type=SchemaObjectType.VIEW,
            )
        )
        assert len(contents) == 0

    def test_wrong_schema_returns_empty(self, catalog_with_functions: ReadOnlyCatalogInterface) -> None:
        """schema_contents with non-existent schema returns empty."""
        attach_result = catalog_with_functions.catalog_attach(name="test", options={})
        contents = list(
            catalog_with_functions.schema_contents(
                attach_id=attach_result.attach_id,
                transaction_id=None,
                name="nonexistent",
                type=SchemaObjectType.TABLE_FUNCTION,
            )
        )
        assert len(contents) == 0
