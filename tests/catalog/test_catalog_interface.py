"""Tests for CatalogInterface ABC and default implementations."""

from collections.abc import Iterable
from typing import Any

import pytest

from vgi.catalog import (
    AttachId,
    CatalogAttachResult,
    CatalogInterface,
    FunctionInfo,
    OnConflict,
    SchemaInfo,
    SerializedSchema,
    TableInfo,
    TransactionId,
    ViewInfo,
)
from vgi.catalog.catalog_interface import ReadOnlyCatalogInterface
from vgi.exceptions import CatalogReadOnlyError


class MinimalCatalog(CatalogInterface):
    """Minimal implementation for testing abstract method requirements."""

    def catalogs(self) -> Iterable[str]:
        """Return list of catalogs."""
        return ["test"]

    def catalog_attach(
        self, *, name: str, options: dict[str, Any]
    ) -> CatalogAttachResult:
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
                is_default=True,
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
        assert schemas[0].is_default is True
        assert schemas[0].comment is None
        assert schemas[0].tags == {}

    def test_catalog_version_returns_zero(self) -> None:
        """Default catalog_version() returns 0."""
        catalog = MinimalCatalog()
        version = catalog.catalog_version(
            attach_id=AttachId(b"test"), transaction_id=None
        )
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


class TestCatalogInterfaceNotImplemented:
    """Test that optional methods raise NotImplementedError by default."""

    def test_catalog_create_not_implemented(self) -> None:
        """catalog_create raises NotImplementedError."""
        catalog = MinimalCatalog()
        with pytest.raises(NotImplementedError, match="Catalog create not implemented"):
            catalog.catalog_create(
                name="test", on_conflict=OnConflict.ERROR, options={}
            )

    def test_catalog_drop_not_implemented(self) -> None:
        """catalog_drop raises NotImplementedError."""
        catalog = MinimalCatalog()
        with pytest.raises(NotImplementedError, match="Catalog drop not implemented"):
            catalog.catalog_drop(name="test")

    def test_transaction_begin_not_implemented(self) -> None:
        """catalog_transaction_begin raises NotImplementedError."""
        catalog = MinimalCatalog()
        with pytest.raises(
            NotImplementedError, match="Catalog transactions not implemented"
        ):
            catalog.catalog_transaction_begin(attach_id=AttachId(b"test"))

    def test_transaction_commit_not_implemented(self) -> None:
        """catalog_transaction_commit raises NotImplementedError."""
        catalog = MinimalCatalog()
        with pytest.raises(
            NotImplementedError, match="Catalog transactions not implemented"
        ):
            catalog.catalog_transaction_commit(
                attach_id=AttachId(b"test"), transaction_id=TransactionId(b"tx")
            )

    def test_transaction_rollback_not_implemented(self) -> None:
        """catalog_transaction_rollback raises NotImplementedError."""
        catalog = MinimalCatalog()
        with pytest.raises(
            NotImplementedError, match="Catalog transactions not implemented"
        ):
            catalog.catalog_transaction_rollback(
                attach_id=AttachId(b"test"), transaction_id=TransactionId(b"tx")
            )

    def test_schema_create_not_implemented(self) -> None:
        """schema_create raises NotImplementedError."""
        catalog = MinimalCatalog()
        with pytest.raises(NotImplementedError, match="Schema create not implemented"):
            catalog.schema_create(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                name="new_schema",
                comment=None,
                tags={},
            )

    def test_schema_drop_not_implemented(self) -> None:
        """schema_drop raises NotImplementedError."""
        catalog = MinimalCatalog()
        with pytest.raises(NotImplementedError, match="Schema drop not implemented"):
            catalog.schema_drop(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                name="schema",
                ignore_not_found=False,
                cascade=False,
            )

    def test_schema_contents_not_implemented(self) -> None:
        """schema_contents raises NotImplementedError."""
        catalog = MinimalCatalog()
        with pytest.raises(
            NotImplementedError, match="Schema contents not implemented"
        ):
            catalog.schema_contents(
                attach_id=AttachId(b"test"), transaction_id=None, name="main"
            )

    def test_table_create_not_implemented(self) -> None:
        """table_create raises NotImplementedError."""
        catalog = MinimalCatalog()
        with pytest.raises(NotImplementedError, match="Table create not implemented"):
            catalog.table_create(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                schema_name="main",
                name="table",
                columns=SerializedSchema(b""),
                on_conflict=OnConflict.ERROR,
                not_null_constraints=[],
                unique_constraints=[],
                check_constraints=[],
            )

    def test_view_create_not_implemented(self) -> None:
        """view_create raises NotImplementedError."""
        catalog = MinimalCatalog()
        with pytest.raises(NotImplementedError, match="View create not implemented"):
            catalog.view_create(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                schema_name="main",
                name="view",
                definition="SELECT 1",
                on_conflict=OnConflict.ERROR,
            )


class MinimalReadOnlyCatalog(ReadOnlyCatalogInterface):
    """Minimal read-only implementation for testing."""

    def catalogs(self) -> Iterable[str]:
        """Return list of catalogs."""
        return ["readonly"]

    def catalog_attach(
        self, *, name: str, options: dict[str, Any]
    ) -> CatalogAttachResult:
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


class TestReadOnlyCatalogInterface:
    """Test ReadOnlyCatalogInterface DDL rejection."""

    def test_catalog_create_raises_readonly_error(self) -> None:
        """catalog_create raises CatalogReadOnlyError."""
        catalog = MinimalReadOnlyCatalog()
        with pytest.raises(CatalogReadOnlyError, match="read-only"):
            catalog.catalog_create(
                name="test", on_conflict=OnConflict.ERROR, options={}
            )

    def test_catalog_drop_raises_readonly_error(self) -> None:
        """catalog_drop raises CatalogReadOnlyError."""
        catalog = MinimalReadOnlyCatalog()
        with pytest.raises(CatalogReadOnlyError, match="read-only"):
            catalog.catalog_drop(name="test")

    def test_transaction_begin_raises_readonly_error(self) -> None:
        """catalog_transaction_begin raises CatalogReadOnlyError."""
        catalog = MinimalReadOnlyCatalog()
        with pytest.raises(CatalogReadOnlyError, match="read-only"):
            catalog.catalog_transaction_begin(attach_id=AttachId(b"test"))

    def test_transaction_commit_raises_readonly_error(self) -> None:
        """catalog_transaction_commit raises CatalogReadOnlyError."""
        catalog = MinimalReadOnlyCatalog()
        with pytest.raises(CatalogReadOnlyError, match="read-only"):
            catalog.catalog_transaction_commit(
                attach_id=AttachId(b"test"), transaction_id=TransactionId(b"tx")
            )

    def test_transaction_rollback_raises_readonly_error(self) -> None:
        """catalog_transaction_rollback raises CatalogReadOnlyError."""
        catalog = MinimalReadOnlyCatalog()
        with pytest.raises(CatalogReadOnlyError, match="read-only"):
            catalog.catalog_transaction_rollback(
                attach_id=AttachId(b"test"), transaction_id=TransactionId(b"tx")
            )

    def test_schema_create_raises_readonly_error(self) -> None:
        """schema_create raises CatalogReadOnlyError."""
        catalog = MinimalReadOnlyCatalog()
        with pytest.raises(CatalogReadOnlyError, match="read-only"):
            catalog.schema_create(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                name="new",
                comment=None,
                tags={},
            )

    def test_schema_drop_raises_readonly_error(self) -> None:
        """schema_drop raises CatalogReadOnlyError."""
        catalog = MinimalReadOnlyCatalog()
        with pytest.raises(CatalogReadOnlyError, match="read-only"):
            catalog.schema_drop(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                name="main",
                ignore_not_found=False,
                cascade=False,
            )

    def test_table_create_raises_readonly_error(self) -> None:
        """table_create raises CatalogReadOnlyError."""
        catalog = MinimalReadOnlyCatalog()
        with pytest.raises(CatalogReadOnlyError, match="read-only"):
            catalog.table_create(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                schema_name="main",
                name="table",
                columns=SerializedSchema(b""),
                on_conflict=OnConflict.ERROR,
                not_null_constraints=[],
                unique_constraints=[],
                check_constraints=[],
            )

    def test_table_drop_raises_readonly_error(self) -> None:
        """table_drop raises CatalogReadOnlyError."""
        catalog = MinimalReadOnlyCatalog()
        with pytest.raises(CatalogReadOnlyError, match="read-only"):
            catalog.table_drop(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                schema_name="main",
                name="table",
                ignore_not_found=False,
            )

    def test_table_rename_raises_readonly_error(self) -> None:
        """table_rename raises CatalogReadOnlyError."""
        catalog = MinimalReadOnlyCatalog()
        with pytest.raises(CatalogReadOnlyError, match="read-only"):
            catalog.table_rename(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                schema_name="main",
                name="old",
                new_name="new",
                ignore_not_found=False,
            )

    def test_view_create_raises_readonly_error(self) -> None:
        """view_create raises CatalogReadOnlyError."""
        catalog = MinimalReadOnlyCatalog()
        with pytest.raises(CatalogReadOnlyError, match="read-only"):
            catalog.view_create(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                schema_name="main",
                name="view",
                definition="SELECT 1",
                on_conflict=OnConflict.ERROR,
            )

    def test_view_drop_raises_readonly_error(self) -> None:
        """view_drop raises CatalogReadOnlyError."""
        catalog = MinimalReadOnlyCatalog()
        with pytest.raises(CatalogReadOnlyError, match="read-only"):
            catalog.view_drop(
                attach_id=AttachId(b"test"),
                transaction_id=None,
                schema_name="main",
                name="view",
                ignore_not_found=False,
            )

    def test_class_attributes(self) -> None:
        """ReadOnlyCatalogInterface has correct class attributes."""
        assert ReadOnlyCatalogInterface.supports_transactions is False
        assert ReadOnlyCatalogInterface.catalog_version_frozen is True


class TestFunctionInfoNewFields:
    """Test FunctionInfo new metadata fields and serialization."""

    def _get_empty_schema_bytes(self) -> SerializedSchema:
        """Create empty serialized schema for tests."""
        import pyarrow as pa

        empty_schema = pa.schema([])
        return SerializedSchema(empty_schema.serialize().to_pybytes())

    def test_default_values(self) -> None:
        """Create FunctionInfo with only required fields, verify defaults."""
        from vgi.catalog import FunctionType
        from vgi.catalog.catalog_interface import (
            DistinctDependence,
            OrderDependence,
        )

        schema_bytes = self._get_empty_schema_bytes()
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
        from vgi.catalog import FunctionType
        from vgi.catalog.catalog_interface import (
            DistinctDependence,
            FunctionStability,
            NullHandling,
            OrderDependence,
            OrderPreservation,
        )
        from vgi.ipc_utils import deserialize_record_batch

        schema_bytes = self._get_empty_schema_bytes()
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
            examples=["SELECT test_func(1)", "SELECT test_func(2)"],
            categories=["math", "utility"],
            projection_pushdown=False,
            filter_pushdown=True,
            order_preservation=OrderPreservation.NO_ORDER_GUARANTEE,
            max_workers=4,
            order_dependent=OrderDependence.ORDER_DEPENDENT,
            distinct_dependent=DistinctDependence.DISTINCT_DEPENDENT,
            required_settings=["vgi_debug", "vgi_verbose"],
        )

        # Serialize
        serialized = info.serialize()
        assert isinstance(serialized, bytes)

        # Deserialize
        batch = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize(batch)

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
        assert restored.examples == info.examples
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
        from vgi.catalog import FunctionType
        from vgi.catalog.catalog_interface import (
            DistinctDependence,
            FunctionStability,
            NullHandling,
            OrderDependence,
            OrderPreservation,
        )
        from vgi.ipc_utils import deserialize_record_batch

        schema_bytes = self._get_empty_schema_bytes()
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
        serialized = info.serialize()
        batch = deserialize_record_batch(serialized)

        # Verify enums were serialized as strings
        row = batch.to_pydict()
        assert row["stability"][0] == "CONSISTENT_WITHIN_QUERY"
        assert row["null_handling"][0] == "SPECIAL"
        assert row["order_preservation"][0] == "NO_ORDER_GUARANTEE"
        assert row["order_dependent"][0] == "ORDER_DEPENDENT"
        assert row["distinct_dependent"][0] == "DISTINCT_DEPENDENT"

        # Verify deserialization produces correct enum values
        restored = FunctionInfo.deserialize(batch)
        assert restored.stability == FunctionStability.CONSISTENT_WITHIN_QUERY
        assert restored.null_handling == NullHandling.SPECIAL
        assert restored.order_preservation == OrderPreservation.NO_ORDER_GUARANTEE
        assert restored.order_dependent == OrderDependence.ORDER_DEPENDENT
        assert restored.distinct_dependent == DistinctDependence.DISTINCT_DEPENDENT

    def test_backward_compatibility_without_new_fields(self) -> None:
        """Deserialize data that was serialized without new fields (legacy data)."""
        import pyarrow as pa

        from vgi.catalog import FunctionInfo, FunctionType
        from vgi.catalog.catalog_interface import (
            DistinctDependence,
            OrderDependence,
        )

        # Create legacy schema without new fields
        empty_schema = pa.schema([])
        empty_schema_bytes = empty_schema.serialize().to_pybytes()

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
                    "arguments": empty_schema_bytes,
                    "output_schema": empty_schema_bytes,
                    "comment": "A legacy function",
                    "tags": {"version": "1.0"},
                }
            ],
            schema=legacy_schema,
        )

        # Deserialize - should use defaults for missing fields
        restored = FunctionInfo.deserialize(legacy_batch)

        # Core fields should be preserved
        assert restored.name == "legacy_func"
        assert restored.schema_name == "main"
        assert restored.function_type == FunctionType.SCALAR
        assert restored.comment == "A legacy function"
        assert restored.tags == {"version": "1.0"}

        # Optional fields should be None when not in legacy data
        assert restored.stability is None
        assert restored.null_handling is None
        assert restored.examples == []
        assert restored.categories == []
        assert restored.projection_pushdown is None
        assert restored.filter_pushdown is None
        assert restored.order_preservation is None
        assert restored.max_workers is None
        assert restored.order_dependent == OrderDependence.NOT_ORDER_DEPENDENT
        assert restored.distinct_dependent == DistinctDependence.NOT_DISTINCT_DEPENDENT
        assert restored.required_settings == []

    def test_max_workers_nullable(self) -> None:
        """Verify max_workers can be None or an integer."""
        from vgi.catalog import FunctionType
        from vgi.ipc_utils import deserialize_record_batch

        schema_bytes = self._get_empty_schema_bytes()

        # Test with None
        info_none = FunctionInfo(
            name="test_func",
            schema_name="main",
            function_type=FunctionType.SCALAR,
            arguments=schema_bytes,
            output_schema=schema_bytes,
            comment=None,
            tags={},
            max_workers=None,
        )
        assert info_none.max_workers is None

        serialized = info_none.serialize()
        batch = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize(batch)
        assert restored.max_workers is None

        # Test with integer
        info_int = FunctionInfo(
            name="test_func",
            schema_name="main",
            function_type=FunctionType.SCALAR,
            arguments=schema_bytes,
            output_schema=schema_bytes,
            comment=None,
            tags={},
            max_workers=8,
        )
        assert info_int.max_workers == 8

        serialized = info_int.serialize()
        batch = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize(batch)
        assert restored.max_workers == 8

    def test_list_fields_serialization(self) -> None:
        """Verify list fields serialize and deserialize correctly."""
        from vgi.catalog import FunctionType
        from vgi.ipc_utils import deserialize_record_batch

        schema_bytes = self._get_empty_schema_bytes()
        info = FunctionInfo(
            name="test_func",
            schema_name="main",
            function_type=FunctionType.SCALAR,
            arguments=schema_bytes,
            output_schema=schema_bytes,
            comment=None,
            tags={},
            examples=["SELECT f(1)", "SELECT f(2)", "SELECT f(3)"],
            categories=["a", "b"],
            required_settings=["setting1"],
        )

        serialized = info.serialize()
        batch = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize(batch)

        assert restored.examples == ["SELECT f(1)", "SELECT f(2)", "SELECT f(3)"]
        assert restored.categories == ["a", "b"]
        assert restored.required_settings == ["setting1"]

    def test_empty_list_fields(self) -> None:
        """Verify empty list fields serialize and deserialize correctly."""
        from vgi.catalog import FunctionType
        from vgi.ipc_utils import deserialize_record_batch

        schema_bytes = self._get_empty_schema_bytes()
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

        serialized = info.serialize()
        batch = deserialize_record_batch(serialized)
        restored = FunctionInfo.deserialize(batch)

        assert restored.examples == []
        assert restored.categories == []
        assert restored.required_settings == []
