"""Tests for CatalogInterface ABC and default implementations."""

from collections.abc import Iterable
from typing import Any

import pytest

from vgi.catalog import (
    AttachId,
    CatalogAttachResult,
    CatalogInterface,
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
