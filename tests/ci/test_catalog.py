"""Unit tests for CICatalog."""

from __future__ import annotations

import pytest

from vgi.catalog import AttachId
from vgi.ci.catalog import CICatalog
from vgi.ci.storage import AttachmentNotFoundError


class TestCatalogs:
    """Tests for catalogs() method."""

    def test_catalogs_returns_available(self, catalog: CICatalog) -> None:
        """catalogs() returns available catalog names."""
        catalogs = list(catalog.catalogs())
        assert set(catalogs) == {"ci", "test"}

    def test_catalogs_after_create(self, catalog: CICatalog) -> None:
        """catalogs() includes newly created catalog."""
        from vgi.catalog import OnConflict

        catalog.catalog_create(name="new", on_conflict=OnConflict.ERROR, options={})

        catalogs = list(catalog.catalogs())
        assert "new" in catalogs

    def test_catalogs_after_drop(self, catalog: CICatalog) -> None:
        """catalogs() excludes dropped catalog."""
        from vgi.catalog import OnConflict

        catalog.catalog_create(name="temp", on_conflict=OnConflict.ERROR, options={})
        catalog.catalog_drop(name="temp")

        catalogs = list(catalog.catalogs())
        assert "temp" not in catalogs


class TestCatalogAttach:
    """Tests for catalog_attach() method."""

    def test_attach_returns_result(self, catalog: CICatalog) -> None:
        """catalog_attach() returns CatalogAttachResult."""
        result = catalog.catalog_attach(name="ci", options={})

        assert result.attach_id is not None
        assert len(result.attach_id) == 16  # UUID bytes
        assert result.supports_transactions is True
        assert result.supports_time_travel is False
        assert result.attach_id_required is True

    def test_attach_creates_unique_ids(self, catalog: CICatalog) -> None:
        """Each attach creates unique attach_id."""
        result1 = catalog.catalog_attach(name="ci", options={})
        result2 = catalog.catalog_attach(name="ci", options={})

        assert result1.attach_id != result2.attach_id

    def test_attach_to_test_catalog(self, catalog: CICatalog) -> None:
        """Can attach to 'test' catalog."""
        result = catalog.catalog_attach(name="test", options={})
        assert result.attach_id is not None

    def test_attach_to_unknown_catalog(self, catalog: CICatalog) -> None:
        """Attaching to unknown catalog raises error."""
        with pytest.raises(ValueError, match="not found"):
            catalog.catalog_attach(name="unknown", options={})


class TestCatalogDetach:
    """Tests for catalog_detach() method."""

    def test_detach_succeeds(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """catalog_detach() cleans up attachment."""
        catalog, attach_id = attached_catalog

        catalog.catalog_detach(attach_id=attach_id)

        # Further operations should fail
        with pytest.raises(AttachmentNotFoundError):
            catalog.catalog_version(attach_id=attach_id, transaction_id=None)

    def test_detach_nonexistent(self, catalog: CICatalog) -> None:
        """Detaching non-existent attachment doesn't raise."""
        catalog.catalog_detach(attach_id=AttachId(b"nonexistent"))


class TestCatalogVersion:
    """Tests for catalog_version() method."""

    def test_initial_version(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """New attachment has version 1."""
        catalog, attach_id = attached_catalog

        version = catalog.catalog_version(attach_id=attach_id, transaction_id=None)
        assert version == 1

    def test_version_increments(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Version increments on DDL operations."""
        catalog, attach_id = attached_catalog
        v1 = catalog.catalog_version(attach_id=attach_id, transaction_id=None)

        catalog.schema_create(
            attach_id=attach_id,
            transaction_id=None,
            name="new_schema",
            comment=None,
            tags={},
        )

        v2 = catalog.catalog_version(attach_id=attach_id, transaction_id=None)
        assert v2 == v1 + 1


class TestCatalogDDL:
    """Tests for catalog_create() and catalog_drop()."""

    def test_create_catalog(self, catalog: CICatalog) -> None:
        """Creating new catalog adds it to available catalogs."""
        from vgi.catalog import OnConflict

        catalog.catalog_create(name="new_cat", on_conflict=OnConflict.ERROR, options={})

        assert "new_cat" in list(catalog.catalogs())

    def test_create_existing_error(self, catalog: CICatalog) -> None:
        """Creating existing catalog with ERROR raises."""
        from vgi.catalog import OnConflict

        with pytest.raises(ValueError, match="already exists"):
            catalog.catalog_create(name="ci", on_conflict=OnConflict.ERROR, options={})

    def test_create_existing_ignore(self, catalog: CICatalog) -> None:
        """Creating existing catalog with IGNORE doesn't raise."""
        from vgi.catalog import OnConflict

        catalog.catalog_create(name="ci", on_conflict=OnConflict.IGNORE, options={})
        # Should not raise

    def test_create_existing_replace(self, catalog: CICatalog) -> None:
        """Creating existing catalog with REPLACE succeeds."""
        from vgi.catalog import OnConflict

        catalog.catalog_create(name="ci", on_conflict=OnConflict.REPLACE, options={})
        # Should not raise

    def test_drop_catalog(self, catalog: CICatalog) -> None:
        """Dropping catalog removes it from available catalogs."""
        from vgi.catalog import OnConflict

        catalog.catalog_create(name="to_drop", on_conflict=OnConflict.ERROR, options={})
        catalog.catalog_drop(name="to_drop")

        assert "to_drop" not in list(catalog.catalogs())

    def test_drop_nonexistent(self, catalog: CICatalog) -> None:
        """Dropping non-existent catalog raises."""
        with pytest.raises(ValueError, match="not found"):
            catalog.catalog_drop(name="nonexistent")


class TestFeatureFlags:
    """Tests for interface_feature_flags property."""

    def test_feature_flags(self, catalog: CICatalog) -> None:
        """Catalog reports correct feature flags."""
        flags = catalog.interface_feature_flags
        assert "transactions" in flags
        assert "table_data" in flags


class TestSchemaGet:
    """Tests for schema_get() method."""

    def test_get_main_schema(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Can get default 'main' schema."""
        catalog, attach_id = attached_catalog

        schema = catalog.schema_get(
            attach_id=attach_id, transaction_id=None, name="main"
        )

        assert schema is not None
        assert schema.name == "main"

    def test_get_nonexistent_schema(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Getting non-existent schema returns None."""
        catalog, attach_id = attached_catalog

        schema = catalog.schema_get(
            attach_id=attach_id, transaction_id=None, name="nonexistent"
        )

        assert schema is None

    def test_get_created_schema(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Can get schema after creation."""
        catalog, attach_id = attached_catalog
        catalog.schema_create(
            attach_id=attach_id,
            transaction_id=None,
            name="custom",
            comment="My schema",
            tags={"foo": "bar"},
        )

        schema = catalog.schema_get(
            attach_id=attach_id, transaction_id=None, name="custom"
        )

        assert schema is not None
        assert schema.name == "custom"
        assert schema.comment == "My schema"
        assert schema.tags == {"foo": "bar"}


class TestTableGet:
    """Tests for table_get() method."""

    def test_get_nonexistent_table(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Getting non-existent table returns None."""
        catalog, attach_id = attached_catalog

        table = catalog.table_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="nonexistent",
        )

        assert table is None


class TestViewGet:
    """Tests for view_get() method."""

    def test_get_nonexistent_view(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Getting non-existent view returns None."""
        catalog, attach_id = attached_catalog

        view = catalog.view_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="nonexistent",
        )

        assert view is None
