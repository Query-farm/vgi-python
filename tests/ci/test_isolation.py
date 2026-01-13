"""Tests for attachment isolation in CICatalog."""

from __future__ import annotations

from vgi.catalog import AttachId, OnConflict, SerializedSchema
from vgi.ci.catalog import CICatalog


class TestSchemaIsolation:
    """Tests for schema isolation between attachments."""

    def test_schemas_isolated_between_attachments(self, catalog: CICatalog) -> None:
        """Schemas in one attachment don't appear in another."""
        result1 = catalog.catalog_attach(name="ci", options={})
        result2 = catalog.catalog_attach(name="ci", options={})

        # Create schema in attachment 1
        catalog.schema_create(
            attach_id=result1.attach_id,
            transaction_id=None,
            name="isolated_schema",
            comment=None,
            tags={},
        )

        # Schema should exist in attachment 1
        schema1 = catalog.schema_get(
            attach_id=result1.attach_id, transaction_id=None, name="isolated_schema"
        )
        assert schema1 is not None

        # Schema should NOT exist in attachment 2
        schema2 = catalog.schema_get(
            attach_id=result2.attach_id, transaction_id=None, name="isolated_schema"
        )
        assert schema2 is None

    def test_same_schema_name_different_attachments(self, catalog: CICatalog) -> None:
        """Same schema name can exist in multiple attachments."""
        result1 = catalog.catalog_attach(name="ci", options={})
        result2 = catalog.catalog_attach(name="ci", options={})

        # Create schema with same name in both attachments
        catalog.schema_create(
            attach_id=result1.attach_id,
            transaction_id=None,
            name="shared_name",
            comment="From attachment 1",
            tags={},
        )
        catalog.schema_create(
            attach_id=result2.attach_id,
            transaction_id=None,
            name="shared_name",
            comment="From attachment 2",
            tags={},
        )

        # Both should have their own version
        schema1 = catalog.schema_get(
            attach_id=result1.attach_id, transaction_id=None, name="shared_name"
        )
        schema2 = catalog.schema_get(
            attach_id=result2.attach_id, transaction_id=None, name="shared_name"
        )

        assert schema1 is not None
        assert schema2 is not None
        assert schema1.comment == "From attachment 1"
        assert schema2.comment == "From attachment 2"


class TestTableIsolation:
    """Tests for table isolation between attachments."""

    def test_tables_isolated_between_attachments(
        self, catalog: CICatalog, sample_schema_bytes: SerializedSchema
    ) -> None:
        """Tables in one attachment don't appear in another."""
        result1 = catalog.catalog_attach(name="ci", options={})
        result2 = catalog.catalog_attach(name="ci", options={})

        # Create table in attachment 1
        catalog.table_create(
            attach_id=result1.attach_id,
            transaction_id=None,
            schema_name="main",
            name="isolated_table",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        # Table should exist in attachment 1
        table1 = catalog.table_get(
            attach_id=result1.attach_id,
            transaction_id=None,
            schema_name="main",
            name="isolated_table",
        )
        assert table1 is not None

        # Table should NOT exist in attachment 2
        table2 = catalog.table_get(
            attach_id=result2.attach_id,
            transaction_id=None,
            schema_name="main",
            name="isolated_table",
        )
        assert table2 is None

    def test_same_table_name_different_attachments(
        self, catalog: CICatalog, sample_schema_bytes: SerializedSchema
    ) -> None:
        """Same table name can exist in multiple attachments."""
        result1 = catalog.catalog_attach(name="ci", options={})
        result2 = catalog.catalog_attach(name="ci", options={})

        # Create table with same name in both attachments
        catalog.table_create(
            attach_id=result1.attach_id,
            transaction_id=None,
            schema_name="main",
            name="shared_table",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[0],  # Different constraints
            unique_constraints=[],
            check_constraints=[],
        )
        catalog.table_create(
            attach_id=result2.attach_id,
            transaction_id=None,
            schema_name="main",
            name="shared_table",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],  # Different constraints
            unique_constraints=[[0]],
            check_constraints=[],
        )

        # Both should have their own version
        table1 = catalog.table_get(
            attach_id=result1.attach_id,
            transaction_id=None,
            schema_name="main",
            name="shared_table",
        )
        table2 = catalog.table_get(
            attach_id=result2.attach_id,
            transaction_id=None,
            schema_name="main",
            name="shared_table",
        )

        assert table1 is not None
        assert table2 is not None
        assert table1.not_null_constraints == [0]
        assert table2.unique_constraints == [[0]]


class TestViewIsolation:
    """Tests for view isolation between attachments."""

    def test_views_isolated_between_attachments(self, catalog: CICatalog) -> None:
        """Views in one attachment don't appear in another."""
        result1 = catalog.catalog_attach(name="ci", options={})
        result2 = catalog.catalog_attach(name="ci", options={})

        # Create view in attachment 1
        catalog.view_create(
            attach_id=result1.attach_id,
            transaction_id=None,
            schema_name="main",
            name="isolated_view",
            definition="SELECT 1 AS n",
            on_conflict=OnConflict.ERROR,
        )

        # View should exist in attachment 1
        view1 = catalog.view_get(
            attach_id=result1.attach_id,
            transaction_id=None,
            schema_name="main",
            name="isolated_view",
        )
        assert view1 is not None

        # View should NOT exist in attachment 2
        view2 = catalog.view_get(
            attach_id=result2.attach_id,
            transaction_id=None,
            schema_name="main",
            name="isolated_view",
        )
        assert view2 is None


class TestVersionIsolation:
    """Tests for version tracking isolation."""

    def test_versions_independent_per_attachment(self, catalog: CICatalog) -> None:
        """Version increments are independent per attachment."""
        result1 = catalog.catalog_attach(name="ci", options={})
        result2 = catalog.catalog_attach(name="ci", options={})

        v1_initial = catalog.catalog_version(
            attach_id=result1.attach_id, transaction_id=None
        )
        v2_initial = catalog.catalog_version(
            attach_id=result2.attach_id, transaction_id=None
        )

        # Make changes only in attachment 1
        catalog.schema_create(
            attach_id=result1.attach_id,
            transaction_id=None,
            name="schema1",
            comment=None,
            tags={},
        )
        catalog.schema_create(
            attach_id=result1.attach_id,
            transaction_id=None,
            name="schema2",
            comment=None,
            tags={},
        )

        v1_after = catalog.catalog_version(
            attach_id=result1.attach_id, transaction_id=None
        )
        v2_after = catalog.catalog_version(
            attach_id=result2.attach_id, transaction_id=None
        )

        # Attachment 1 version should have increased by 2
        assert v1_after == v1_initial + 2

        # Attachment 2 version should be unchanged
        assert v2_after == v2_initial


class TestDDLIsolation:
    """Tests for DDL operation isolation."""

    def test_drop_in_one_attachment_doesnt_affect_other(
        self, catalog: CICatalog, sample_schema_bytes: SerializedSchema
    ) -> None:
        """Dropping object in one attachment doesn't affect another."""
        result1 = catalog.catalog_attach(name="ci", options={})
        result2 = catalog.catalog_attach(name="ci", options={})

        # Create same named table in both attachments
        for result in [result1, result2]:
            catalog.table_create(
                attach_id=result.attach_id,
                transaction_id=None,
                schema_name="main",
                name="test_table",
                columns=sample_schema_bytes,
                on_conflict=OnConflict.ERROR,
                not_null_constraints=[],
                unique_constraints=[],
                check_constraints=[],
            )

        # Drop table in attachment 1
        catalog.table_drop(
            attach_id=result1.attach_id,
            transaction_id=None,
            schema_name="main",
            name="test_table",
            ignore_not_found=False,
        )

        # Table should be gone from attachment 1
        assert (
            catalog.table_get(
                attach_id=result1.attach_id,
                transaction_id=None,
                schema_name="main",
                name="test_table",
            )
            is None
        )

        # Table should still exist in attachment 2
        assert (
            catalog.table_get(
                attach_id=result2.attach_id,
                transaction_id=None,
                schema_name="main",
                name="test_table",
            )
            is not None
        )

    def test_rename_in_one_attachment_doesnt_affect_other(
        self, catalog: CICatalog, sample_schema_bytes: SerializedSchema
    ) -> None:
        """Renaming object in one attachment doesn't affect another."""
        result1 = catalog.catalog_attach(name="ci", options={})
        result2 = catalog.catalog_attach(name="ci", options={})

        # Create same named table in both attachments
        for result in [result1, result2]:
            catalog.table_create(
                attach_id=result.attach_id,
                transaction_id=None,
                schema_name="main",
                name="original_name",
                columns=sample_schema_bytes,
                on_conflict=OnConflict.ERROR,
                not_null_constraints=[],
                unique_constraints=[],
                check_constraints=[],
            )

        # Rename table in attachment 1
        catalog.table_rename(
            attach_id=result1.attach_id,
            transaction_id=None,
            schema_name="main",
            name="original_name",
            new_name="new_name",
            ignore_not_found=False,
        )

        # Attachment 1: old name gone, new name exists
        assert (
            catalog.table_get(
                attach_id=result1.attach_id,
                transaction_id=None,
                schema_name="main",
                name="original_name",
            )
            is None
        )
        assert (
            catalog.table_get(
                attach_id=result1.attach_id,
                transaction_id=None,
                schema_name="main",
                name="new_name",
            )
            is not None
        )

        # Attachment 2: still has original name, no new name
        assert (
            catalog.table_get(
                attach_id=result2.attach_id,
                transaction_id=None,
                schema_name="main",
                name="original_name",
            )
            is not None
        )
        assert (
            catalog.table_get(
                attach_id=result2.attach_id,
                transaction_id=None,
                schema_name="main",
                name="new_name",
            )
            is None
        )


class TestDetachIsolation:
    """Tests for detach behavior isolation."""

    def test_detach_doesnt_affect_other_attachments(
        self, catalog: CICatalog, sample_schema_bytes: SerializedSchema
    ) -> None:
        """Detaching one attachment doesn't affect others."""
        result1 = catalog.catalog_attach(name="ci", options={})
        result2 = catalog.catalog_attach(name="ci", options={})

        # Create table in attachment 2
        catalog.table_create(
            attach_id=result2.attach_id,
            transaction_id=None,
            schema_name="main",
            name="survivor_table",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        # Detach attachment 1
        catalog.catalog_detach(attach_id=result1.attach_id)

        # Attachment 2 should still work
        table = catalog.table_get(
            attach_id=result2.attach_id,
            transaction_id=None,
            schema_name="main",
            name="survivor_table",
        )
        assert table is not None

    def test_multiple_attachments_to_same_catalog(self, catalog: CICatalog) -> None:
        """Multiple attachments to same catalog are independent."""
        # Create multiple attachments to "ci" catalog
        attachments: list[AttachId] = []
        for _ in range(5):
            result = catalog.catalog_attach(name="ci", options={})
            attachments.append(result.attach_id)

        # Each should have its own "main" schema
        for attach_id in attachments:
            schema = catalog.schema_get(
                attach_id=attach_id, transaction_id=None, name="main"
            )
            assert schema is not None
            assert schema.is_default is True

        # Detach all except the last
        for attach_id in attachments[:-1]:
            catalog.catalog_detach(attach_id=attach_id)

        # Last one should still work
        schema = catalog.schema_get(
            attach_id=attachments[-1], transaction_id=None, name="main"
        )
        assert schema is not None
