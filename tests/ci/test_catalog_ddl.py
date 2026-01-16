"""Tests for CICatalog DDL operations."""

from __future__ import annotations

import pytest

from vgi.catalog import AttachId, OnConflict, SchemaObjectType, SerializedSchema
from vgi.ci.catalog import CICatalog


class TestSchemaCreate:
    """Tests for schema_create() method."""

    def test_create_schema(self, attached_catalog: tuple[CICatalog, AttachId]) -> None:
        """Creating schema succeeds."""
        catalog, attach_id = attached_catalog

        catalog.schema_create(
            attach_id=attach_id,
            transaction_id=None,
            name="my_schema",
            comment=None,
            tags={},
        )

        schema = catalog.schema_get(
            attach_id=attach_id, transaction_id=None, name="my_schema"
        )
        assert schema is not None
        assert schema.name == "my_schema"

    def test_create_schema_with_comment(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Creating schema with comment preserves it."""
        catalog, attach_id = attached_catalog

        catalog.schema_create(
            attach_id=attach_id,
            transaction_id=None,
            name="commented",
            comment="My schema description",
            tags={},
        )

        schema = catalog.schema_get(
            attach_id=attach_id, transaction_id=None, name="commented"
        )
        assert schema is not None
        assert schema.comment == "My schema description"

    def test_create_schema_with_tags(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Creating schema with tags preserves them."""
        catalog, attach_id = attached_catalog

        catalog.schema_create(
            attach_id=attach_id,
            transaction_id=None,
            name="tagged",
            comment=None,
            tags={"env": "prod", "team": "data"},
        )

        schema = catalog.schema_get(
            attach_id=attach_id, transaction_id=None, name="tagged"
        )
        assert schema is not None
        assert schema.tags == {"env": "prod", "team": "data"}


class TestSchemaDrop:
    """Tests for schema_drop() method."""

    def test_drop_schema(self, attached_catalog: tuple[CICatalog, AttachId]) -> None:
        """Dropping schema removes it."""
        catalog, attach_id = attached_catalog
        catalog.schema_create(
            attach_id=attach_id,
            transaction_id=None,
            name="to_drop",
            comment=None,
            tags={},
        )

        catalog.schema_drop(
            attach_id=attach_id,
            transaction_id=None,
            name="to_drop",
            ignore_not_found=False,
            cascade=False,
        )

        assert (
            catalog.schema_get(attach_id=attach_id, transaction_id=None, name="to_drop")
            is None
        )

    def test_drop_schema_ignore_not_found(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Dropping non-existent schema with ignore_not_found succeeds."""
        catalog, attach_id = attached_catalog

        catalog.schema_drop(
            attach_id=attach_id,
            transaction_id=None,
            name="nonexistent",
            ignore_not_found=True,
            cascade=False,
        )

    def test_drop_nonempty_schema_cascade(
        self,
        attached_catalog: tuple[CICatalog, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Dropping non-empty schema with cascade succeeds."""
        catalog, attach_id = attached_catalog
        catalog.schema_create(
            attach_id=attach_id,
            transaction_id=None,
            name="has_table",
            comment=None,
            tags={},
        )
        catalog.table_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="has_table",
            name="test_table",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        catalog.schema_drop(
            attach_id=attach_id,
            transaction_id=None,
            name="has_table",
            ignore_not_found=False,
            cascade=True,
        )

        assert (
            catalog.schema_get(
                attach_id=attach_id, transaction_id=None, name="has_table"
            )
            is None
        )


class TestSchemas:
    """Tests for schemas() method."""

    def test_list_schemas(self, attached_catalog: tuple[CICatalog, AttachId]) -> None:
        """schemas() returns all schemas."""
        catalog, attach_id = attached_catalog
        catalog.schema_create(
            attach_id=attach_id,
            transaction_id=None,
            name="schema1",
            comment=None,
            tags={},
        )
        catalog.schema_create(
            attach_id=attach_id,
            transaction_id=None,
            name="schema2",
            comment=None,
            tags={},
        )

        schemas = list(catalog.schemas(attach_id=attach_id, transaction_id=None))
        names = {s.name for s in schemas}
        assert names == {"main", "schema1", "schema2"}


class TestSchemaContents:
    """Tests for schema_contents() method."""

    def test_contents_empty(self, attached_catalog: tuple[CICatalog, AttachId]) -> None:
        """Empty schema has no contents."""
        catalog, attach_id = attached_catalog

        contents = list(
            catalog.schema_contents(
                attach_id=attach_id,
                transaction_id=None,
                name="main",
                type=SchemaObjectType.TABLE,
            )
        )
        assert contents == []

    def test_contents_with_table(
        self,
        attached_catalog: tuple[CICatalog, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Schema contents includes tables."""
        catalog, attach_id = attached_catalog
        catalog.table_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="my_table",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        contents = list(
            catalog.schema_contents(
                attach_id=attach_id,
                transaction_id=None,
                name="main",
                type=SchemaObjectType.TABLE,
            )
        )
        assert len(contents) == 1
        assert contents[0].name == "my_table"


class TestTableCreate:
    """Tests for table_create() method."""

    def test_create_table(
        self,
        attached_catalog: tuple[CICatalog, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Creating table succeeds."""
        catalog, attach_id = attached_catalog

        catalog.table_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="users",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[0],
            unique_constraints=[[0]],
            check_constraints=[],
        )

        table = catalog.table_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="users",
        )
        assert table is not None
        assert table.name == "users"
        assert table.not_null_constraints == [0]
        assert table.unique_constraints == [[0]]

    def test_create_table_on_conflict_ignore(
        self,
        attached_catalog: tuple[CICatalog, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Creating duplicate table with IGNORE succeeds."""
        catalog, attach_id = attached_catalog
        catalog.table_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="dup",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        # Should not raise
        catalog.table_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="dup",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.IGNORE,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

    def test_create_table_on_conflict_replace(
        self,
        attached_catalog: tuple[CICatalog, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Creating duplicate table with REPLACE replaces it."""
        catalog, attach_id = attached_catalog
        catalog.table_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="to_replace",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[0],
            unique_constraints=[],
            check_constraints=[],
        )

        catalog.table_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="to_replace",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.REPLACE,
            not_null_constraints=[],  # Different constraints
            unique_constraints=[],
            check_constraints=[],
        )

        table = catalog.table_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="to_replace",
        )
        assert table is not None
        assert table.not_null_constraints == []  # Should be new constraints

    def test_create_table_on_conflict_error(
        self,
        attached_catalog: tuple[CICatalog, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Creating duplicate table with ERROR raises."""
        catalog, attach_id = attached_catalog
        catalog.table_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="dup_error",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        with pytest.raises(ValueError, match="already exists"):
            catalog.table_create(
                attach_id=attach_id,
                transaction_id=None,
                schema_name="main",
                name="dup_error",
                columns=sample_schema_bytes,
                on_conflict=OnConflict.ERROR,
                not_null_constraints=[],
                unique_constraints=[],
                check_constraints=[],
            )


class TestTableDrop:
    """Tests for table_drop() method."""

    def test_drop_table(
        self,
        attached_catalog: tuple[CICatalog, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Dropping table removes it."""
        catalog, attach_id = attached_catalog
        catalog.table_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="to_drop",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        catalog.table_drop(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="to_drop",
            ignore_not_found=False,
        )

        assert (
            catalog.table_get(
                attach_id=attach_id,
                transaction_id=None,
                schema_name="main",
                name="to_drop",
            )
            is None
        )

    def test_drop_table_ignore_not_found(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Dropping non-existent table with ignore_not_found succeeds."""
        catalog, attach_id = attached_catalog

        catalog.table_drop(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="nonexistent",
            ignore_not_found=True,
        )


class TestTableRename:
    """Tests for table_rename() method."""

    def test_rename_table(
        self,
        attached_catalog: tuple[CICatalog, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Renaming table changes its name."""
        catalog, attach_id = attached_catalog
        catalog.table_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="old_name",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        catalog.table_rename(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="old_name",
            new_name="new_name",
            ignore_not_found=False,
        )

        assert (
            catalog.table_get(
                attach_id=attach_id,
                transaction_id=None,
                schema_name="main",
                name="old_name",
            )
            is None
        )
        assert (
            catalog.table_get(
                attach_id=attach_id,
                transaction_id=None,
                schema_name="main",
                name="new_name",
            )
            is not None
        )


class TestTableCommentSet:
    """Tests for table_comment_set() method."""

    def test_set_table_comment(
        self,
        attached_catalog: tuple[CICatalog, AttachId],
        sample_schema_bytes: SerializedSchema,
    ) -> None:
        """Setting table comment updates it."""
        catalog, attach_id = attached_catalog
        catalog.table_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="commented",
            columns=sample_schema_bytes,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        catalog.table_comment_set(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="commented",
            comment="My table comment",
            ignore_not_found=False,
        )

        table = catalog.table_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="commented",
        )
        assert table is not None
        assert table.comment == "My table comment"


class TestViewCreate:
    """Tests for view_create() method."""

    def test_create_view(self, attached_catalog: tuple[CICatalog, AttachId]) -> None:
        """Creating view succeeds."""
        catalog, attach_id = attached_catalog

        catalog.view_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="my_view",
            definition="SELECT 1 as n",
            on_conflict=OnConflict.ERROR,
        )

        view = catalog.view_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="my_view",
        )
        assert view is not None
        assert view.name == "my_view"
        assert view.definition == "SELECT 1 as n"

    def test_create_view_on_conflict_ignore(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Creating duplicate view with IGNORE succeeds."""
        catalog, attach_id = attached_catalog
        catalog.view_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="dup_view",
            definition="SELECT 1",
            on_conflict=OnConflict.ERROR,
        )

        catalog.view_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="dup_view",
            definition="SELECT 2",
            on_conflict=OnConflict.IGNORE,
        )

        # Original view should remain
        view = catalog.view_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="dup_view",
        )
        assert view is not None
        assert view.definition == "SELECT 1"

    def test_create_view_on_conflict_replace(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Creating duplicate view with REPLACE replaces it."""
        catalog, attach_id = attached_catalog
        catalog.view_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="to_replace",
            definition="SELECT 1",
            on_conflict=OnConflict.ERROR,
        )

        catalog.view_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="to_replace",
            definition="SELECT 2",
            on_conflict=OnConflict.REPLACE,
        )

        view = catalog.view_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="to_replace",
        )
        assert view is not None
        assert view.definition == "SELECT 2"


class TestViewDrop:
    """Tests for view_drop() method."""

    def test_drop_view(self, attached_catalog: tuple[CICatalog, AttachId]) -> None:
        """Dropping view removes it."""
        catalog, attach_id = attached_catalog
        catalog.view_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="to_drop",
            definition="SELECT 1",
            on_conflict=OnConflict.ERROR,
        )

        catalog.view_drop(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="to_drop",
            ignore_not_found=False,
        )

        assert (
            catalog.view_get(
                attach_id=attach_id,
                transaction_id=None,
                schema_name="main",
                name="to_drop",
            )
            is None
        )


class TestViewRename:
    """Tests for view_rename() method."""

    def test_rename_view(self, attached_catalog: tuple[CICatalog, AttachId]) -> None:
        """Renaming view changes its name."""
        catalog, attach_id = attached_catalog
        catalog.view_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="old_view",
            definition="SELECT 1",
            on_conflict=OnConflict.ERROR,
        )

        catalog.view_rename(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="old_view",
            new_name="new_view",
            ignore_not_found=False,
        )

        assert (
            catalog.view_get(
                attach_id=attach_id,
                transaction_id=None,
                schema_name="main",
                name="old_view",
            )
            is None
        )
        view = catalog.view_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="new_view",
        )
        assert view is not None
        assert view.definition == "SELECT 1"


class TestViewCommentSet:
    """Tests for view_comment_set() method."""

    def test_set_view_comment(
        self, attached_catalog: tuple[CICatalog, AttachId]
    ) -> None:
        """Setting view comment updates it."""
        catalog, attach_id = attached_catalog
        catalog.view_create(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="commented",
            definition="SELECT 1",
            on_conflict=OnConflict.ERROR,
        )

        catalog.view_comment_set(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="commented",
            comment="My view comment",
            ignore_not_found=False,
        )

        view = catalog.view_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="commented",
        )
        assert view is not None
        assert view.comment == "My view comment"
