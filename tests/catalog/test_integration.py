"""Integration tests for catalog interface using InMemoryCatalog."""

import pyarrow as pa
import pytest

from vgi import schema
from vgi.catalog import OnConflict, SerializedSchema
from vgi.examples.catalog import InMemoryCatalog


class TestInMemoryCatalogBasic:
    """Test basic InMemoryCatalog functionality."""

    def test_default_catalog_exists(self) -> None:
        """Default 'memory' catalog exists on creation."""
        catalog = InMemoryCatalog()
        catalogs = list(catalog.catalogs())
        assert "memory" in catalogs

    def test_attach_to_default_catalog(self) -> None:
        """Can attach to the default memory catalog."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        assert result.attach_id is not None
        assert len(result.attach_id) == 16  # UUID bytes
        assert result.supports_transactions is False
        assert result.supports_time_travel is False
        assert result.catalog_version_frozen is False

    def test_attach_to_nonexistent_catalog_raises(self) -> None:
        """Attaching to nonexistent catalog raises error."""
        catalog = InMemoryCatalog()
        with pytest.raises(ValueError, match="not found"):
            catalog.catalog_attach(name="nonexistent", options={})

    def test_detach(self) -> None:
        """Can detach from an attached catalog."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})
        # Should not raise
        catalog.catalog_detach(attach_id=result.attach_id)


class TestInMemoryCatalogSchemas:
    """Test schema operations."""

    def test_default_main_schema_exists(self) -> None:
        """Default 'main' schema exists after attach."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})
        schemas = list(catalog.schemas(attach_id=result.attach_id, transaction_id=None))

        assert len(schemas) == 1
        assert schemas[0].name == "main"
        assert schemas[0].is_default is True

    def test_schema_get_main(self) -> None:
        """Can get the main schema."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})
        schema = catalog.schema_get(
            attach_id=result.attach_id, transaction_id=None, name="main"
        )

        assert schema is not None
        assert schema.name == "main"

    def test_schema_get_nonexistent(self) -> None:
        """Getting nonexistent schema returns None."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})
        schema = catalog.schema_get(
            attach_id=result.attach_id, transaction_id=None, name="nonexistent"
        )

        assert schema is None

    def test_schema_create(self) -> None:
        """Can create a new schema."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        catalog.schema_create(
            attach_id=result.attach_id,
            transaction_id=None,
            name="analytics",
            comment="Analytics schema",
            tags={"team", "data"},
        )

        schemas = list(catalog.schemas(attach_id=result.attach_id, transaction_id=None))
        schema_names = [s.name for s in schemas]
        assert "analytics" in schema_names

        schema = catalog.schema_get(
            attach_id=result.attach_id, transaction_id=None, name="analytics"
        )
        assert schema is not None
        assert schema.comment == "Analytics schema"
        assert schema.tags == {"team", "data"}

    def test_schema_create_duplicate_raises(self) -> None:
        """Creating duplicate schema raises error."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        with pytest.raises(ValueError, match="already exists"):
            catalog.schema_create(
                attach_id=result.attach_id,
                transaction_id=None,
                name="main",  # Already exists
                comment=None,
                tags=set(),
            )

    def test_schema_drop(self) -> None:
        """Can drop a schema."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        catalog.schema_create(
            attach_id=result.attach_id,
            transaction_id=None,
            name="to_drop",
            comment=None,
            tags=set(),
        )

        catalog.schema_drop(
            attach_id=result.attach_id,
            transaction_id=None,
            name="to_drop",
            ignore_not_found=False,
            cascade=False,
        )

        schema = catalog.schema_get(
            attach_id=result.attach_id, transaction_id=None, name="to_drop"
        )
        assert schema is None

    def test_schema_drop_ignore_not_found(self) -> None:
        """Dropping nonexistent schema with ignore_not_found=True succeeds."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        # Should not raise
        catalog.schema_drop(
            attach_id=result.attach_id,
            transaction_id=None,
            name="nonexistent",
            ignore_not_found=True,
            cascade=False,
        )


class TestInMemoryCatalogTables:
    """Test table operations."""

    def test_table_create_and_get(self) -> None:
        """Can create and retrieve a table."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        columns_schema = schema(id=pa.int64(), name=pa.string())
        columns = SerializedSchema(columns_schema.serialize().to_pybytes())

        catalog.table_create(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="users",
            columns=columns,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[0],
            unique_constraints=[[0]],
            check_constraints=[],
        )

        table = catalog.table_get(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="users",
        )

        assert table is not None
        assert table.name == "users"
        assert table.schema_name == "main"
        assert table.columns == columns
        assert table.not_null_constraints == [0]
        assert table.unique_constraints == [[0]]

    def test_table_get_nonexistent(self) -> None:
        """Getting nonexistent table returns None."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        table = catalog.table_get(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="nonexistent",
        )
        assert table is None

    def test_table_drop(self) -> None:
        """Can drop a table."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        columns = SerializedSchema(schema().serialize().to_pybytes())
        catalog.table_create(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="to_drop",
            columns=columns,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        catalog.table_drop(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="to_drop",
            ignore_not_found=False,
        )

        table = catalog.table_get(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="to_drop",
        )
        assert table is None

    def test_table_rename(self) -> None:
        """Can rename a table."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        columns = SerializedSchema(schema().serialize().to_pybytes())
        catalog.table_create(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="old_name",
            columns=columns,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        catalog.table_rename(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="old_name",
            new_name="new_name",
            ignore_not_found=False,
        )

        old = catalog.table_get(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="old_name",
        )
        new = catalog.table_get(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="new_name",
        )

        assert old is None
        assert new is not None
        assert new.name == "new_name"

    def test_table_comment_set(self) -> None:
        """Can set table comment."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        columns = SerializedSchema(schema().serialize().to_pybytes())
        catalog.table_create(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="commented",
            columns=columns,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        catalog.table_comment_set(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="commented",
            comment="This is a comment",
            ignore_not_found=False,
        )

        table = catalog.table_get(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="commented",
        )
        assert table is not None
        assert table.comment == "This is a comment"


class TestInMemoryCatalogViews:
    """Test view operations."""

    def test_view_create_and_get(self) -> None:
        """Can create and retrieve a view."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        catalog.view_create(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="user_view",
            definition="SELECT * FROM users",
            on_conflict=OnConflict.ERROR,
        )

        view = catalog.view_get(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="user_view",
        )

        assert view is not None
        assert view.name == "user_view"
        assert view.definition == "SELECT * FROM users"

    def test_view_drop(self) -> None:
        """Can drop a view."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        catalog.view_create(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="to_drop",
            definition="SELECT 1",
            on_conflict=OnConflict.ERROR,
        )

        catalog.view_drop(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="to_drop",
            ignore_not_found=False,
        )

        view = catalog.view_get(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="to_drop",
        )
        assert view is None

    def test_view_rename(self) -> None:
        """Can rename a view."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        catalog.view_create(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="old_view",
            definition="SELECT 1",
            on_conflict=OnConflict.ERROR,
        )

        catalog.view_rename(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="old_view",
            new_name="new_view",
            ignore_not_found=False,
        )

        old = catalog.view_get(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="old_view",
        )
        new = catalog.view_get(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="new_view",
        )

        assert old is None
        assert new is not None


class TestInMemoryCatalogVersioning:
    """Test catalog versioning."""

    def test_version_increments_on_schema_create(self) -> None:
        """Version increments when schema is created."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        version1 = catalog.catalog_version(
            attach_id=result.attach_id, transaction_id=None
        )

        catalog.schema_create(
            attach_id=result.attach_id,
            transaction_id=None,
            name="new_schema",
            comment=None,
            tags=set(),
        )

        version2 = catalog.catalog_version(
            attach_id=result.attach_id, transaction_id=None
        )

        assert version2 > version1

    def test_version_increments_on_table_create(self) -> None:
        """Version increments when table is created."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        version1 = catalog.catalog_version(
            attach_id=result.attach_id, transaction_id=None
        )

        columns = SerializedSchema(schema().serialize().to_pybytes())
        catalog.table_create(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="table",
            columns=columns,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        version2 = catalog.catalog_version(
            attach_id=result.attach_id, transaction_id=None
        )

        assert version2 > version1


class TestInMemoryCatalogSchemaContents:
    """Test schema_contents operation."""

    def test_schema_contents_lists_tables_and_views(self) -> None:
        """schema_contents returns tables and views."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        columns = SerializedSchema(schema().serialize().to_pybytes())
        catalog.table_create(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="users",
            columns=columns,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        catalog.view_create(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="user_view",
            definition="SELECT * FROM users",
            on_conflict=OnConflict.ERROR,
        )

        contents = list(
            catalog.schema_contents(
                attach_id=result.attach_id, transaction_id=None, name="main"
            )
        )

        names = [c.name for c in contents]
        assert "users" in names
        assert "user_view" in names


class TestInMemoryCatalogOnConflict:
    """Test OnConflict behavior."""

    def test_table_create_error_on_duplicate(self) -> None:
        """OnConflict.ERROR raises on duplicate table."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        columns = SerializedSchema(schema().serialize().to_pybytes())
        catalog.table_create(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="table",
            columns=columns,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        with pytest.raises(ValueError, match="already exists"):
            catalog.table_create(
                attach_id=result.attach_id,
                transaction_id=None,
                schema_name="main",
                name="table",
                columns=columns,
                on_conflict=OnConflict.ERROR,
                not_null_constraints=[],
                unique_constraints=[],
                check_constraints=[],
            )

    def test_table_create_ignore_on_duplicate(self) -> None:
        """OnConflict.IGNORE does nothing on duplicate table."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        columns = SerializedSchema(schema().serialize().to_pybytes())
        catalog.table_create(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="table",
            columns=columns,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        # Should not raise
        catalog.table_create(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="table",
            columns=columns,
            on_conflict=OnConflict.IGNORE,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

    def test_table_create_replace_on_duplicate(self) -> None:
        """OnConflict.REPLACE replaces duplicate table."""
        catalog = InMemoryCatalog()
        result = catalog.catalog_attach(name="memory", options={})

        columns1 = SerializedSchema(schema(a=pa.int32()).serialize().to_pybytes())
        columns2 = SerializedSchema(schema(b=pa.string()).serialize().to_pybytes())

        catalog.table_create(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="table",
            columns=columns1,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        catalog.table_create(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="table",
            columns=columns2,
            on_conflict=OnConflict.REPLACE,
            not_null_constraints=[],
            unique_constraints=[],
            check_constraints=[],
        )

        table = catalog.table_get(
            attach_id=result.attach_id,
            transaction_id=None,
            schema_name="main",
            name="table",
        )
        assert table is not None
        assert table.columns == columns2
