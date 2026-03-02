"""Integration tests for catalog operations via full client→worker round-trip.

Each test exercises the complete pipeline:
  Client method → WorkerPool → subprocess → Worker → CatalogInterface → response

The module-level _catalog_pool is replaced with a fresh pool between tests
to ensure each test gets a fresh worker process with a fresh InMemoryCatalog.
"""

from collections.abc import Iterator

import pyarrow as pa
import pytest
from vgi_rpc import WorkerPool

from vgi import schema
from vgi.catalog import MacroType, OnConflict, SchemaObjectType, SerializedSchema
from vgi.client import Client
from vgi.client import catalog_mixin as _cm
from vgi.client.catalog_mixin import CatalogClientError

CATALOG_WORKER = "vgi-example-catalog-worker"


@pytest.fixture(autouse=True)
def _fresh_worker() -> Iterator[None]:
    """Replace the worker pool so each test gets a fresh InMemoryCatalog."""
    old_pool = _cm._catalog_pool
    _cm._catalog_pool = WorkerPool(max_idle=4, idle_timeout=30.0)
    yield
    _cm._catalog_pool.close()
    _cm._catalog_pool = old_pool


class TestCatalogBasic:
    """Test basic catalog operations via client→worker round-trip."""

    def test_default_catalog_exists(self) -> None:
        """Default 'memory' catalog exists."""
        client = Client(CATALOG_WORKER)
        catalogs = client.catalogs()
        assert "memory" in catalogs

    def test_attach_to_default_catalog(self) -> None:
        """Can attach to the default memory catalog."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        assert result.attach_id is not None
        assert len(result.attach_id) == 16  # UUID bytes
        assert result.supports_transactions is False
        assert result.supports_time_travel is False
        assert result.catalog_version_frozen is False

    def test_attach_to_nonexistent_catalog_raises(self) -> None:
        """Attaching to nonexistent catalog raises CatalogClientError."""
        client = Client(CATALOG_WORKER)
        with pytest.raises(CatalogClientError, match="not found"):
            client.catalog_attach(name="nonexistent", options={})

    def test_detach(self) -> None:
        """Can detach from an attached catalog."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})
        # Should not raise
        client.catalog_detach(attach_id=result.attach_id)


class TestCatalogSchemas:
    """Test schema operations via client→worker round-trip."""

    def test_default_main_schema_exists(self) -> None:
        """Default 'main' schema exists after attach."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})
        schemas = client.schemas(attach_id=result.attach_id)

        assert len(schemas) == 1
        assert schemas[0].name == "main"

    def test_schema_get_main(self) -> None:
        """Can get the main schema."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})
        info = client.schema_get(attach_id=result.attach_id, name="main")

        assert info is not None
        assert info.name == "main"

    def test_schema_get_nonexistent(self) -> None:
        """Getting nonexistent schema returns None."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})
        info = client.schema_get(attach_id=result.attach_id, name="nonexistent")

        assert info is None

    def test_schema_create(self) -> None:
        """Can create a new schema and read it back."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        client.schema_create(
            attach_id=result.attach_id,
            name="analytics",
            comment="Analytics schema",
            tags={"team": "data"},
        )

        schemas = client.schemas(attach_id=result.attach_id)
        schema_names = [s.name for s in schemas]
        assert "analytics" in schema_names

        info = client.schema_get(attach_id=result.attach_id, name="analytics")
        assert info is not None
        assert info.comment == "Analytics schema"
        assert info.tags == {"team": "data"}

    def test_schema_create_duplicate_raises(self) -> None:
        """Creating duplicate schema raises CatalogClientError."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        with pytest.raises(CatalogClientError, match="already exists"):
            client.schema_create(
                attach_id=result.attach_id,
                name="main",  # Already exists
            )

    def test_schema_drop(self) -> None:
        """Can create and drop a schema."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        client.schema_create(
            attach_id=result.attach_id,
            name="to_drop",
        )

        client.schema_drop(
            attach_id=result.attach_id,
            name="to_drop",
        )

        info = client.schema_get(attach_id=result.attach_id, name="to_drop")
        assert info is None

    def test_schema_drop_ignore_not_found(self) -> None:
        """Dropping nonexistent schema with ignore_not_found=True succeeds."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        # Should not raise
        client.schema_drop(
            attach_id=result.attach_id,
            name="nonexistent",
            ignore_not_found=True,
        )


class TestCatalogTables:
    """Test table operations via client→worker round-trip."""

    def test_table_create_and_get(self) -> None:
        """Can create and retrieve a table with constraints."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        columns_schema = schema(id=pa.int64(), name=pa.string())
        columns = SerializedSchema(columns_schema.serialize().to_pybytes())

        client.table_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="users",
            columns=columns,
            on_conflict=OnConflict.ERROR,
            not_null_constraints=[0],
            unique_constraints=[[0]],
            check_constraints=[],
        )

        table = client.table_get(
            attach_id=result.attach_id,
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
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        table = client.table_get(
            attach_id=result.attach_id,
            schema_name="main",
            name="nonexistent",
        )
        assert table is None

    def test_table_drop(self) -> None:
        """Can create and drop a table."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        columns = SerializedSchema(schema().serialize().to_pybytes())
        client.table_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="to_drop",
            columns=columns,
        )

        client.table_drop(
            attach_id=result.attach_id,
            schema_name="main",
            name="to_drop",
        )

        table = client.table_get(
            attach_id=result.attach_id,
            schema_name="main",
            name="to_drop",
        )
        assert table is None

    def test_table_rename(self) -> None:
        """Can rename a table."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        columns = SerializedSchema(schema().serialize().to_pybytes())
        client.table_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="old_name",
            columns=columns,
        )

        client.table_rename(
            attach_id=result.attach_id,
            schema_name="main",
            name="old_name",
            new_name="new_name",
        )

        old = client.table_get(
            attach_id=result.attach_id,
            schema_name="main",
            name="old_name",
        )
        new = client.table_get(
            attach_id=result.attach_id,
            schema_name="main",
            name="new_name",
        )

        assert old is None
        assert new is not None
        assert new.name == "new_name"

    def test_table_comment_set(self) -> None:
        """Can set a table comment."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        columns = SerializedSchema(schema().serialize().to_pybytes())
        client.table_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="commented",
            columns=columns,
        )

        client.table_comment_set(
            attach_id=result.attach_id,
            schema_name="main",
            name="commented",
            comment="This is a comment",
        )

        table = client.table_get(
            attach_id=result.attach_id,
            schema_name="main",
            name="commented",
        )
        assert table is not None
        assert table.comment == "This is a comment"


class TestCatalogViews:
    """Test view operations via client→worker round-trip."""

    def test_view_create_and_get(self) -> None:
        """Can create and retrieve a view."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        client.view_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="user_view",
            definition="SELECT * FROM users",
            on_conflict=OnConflict.ERROR,
        )

        view = client.view_get(
            attach_id=result.attach_id,
            schema_name="main",
            name="user_view",
        )

        assert view is not None
        assert view.name == "user_view"
        assert view.definition == "SELECT * FROM users"

    def test_view_drop(self) -> None:
        """Can create and drop a view."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        client.view_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="to_drop",
            definition="SELECT 1",
        )

        client.view_drop(
            attach_id=result.attach_id,
            schema_name="main",
            name="to_drop",
        )

        view = client.view_get(
            attach_id=result.attach_id,
            schema_name="main",
            name="to_drop",
        )
        assert view is None

    def test_view_rename(self) -> None:
        """Can rename a view."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        client.view_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="old_view",
            definition="SELECT 1",
        )

        client.view_rename(
            attach_id=result.attach_id,
            schema_name="main",
            name="old_view",
            new_name="new_view",
        )

        old = client.view_get(
            attach_id=result.attach_id,
            schema_name="main",
            name="old_view",
        )
        new = client.view_get(
            attach_id=result.attach_id,
            schema_name="main",
            name="new_view",
        )

        assert old is None
        assert new is not None


class TestCatalogVersioning:
    """Test catalog versioning via client→worker round-trip."""

    def test_version_increments_on_schema_create(self) -> None:
        """Version increments when schema is created."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        version1 = client.catalog_version(attach_id=result.attach_id)

        client.schema_create(
            attach_id=result.attach_id,
            name="new_schema",
        )

        version2 = client.catalog_version(attach_id=result.attach_id)

        assert version2 > version1

    def test_version_increments_on_table_create(self) -> None:
        """Version increments when table is created."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        version1 = client.catalog_version(attach_id=result.attach_id)

        columns = SerializedSchema(schema().serialize().to_pybytes())
        client.table_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="table",
            columns=columns,
        )

        version2 = client.catalog_version(attach_id=result.attach_id)

        assert version2 > version1


class TestCatalogSchemaContents:
    """Test schema_contents operation via client→worker round-trip."""

    def test_schema_contents_lists_tables_and_views(self) -> None:
        """schema_contents returns tables and views when queried by type."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        columns = SerializedSchema(schema().serialize().to_pybytes())
        client.table_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="users",
            columns=columns,
        )

        client.view_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="user_view",
            definition="SELECT * FROM users",
        )

        # Get tables
        tables = client.schema_contents(
            attach_id=result.attach_id,
            name="main",
            type=SchemaObjectType.TABLE,
        )

        # Get views
        views = client.schema_contents(
            attach_id=result.attach_id,
            name="main",
            type=SchemaObjectType.VIEW,
        )

        table_names = [t.name for t in tables]
        view_names = [v.name for v in views]
        assert "users" in table_names
        assert "user_view" in view_names


class TestCatalogOnConflict:
    """Test OnConflict behavior via client→worker round-trip."""

    def test_table_create_error_on_duplicate(self) -> None:
        """OnConflict.ERROR raises on duplicate table."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        columns = SerializedSchema(schema().serialize().to_pybytes())
        client.table_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="table",
            columns=columns,
            on_conflict=OnConflict.ERROR,
        )

        with pytest.raises(CatalogClientError, match="already exists"):
            client.table_create(
                attach_id=result.attach_id,
                schema_name="main",
                name="table",
                columns=columns,
                on_conflict=OnConflict.ERROR,
            )

    def test_table_create_ignore_on_duplicate(self) -> None:
        """OnConflict.IGNORE does nothing on duplicate table."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        columns = SerializedSchema(schema().serialize().to_pybytes())
        client.table_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="table",
            columns=columns,
            on_conflict=OnConflict.ERROR,
        )

        # Should not raise
        client.table_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="table",
            columns=columns,
            on_conflict=OnConflict.IGNORE,
        )

    def test_table_create_replace_on_duplicate(self) -> None:
        """OnConflict.REPLACE replaces duplicate table."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        columns1 = SerializedSchema(schema(a=pa.int32()).serialize().to_pybytes())
        columns2 = SerializedSchema(schema(b=pa.string()).serialize().to_pybytes())

        client.table_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="table",
            columns=columns1,
            on_conflict=OnConflict.ERROR,
        )

        client.table_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="table",
            columns=columns2,
            on_conflict=OnConflict.REPLACE,
        )

        table = client.table_get(
            attach_id=result.attach_id,
            schema_name="main",
            name="table",
        )
        assert table is not None
        assert table.columns == columns2


class TestCatalogMacros:
    """Test macro CRUD operations via client→worker round-trip."""

    def test_macro_create_and_get(self) -> None:
        """Can create and retrieve a macro."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        client.macro_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="double",
            macro_type=MacroType.SCALAR,
            parameters=["x"],
            definition="x * 2",
        )

        macro = client.macro_get(
            attach_id=result.attach_id,
            schema_name="main",
            name="double",
        )

        assert macro is not None
        assert macro.name == "double"
        assert macro.schema_name == "main"
        assert macro.macro_type == MacroType.SCALAR
        assert macro.parameters == ["x"]
        assert macro.definition == "x * 2"

    def test_macro_drop(self) -> None:
        """Can create and drop a macro."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        client.macro_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="to_drop",
            macro_type=MacroType.SCALAR,
            parameters=["x"],
            definition="x",
        )

        client.macro_drop(
            attach_id=result.attach_id,
            schema_name="main",
            name="to_drop",
        )

        macro = client.macro_get(
            attach_id=result.attach_id,
            schema_name="main",
            name="to_drop",
        )
        assert macro is None

    def test_macro_drop_nonexistent_raises(self) -> None:
        """Dropping nonexistent macro raises CatalogClientError."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        with pytest.raises(CatalogClientError, match="not found"):
            client.macro_drop(
                attach_id=result.attach_id,
                schema_name="main",
                name="nonexistent",
            )

    def test_macro_drop_nonexistent_ignore(self) -> None:
        """Dropping nonexistent macro with ignore_not_found=True succeeds."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        # Should not raise
        client.macro_drop(
            attach_id=result.attach_id,
            schema_name="main",
            name="nonexistent",
            ignore_not_found=True,
        )

    def test_schema_contents_filters_by_type(self) -> None:
        """schema_contents correctly filters scalar vs table macros."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        client.macro_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="scalar_one",
            macro_type=MacroType.SCALAR,
            parameters=["x"],
            definition="x",
        )
        client.macro_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="table_one",
            macro_type=MacroType.TABLE,
            parameters=["n"],
            definition="SELECT * FROM range(n)",
        )

        scalar_macros = client.schema_contents(
            attach_id=result.attach_id,
            name="main",
            type=SchemaObjectType.SCALAR_MACRO,
        )
        table_macros = client.schema_contents(
            attach_id=result.attach_id,
            name="main",
            type=SchemaObjectType.TABLE_MACRO,
        )

        assert len(scalar_macros) == 1
        assert scalar_macros[0].name == "scalar_one"
        assert len(table_macros) == 1
        assert table_macros[0].name == "table_one"

    def test_parameter_default_values_preserved(self) -> None:
        """RecordBatch defaults survive create/get round-trip."""
        client = Client(CATALOG_WORKER)
        result = client.catalog_attach(name="memory", options={})

        defaults = pa.RecordBatch.from_pydict(
            {"lo": pa.array([0], type=pa.int64()), "hi": pa.array([100], type=pa.int64())}
        )

        client.macro_create(
            attach_id=result.attach_id,
            schema_name="main",
            name="clamp",
            macro_type=MacroType.SCALAR,
            parameters=["val", "lo", "hi"],
            definition="GREATEST(lo, LEAST(hi, val))",
            parameter_default_values=defaults,
        )

        macro = client.macro_get(
            attach_id=result.attach_id,
            schema_name="main",
            name="clamp",
        )

        assert macro is not None
        assert macro.parameter_default_values is not None
        restored = macro.parameter_default_values
        assert restored.num_rows == 1
        assert set(restored.schema.names) == {"lo", "hi"}
        assert restored.column("lo").type == pa.int64()
        assert restored.column("hi").type == pa.int64()
        assert restored.column("lo")[0].as_py() == 0
        assert restored.column("hi")[0].as_py() == 100
