"""Tests for declarative catalog descriptor classes.

Tests cover:
- Table descriptor: explicit columns and function-backed tables
- View descriptor
- Schema descriptor
- Catalog descriptor: validation and registry building
- ReadOnlyCatalogInterface with Catalog object
- Worker integration with catalog attribute
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

import pyarrow as pa
import pytest
from vgi_rpc.rpc import OutputCollector

from vgi import Worker
from vgi.catalog import (
    AttachId,
    Catalog,
    CatalogAttachResult,
    Macro,
    MacroInfo,
    MacroType,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    Schema,
    SchemaInfo,
    SchemaObjectType,
    Table,
    TableInfo,
    View,
    ViewInfo,
)
from vgi.schema_utils import schema
from vgi.table_function import (
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

# =============================================================================
# Test Fixtures: Example Functions for Function-Backed Tables
# =============================================================================


@dataclass(slots=True, frozen=True)
class EmptyArgs:
    """No arguments."""


@init_single_worker
@bind_fixed_schema
class UsersFunction(TableFunctionGenerator[EmptyArgs]):
    """Example table function for testing function-backed tables."""

    class Meta:  # noqa: D106
        name = "users"
        description = "Generate user data"

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema({"id": pa.int64(), "name": pa.string()})

    @classmethod
    def process(cls, params: ProcessParams[EmptyArgs], state: None, out: OutputCollector) -> None:  # noqa: D102
        out.emit(
            pa.RecordBatch.from_pydict(
                {"id": [1, 2], "name": ["Alice", "Bob"]},
                schema=params.output_schema,
            )
        )
        out.finish()


@init_single_worker
@bind_fixed_schema
class EventsFunction(TableFunctionGenerator[EmptyArgs]):
    """Another example function for multi-schema tests."""

    class Meta:  # noqa: D106
        name = "events"
        description = "Generate event data"

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema({"event_id": pa.int64(), "timestamp": pa.timestamp("us")})

    @classmethod
    def process(cls, params: ProcessParams[EmptyArgs], state: None, out: OutputCollector) -> None:  # noqa: D102
        out.emit(
            pa.RecordBatch.from_pydict(
                {"event_id": [1], "timestamp": [1000000]},
                schema=params.output_schema,
            )
        )
        out.finish()


# =============================================================================
# Table Descriptor Tests
# =============================================================================


class TestTableWithExplicitColumns:
    """Tests for Table descriptor with explicit column schema."""

    def test_table_with_columns_schema(self) -> None:
        """Table can be created with explicit PyArrow schema."""
        table = Table(
            name="users",
            columns=pa.schema(
                [("id", pa.int64()), ("name", pa.string())]  # type: ignore[arg-type]
            ),
        )
        assert table.name == "users"
        assert table.columns is not None
        assert len(table.columns) == 2

    def test_table_resolved_columns_explicit(self) -> None:
        """resolved_columns returns the explicit schema."""
        columns = pa.schema([("id", pa.int64())])
        table = Table(name="test", columns=columns)
        assert table.resolved_columns == columns

    def test_table_with_not_null_constraints(self) -> None:
        """Table validates not_null constraint column names."""
        table = Table(
            name="users",
            columns=pa.schema(
                [("id", pa.int64()), ("name", pa.string())]  # type: ignore[arg-type]
            ),
            not_null=("id",),
        )
        assert table.not_null == ("id",)

    def test_table_with_unique_constraints(self) -> None:
        """Table validates unique constraint column names."""
        table = Table(
            name="users",
            columns=pa.schema(
                [("id", pa.int64()), ("email", pa.string())]  # type: ignore[arg-type]
            ),
            unique=(("id",), ("email",)),
        )
        assert table.unique == (("id",), ("email",))

    def test_table_with_check_constraints(self) -> None:
        """Table stores check constraint expressions."""
        table = Table(
            name="users",
            columns=pa.schema([("age", pa.int32())]),
            check=("age >= 0", "age < 150"),
        )
        assert table.check == ("age >= 0", "age < 150")

    def test_table_with_comment_and_tags(self) -> None:
        """Table stores optional metadata."""
        table = Table(
            name="users",
            columns=pa.schema([("id", pa.int64())]),
            comment="User accounts",
            tags={"category": "core"},
        )
        assert table.comment == "User accounts"
        assert table.tags == {"category": "core"}


class TestTableWithFunction:
    """Tests for Table descriptor with function-backed schema."""

    def test_table_with_function_derives_schema(self) -> None:
        """Table derives schema from function's output_schema."""
        table = Table(name="users", function=UsersFunction)
        assert table.function is UsersFunction
        assert table.columns is None
        # Schema is derived from function
        resolved = table.resolved_columns
        assert len(resolved) == 2
        assert resolved.field(0).name == "id"
        assert resolved.field(1).name == "name"

    def test_table_function_backed_with_constraints(self) -> None:
        """Function-backed table can have constraints validated."""
        table = Table(
            name="users",
            function=UsersFunction,
            not_null=("id",),
            unique=(("id",),),
        )
        assert table.not_null == ("id",)
        assert table.unique == (("id",),)


class TestTableValidation:
    """Tests for Table validation errors."""

    def test_table_requires_columns_or_function(self) -> None:
        """Table raises ValueError if neither columns nor function provided."""
        with pytest.raises(ValueError, match="must specify either 'columns' or 'function'"):
            Table(name="test")

    def test_table_rejects_both_columns_and_function(self) -> None:
        """Table raises ValueError if both columns and function provided."""
        with pytest.raises(ValueError, match="cannot specify both 'columns' and 'function'"):
            Table(
                name="test",
                columns=pa.schema([("id", pa.int64())]),
                function=UsersFunction,
            )

    def test_table_rejects_invalid_not_null_column(self) -> None:
        """Table raises ValueError for not_null column not in schema."""
        with pytest.raises(ValueError, match="not_null column 'invalid' not found"):
            Table(
                name="test",
                columns=pa.schema([("id", pa.int64())]),
                not_null=("invalid",),
            )

    def test_table_rejects_invalid_unique_column(self) -> None:
        """Table raises ValueError for unique column not in schema."""
        with pytest.raises(ValueError, match="unique column 'invalid' not found"):
            Table(
                name="test",
                columns=pa.schema([("id", pa.int64())]),
                unique=(("invalid",),),
            )


class TestTableToTableInfo:
    """Tests for Table.to_table_info() conversion."""

    def test_to_table_info_basic(self) -> None:
        """Table converts to TableInfo correctly."""
        table = Table(
            name="users",
            columns=pa.schema(
                [("id", pa.int64()), ("name", pa.string())]  # type: ignore[arg-type]
            ),
            comment="User table",
            tags={"type": "core"},
        )
        info = table.to_table_info("main")
        assert isinstance(info, TableInfo)
        assert info.name == "users"
        assert info.schema_name == "main"
        assert info.comment == "User table"
        assert info.tags == {"type": "core"}

    def test_to_table_info_with_constraints(self) -> None:
        """Table converts constraints to indices."""
        table = Table(
            name="users",
            columns=pa.schema(
                [("id", pa.int64()), ("email", pa.string())]  # type: ignore[arg-type]
            ),
            not_null=("id", "email"),
            unique=(("id",), ("email",)),
            check=("id > 0",),
        )
        info = table.to_table_info("main")
        # Column indices: id=0, email=1
        assert info.not_null_constraints == [0, 1]
        assert info.unique_constraints == [[0], [1]]
        assert info.check_constraints == ["id > 0"]


# =============================================================================
# View Descriptor Tests
# =============================================================================


class TestViewDescriptor:
    """Tests for View descriptor."""

    def test_view_basic(self) -> None:
        """View stores name and definition."""
        view = View(
            name="active_users",
            definition="SELECT * FROM users WHERE active = true",
        )
        assert view.name == "active_users"
        assert view.definition == "SELECT * FROM users WHERE active = true"

    def test_view_with_metadata(self) -> None:
        """View stores optional comment and tags."""
        view = View(
            name="active_users",
            definition="SELECT * FROM users",
            comment="Active users only",
            tags={"category": "filtered"},
        )
        assert view.comment == "Active users only"
        assert view.tags == {"category": "filtered"}

    def test_view_to_view_info(self) -> None:
        """View converts to ViewInfo correctly."""
        view = View(
            name="active_users",
            definition="SELECT * FROM users WHERE active = true",
            comment="Active",
            tags={"type": "view"},
        )
        info = view.to_view_info("main")
        assert isinstance(info, ViewInfo)
        assert info.name == "active_users"
        assert info.schema_name == "main"
        assert info.definition == "SELECT * FROM users WHERE active = true"
        assert info.comment == "Active"
        assert info.tags == {"type": "view"}


# =============================================================================
# Macro Descriptor Tests
# =============================================================================


class TestMacroDescriptor:
    """Tests for Macro descriptor."""

    def test_macro_basic(self) -> None:
        """Macro stores name, type, parameters, and definition."""
        macro = Macro(
            name="multiply",
            macro_type=MacroType.SCALAR,
            parameters=["x", "y"],
            definition="x * y",
        )
        assert macro.name == "multiply"
        assert macro.macro_type == MacroType.SCALAR
        assert macro.parameters == ["x", "y"]
        assert macro.definition == "x * y"

    def test_macro_with_metadata(self) -> None:
        """Macro stores optional comment and tags."""
        macro = Macro(
            name="multiply",
            macro_type=MacroType.SCALAR,
            parameters=["x", "y"],
            definition="x * y",
            comment="Multiply two values",
            tags={"category": "math"},
        )
        assert macro.comment == "Multiply two values"
        assert macro.tags == {"category": "math"}

    def test_macro_with_defaults(self) -> None:
        """Macro stores parameter_default_values as RecordBatch."""
        defaults = pa.RecordBatch.from_pydict({"lo": [0], "hi": [100]})
        macro = Macro(
            name="clamp",
            macro_type=MacroType.SCALAR,
            parameters=["val", "lo", "hi"],
            parameter_default_values=defaults,
            definition="GREATEST(lo, LEAST(hi, val))",
        )
        assert macro.parameter_default_values is not None
        assert macro.parameter_default_values.num_rows == 1
        assert macro.parameter_default_values.schema.names == ["lo", "hi"]

    def test_macro_table_type(self) -> None:
        """Macro can be a table macro."""
        macro = Macro(
            name="my_range",
            macro_type=MacroType.TABLE,
            parameters=["n"],
            definition="SELECT * FROM range(n)",
        )
        assert macro.macro_type == MacroType.TABLE

    def test_macro_zero_parameters(self) -> None:
        """Macro can have zero parameters."""
        macro = Macro(
            name="one",
            macro_type=MacroType.SCALAR,
            parameters=[],
            definition="1",
        )
        assert macro.parameters == []

    def test_macro_to_macro_info(self) -> None:
        """Macro converts to MacroInfo correctly."""
        defaults = pa.RecordBatch.from_pydict({"y": [42]})
        macro = Macro(
            name="add",
            macro_type=MacroType.SCALAR,
            parameters=["x", "y"],
            parameter_default_values=defaults,
            definition="x + y",
            comment="Add two values",
            tags={"type": "math"},
        )
        info = macro.to_macro_info("main")
        assert isinstance(info, MacroInfo)
        assert info.name == "add"
        assert info.schema_name == "main"
        assert info.macro_type == MacroType.SCALAR
        assert info.parameters == ["x", "y"]
        assert info.parameter_default_values is not None
        assert info.definition == "x + y"
        assert info.comment == "Add two values"
        assert info.tags == {"type": "math"}

    def test_macro_validation_invalid_default_param_name(self) -> None:
        """Macro raises ValueError for default param not in parameters list."""
        defaults = pa.RecordBatch.from_pydict({"z": [1]})
        with pytest.raises(ValueError, match="default parameter 'z' not found"):
            Macro(
                name="bad",
                macro_type=MacroType.SCALAR,
                parameters=["x", "y"],
                parameter_default_values=defaults,
                definition="x + y",
            )

    def test_macro_validation_recordbatch_multiple_rows(self) -> None:
        """Macro raises ValueError if RecordBatch has more than 1 row."""
        defaults = pa.RecordBatch.from_pydict({"x": [1, 2]})
        with pytest.raises(ValueError, match="must have exactly 1 row"):
            Macro(
                name="bad",
                macro_type=MacroType.SCALAR,
                parameters=["x"],
                parameter_default_values=defaults,
                definition="x",
            )


class TestSchemaWithMacros:
    """Tests for Schema containing macros."""

    def test_schema_with_macros(self) -> None:
        """Schema can contain macros."""
        macro = Macro(
            name="multiply",
            macro_type=MacroType.SCALAR,
            parameters=["x", "y"],
            definition="x * y",
        )
        s = Schema(name="main", macros=[macro])
        assert len(s.macros) == 1
        assert s.macros[0].name == "multiply"

    def test_schema_default_empty_macros(self) -> None:
        """Schema defaults to empty macros."""
        s = Schema(name="main")
        assert s.macros == ()


# =============================================================================
# Schema Descriptor Tests
# =============================================================================


class TestSchemaDescriptor:
    """Tests for Schema descriptor."""

    def test_schema_basic(self) -> None:
        """Schema stores name."""
        schema = Schema(name="main")
        assert schema.name == "main"
        assert schema.tables == ()
        assert schema.views == ()
        assert schema.functions == ()

    def test_schema_with_tables(self) -> None:
        """Schema can contain tables."""
        users = Table(name="users", columns=pa.schema([("id", pa.int64())]))
        schema = Schema(name="main", tables=[users])
        assert len(schema.tables) == 1
        assert schema.tables[0].name == "users"

    def test_schema_with_views(self) -> None:
        """Schema can contain views."""
        view = View(name="active_users", definition="SELECT * FROM users")
        schema = Schema(name="main", views=[view])
        assert len(schema.views) == 1
        assert schema.views[0].name == "active_users"

    def test_schema_with_functions(self) -> None:
        """Schema can contain functions."""
        schema = Schema(name="main", functions=[UsersFunction])
        assert len(schema.functions) == 1
        assert schema.functions[0] is UsersFunction

    def test_schema_with_metadata(self) -> None:
        """Schema stores optional comment and tags."""
        schema = Schema(
            name="analytics",
            comment="Analytics data",
            tags={"team": "data"},
        )
        assert schema.comment == "Analytics data"
        assert schema.tags == {"team": "data"}

    def test_schema_to_schema_info(self) -> None:
        """Schema converts to SchemaInfo correctly."""
        schema = Schema(
            name="main",
            comment="Main schema",
            tags={"type": "core"},
        )
        attach_id = AttachId(b"test-attach-id")
        info = schema.to_schema_info(attach_id)
        assert isinstance(info, SchemaInfo)
        assert info.name == "main"
        assert info.attach_id == attach_id
        assert info.comment == "Main schema"
        assert info.tags == {"type": "core"}


# =============================================================================
# Catalog Descriptor Tests
# =============================================================================


class TestCatalogDescriptor:
    """Tests for Catalog descriptor."""

    def test_catalog_basic(self) -> None:
        """Catalog requires at least default_schema to exist."""
        schema = Schema(name="main")
        catalog = Catalog(name="myapp", schemas=[schema])
        assert catalog.name == "myapp"
        assert catalog.default_schema == "main"
        assert len(catalog.schemas) == 1

    def test_catalog_custom_default_schema(self) -> None:
        """Catalog can use non-main default schema."""
        schema = Schema(name="analytics")
        catalog = Catalog(name="myapp", default_schema="analytics", schemas=[schema])
        assert catalog.default_schema == "analytics"

    def test_catalog_multiple_schemas(self) -> None:
        """Catalog can contain multiple schemas."""
        main = Schema(name="main")
        analytics = Schema(name="analytics")
        catalog = Catalog(name="myapp", schemas=[main, analytics])
        assert len(catalog.schemas) == 2


class TestCatalogValidation:
    """Tests for Catalog validation."""

    def test_catalog_rejects_missing_default_schema(self) -> None:
        """Catalog raises ValueError if default_schema not in schemas."""
        schema = Schema(name="analytics")
        with pytest.raises(ValueError, match="default_schema 'main' not found"):
            Catalog(name="myapp", schemas=[schema])

    def test_catalog_rejects_duplicate_schema_names(self) -> None:
        """Catalog raises ValueError for duplicate schema names."""
        s1 = Schema(name="main")
        s2 = Schema(name="Main")  # Case-insensitive duplicate
        with pytest.raises(ValueError, match="duplicate schema name"):
            Catalog(name="myapp", schemas=[s1, s2])


# =============================================================================
# ReadOnlyCatalogInterface with Catalog Object Tests
# =============================================================================


class TestReadOnlyCatalogWithCatalog:
    """Tests for ReadOnlyCatalogInterface using Catalog object."""

    @pytest.fixture
    def users_table(self) -> Table:
        """Create a users table for testing."""
        return Table(
            name="users",
            function=UsersFunction,
            not_null=("id",),
            comment="User accounts",
        )

    @pytest.fixture
    def active_users_view(self) -> View:
        """Create a view for testing."""
        return View(
            name="active_users",
            definition="SELECT * FROM users WHERE active = true",
            comment="Active users",
        )

    @pytest.fixture
    def scalar_macro(self) -> Macro:
        """Create a scalar macro for testing."""
        return Macro(
            name="multiply",
            macro_type=MacroType.SCALAR,
            parameters=["x", "y"],
            definition="x * y",
            comment="Multiply two values",
        )

    @pytest.fixture
    def table_macro(self) -> Macro:
        """Create a table macro for testing."""
        return Macro(
            name="my_range",
            macro_type=MacroType.TABLE,
            parameters=["n"],
            definition="SELECT * FROM range(n)",
            comment="Table macro range",
        )

    @pytest.fixture
    def catalog_interface(
        self, users_table: Table, active_users_view: View, scalar_macro: Macro, table_macro: Macro
    ) -> ReadOnlyCatalogInterface:
        """Create a catalog interface with Catalog object."""

        class TestCatalog(ReadOnlyCatalogInterface):
            catalog = Catalog(
                name="testapp",
                default_schema="main",
                schemas=[
                    Schema(
                        name="main",
                        tables=[users_table],
                        views=[active_users_view],
                        functions=[UsersFunction],
                        macros=[scalar_macro, table_macro],
                        comment="Main schema",
                    ),
                ],
            )

        return TestCatalog()

    def test_effective_catalog_name(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """Catalog name comes from Catalog object."""
        assert catalog_interface._effective_catalog_name == "testapp"

    def test_default_schema_name(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """Default schema name comes from Catalog object."""
        assert catalog_interface._default_schema_name == "main"

    def test_catalogs_returns_catalog_name(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """catalogs() returns the catalog name from Catalog object."""
        names = catalog_interface.catalogs()
        assert names == ["testapp"]

    def test_catalog_attach(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """catalog_attach returns result with correct defaults."""
        result = catalog_interface.catalog_attach(name="testapp", options={})
        assert isinstance(result, CatalogAttachResult)
        assert result.default_schema == "main"

    def test_schemas_returns_all(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """schemas() returns all schemas from Catalog."""
        attach_id = AttachId(b"test")
        schemas = catalog_interface.schemas(attach_id=attach_id, transaction_id=None)
        assert len(schemas) == 1
        assert schemas[0].name == "main"

    def test_schema_get_found(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """schema_get() finds schema by name."""
        attach_id = AttachId(b"test")
        info = catalog_interface.schema_get(attach_id=attach_id, transaction_id=None, name="main")
        assert info is not None
        assert info.name == "main"
        assert info.comment == "Main schema"

    def test_schema_get_case_insensitive(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """schema_get() is case-insensitive."""
        attach_id = AttachId(b"test")
        info = catalog_interface.schema_get(attach_id=attach_id, transaction_id=None, name="MAIN")
        assert info is not None
        assert info.name == "main"  # Original case preserved

    def test_schema_get_not_found(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """schema_get() returns None for unknown schema."""
        attach_id = AttachId(b"test")
        info = catalog_interface.schema_get(attach_id=attach_id, transaction_id=None, name="unknown")
        assert info is None

    def test_table_get_found(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """table_get() finds table by schema and name."""
        attach_id = AttachId(b"test")
        info = catalog_interface.table_get(attach_id=attach_id, transaction_id=None, schema_name="main", name="users")
        assert info is not None
        assert info.name == "users"
        assert info.comment == "User accounts"

    def test_table_get_case_insensitive(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """table_get() is case-insensitive for both schema and table."""
        attach_id = AttachId(b"test")
        info = catalog_interface.table_get(attach_id=attach_id, transaction_id=None, schema_name="MAIN", name="USERS")
        assert info is not None
        assert info.name == "users"

    def test_table_get_not_found(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """table_get() returns None for unknown table."""
        attach_id = AttachId(b"test")
        info = catalog_interface.table_get(attach_id=attach_id, transaction_id=None, schema_name="main", name="unknown")
        assert info is None

    def test_view_get_found(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """view_get() finds view by schema and name."""
        attach_id = AttachId(b"test")
        info = catalog_interface.view_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="active_users",
        )
        assert info is not None
        assert info.name == "active_users"
        assert "WHERE active = true" in info.definition

    def test_view_get_case_insensitive(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """view_get() is case-insensitive."""
        attach_id = AttachId(b"test")
        info = catalog_interface.view_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="MAIN",
            name="ACTIVE_USERS",
        )
        assert info is not None
        assert info.name == "active_users"

    def test_view_get_not_found(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """view_get() returns None for unknown view."""
        attach_id = AttachId(b"test")
        info = catalog_interface.view_get(attach_id=attach_id, transaction_id=None, schema_name="main", name="unknown")
        assert info is None

    def test_macro_get_found(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """macro_get() finds macro by schema and name."""
        attach_id = AttachId(b"test")
        info = catalog_interface.macro_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="multiply",
        )
        assert info is not None
        assert info.name == "multiply"
        assert info.macro_type == MacroType.SCALAR
        assert info.comment == "Multiply two values"

    def test_macro_get_case_insensitive(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """macro_get() is case-insensitive."""
        attach_id = AttachId(b"test")
        info = catalog_interface.macro_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="MAIN",
            name="MULTIPLY",
        )
        assert info is not None
        assert info.name == "multiply"

    def test_macro_get_not_found(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """macro_get() returns None for unknown macro."""
        attach_id = AttachId(b"test")
        info = catalog_interface.macro_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="unknown",
        )
        assert info is None


class TestSchemaContentsWithCatalog:
    """Tests for schema_contents() with Catalog object."""

    @pytest.fixture
    def catalog_interface(self) -> ReadOnlyCatalogInterface:
        """Create catalog interface with multiple object types."""
        users_table = Table(name="users", function=UsersFunction)
        events_table = Table(name="events", function=EventsFunction)
        users_view = View(name="active_users", definition="SELECT * FROM users")
        scalar_macro = Macro(
            name="multiply",
            macro_type=MacroType.SCALAR,
            parameters=["x", "y"],
            definition="x * y",
        )
        table_macro = Macro(
            name="my_range",
            macro_type=MacroType.TABLE,
            parameters=["n"],
            definition="SELECT * FROM range(n)",
        )

        class TestCatalog(ReadOnlyCatalogInterface):
            catalog = Catalog(
                name="test",
                schemas=[
                    Schema(
                        name="main",
                        tables=[users_table, events_table],
                        views=[users_view],
                        functions=[UsersFunction],
                        macros=[scalar_macro, table_macro],
                    )
                ],
            )

        return TestCatalog()

    def test_schema_contents_tables(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """schema_contents returns tables for TABLE type."""
        attach_id = AttachId(b"test")
        contents = catalog_interface.schema_contents(
            attach_id=attach_id,
            transaction_id=None,
            name="main",
            type=SchemaObjectType.TABLE,
        )
        assert len(contents) == 2
        names = {c.name for c in contents}
        assert names == {"users", "events"}

    def test_schema_contents_views(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """schema_contents returns views for VIEW type."""
        attach_id = AttachId(b"test")
        contents = catalog_interface.schema_contents(
            attach_id=attach_id,
            transaction_id=None,
            name="main",
            type=SchemaObjectType.VIEW,
        )
        assert len(contents) == 1
        assert contents[0].name == "active_users"

    def test_schema_contents_unknown_schema(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """schema_contents returns empty for unknown schema."""
        attach_id = AttachId(b"test")
        contents = catalog_interface.schema_contents(
            attach_id=attach_id,
            transaction_id=None,
            name="unknown",
            type=SchemaObjectType.TABLE,
        )
        assert contents == []

    def test_schema_contents_scalar_macros(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """schema_contents returns scalar macros for SCALAR_MACRO type."""
        attach_id = AttachId(b"test")
        contents = catalog_interface.schema_contents(
            attach_id=attach_id,
            transaction_id=None,
            name="main",
            type=SchemaObjectType.SCALAR_MACRO,
        )
        assert len(contents) == 1
        assert contents[0].name == "multiply"
        assert contents[0].macro_type == MacroType.SCALAR

    def test_schema_contents_table_macros(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """schema_contents returns table macros for TABLE_MACRO type."""
        attach_id = AttachId(b"test")
        contents = catalog_interface.schema_contents(
            attach_id=attach_id,
            transaction_id=None,
            name="main",
            type=SchemaObjectType.TABLE_MACRO,
        )
        assert len(contents) == 1
        assert contents[0].name == "my_range"
        assert contents[0].macro_type == MacroType.TABLE


class TestTableScanFunctionGet:
    """Tests for table_scan_function_get with function-backed tables."""

    @pytest.fixture
    def catalog_interface(self) -> ReadOnlyCatalogInterface:
        """Create catalog interface with function-backed table."""
        users_table = Table(name="users", function=UsersFunction)

        class TestCatalog(ReadOnlyCatalogInterface):
            catalog = Catalog(
                name="test",
                schemas=[Schema(name="main", tables=[users_table])],
            )

        return TestCatalog()

    def test_function_backed_table_auto_scan(self, catalog_interface: ReadOnlyCatalogInterface) -> None:
        """Function-backed tables return auto-implemented scan result."""
        attach_id = AttachId(b"test")
        result = catalog_interface.table_scan_function_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="main",
            name="users",
            at_unit=None,
            at_value=None,
        )
        assert isinstance(result, ScanFunctionResult)
        assert result.function_name == "users"

    def test_explicit_columns_table_raises(self) -> None:
        """Tables with explicit columns raise NotImplementedError."""
        explicit_table = Table(name="orders", columns=pa.schema([("id", pa.int64())]))

        class TestCatalog(ReadOnlyCatalogInterface):
            catalog = Catalog(
                name="test",
                schemas=[Schema(name="main", tables=[explicit_table])],
            )

        interface = TestCatalog()
        attach_id = AttachId(b"test")
        with pytest.raises(NotImplementedError, match="table_scan_function_get not implemented"):
            interface.table_scan_function_get(
                attach_id=attach_id,
                transaction_id=None,
                schema_name="main",
                name="orders",
                at_unit=None,
                at_value=None,
            )


# =============================================================================
# Worker Integration Tests
# =============================================================================


class TestWorkerWithCatalog:
    """Tests for Worker integration with Catalog object."""

    def test_worker_catalog_attribute_creates_interface(self) -> None:
        """Worker with catalog attribute creates catalog interface."""
        users_table = Table(name="users", function=UsersFunction)

        class MyWorker(Worker):
            catalog = Catalog(
                name="myapp",
                schemas=[Schema(name="main", tables=[users_table])],
            )

        interface_cls = MyWorker._get_catalog_interface()
        assert interface_cls is not None
        assert issubclass(interface_cls, ReadOnlyCatalogInterface)

    def test_worker_catalog_interface_has_catalog(self) -> None:
        """Generated catalog interface has correct catalog."""
        users_table = Table(name="users", function=UsersFunction)

        class MyWorker(Worker):
            catalog = Catalog(
                name="myapp",
                schemas=[Schema(name="main", tables=[users_table])],
            )

        interface_cls = MyWorker._get_catalog_interface()
        assert interface_cls is not None
        interface = interface_cls()
        assert interface._effective_catalog_name == "myapp"  # type: ignore[attr-defined]

    def test_worker_custom_table_scan_function_get(self) -> None:
        """Worker.table_scan_function_get is copied to interface."""
        explicit_table = Table(name="orders", columns=pa.schema([("id", pa.int64())]))

        class MyWorker(Worker):
            catalog = Catalog(
                name="myapp",
                schemas=[Schema(name="main", tables=[explicit_table])],
            )

            def table_scan_function_get(
                self,
                *,
                attach_id: Any,
                transaction_id: Any,
                schema_name: str,
                name: str,
                at_unit: str | None,
                at_value: str | None,
            ) -> ScanFunctionResult:
                return ScanFunctionResult(
                    function_name="read_parquet",
                    positional_arguments=[pa.scalar("orders.parquet")],
                    named_arguments={},
                )

        interface_cls = MyWorker._get_catalog_interface()
        assert interface_cls is not None
        interface = interface_cls()

        result = interface.table_scan_function_get(
            attach_id=AttachId(b"test"),
            transaction_id=None,
            schema_name="main",
            name="orders",
            at_unit=None,
            at_value=None,
        )
        assert result.function_name == "read_parquet"


class TestBackwardCompatibility:
    """Tests for backward compatibility with legacy patterns."""

    def test_legacy_catalog_name_functions_pattern(self) -> None:
        """Legacy catalog_name + functions pattern still works."""

        class LegacyWorker(Worker):
            catalog_name = "legacy"
            functions = [UsersFunction]

        interface_cls = LegacyWorker._get_catalog_interface()
        assert interface_cls is not None
        interface = interface_cls()
        assert interface._effective_catalog_name == "legacy"  # type: ignore[attr-defined]

    def test_no_catalog_returns_none(self) -> None:
        """Worker without catalog/functions returns None interface."""

        class EmptyWorker(Worker):
            pass

        interface_cls = EmptyWorker._get_catalog_interface()
        assert interface_cls is None


# =============================================================================
# Multi-Schema Tests
# =============================================================================


class TestMultiSchemaCatalog:
    """Tests for catalogs with multiple schemas."""

    @pytest.fixture
    def multi_schema_interface(self) -> ReadOnlyCatalogInterface:
        """Create catalog interface with multiple schemas."""
        users_table = Table(name="users", function=UsersFunction)
        events_table = Table(name="events", function=EventsFunction)

        class TestCatalog(ReadOnlyCatalogInterface):
            catalog = Catalog(
                name="warehouse",
                default_schema="analytics",
                schemas=[
                    Schema(
                        name="analytics",
                        tables=[users_table],
                        comment="Analytics data",
                    ),
                    Schema(
                        name="raw",
                        tables=[events_table],
                        comment="Raw ingested data",
                    ),
                ],
            )

        return TestCatalog()

    def test_schemas_returns_all(self, multi_schema_interface: ReadOnlyCatalogInterface) -> None:
        """schemas() returns all schemas."""
        attach_id = AttachId(b"test")
        schemas = multi_schema_interface.schemas(attach_id=attach_id, transaction_id=None)
        names = {s.name for s in schemas}
        assert names == {"analytics", "raw"}

    def test_table_in_correct_schema(self, multi_schema_interface: ReadOnlyCatalogInterface) -> None:
        """Tables are found in their correct schemas."""
        attach_id = AttachId(b"test")

        # users in analytics
        users = multi_schema_interface.table_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="analytics",
            name="users",
        )
        assert users is not None

        # users not in raw
        users_raw = multi_schema_interface.table_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="raw",
            name="users",
        )
        assert users_raw is None

        # events in raw
        events = multi_schema_interface.table_get(
            attach_id=attach_id,
            transaction_id=None,
            schema_name="raw",
            name="events",
        )
        assert events is not None

    def test_default_schema_in_attach_result(self, multi_schema_interface: ReadOnlyCatalogInterface) -> None:
        """Attach result has correct default_schema."""
        result = multi_schema_interface.catalog_attach(name="warehouse", options={})
        assert result.default_schema == "analytics"
