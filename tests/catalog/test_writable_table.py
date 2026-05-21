# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for writable table support: Table descriptor, TableInfo, and catalog interface."""

from __future__ import annotations

import pyarrow as pa
import pytest

from vgi.catalog import Catalog, ReadOnlyCatalogInterface, Schema, Table
from vgi.catalog.catalog_interface import (
    AttachOpaqueData,
    ScanFunctionResult,
    TableInfo,
    WriteFunctionResult,
)
from vgi.exceptions import CatalogReadOnlyError
from vgi.invocation import BindResponse
from vgi.table_function import BindParams, TableFunctionGenerator
from vgi.table_in_out_function import TableInOutGenerator


# Minimal stub functions: just enough for the Table descriptor's bind() probe.
# We keep them here (instead of importing the real Generic* writable functions)
# because those require a live transactor/attach_opaque_data to bind successfully.
class WritableTableScan(TableFunctionGenerator[None, None]):
    """Stub scan for descriptor tests."""

    class Meta:
        """Metadata."""

        name = "generic_writable_scan"

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        """Return a fixed schema."""
        del params
        return BindResponse(output_schema=pa.schema([("id", pa.int64())]))


class WritableTableInsert(TableInOutGenerator[None, None]):
    """Stub insert for descriptor tests."""

    class Meta:
        """Metadata."""

        name = "generic_writable_insert"


class WritableTableUpdate(TableInOutGenerator[None, None]):
    """Stub update for descriptor tests."""

    class Meta:
        """Metadata."""

        name = "generic_writable_update"


class WritableTableDelete(TableInOutGenerator[None, None]):
    """Stub delete for descriptor tests."""

    class Meta:
        """Metadata."""

        name = "generic_writable_delete"


class TestWriteFunctionResultAlias:
    """WriteFunctionResult is an alias for ScanFunctionResult."""

    def test_is_same_type(self) -> None:
        """WriteFunctionResult and ScanFunctionResult are the same class."""
        assert WriteFunctionResult is ScanFunctionResult


class TestTableDescriptorWriteFields:
    """Table descriptor accepts optional write function fields."""

    def test_table_with_all_write_functions(self) -> None:
        """Table stores references to insert, update, and delete functions."""
        table = Table(
            name="t",
            function=WritableTableScan,
            insert_function=WritableTableInsert,
            update_function=WritableTableUpdate,
            delete_function=WritableTableDelete,
        )
        assert table.insert_function is WritableTableInsert
        assert table.update_function is WritableTableUpdate
        assert table.delete_function is WritableTableDelete

    def test_table_without_write_functions(self) -> None:
        """Write function fields default to None."""
        table = Table(
            name="t",
            function=WritableTableScan,
        )
        assert table.insert_function is None
        assert table.update_function is None
        assert table.delete_function is None

    def test_insert_only(self) -> None:
        """INSERT can work without a scan function (explicit columns)."""
        table = Table(
            name="t",
            columns=pa.schema([("id", pa.int64())]),
            insert_function=WritableTableInsert,
        )
        assert table.insert_function is WritableTableInsert
        assert table.update_function is None
        assert table.delete_function is None

    def test_update_requires_scan_function(self) -> None:
        """UPDATE requires a scan function for row IDs."""
        with pytest.raises(ValueError, match="update_function.*require.*scan function"):
            Table(
                name="t",
                columns=pa.schema([("id", pa.int64())]),
                update_function=WritableTableUpdate,
            )

    def test_delete_requires_scan_function(self) -> None:
        """DELETE requires a scan function for row IDs."""
        with pytest.raises(ValueError, match="delete_function.*require.*scan function"):
            Table(
                name="t",
                columns=pa.schema([("id", pa.int64())]),
                delete_function=WritableTableDelete,
            )


class TestTableInfoWriteFlags:
    """TableInfo serialization includes write support flags."""

    def test_write_flags_set_when_functions_present(self) -> None:
        """Write flags are True when corresponding functions are set."""
        table = Table(
            name="t",
            function=WritableTableScan,
            insert_function=WritableTableInsert,
            update_function=WritableTableUpdate,
            delete_function=WritableTableDelete,
        )
        info = table.to_table_info("main")
        assert info.supports_insert is True
        assert info.supports_update is True
        assert info.supports_delete is True

    def test_write_flags_false_by_default(self) -> None:
        """Write flags are False when no write functions are set."""
        table = Table(
            name="t",
            function=WritableTableScan,
        )
        info = table.to_table_info("main")
        assert info.supports_insert is False
        assert info.supports_update is False
        assert info.supports_delete is False

    def test_partial_write_flags(self) -> None:
        """Only the flags for defined write functions are True."""
        table = Table(
            name="t",
            function=WritableTableScan,
            insert_function=WritableTableInsert,
        )
        info = table.to_table_info("main")
        assert info.supports_insert is True
        assert info.supports_update is False
        assert info.supports_delete is False

    def test_table_info_serialization_roundtrip(self) -> None:
        """Write flags survive Arrow serialization roundtrip."""
        table = Table(
            name="t",
            function=WritableTableScan,
            insert_function=WritableTableInsert,
            update_function=WritableTableUpdate,
            delete_function=WritableTableDelete,
        )
        info = table.to_table_info("main")
        data = info.serialize_to_bytes()
        restored = TableInfo.deserialize_from_bytes(data)
        assert restored.supports_insert is True
        assert restored.supports_update is True
        assert restored.supports_delete is True


class TestReadOnlyCatalogInterfaceWriteMethods:
    """ReadOnlyCatalogInterface auto-implements write function getters."""

    @pytest.fixture()
    def catalog_with_writable_table(self) -> ReadOnlyCatalogInterface:
        """Create a catalog with one writable and one read-only table."""
        catalog = Catalog(
            name="test",
            schemas=[
                Schema(
                    name="main",
                    tables=[
                        Table(
                            name="writable",
                            function=WritableTableScan,
                            insert_function=WritableTableInsert,
                            update_function=WritableTableUpdate,
                            delete_function=WritableTableDelete,
                        ),
                        Table(
                            name="readonly",
                            function=WritableTableScan,
                        ),
                    ],
                ),
            ],
        )

        class TestCatalog(ReadOnlyCatalogInterface):
            """Test catalog for writable table tests."""

        TestCatalog.catalog = catalog
        return TestCatalog()

    def test_insert_function_get(self, catalog_with_writable_table: ReadOnlyCatalogInterface) -> None:
        """Returns the insert function name for a writable table."""
        result = catalog_with_writable_table.table_insert_function_get(
            attach_opaque_data=AttachOpaqueData(b"test"),
            transaction_opaque_data=None,
            schema_name="main",
            name="writable",
        )
        assert result.function_name == "generic_writable_insert"

    def test_update_function_get(self, catalog_with_writable_table: ReadOnlyCatalogInterface) -> None:
        """Returns the update function name for a writable table."""
        result = catalog_with_writable_table.table_update_function_get(
            attach_opaque_data=AttachOpaqueData(b"test"),
            transaction_opaque_data=None,
            schema_name="main",
            name="writable",
        )
        assert result.function_name == "generic_writable_update"

    def test_delete_function_get(self, catalog_with_writable_table: ReadOnlyCatalogInterface) -> None:
        """Returns the delete function name for a writable table."""
        result = catalog_with_writable_table.table_delete_function_get(
            attach_opaque_data=AttachOpaqueData(b"test"),
            transaction_opaque_data=None,
            schema_name="main",
            name="writable",
        )
        assert result.function_name == "generic_writable_delete"

    def test_readonly_table_raises_on_insert(self, catalog_with_writable_table: ReadOnlyCatalogInterface) -> None:
        """Read-only tables raise CatalogReadOnlyError on insert."""
        with pytest.raises(CatalogReadOnlyError, match="does not support INSERT"):
            catalog_with_writable_table.table_insert_function_get(
                attach_opaque_data=AttachOpaqueData(b"test"),
                transaction_opaque_data=None,
                schema_name="main",
                name="readonly",
            )

    def test_readonly_table_raises_on_update(self, catalog_with_writable_table: ReadOnlyCatalogInterface) -> None:
        """Read-only tables raise CatalogReadOnlyError on update."""
        with pytest.raises(CatalogReadOnlyError, match="does not support UPDATE"):
            catalog_with_writable_table.table_update_function_get(
                attach_opaque_data=AttachOpaqueData(b"test"),
                transaction_opaque_data=None,
                schema_name="main",
                name="readonly",
            )

    def test_readonly_table_raises_on_delete(self, catalog_with_writable_table: ReadOnlyCatalogInterface) -> None:
        """Read-only tables raise CatalogReadOnlyError on delete."""
        with pytest.raises(CatalogReadOnlyError, match="does not support DELETE"):
            catalog_with_writable_table.table_delete_function_get(
                attach_opaque_data=AttachOpaqueData(b"test"),
                transaction_opaque_data=None,
                schema_name="main",
                name="readonly",
            )

    def test_nonexistent_table_raises(self, catalog_with_writable_table: ReadOnlyCatalogInterface) -> None:
        """Non-existent tables raise NotImplementedError."""
        with pytest.raises(NotImplementedError, match="not found"):
            catalog_with_writable_table.table_insert_function_get(
                attach_opaque_data=AttachOpaqueData(b"test"),
                transaction_opaque_data=None,
                schema_name="main",
                name="no_such_table",
            )

    def test_case_insensitive_lookup(self, catalog_with_writable_table: ReadOnlyCatalogInterface) -> None:
        """Write function lookup is case-insensitive."""
        result = catalog_with_writable_table.table_insert_function_get(
            attach_opaque_data=AttachOpaqueData(b"test"),
            transaction_opaque_data=None,
            schema_name="MAIN",
            name="WRITABLE",
        )
        assert result.function_name == "generic_writable_insert"


class TestWriteFunctionAutoRegistration:
    """Write functions from Table descriptors are auto-registered in the worker."""

    def test_write_functions_in_registry(self) -> None:
        """Write functions from table descriptors appear in the worker's function registry."""
        from vgi._test_fixtures.writable.worker import WritableWorker

        # Reset cached registry so it rebuilds with our changes
        WritableWorker._registry = None
        registry = WritableWorker._build_registry()
        assert "generic_writable_scan" in registry
        assert "generic_writable_insert" in registry
        assert "generic_writable_update" in registry
        assert "generic_writable_delete" in registry
