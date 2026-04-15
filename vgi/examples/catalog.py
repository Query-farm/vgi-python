"""In-memory catalog implementation for testing and examples.

This module provides an in-memory implementation of CatalogInterface that can
be used for testing and as a reference implementation.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, overload

if TYPE_CHECKING:
    import pyarrow as pa

from vgi.catalog import (
    AttachId,
    CatalogAttachResult,
    CatalogInterface,
    FunctionInfo,
    IndexInfo,
    MacroInfo,
    MacroType,
    OnConflict,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    TableInfo,
    TransactionId,
    ViewInfo,
)
from vgi.worker import Worker


@dataclass
class TableData:
    """In-memory storage for table metadata."""

    info: TableInfo


@dataclass
class ViewData:
    """In-memory storage for view metadata."""

    info: ViewInfo


@dataclass
class MacroData:
    """In-memory storage for macro metadata."""

    info: MacroInfo


@dataclass
class SchemaData:
    """In-memory storage for schema metadata."""

    info: SchemaInfo
    tables: dict[str, TableData] = field(default_factory=dict)
    views: dict[str, ViewData] = field(default_factory=dict)
    macros: dict[str, MacroData] = field(default_factory=dict)


@dataclass
class CatalogData:
    """In-memory storage for catalog metadata."""

    name: str
    schemas: dict[str, SchemaData] = field(default_factory=dict)
    version: int = 1
    comment: str | None = None
    tags: dict[str, str] = field(default_factory=dict)


class InMemoryCatalog(CatalogInterface):
    """In-memory catalog implementation for testing.

    This implementation stores all catalog, schema, table, and view data
    in memory using Python dictionaries. It supports basic DDL operations
    but does not support transactions.

    Attach IDs are generated as random UUIDs.
    """

    def __init__(self) -> None:
        """Initialize the in-memory catalog."""
        # Maps catalog name -> CatalogData
        self._catalogs: dict[str, CatalogData] = {}
        # Maps attach_id -> catalog_name
        self._attachments: dict[AttachId, str] = {}
        # Create default "memory" catalog with "main" schema
        self._create_default_catalog()

    def _create_default_catalog(self) -> None:
        """Create the default memory catalog with main schema."""
        catalog = CatalogData(name="memory")
        # Create a placeholder attach_id for internal use
        placeholder_attach_id = AttachId(b"\x00" * 16)
        catalog.schemas["main"] = SchemaData(
            info=SchemaInfo(
                attach_id=placeholder_attach_id,
                name="main",
                comment=None,
                tags={},
            )
        )
        self._catalogs["memory"] = catalog

    def _get_catalog(self, attach_id: AttachId) -> CatalogData:
        """Get the catalog for the given attach_id."""
        catalog_name = self._attachments.get(attach_id)
        if catalog_name is None:
            msg = f"No catalog attached with id {attach_id!r}"
            raise ValueError(msg)
        catalog = self._catalogs.get(catalog_name)
        if catalog is None:
            msg = f"Catalog {catalog_name!r} not found"
            raise ValueError(msg)
        return catalog

    def _get_schema(self, attach_id: AttachId, schema_name: str) -> SchemaData:
        """Get the schema for the given attach_id and schema name."""
        catalog = self._get_catalog(attach_id)
        schema = catalog.schemas.get(schema_name)
        if schema is None:
            msg = f"Schema {schema_name!r} not found in catalog"
            raise ValueError(msg)
        return schema

    def _increment_version(self, attach_id: AttachId) -> None:
        """Increment the catalog version after a modification."""
        catalog = self._get_catalog(attach_id)
        catalog.version += 1

    # Required abstract methods

    def catalogs(self) -> list[str]:
        """Get a list of catalog names."""
        return list(self._catalogs.keys())

    def catalog_attach(self, *, name: str, options: dict[str, Any]) -> CatalogAttachResult:
        """Attach to a catalog with the given name."""
        if name not in self._catalogs:
            msg = f"Catalog {name!r} not found"
            raise ValueError(msg)

        # Generate a unique attach_id
        attach_id = AttachId(uuid.uuid4().bytes)
        self._attachments[attach_id] = name

        catalog = self._catalogs[name]
        return CatalogAttachResult(
            attach_id=attach_id,
            supports_transactions=False,
            supports_time_travel=False,
            catalog_version_frozen=False,
            catalog_version=catalog.version,
            comment=catalog.comment,
            tags=dict(catalog.tags),
        )

    def catalog_detach(self, *, attach_id: AttachId) -> None:
        """Detach from the catalog."""
        self._attachments.pop(attach_id, None)

    def schema_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
    ) -> SchemaInfo | None:
        """Get information about a schema."""
        catalog = self._get_catalog(attach_id)
        schema_data = catalog.schemas.get(name)
        if schema_data is None:
            return None
        # Update the attach_id in the returned info
        return SchemaInfo(
            attach_id=attach_id,
            name=schema_data.info.name,
            comment=schema_data.info.comment,
            tags=schema_data.info.tags,
        )

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
        """Get information about a table."""
        catalog = self._get_catalog(attach_id)
        schema_data = catalog.schemas.get(schema_name)
        if schema_data is None:
            return None
        table_data = schema_data.tables.get(name)
        if table_data is None:
            return None
        return table_data.info

    def view_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> ViewInfo | None:
        """Get information about a view."""
        catalog = self._get_catalog(attach_id)
        schema_data = catalog.schemas.get(schema_name)
        if schema_data is None:
            return None
        view_data = schema_data.views.get(name)
        if view_data is None:
            return None
        return view_data.info

    def macro_get(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
    ) -> MacroInfo | None:
        """Get information about a macro."""
        catalog = self._get_catalog(attach_id)
        schema_data = catalog.schemas.get(schema_name)
        if schema_data is None:
            return None
        macro_data = schema_data.macros.get(name)
        if macro_data is None:
            return None
        return macro_data.info

    # Optional methods with implementations

    def catalog_version(self, *, attach_id: AttachId, transaction_id: TransactionId | None) -> int:
        """Get the current catalog version."""
        catalog = self._get_catalog(attach_id)
        return catalog.version

    def catalog_create(self, *, name: str, on_conflict: OnConflict, options: dict[str, Any]) -> None:
        """Create a new catalog."""
        if name in self._catalogs:
            if on_conflict == OnConflict.ERROR:
                msg = f"Catalog {name!r} already exists"
                raise ValueError(msg)
            if on_conflict == OnConflict.IGNORE:
                return
            # REPLACE: fall through to create

        catalog = CatalogData(name=name)
        # Create a placeholder attach_id for internal use
        placeholder_attach_id = AttachId(b"\x00" * 16)
        catalog.schemas["main"] = SchemaData(
            info=SchemaInfo(
                attach_id=placeholder_attach_id,
                name="main",
                comment=None,
                tags={},
            )
        )
        self._catalogs[name] = catalog

    def catalog_drop(self, *, name: str) -> None:
        """Drop a catalog."""
        if name not in self._catalogs:
            msg = f"Catalog {name!r} not found"
            raise ValueError(msg)
        # Remove any attachments to this catalog
        to_remove = [aid for aid, cname in self._attachments.items() if cname == name]
        for aid in to_remove:
            del self._attachments[aid]
        del self._catalogs[name]

    def schemas(self, *, attach_id: AttachId, transaction_id: TransactionId | None) -> list[SchemaInfo]:
        """Get a list of schemas in the catalog."""
        catalog = self._get_catalog(attach_id)
        result = []
        for schema_data in catalog.schemas.values():
            # Update the attach_id in the returned info
            result.append(
                SchemaInfo(
                    attach_id=attach_id,
                    name=schema_data.info.name,
                    comment=schema_data.info.comment,
                    tags=schema_data.info.tags,
                )
            )
        return result

    def schema_create(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        on_conflict: OnConflict = OnConflict.ERROR,
        comment: str | None,
        tags: dict[str, str],
    ) -> None:
        """Create a new schema."""
        catalog = self._get_catalog(attach_id)
        if name in catalog.schemas:
            msg = f"Schema {name!r} already exists"
            raise ValueError(msg)
        catalog.schemas[name] = SchemaData(
            info=SchemaInfo(
                attach_id=attach_id,
                name=name,
                comment=comment,
                tags=tags,
            )
        )
        self._increment_version(attach_id)

    def schema_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        ignore_not_found: bool,
        cascade: bool,
    ) -> None:
        """Drop a schema."""
        catalog = self._get_catalog(attach_id)
        if name not in catalog.schemas:
            if ignore_not_found:
                return
            msg = f"Schema {name!r} not found"
            raise ValueError(msg)
        schema_data = catalog.schemas[name]
        if not cascade and (schema_data.tables or schema_data.views or schema_data.macros):
            msg = f"Schema {name!r} is not empty, use CASCADE to drop"
            raise ValueError(msg)
        del catalog.schemas[name]
        self._increment_version(attach_id)

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: Literal[SchemaObjectType.TABLE],
    ) -> Sequence[TableInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: Literal[SchemaObjectType.VIEW],
    ) -> Sequence[ViewInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: Literal[
            SchemaObjectType.SCALAR_FUNCTION,
            SchemaObjectType.TABLE_FUNCTION,
            SchemaObjectType.AGGREGATE_FUNCTION,
        ],
    ) -> Sequence[FunctionInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: Literal[SchemaObjectType.SCALAR_MACRO, SchemaObjectType.TABLE_MACRO],
    ) -> Sequence[MacroInfo]: ...

    @overload
    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: Literal[SchemaObjectType.INDEX],
    ) -> Sequence[IndexInfo]: ...

    def schema_contents(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        name: str,
        type: SchemaObjectType,
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo | MacroInfo | IndexInfo]:
        """Get the contents of a schema.

        Args:
            attach_id: The attachment identifier.
            transaction_id: The transaction identifier, if any.
            name: The name of the schema.
            type: The type of objects to return. Must be a SchemaObjectType enum.

        Returns:
            An iterable of TableInfo, ViewInfo, FunctionInfo, or MacroInfo objects
            depending on the type parameter.

        """
        schema_data = self._get_schema(attach_id, name)
        result: list[TableInfo | ViewInfo | FunctionInfo | MacroInfo | IndexInfo] = []

        # Normalize type parameter (may be string from wire protocol)
        type_enum = type if isinstance(type, SchemaObjectType) else SchemaObjectType(type)

        # Return tables for TABLE type
        if type_enum == SchemaObjectType.TABLE:
            for table_data in schema_data.tables.values():
                result.append(table_data.info)

        # Return views for VIEW type
        elif type_enum == SchemaObjectType.VIEW:
            for view_data in schema_data.views.values():
                result.append(view_data.info)

        # Return macros for SCALAR_MACRO or TABLE_MACRO type
        elif type_enum in (SchemaObjectType.SCALAR_MACRO, SchemaObjectType.TABLE_MACRO):
            target_macro_type = MacroType.SCALAR if type_enum == SchemaObjectType.SCALAR_MACRO else MacroType.TABLE
            for macro_data in schema_data.macros.values():
                if macro_data.info.macro_type == target_macro_type:
                    result.append(macro_data.info)

        # Note: This example catalog doesn't store functions,
        # so SCALAR_FUNCTION and TABLE_FUNCTION types return nothing

        return result

    def table_create(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        columns: SerializedSchema,
        on_conflict: OnConflict,
        not_null_constraints: list[int],
        unique_constraints: list[list[int]],
        check_constraints: list[str],
        primary_key_constraints: list[list[int]] | None = None,
        foreign_key_constraints: list[bytes] | None = None,
    ) -> None:
        """Create a new table."""
        schema_data = self._get_schema(attach_id, schema_name)
        if name in schema_data.tables:
            if on_conflict == OnConflict.ERROR:
                msg = f"Table {name!r} already exists in schema {schema_name!r}"
                raise ValueError(msg)
            if on_conflict == OnConflict.IGNORE:
                return
            # REPLACE: fall through to create

        schema_data.tables[name] = TableData(
            info=TableInfo(
                name=name,
                schema_name=schema_name,
                columns=columns,
                not_null_constraints=not_null_constraints,
                unique_constraints=unique_constraints,
                check_constraints=check_constraints,
                comment=None,
                tags={},
            )
        )
        self._increment_version(attach_id)

    def table_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        ignore_not_found: bool,
        cascade: bool = False,
    ) -> None:
        """Drop a table."""
        schema_data = self._get_schema(attach_id, schema_name)
        if name not in schema_data.tables:
            if ignore_not_found:
                return
            msg = f"Table {name!r} not found in schema {schema_name!r}"
            raise ValueError(msg)
        del schema_data.tables[name]
        self._increment_version(attach_id)

    def table_comment_set(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        comment: str | None,
        ignore_not_found: bool,
    ) -> None:
        """Set the comment for a table."""
        schema_data = self._get_schema(attach_id, schema_name)
        table_data = schema_data.tables.get(name)
        if table_data is None:
            if ignore_not_found:
                return
            msg = f"Table {name!r} not found in schema {schema_name!r}"
            raise ValueError(msg)
        # Create a new TableInfo with the updated comment
        old_info = table_data.info
        schema_data.tables[name] = TableData(
            info=TableInfo(
                name=old_info.name,
                schema_name=old_info.schema_name,
                columns=old_info.columns,
                not_null_constraints=old_info.not_null_constraints,
                unique_constraints=old_info.unique_constraints,
                check_constraints=old_info.check_constraints,
                comment=comment,
                tags=old_info.tags,
            )
        )
        self._increment_version(attach_id)

    def table_rename(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        new_name: str,
        ignore_not_found: bool,
    ) -> None:
        """Rename a table."""
        schema_data = self._get_schema(attach_id, schema_name)
        if name not in schema_data.tables:
            if ignore_not_found:
                return
            msg = f"Table {name!r} not found in schema {schema_name!r}"
            raise ValueError(msg)
        if new_name in schema_data.tables:
            msg = f"Table {new_name!r} already exists in schema {schema_name!r}"
            raise ValueError(msg)
        table_data = schema_data.tables.pop(name)
        # Create new TableInfo with updated name
        old_info = table_data.info
        schema_data.tables[new_name] = TableData(
            info=TableInfo(
                name=new_name,
                schema_name=old_info.schema_name,
                columns=old_info.columns,
                not_null_constraints=old_info.not_null_constraints,
                unique_constraints=old_info.unique_constraints,
                check_constraints=old_info.check_constraints,
                comment=old_info.comment,
                tags=old_info.tags,
            )
        )
        self._increment_version(attach_id)

    def view_create(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        definition: str,
        on_conflict: OnConflict,
    ) -> None:
        """Create a new view."""
        schema_data = self._get_schema(attach_id, schema_name)
        if name in schema_data.views:
            if on_conflict == OnConflict.ERROR:
                msg = f"View {name!r} already exists in schema {schema_name!r}"
                raise ValueError(msg)
            if on_conflict == OnConflict.IGNORE:
                return
            # REPLACE: fall through to create

        schema_data.views[name] = ViewData(
            info=ViewInfo(
                name=name,
                schema_name=schema_name,
                definition=definition,
                comment=None,
                tags={},
            )
        )
        self._increment_version(attach_id)

    def view_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        ignore_not_found: bool,
        cascade: bool = False,
    ) -> None:
        """Drop a view."""
        schema_data = self._get_schema(attach_id, schema_name)
        if name not in schema_data.views:
            if ignore_not_found:
                return
            msg = f"View {name!r} not found in schema {schema_name!r}"
            raise ValueError(msg)
        del schema_data.views[name]
        self._increment_version(attach_id)

    def view_rename(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        new_name: str,
        ignore_not_found: bool,
    ) -> None:
        """Rename a view."""
        schema_data = self._get_schema(attach_id, schema_name)
        if name not in schema_data.views:
            if ignore_not_found:
                return
            msg = f"View {name!r} not found in schema {schema_name!r}"
            raise ValueError(msg)
        if new_name in schema_data.views:
            msg = f"View {new_name!r} already exists in schema {schema_name!r}"
            raise ValueError(msg)
        view_data = schema_data.views.pop(name)
        # Create new ViewInfo with updated name
        old_info = view_data.info
        schema_data.views[new_name] = ViewData(
            info=ViewInfo(
                name=new_name,
                schema_name=old_info.schema_name,
                definition=old_info.definition,
                comment=old_info.comment,
                tags=old_info.tags,
            )
        )
        self._increment_version(attach_id)

    def view_comment_set(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        comment: str | None,
        ignore_not_found: bool,
    ) -> None:
        """Set the comment for a view."""
        schema_data = self._get_schema(attach_id, schema_name)
        view_data = schema_data.views.get(name)
        if view_data is None:
            if ignore_not_found:
                return
            msg = f"View {name!r} not found in schema {schema_name!r}"
            raise ValueError(msg)
        # Create a new ViewInfo with the updated comment
        old_info = view_data.info
        schema_data.views[name] = ViewData(
            info=ViewInfo(
                name=old_info.name,
                schema_name=old_info.schema_name,
                definition=old_info.definition,
                comment=comment,
                tags=old_info.tags,
            )
        )
        self._increment_version(attach_id)

    def macro_create(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        macro_type: MacroType,
        parameters: list[str],
        definition: str,
        on_conflict: OnConflict,
        parameter_default_values: pa.RecordBatch | None = None,
    ) -> None:
        """Create a new macro."""
        schema_data = self._get_schema(attach_id, schema_name)
        if name in schema_data.macros:
            if on_conflict == OnConflict.ERROR:
                msg = f"Macro {name!r} already exists in schema {schema_name!r}"
                raise ValueError(msg)
            if on_conflict == OnConflict.IGNORE:
                return
            # REPLACE: fall through to create

        schema_data.macros[name] = MacroData(
            info=MacroInfo(
                name=name,
                schema_name=schema_name,
                macro_type=macro_type,
                parameters=parameters,
                parameter_default_values=parameter_default_values,
                definition=definition,
                comment=None,
                tags={},
            )
        )
        self._increment_version(attach_id)

    def macro_drop(
        self,
        *,
        attach_id: AttachId,
        transaction_id: TransactionId | None,
        schema_name: str,
        name: str,
        ignore_not_found: bool,
    ) -> None:
        """Drop a macro."""
        schema_data = self._get_schema(attach_id, schema_name)
        if name not in schema_data.macros:
            if ignore_not_found:
                return
            msg = f"Macro {name!r} not found in schema {schema_name!r}"
            raise ValueError(msg)
        del schema_data.macros[name]
        self._increment_version(attach_id)


class InMemoryCatalogWorker(Worker):
    """Example worker with InMemoryCatalog support."""

    catalog_interface = InMemoryCatalog
    functions = []  # No functions, just catalog support


def main() -> None:
    """Run the in-memory catalog worker process."""
    InMemoryCatalogWorker.main()


if __name__ == "__main__":
    main()
