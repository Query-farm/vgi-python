"""VGI Catalog Interface for exposing catalogs, schemas, tables, and views.

This module provides the abstract base class and data types for implementing
catalog interfaces in VGI workers, enabling DuckDB ATTACH support.

Example:
    from vgi.catalog import CatalogInterface, CatalogAttachResult, SchemaInfo

    class MyCatalog(CatalogInterface):
        def catalogs(self) -> Iterable[str]:
            return ["my_catalog"]

        def catalog_attach(self, *, name: str, options: dict) -> CatalogAttachResult:
            return CatalogAttachResult(
                attach_id=AttachId(b"my-id"),
                supports_transactions=False,
                supports_time_travel=False,
                catalog_version_frozen=True,
                catalog_version=1,
            )
        # ... implement other abstract methods

"""

from vgi.catalog.catalog_interface import (
    CATALOG_METHOD_SCHEMAS,
    AttachId,
    CatalogAttachResult,
    CatalogExample,
    CatalogInterface,
    CatalogObject,
    CatalogSchemaObject,
    FunctionInfo,
    FunctionType,
    OnConflict,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    SqlExpression,
    TableInfo,
    TransactionId,
    ViewInfo,
    get_catalog_method_schema,
)
from vgi.catalog.descriptors import Catalog, Schema, Table, View
from vgi.catalog.setting import Setting, SettingSpec
from vgi.catalog.storage import CatalogStorage, CatalogStorageSqlite

__all__ = [
    # Type aliases
    "AttachId",
    "TransactionId",
    "SerializedSchema",
    "SqlExpression",
    # Enums
    "FunctionType",
    "OnConflict",
    "SchemaObjectType",
    # Data classes
    "CatalogAttachResult",
    "CatalogExample",
    "CatalogObject",
    "CatalogSchemaObject",
    "SchemaInfo",
    "Setting",
    "SettingSpec",
    "TableInfo",
    "ViewInfo",
    "FunctionInfo",
    "ScanFunctionResult",
    # Declarative descriptors
    "Catalog",
    "Schema",
    "Table",
    "View",
    # Interfaces
    "CatalogInterface",
    "ReadOnlyCatalogInterface",
    # Storage
    "CatalogStorage",
    "CatalogStorageSqlite",
    # Schema mapping for catalog methods
    "CATALOG_METHOD_SCHEMAS",
    "get_catalog_method_schema",
]
