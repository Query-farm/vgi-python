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
    AttachId,
    CatalogAttachResult,
    CatalogInterface,
    CatalogObject,
    CatalogSchemaObject,
    FunctionInfo,
    FunctionType,
    OnConflict,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    SchemaInfo,
    SerializedSchema,
    SqlExpression,
    TableInfo,
    TransactionId,
    ViewInfo,
)

__all__ = [
    # Type aliases
    "AttachId",
    "TransactionId",
    "SerializedSchema",
    "SqlExpression",
    # Enums
    "FunctionType",
    "OnConflict",
    # Data classes
    "CatalogAttachResult",
    "CatalogObject",
    "CatalogSchemaObject",
    "SchemaInfo",
    "TableInfo",
    "ViewInfo",
    "FunctionInfo",
    "ScanFunctionResult",
    # Interfaces
    "CatalogInterface",
    "ReadOnlyCatalogInterface",
]
