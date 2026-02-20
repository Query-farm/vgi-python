"""VGI Catalog Interface for exposing catalogs, schemas, tables, and views.

This module provides the abstract base class and data types for implementing
catalog interfaces in VGI workers, enabling DuckDB ATTACH support.
"""

from vgi.catalog.catalog_interface import (
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
]
