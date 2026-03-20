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
    MacroInfo,
    MacroType,
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
from vgi.catalog.descriptors import Catalog, ForeignKeyDef, Macro, Schema, Sql, Table, View
from vgi.catalog.secret_type import SecretTypeSpec
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
    "MacroType",
    "OnConflict",
    "SchemaObjectType",
    # Data classes
    "CatalogAttachResult",
    "CatalogExample",
    "CatalogObject",
    "CatalogSchemaObject",
    "MacroInfo",
    "SchemaInfo",
    "SecretTypeSpec",
    "Setting",
    "SettingSpec",
    "TableInfo",
    "ViewInfo",
    "FunctionInfo",
    "ScanFunctionResult",
    # Declarative descriptors
    "Catalog",
    "ForeignKeyDef",
    "Macro",
    "Schema",
    "Sql",
    "Table",
    "View",
    # Interfaces
    "CatalogInterface",
    "ReadOnlyCatalogInterface",
    # Storage
    "CatalogStorage",
    "CatalogStorageSqlite",
]
