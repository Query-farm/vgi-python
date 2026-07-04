# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""VGI Catalog Interface for exposing catalogs, schemas, tables, and views.

This module provides the abstract base class and data types for implementing
catalog interfaces in VGI workers, enabling DuckDB ATTACH support.
"""

from vgi.catalog.catalog_interface import (
    AttachOpaqueData,
    CatalogAttachResult,
    CatalogDataVersionRelease,
    CatalogExample,
    AttachCatalogInfo,
    CatalogInfo,
    CatalogInterface,
    CatalogObject,
    CatalogSchemaObject,
    CopyFromFormatInfo,
    FunctionInfo,
    FunctionType,
    IndexConstraintType,
    IndexInfo,
    MacroInfo,
    MacroType,
    OnConflict,
    ReadOnlyCatalogInterface,
    ScanBranch,
    ScanBranchesResult,
    ScanFunctionResult,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    SqlExpression,
    TableInfo,
    TransactionOpaqueData,
    ViewInfo,
)
from vgi.catalog.descriptors import Catalog, ForeignKeyDef, Index, Macro, Schema, Sql, Table, View
from vgi.catalog.secret_type import SecretTypeSpec
from vgi.catalog.setting import Setting, SettingSpec
from vgi.catalog.storage import CatalogStorage, CatalogStorageSqlite

__all__ = [
    # Type aliases
    "AttachOpaqueData",
    "TransactionOpaqueData",
    "SerializedSchema",
    "SqlExpression",
    # Enums
    "FunctionType",
    "IndexConstraintType",
    "MacroType",
    "OnConflict",
    "SchemaObjectType",
    # Data classes
    "CatalogAttachResult",
    "CatalogDataVersionRelease",
    "CatalogExample",
    "CatalogInfo",
    "CatalogObject",
    "CatalogSchemaObject",
    "IndexInfo",
    "MacroInfo",
    "SchemaInfo",
    "SecretTypeSpec",
    "Setting",
    "SettingSpec",
    "TableInfo",
    "ViewInfo",
    "FunctionInfo",
    "AttachCatalogInfo",
    "CopyFromFormatInfo",
    "ScanBranch",
    "ScanBranchesResult",
    "ScanFunctionResult",
    # Declarative descriptors
    "Catalog",
    "ForeignKeyDef",
    "Index",
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
