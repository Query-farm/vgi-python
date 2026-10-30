# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""VGI Catalog Interface for exposing catalogs, schemas, tables, and views.

This module provides the abstract base class and data types for implementing
catalog interfaces in VGI workers, enabling DuckDB ATTACH support.
"""

from vgi.catalog.catalog_interface import (
    AttachCatalogInfo,
    AttachOpaqueData,
    CatalogAttachResult,
    CatalogContentsResult,
    CatalogDataVersionRelease,
    CatalogExample,
    CatalogInfo,
    CatalogInterface,
    CatalogObject,
    CatalogSchemaObject,
    ColumnStatistics,
    CopyFromFormatInfo,
    ForeignKeyInfo,
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
    SchemaContentsInfo,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    SqlExpression,
    TableInfo,
    TransactionOpaqueData,
    ViewInfo,
    deserialize_column_statistics,
    serialize_column_statistics,
)
from vgi.catalog.column_metadata import (
    ColumnMetadata,
    column_metadata,
    validate_column_metadata,
    with_column_metadata,
)
from vgi.catalog.constraints import (
    ConstraintChange,
    ConstraintDefinition,
    ConstraintEnforcement,
    ConstraintInfo,
    ConstraintKind,
    ConstraintOperation,
    ConstraintsApplyRequest,
    ConstraintSnapshot,
    ConstraintValidation,
    ForeignKeyDefinition,
    ForeignKeyMatch,
    ReferencedKey,
    ReferentialAction,
    constraint_record,
    validate_constraint_records,
)
from vgi.catalog.dependencies import CatalogObjectRef, DependencySnapshot, ObjectDependency
from vgi.catalog.descriptors import Catalog, ForeignKeyDef, Index, Macro, Schema, Sql, Table, View
from vgi.catalog.indexes import IndexDefinition, IndexKey
from vgi.catalog.macros import MacroOverload, MacroParameter
from vgi.catalog.metadata_changes import CatalogMetadataChanges
from vgi.catalog.query import (
    CatalogQueryPrepareRequest,
    CatalogQueryPrepareResult,
    ClientQueryCapabilities,
    QueryCapabilities,
    QueryEvaluationContext,
    QueryObjectReference,
)
from vgi.catalog.secret_type import SecretTypeSpec
from vgi.catalog.setting import Setting, SettingSpec
from vgi.catalog.sql import ColumnExpressions, SqlStatement, column_expressions, with_column_expressions
from vgi.catalog.storage import CatalogStorage, CatalogStorageSqlite
from vgi.column_path import RequiredFilterPaths
from vgi.write_results import WriteOperation, WriteResultMode, write_changes_batch, write_result_schema

__all__ = [
    "ColumnMetadata",
    "column_metadata",
    "validate_column_metadata",
    "with_column_metadata",
    "IndexDefinition",
    "IndexKey",
    "ConstraintChange",
    "ConstraintDefinition",
    "ConstraintEnforcement",
    "ConstraintInfo",
    "ConstraintKind",
    "ConstraintOperation",
    "ConstraintsApplyRequest",
    "ConstraintSnapshot",
    "ConstraintValidation",
    "ForeignKeyDefinition",
    "ForeignKeyMatch",
    "ReferencedKey",
    "ReferentialAction",
    "constraint_record",
    "validate_constraint_records",
    "RequiredFilterPaths",
    "ColumnExpressions",
    "SqlStatement",
    "column_expressions",
    "with_column_expressions",
    "CatalogQueryPrepareRequest",
    "CatalogQueryPrepareResult",
    "ClientQueryCapabilities",
    "QueryCapabilities",
    "QueryEvaluationContext",
    "QueryObjectReference",
    "CatalogObjectRef",
    "DependencySnapshot",
    "ObjectDependency",
    "CatalogMetadataChanges",
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
    "ColumnStatistics",
    "ForeignKeyInfo",
    "IndexInfo",
    "MacroInfo",
    "MacroOverload",
    "MacroParameter",
    "CatalogContentsResult",
    "SchemaContentsInfo",
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
    "WriteOperation",
    "WriteResultMode",
    "write_result_schema",
    "write_changes_batch",
    # Interfaces
    "CatalogInterface",
    "ReadOnlyCatalogInterface",
    # Storage
    "CatalogStorage",
    "CatalogStorageSqlite",
    # Column statistics wire format
    "serialize_column_statistics",
    "deserialize_column_statistics",
]
