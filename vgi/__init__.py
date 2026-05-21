# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""VGI (Vector Gateway Interface) - Apache Arrow-based protocol for DuckDB extensions.

VGI provides a framework for connecting DuckDB to external programs via
streaming Arrow IPC. User-defined functions run in worker subprocesses
and communicate with the database through stdin/stdout.

"""

from vgi_rpc.log import Level, Message

from vgi.aggregate_function import (
    AggregateBindParams,
    AggregateFunction,
)
from vgi.argument_spec import (
    VGI_ARG_KEY,
    VGI_ARG_NAMED,
    VGI_CONST_KEY,
    VGI_CONST_TRUE,
    VGI_TYPE_ANY,
    VGI_TYPE_KEY,
    VGI_TYPE_TABLE,
    VGI_VARARGS_KEY,
    VGI_VARARGS_TRUE,
    ArgumentSpec,
    argument_specs_to_schema,
    schema_to_argument_specs,
)
from vgi.arguments import (
    AnyArrow,
    AnyArrowValue,
    Arg,
    Arguments,
    ArgumentValidationError,
    Auth,
    ConstParam,
    Param,
    Returns,
    TableInput,
)
from vgi.auth import AuthContext, CallContext
from vgi.metadata import (
    CatalogFunctionType,
    FunctionExample,
    FunctionStability,
    OrderPreservation,
    ParameterInfo,
    ResolvedMetadata,
    TableInputValidationError,
    functions_to_arrow,
)

# Re-export commonly used protocol types
from vgi.protocol import (
    BindRequest,
    InitRequest,
    VgiOutputCollector,
)
from vgi.scalar_function import (
    RowCountMismatchError,
    ScalarFunction,
    ScalarFunctionGenerator,
    TypeMismatchError,
)
from vgi.schema_utils import schema, schema_like
from vgi.table_filter_pushdown import (
    ColumnBounds,
    ColumnRefNode,
    ComparisonNode,
    ConjunctionNode,
    ConstantNode,
    ExpressionFilter,
    ExpressionNode,
    FilterDeserializationError,
    FilterError,
    FilterVersionError,
    FunctionNode,
    PushdownFilters,
    deserialize_filters,
)
from vgi.table_in_out_function import (
    TableInOutFunction,
    TableInOutGenerator,
)
from vgi.worker import Worker

__all__ = [
    "AggregateBindParams",
    "AggregateFunction",
    "AnyArrow",
    "AnyArrowValue",
    "Arg",
    "ArgumentSpec",
    "ArgumentValidationError",
    "Arguments",
    "Auth",
    "AuthContext",
    "CallContext",
    "argument_specs_to_schema",
    "BindRequest",
    "ColumnBounds",
    "ColumnRefNode",
    "ComparisonNode",
    "ConjunctionNode",
    "ConstantNode",
    "ConstParam",
    "deserialize_filters",
    "ExpressionFilter",
    "ExpressionNode",
    "FilterDeserializationError",
    "FilterError",
    "FilterVersionError",
    "Param",
    "PushdownFilters",
    "Returns",
    # Metadata constants for parsing argument spec schemas
    "VGI_ARG_KEY",
    "VGI_ARG_NAMED",
    "VGI_CONST_KEY",
    "VGI_CONST_TRUE",
    "VGI_TYPE_KEY",
    "VGI_TYPE_TABLE",
    "VGI_TYPE_ANY",
    "VGI_VARARGS_KEY",
    "VGI_VARARGS_TRUE",
    "FunctionNode",
    "FunctionExample",
    "FunctionStability",
    "CatalogFunctionType",
    "InitRequest",
    "Level",
    "VgiOutputCollector",
    "Message",
    "OrderPreservation",
    "ParameterInfo",
    "ResolvedMetadata",
    "RowCountMismatchError",
    "ScalarFunction",
    "ScalarFunctionGenerator",
    "TableInOutFunction",
    "TableInOutGenerator",
    "TableInput",
    "TableInputValidationError",
    "TypeMismatchError",
    "Worker",
    "functions_to_arrow",
    "schema",
    "schema_like",
    "schema_to_argument_specs",
]
