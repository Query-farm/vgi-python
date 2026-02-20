"""Shared utilities for VGI CLI commands.

This module provides common utilities used across CLI command groups:
- Hex string conversion for AttachId and TransactionId
- JSON to Arrow schema conversion for table columns
- Output formatting helpers

"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import click
import pyarrow as pa

from vgi.catalog import AttachId, TransactionId

if TYPE_CHECKING:
    from vgi.catalog import CatalogAttachResult, FunctionInfo, SchemaInfo, TableInfo, ViewInfo
    from vgi.catalog.catalog_interface import ScanFunctionResult
    from vgi.client import Client

# Map of type names to PyArrow types for JSON schema definitions
ARROW_TYPE_MAP: dict[str, pa.DataType] = {
    # Signed integers
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    # Unsigned integers
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    # Floating point
    "float16": pa.float16(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    # Strings and binary
    "string": pa.string(),
    "utf8": pa.utf8(),
    "large_string": pa.large_string(),
    "binary": pa.binary(),
    "large_binary": pa.large_binary(),
    # Boolean
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
    # Date types
    "date32": pa.date32(),
    "date64": pa.date64(),
    # Timestamp types (microsecond precision by default)
    "timestamp": pa.timestamp("us"),
    "timestamp_s": pa.timestamp("s"),
    "timestamp_ms": pa.timestamp("ms"),
    "timestamp_us": pa.timestamp("us"),
    "timestamp_ns": pa.timestamp("ns"),
    # Duration types
    "duration": pa.duration("us"),
    "duration_s": pa.duration("s"),
    "duration_ms": pa.duration("ms"),
    "duration_us": pa.duration("us"),
    "duration_ns": pa.duration("ns"),
    # Time types
    "time32": pa.time32("ms"),
    "time64": pa.time64("us"),
}


def hex_to_bytes(hex_string: str) -> bytes:
    """Convert a hex string to bytes.

    Args:
        hex_string: Hexadecimal string (e.g., "deadbeef")

    Returns:
        Bytes representation

    Raises:
        click.ClickException: If hex string is invalid

    """
    try:
        return bytes.fromhex(hex_string)
    except ValueError as e:
        raise click.ClickException(f"Invalid hex string '{hex_string}': {e}") from e


def hex_to_attach_id(hex_string: str) -> AttachId:
    """Convert a hex string to AttachId.

    Args:
        hex_string: Hexadecimal string (e.g., "deadbeef")

    Returns:
        AttachId

    Raises:
        click.ClickException: If hex string is invalid

    """
    return AttachId(hex_to_bytes(hex_string))


def hex_to_transaction_id(hex_string: str) -> TransactionId:
    """Convert a hex string to TransactionId.

    Args:
        hex_string: Hexadecimal string (e.g., "deadbeef")

    Returns:
        TransactionId

    Raises:
        click.ClickException: If hex string is invalid

    """
    return TransactionId(hex_to_bytes(hex_string))


def bytes_to_hex(data: bytes) -> str:
    """Convert bytes to a hex string.

    Args:
        data: Bytes to convert

    Returns:
        Hexadecimal string representation

    """
    return data.hex()


def json_to_arrow_schema(columns: list[dict[str, str]]) -> pa.Schema:
    """Convert JSON column definitions to PyArrow schema.

    Args:
        columns: List of dicts with 'name' and 'type' keys.
            Example: [{"name": "id", "type": "int64"}]

    Returns:
        PyArrow Schema

    Raises:
        click.ClickException: If type is unknown or column definition is invalid.

    """
    fields = []
    for i, col in enumerate(columns):
        if "name" not in col:
            raise click.ClickException(f"Column {i} missing 'name' field: {json.dumps(col)}")
        if "type" not in col:
            raise click.ClickException(f"Column {i} missing 'type' field: {json.dumps(col)}")

        type_name = col["type"]
        if type_name not in ARROW_TYPE_MAP:
            valid_types = ", ".join(sorted(ARROW_TYPE_MAP.keys()))
            raise click.ClickException(
                f"Unknown type '{type_name}' for column '{col['name']}'. Valid types: {valid_types}"
            )

        fields.append(pa.field(col["name"], ARROW_TYPE_MAP[type_name]))

    return pa.schema(fields)


def arrow_schema_to_json(serialized: bytes) -> list[dict[str, str | bool]]:
    """Convert serialized Arrow schema to JSON for display.

    Args:
        serialized: Serialized Arrow schema bytes

    Returns:
        List of column definitions with name, type, and optional flags (varargs, const)

    """
    reader = pa.BufferReader(serialized)
    schema = pa.ipc.read_schema(reader)  # type: ignore[arg-type]
    result: list[dict[str, str | bool]] = []
    for f in schema:
        type_str = str(f.type)
        is_varargs = False
        is_const = False
        if f.metadata:
            # Check for vgi:any metadata (output schema)
            if f.metadata.get(b"vgi:any") == b"true":
                type_str = "any"
            # Check for vgi_type metadata (argument schema)
            elif f.metadata.get(b"vgi_type") == b"table":
                type_str = "table"
            elif f.metadata.get(b"vgi_type") == b"any":
                type_str = "any"
            # Check for varargs metadata
            if f.metadata.get(b"vgi_varargs") == b"true":
                is_varargs = True
            # Check for const metadata (ConstParam)
            if f.metadata.get(b"vgi_const") == b"true":
                is_const = True

        entry: dict[str, str | bool] = {"name": f.name, "type": type_str}
        if is_varargs:
            entry["varargs"] = True
        if is_const:
            entry["const"] = True
        result.append(entry)
    return result


def output_json(data: Any) -> None:
    """Output data as JSON to stdout.

    Args:
        data: Data to serialize as JSON

    """
    click.echo(json.dumps(data))


def parse_json_option(value: str, option_name: str) -> Any:
    """Parse a JSON string from a CLI option.

    Args:
        value: JSON string to parse
        option_name: Name of the option (for error messages)

    Returns:
        Parsed JSON value

    Raises:
        click.ClickException: If JSON is invalid

    """
    try:
        return json.loads(value)
    except json.JSONDecodeError as e:
        raise click.ClickException(f"Invalid JSON for {option_name}: {e}") from e


def schema_info_to_dict(schema_info: SchemaInfo) -> dict[str, Any]:
    """Convert SchemaInfo to a dictionary for JSON output.

    Args:
        schema_info: SchemaInfo object from catalog

    Returns:
        Dictionary representation

    """
    return {
        "name": schema_info.name,
        "comment": schema_info.comment,
        "tags": dict(schema_info.tags),
    }


def table_info_to_dict(table_info: TableInfo) -> dict[str, Any]:
    """Convert TableInfo to a dictionary for JSON output.

    Args:
        table_info: TableInfo object from catalog

    Returns:
        Dictionary representation

    """
    return {
        "name": table_info.name,
        "schema_name": table_info.schema_name,
        "columns": arrow_schema_to_json(table_info.columns),
        "not_null_constraints": table_info.not_null_constraints,
        "unique_constraints": table_info.unique_constraints,
        "check_constraints": table_info.check_constraints,
        "comment": table_info.comment,
        "tags": dict(table_info.tags),
    }


def view_info_to_dict(view_info: ViewInfo) -> dict[str, Any]:
    """Convert ViewInfo to a dictionary for JSON output.

    Args:
        view_info: ViewInfo object from catalog

    Returns:
        Dictionary representation

    """
    return {
        "name": view_info.name,
        "schema_name": view_info.schema_name,
        "definition": view_info.definition,
        "comment": view_info.comment,
        "tags": dict(view_info.tags),
    }


def function_info_to_dict(function_info: FunctionInfo) -> dict[str, Any]:
    """Convert FunctionInfo to a dictionary for JSON output.

    Args:
        function_info: FunctionInfo object from catalog

    Returns:
        Dictionary representation

    """
    result: dict[str, Any] = {
        "name": function_info.name,
        "schema_name": function_info.schema_name,
        "function_type": function_info.function_type.value,
        "arguments": arrow_schema_to_json(function_info.arguments),
        "description": function_info.description,
        "tags": dict(function_info.tags),
        # Scalar function behavior fields (None for non-scalar)
        "stability": (function_info.stability.name if function_info.stability else None),
        "null_handling": (function_info.null_handling.name if function_info.null_handling else None),
        # Documentation fields (convert CatalogExample to dict for JSON)
        "examples": [
            {"sql": ex.sql, "description": ex.description} if hasattr(ex, "sql") else ex
            for ex in function_info.examples
        ],
        "categories": function_info.categories,
        # Table function capabilities (None for scalar)
        "projection_pushdown": function_info.projection_pushdown,
        "filter_pushdown": function_info.filter_pushdown,
        "order_preservation": (function_info.order_preservation.name if function_info.order_preservation else None),
        "max_workers": function_info.max_workers,
        # Aggregate function fields
        "order_dependent": function_info.order_dependent.name,
        "distinct_dependent": function_info.distinct_dependent.name,
        # Settings
        "required_settings": function_info.required_settings,
    }
    # Only include output_schema for scalar functions
    if function_info.function_type.value == "scalar":
        result["output_schema"] = arrow_schema_to_json(function_info.output_schema)
    return result


def catalog_attach_result_to_dict(result: CatalogAttachResult) -> dict[str, Any]:
    """Convert CatalogAttachResult to a dictionary for JSON output.

    Args:
        result: CatalogAttachResult object

    Returns:
        Dictionary representation with attach_id as hex

    """
    return {
        "attach_id": bytes_to_hex(result.attach_id),
        "supports_transactions": result.supports_transactions,
        "supports_time_travel": result.supports_time_travel,
        "catalog_version_frozen": result.catalog_version_frozen,
        "catalog_version": result.catalog_version,
        "attach_id_required": result.attach_id_required,
        "default_schema": result.default_schema,
        "settings": [bytes_to_hex(s) for s in result.settings],
    }


def scan_function_result_to_dict(result: ScanFunctionResult) -> dict[str, Any]:
    """Convert ScanFunctionResult to a dictionary for JSON output.

    ScanFunctionResult allows the VGI DuckDB extension to call any DuckDB
    function with specified positional and named arguments, and load any
    required extensions.

    Args:
        result: ScanFunctionResult object

    Returns:
        Dictionary representation with function_name, positional_arguments,
        named_arguments, and required_extensions.

    """
    return {
        "function_name": result.function_name,
        "positional_arguments": [arg.as_py() for arg in result.positional_arguments],
        "named_arguments": {name: arg.as_py() for name, arg in result.named_arguments.items()},
        "required_extensions": result.required_extensions,
    }


def get_attach_id_from_options(
    client: Client,
    attach_id: str | None,
    catalog: str | None,
    attach_options: dict[str, Any] | None,
) -> tuple[AttachId, bool]:
    """Get attach_id from either explicit --attach-id or auto-attach via --catalog.

    This helper supports two workflows:
    1. Explicit attach_id: Use a pre-obtained attach_id (for stateful catalogs)
    2. Auto-attach: Attach to catalog on-the-fly (for stateless catalogs)

    Args:
        client: VGI Client instance
        attach_id: Hex-encoded attach ID (from --attach-id option)
        catalog: Catalog name (from --catalog option)
        attach_options: Options for catalog attach (from --attach-options option)

    Returns:
        Tuple of (attach_id, is_stateful) where is_stateful indicates if
        a warning should be shown for stateful catalogs using auto-attach.

    Raises:
        click.ClickException: If neither attach_id nor catalog is provided,
            or if both are provided.

    """
    if attach_id and catalog:
        raise click.ClickException(
            "Cannot specify both --attach-id and --catalog. "
            "Use --attach-id for stateful catalogs or --catalog for auto-attach."
        )

    if not attach_id and not catalog:
        raise click.ClickException(
            "Must specify either --attach-id or --catalog. "
            "Use --attach-id with a previously attached catalog, "
            "or --catalog to auto-attach."
        )

    if attach_id:
        return hex_to_attach_id(attach_id), False

    # Auto-attach via --catalog
    assert catalog is not None
    options = attach_options or {}
    result = client.catalog_attach(name=catalog, options=options)

    # Return the attach_id and whether this is a stateful catalog
    # (is_stateful=True means caller should warn about using --catalog
    # with a stateful catalog)
    return result.attach_id, result.attach_id_required
