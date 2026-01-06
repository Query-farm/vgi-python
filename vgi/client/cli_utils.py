"""Shared utilities for VGI CLI commands.

This module provides common utilities used across CLI command groups:
- Hex string conversion for AttachId and TransactionId
- JSON to Arrow schema conversion for table columns
- Output formatting helpers

"""

from __future__ import annotations

import json
from typing import Any

import click
import pyarrow as pa

from vgi.catalog import AttachId, TransactionId

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


def json_to_arrow_schema(columns: list[dict[str, Any]]) -> pa.Schema:
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
            raise click.ClickException(
                f"Column {i} missing 'name' field: {json.dumps(col)}"
            )
        if "type" not in col:
            raise click.ClickException(
                f"Column {i} missing 'type' field: {json.dumps(col)}"
            )

        type_name = col["type"]
        if type_name not in ARROW_TYPE_MAP:
            valid_types = ", ".join(sorted(ARROW_TYPE_MAP.keys()))
            raise click.ClickException(
                f"Unknown type '{type_name}' for column '{col['name']}'. "
                f"Valid types: {valid_types}"
            )

        fields.append(pa.field(col["name"], ARROW_TYPE_MAP[type_name]))

    return pa.schema(fields)


def arrow_schema_to_json(serialized: bytes) -> list[dict[str, str]]:
    """Convert serialized Arrow schema to JSON for display.

    Args:
        serialized: Serialized Arrow schema bytes

    Returns:
        List of column definitions with name and type

    """
    reader = pa.BufferReader(serialized)
    schema = pa.ipc.read_schema(reader)  # type: ignore[arg-type]
    return [{"name": f.name, "type": str(f.type)} for f in schema]


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


def schema_info_to_dict(schema_info: Any) -> dict[str, Any]:
    """Convert SchemaInfo to a dictionary for JSON output.

    Args:
        schema_info: SchemaInfo object from catalog

    Returns:
        Dictionary representation

    """
    return {
        "name": schema_info.name,
        "is_default": schema_info.is_default,
        "comment": schema_info.comment,
        "tags": schema_info.tags,
    }


def table_info_to_dict(table_info: Any) -> dict[str, Any]:
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
        "tags": table_info.tags,
    }


def view_info_to_dict(view_info: Any) -> dict[str, Any]:
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
        "tags": view_info.tags,
    }


def function_info_to_dict(function_info: Any) -> dict[str, Any]:
    """Convert FunctionInfo to a dictionary for JSON output.

    Args:
        function_info: FunctionInfo object from catalog

    Returns:
        Dictionary representation

    """
    return {
        "name": function_info.name,
        "schema_name": function_info.schema_name,
        "function_type": function_info.function_type.value,
        "arguments": arrow_schema_to_json(function_info.arguments),
        "comment": function_info.comment,
        "tags": function_info.tags,
    }


def catalog_attach_result_to_dict(result: Any) -> dict[str, Any]:
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
    }


def scan_function_result_to_dict(result: Any) -> dict[str, Any]:
    """Convert ScanFunctionResult to a dictionary for JSON output.

    Args:
        result: ScanFunctionResult object

    Returns:
        Dictionary representation

    """
    return {
        "function_name": result.function_name,
        "max_processes": result.max_processes,
        "invocation_id": (
            bytes_to_hex(result.invocation_id) if result.invocation_id else None
        ),
    }
