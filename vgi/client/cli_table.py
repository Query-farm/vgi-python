"""Table CLI commands for VGI.

This module provides CLI commands for table operations:
- get: Get table info
- create: Create a new table
- drop: Drop a table
- rename: Rename a table
- comment: Set or clear table comment
- scan-function: Get scan function for a table
- column: Column subcommands (add, drop, rename, etc.)

"""

from __future__ import annotations

import click

from vgi.catalog import OnConflict, SerializedSchema, SqlExpression
from vgi.client.cli_utils import (
    get_attach_id_from_options,
    hex_to_transaction_id,
    json_to_arrow_schema,
    output_json,
    parse_json_option,
    scan_function_result_to_dict,
    table_info_to_dict,
)
from vgi.client.client import Client


@click.group()
def table() -> None:
    """Manage tables in a schema."""


@table.command("get")
@click.argument("schema_name")
@click.argument("name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex) for transactional read")
def table_get(
    schema_name: str,
    name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
) -> None:
    """Get information about a table.

    SCHEMA_NAME is the schema containing the table.
    NAME is the table name.

    """
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(client, attach_id, catalog_name, opts)
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. Consider using --attach-id for session persistence.",
            err=True,
        )
    table_info = client.table_get(
        attach_id=resolved_attach_id,
        transaction_id=(hex_to_transaction_id(transaction_id) if transaction_id else None),
        schema_name=schema_name,
        name=name,
    )
    if table_info:
        output_json(table_info_to_dict(table_info))
    else:
        output_json({"error": "not_found", "schema": schema_name, "name": name})


@table.command("create")
@click.argument("schema_name")
@click.argument("name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option(
    "--columns",
    required=True,
    help='Column definitions as JSON array: [{"name":"id","type":"int64"}]',
)
@click.option(
    "--on-conflict",
    type=click.Choice(["error", "ignore", "replace"]),
    default="error",
    help="Behavior if table already exists",
)
@click.option(
    "--not-null",
    multiple=True,
    type=int,
    help="Column index with NOT NULL constraint (can repeat)",
)
@click.option(
    "--unique",
    multiple=True,
    help="Column indices for unique constraint as comma-separated list (can repeat)",
)
@click.option("--check", multiple=True, help="SQL check constraint (can repeat)")
def table_create(
    schema_name: str,
    name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    columns: str,
    on_conflict: str,
    not_null: tuple[int, ...],
    unique: tuple[str, ...],
    check: tuple[str, ...],
) -> None:
    """Create a new table.

    SCHEMA_NAME is the schema to create the table in.
    NAME is the name for the new table.

    """
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(client, attach_id, catalog_name, opts)
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. Consider using --attach-id for session persistence.",
            err=True,
        )

    columns_json = parse_json_option(columns, "--columns")
    arrow_schema = json_to_arrow_schema(columns_json)

    # Parse unique constraints: each is a comma-separated list of column indices
    unique_constraints = []
    for u in unique:
        indices = [int(i.strip()) for i in u.split(",")]
        unique_constraints.append(indices)

    client.table_create(
        attach_id=resolved_attach_id,
        transaction_id=(hex_to_transaction_id(transaction_id) if transaction_id else None),
        schema_name=schema_name,
        name=name,
        columns=SerializedSchema(arrow_schema.serialize().to_pybytes()),
        on_conflict=OnConflict(on_conflict),
        not_null_constraints=list(not_null),
        unique_constraints=unique_constraints,
        check_constraints=list(check),
    )
    output_json({"status": "created", "schema": schema_name, "name": name})


@table.command("drop")
@click.argument("schema_name")
@click.argument("name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option("--ignore-not-found", is_flag=True, help="Don't error if not found")
def table_drop(
    schema_name: str,
    name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    ignore_not_found: bool,
) -> None:
    """Drop a table.

    SCHEMA_NAME is the schema containing the table.
    NAME is the table name to drop.

    """
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(client, attach_id, catalog_name, opts)
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. Consider using --attach-id for session persistence.",
            err=True,
        )
    client.table_drop(
        attach_id=resolved_attach_id,
        transaction_id=(hex_to_transaction_id(transaction_id) if transaction_id else None),
        schema_name=schema_name,
        name=name,
        ignore_not_found=ignore_not_found,
    )
    output_json({"status": "dropped", "schema": schema_name, "name": name})


@table.command("rename")
@click.argument("schema_name")
@click.argument("name")
@click.argument("new_name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option("--ignore-not-found", is_flag=True, help="Don't error if not found")
def table_rename(
    schema_name: str,
    name: str,
    new_name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    ignore_not_found: bool,
) -> None:
    """Rename a table.

    SCHEMA_NAME is the schema containing the table.
    NAME is the current table name.
    NEW_NAME is the new name for the table.

    """
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(client, attach_id, catalog_name, opts)
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. Consider using --attach-id for session persistence.",
            err=True,
        )
    client.table_rename(
        attach_id=resolved_attach_id,
        transaction_id=(hex_to_transaction_id(transaction_id) if transaction_id else None),
        schema_name=schema_name,
        name=name,
        new_name=new_name,
        ignore_not_found=ignore_not_found,
    )
    output_json(
        {
            "status": "renamed",
            "schema": schema_name,
            "old": name,
            "new": new_name,
        }
    )


@table.command("comment")
@click.argument("schema_name")
@click.argument("name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option("--set", "comment_text", help="Set comment to this text")
@click.option("--clear", is_flag=True, help="Clear the comment")
@click.option("--ignore-not-found", is_flag=True, help="Don't error if not found")
def table_comment(
    schema_name: str,
    name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    comment_text: str | None,
    clear: bool,
    ignore_not_found: bool,
) -> None:
    """Set or clear a table's comment.

    SCHEMA_NAME is the schema containing the table.
    NAME is the table name.

    Use --set to set the comment, or --clear to remove it.

    """
    if not comment_text and not clear:
        raise click.ClickException("Must specify --set or --clear")
    if comment_text and clear:
        raise click.ClickException("Cannot specify both --set and --clear")

    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(client, attach_id, catalog_name, opts)
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. Consider using --attach-id for session persistence.",
            err=True,
        )
    client.table_comment_set(
        attach_id=resolved_attach_id,
        transaction_id=(hex_to_transaction_id(transaction_id) if transaction_id else None),
        schema_name=schema_name,
        name=name,
        comment=None if clear else comment_text,
        ignore_not_found=ignore_not_found,
    )
    action = "cleared" if clear else "set"
    output_json({"status": f"comment_{action}", "schema": schema_name, "name": name})


@table.command("scan-function")
@click.argument("schema_name")
@click.argument("name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex) for transactional read")
@click.option("--at-unit", help="Time travel unit (e.g., 'timestamp', 'version')")
@click.option("--at-value", help="Time travel value")
def table_scan_function(
    schema_name: str,
    name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    at_unit: str | None,
    at_value: str | None,
) -> None:
    """Get the scan function for a table.

    SCHEMA_NAME is the schema containing the table.
    NAME is the table name.

    """
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(client, attach_id, catalog_name, opts)
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. Consider using --attach-id for session persistence.",
            err=True,
        )
    result = client.table_scan_function_get(
        attach_id=resolved_attach_id,
        transaction_id=(hex_to_transaction_id(transaction_id) if transaction_id else None),
        schema_name=schema_name,
        name=name,
        at_unit=at_unit,
        at_value=at_value,
    )
    output_json(scan_function_result_to_dict(result))


# Column subcommands
@table.group("column")
def column() -> None:
    """Manage table columns."""


@column.command("add")
@click.argument("schema_name")
@click.argument("table_name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option(
    "--column",
    "column_def",
    required=True,
    help='Column definition as JSON: {"name":"col","type":"int64"}',
)
@click.option("--ignore-not-found", is_flag=True, help="Don't error if table not found")
@click.option("--if-not-exists", is_flag=True, help="Don't error if column already exists")
def column_add(
    schema_name: str,
    table_name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    column_def: str,
    ignore_not_found: bool,
    if_not_exists: bool,
) -> None:
    """Add a column to a table.

    SCHEMA_NAME is the schema containing the table.
    TABLE_NAME is the table to add the column to.

    """
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(client, attach_id, catalog_name, opts)
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. Consider using --attach-id for session persistence.",
            err=True,
        )

    col_json = parse_json_option(column_def, "--column")
    arrow_schema = json_to_arrow_schema([col_json])

    client.table_column_add(
        attach_id=resolved_attach_id,
        transaction_id=(hex_to_transaction_id(transaction_id) if transaction_id else None),
        schema_name=schema_name,
        name=table_name,
        column_definition=SerializedSchema(arrow_schema.serialize().to_pybytes()),
        ignore_not_found=ignore_not_found,
        if_column_not_exists=if_not_exists,
    )
    output_json(
        {
            "status": "column_added",
            "schema": schema_name,
            "table": table_name,
            "column": col_json["name"],
        }
    )


@column.command("drop")
@click.argument("schema_name")
@click.argument("table_name")
@click.argument("column_name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option("--ignore-not-found", is_flag=True, help="Don't error if table not found")
@click.option("--if-exists", is_flag=True, help="Don't error if column doesn't exist")
@click.option("--cascade", is_flag=True, help="Drop dependent constraints")
def column_drop(
    schema_name: str,
    table_name: str,
    column_name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    ignore_not_found: bool,
    if_exists: bool,
    cascade: bool,
) -> None:
    """Drop a column from a table.

    SCHEMA_NAME is the schema containing the table.
    TABLE_NAME is the table to drop the column from.
    COLUMN_NAME is the column to drop.

    """
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(client, attach_id, catalog_name, opts)
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. Consider using --attach-id for session persistence.",
            err=True,
        )
    client.table_column_drop(
        attach_id=resolved_attach_id,
        transaction_id=(hex_to_transaction_id(transaction_id) if transaction_id else None),
        schema_name=schema_name,
        name=table_name,
        column_name=column_name,
        ignore_not_found=ignore_not_found,
        if_column_exists=if_exists,
        cascade=cascade,
    )
    output_json(
        {
            "status": "column_dropped",
            "schema": schema_name,
            "table": table_name,
            "column": column_name,
        }
    )


@column.command("rename")
@click.argument("schema_name")
@click.argument("table_name")
@click.argument("column_name")
@click.argument("new_column_name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option("--ignore-not-found", is_flag=True, help="Don't error if table not found")
def column_rename(
    schema_name: str,
    table_name: str,
    column_name: str,
    new_column_name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    ignore_not_found: bool,
) -> None:
    """Rename a column.

    SCHEMA_NAME is the schema containing the table.
    TABLE_NAME is the table containing the column.
    COLUMN_NAME is the current column name.
    NEW_COLUMN_NAME is the new name for the column.

    """
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(client, attach_id, catalog_name, opts)
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. Consider using --attach-id for session persistence.",
            err=True,
        )
    client.table_column_rename(
        attach_id=resolved_attach_id,
        transaction_id=(hex_to_transaction_id(transaction_id) if transaction_id else None),
        schema_name=schema_name,
        name=table_name,
        column_name=column_name,
        new_column_name=new_column_name,
        ignore_not_found=ignore_not_found,
    )
    output_json(
        {
            "status": "column_renamed",
            "schema": schema_name,
            "table": table_name,
            "old_column": column_name,
            "new_column": new_column_name,
        }
    )


@column.command("set-default")
@click.argument("schema_name")
@click.argument("table_name")
@click.argument("column_name")
@click.argument("expression")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option("--ignore-not-found", is_flag=True, help="Don't error if table not found")
def column_set_default(
    schema_name: str,
    table_name: str,
    column_name: str,
    expression: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    ignore_not_found: bool,
) -> None:
    """Set the default value for a column.

    SCHEMA_NAME is the schema containing the table.
    TABLE_NAME is the table containing the column.
    COLUMN_NAME is the column to set the default for.
    EXPRESSION is the SQL expression for the default value.

    """
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(client, attach_id, catalog_name, opts)
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. Consider using --attach-id for session persistence.",
            err=True,
        )
    client.table_column_default_set(
        attach_id=resolved_attach_id,
        transaction_id=(hex_to_transaction_id(transaction_id) if transaction_id else None),
        schema_name=schema_name,
        name=table_name,
        column_name=column_name,
        expression=SqlExpression(expression),
        ignore_not_found=ignore_not_found,
    )
    output_json(
        {
            "status": "default_set",
            "schema": schema_name,
            "table": table_name,
            "column": column_name,
        }
    )


@column.command("drop-default")
@click.argument("schema_name")
@click.argument("table_name")
@click.argument("column_name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option("--ignore-not-found", is_flag=True, help="Don't error if table not found")
def column_drop_default(
    schema_name: str,
    table_name: str,
    column_name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    ignore_not_found: bool,
) -> None:
    """Remove the default value from a column.

    SCHEMA_NAME is the schema containing the table.
    TABLE_NAME is the table containing the column.
    COLUMN_NAME is the column to remove the default from.

    """
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(client, attach_id, catalog_name, opts)
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. Consider using --attach-id for session persistence.",
            err=True,
        )
    client.table_column_default_drop(
        attach_id=resolved_attach_id,
        transaction_id=(hex_to_transaction_id(transaction_id) if transaction_id else None),
        schema_name=schema_name,
        name=table_name,
        column_name=column_name,
        ignore_not_found=ignore_not_found,
    )
    output_json(
        {
            "status": "default_dropped",
            "schema": schema_name,
            "table": table_name,
            "column": column_name,
        }
    )


@column.command("set-type")
@click.argument("schema_name")
@click.argument("table_name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option(
    "--column",
    "column_def",
    required=True,
    help='Column definition as JSON: {"name":"col","type":"string"}',
)
@click.option("--using", "expression", help="SQL expression to convert values")
@click.option("--ignore-not-found", is_flag=True, help="Don't error if table not found")
def column_set_type(
    schema_name: str,
    table_name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    column_def: str,
    expression: str | None,
    ignore_not_found: bool,
) -> None:
    """Change the type of a column.

    SCHEMA_NAME is the schema containing the table.
    TABLE_NAME is the table containing the column.

    The --column option specifies the column name and new type.

    """
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(client, attach_id, catalog_name, opts)
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. Consider using --attach-id for session persistence.",
            err=True,
        )

    col_json = parse_json_option(column_def, "--column")
    arrow_schema = json_to_arrow_schema([col_json])

    client.table_column_type_change(
        attach_id=resolved_attach_id,
        transaction_id=(hex_to_transaction_id(transaction_id) if transaction_id else None),
        schema_name=schema_name,
        name=table_name,
        column_definition=SerializedSchema(arrow_schema.serialize().to_pybytes()),
        expression=SqlExpression(expression) if expression else None,
        ignore_not_found=ignore_not_found,
    )
    output_json(
        {
            "status": "type_changed",
            "schema": schema_name,
            "table": table_name,
            "column": col_json["name"],
        }
    )


@column.command("set-not-null")
@click.argument("schema_name")
@click.argument("table_name")
@click.argument("column_name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option("--ignore-not-found", is_flag=True, help="Don't error if table not found")
def column_set_not_null(
    schema_name: str,
    table_name: str,
    column_name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    ignore_not_found: bool,
) -> None:
    """Add NOT NULL constraint to a column.

    SCHEMA_NAME is the schema containing the table.
    TABLE_NAME is the table containing the column.
    COLUMN_NAME is the column to add NOT NULL to.

    """
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(client, attach_id, catalog_name, opts)
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. Consider using --attach-id for session persistence.",
            err=True,
        )
    client.table_not_null_set(
        attach_id=resolved_attach_id,
        transaction_id=(hex_to_transaction_id(transaction_id) if transaction_id else None),
        schema_name=schema_name,
        name=table_name,
        column_name=column_name,
        ignore_not_found=ignore_not_found,
    )
    output_json(
        {
            "status": "not_null_set",
            "schema": schema_name,
            "table": table_name,
            "column": column_name,
        }
    )


@column.command("drop-not-null")
@click.argument("schema_name")
@click.argument("table_name")
@click.argument("column_name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option("--ignore-not-found", is_flag=True, help="Don't error if table not found")
def column_drop_not_null(
    schema_name: str,
    table_name: str,
    column_name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    ignore_not_found: bool,
) -> None:
    """Remove NOT NULL constraint from a column.

    SCHEMA_NAME is the schema containing the table.
    TABLE_NAME is the table containing the column.
    COLUMN_NAME is the column to remove NOT NULL from.

    """
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(client, attach_id, catalog_name, opts)
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. Consider using --attach-id for session persistence.",
            err=True,
        )
    client.table_not_null_drop(
        attach_id=resolved_attach_id,
        transaction_id=(hex_to_transaction_id(transaction_id) if transaction_id else None),
        schema_name=schema_name,
        name=table_name,
        column_name=column_name,
        ignore_not_found=ignore_not_found,
    )
    output_json(
        {
            "status": "not_null_dropped",
            "schema": schema_name,
            "table": table_name,
            "column": column_name,
        }
    )
