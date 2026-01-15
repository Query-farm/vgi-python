"""Schema CLI commands for VGI.

This module provides CLI commands for schema operations:
- list: List schemas in a catalog
- get: Get schema info
- create: Create a new schema
- drop: Drop a schema
- contents: List contents of a schema (tables, views, functions)

"""

from __future__ import annotations

import click

from vgi.catalog import FunctionInfo, SchemaObjectType, TableInfo, ViewInfo
from vgi.client.cli_utils import (
    function_info_to_dict,
    get_attach_id_from_options,
    hex_to_transaction_id,
    output_json,
    parse_json_option,
    schema_info_to_dict,
    table_info_to_dict,
    view_info_to_dict,
)
from vgi.client.client import Client


@click.group()
def schema() -> None:
    """Manage schemas in a catalog."""


@schema.command("list")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex) for transactional read")
def schema_list(
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
) -> None:
    """List schemas in a catalog."""
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(
        client, attach_id, catalog_name, opts
    )
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. "
            "Consider using --attach-id for session persistence.",
            err=True,
        )
    for schema_info in client.schemas(
        attach_id=resolved_attach_id,
        transaction_id=(
            hex_to_transaction_id(transaction_id) if transaction_id else None
        ),
    ):
        output_json(schema_info_to_dict(schema_info))


@schema.command("get")
@click.argument("name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex) for transactional read")
def schema_get(
    name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
) -> None:
    """Get information about a schema.

    NAME is the schema name.

    """
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(
        client, attach_id, catalog_name, opts
    )
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. "
            "Consider using --attach-id for session persistence.",
            err=True,
        )
    schema_info = client.schema_get(
        attach_id=resolved_attach_id,
        transaction_id=(
            hex_to_transaction_id(transaction_id) if transaction_id else None
        ),
        name=name,
    )
    if schema_info:
        output_json(schema_info_to_dict(schema_info))
    else:
        output_json({"error": "not_found", "name": name})


@schema.command("create")
@click.argument("name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option("--comment", help="Description of the schema")
@click.option("--tags", default="{}", help="Metadata tags as JSON object")
def schema_create(
    name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    comment: str | None,
    tags: str,
) -> None:
    """Create a new schema.

    NAME is the name for the new schema.

    """
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(
        client, attach_id, catalog_name, opts
    )
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. "
            "Consider using --attach-id for session persistence.",
            err=True,
        )
    tags_dict = parse_json_option(tags, "--tags")
    client.schema_create(
        attach_id=resolved_attach_id,
        transaction_id=(
            hex_to_transaction_id(transaction_id) if transaction_id else None
        ),
        name=name,
        comment=comment,
        tags=tags_dict,
    )
    output_json({"status": "created", "name": name})


@schema.command("drop")
@click.argument("name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option("--ignore-not-found", is_flag=True, help="Don't error if not found")
@click.option("--cascade", is_flag=True, help="Drop contained tables and views")
def schema_drop(
    name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    ignore_not_found: bool,
    cascade: bool,
) -> None:
    """Drop a schema.

    NAME is the name of the schema to drop.

    """
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(
        client, attach_id, catalog_name, opts
    )
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. "
            "Consider using --attach-id for session persistence.",
            err=True,
        )
    client.schema_drop(
        attach_id=resolved_attach_id,
        transaction_id=(
            hex_to_transaction_id(transaction_id) if transaction_id else None
        ),
        name=name,
        ignore_not_found=ignore_not_found,
        cascade=cascade,
    )
    output_json({"status": "dropped", "name": name})


@schema.command("contents")
@click.argument("name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex) for transactional read")
@click.option(
    "--type",
    "object_type",
    type=click.Choice(["table", "view", "scalar_function", "table_function"]),
    help="Filter by object type",
)
def schema_contents(
    name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    object_type: str | None,
) -> None:
    """List contents of a schema (tables, views, functions).

    NAME is the schema name.

    """
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(
        client, attach_id, catalog_name, opts
    )
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. "
            "Consider using --attach-id for session persistence.",
            err=True,
        )

    # Convert string type to SchemaObjectType enum
    type_filter = SchemaObjectType(object_type) if object_type else None

    for item in client.schema_contents(
        attach_id=resolved_attach_id,
        transaction_id=(
            hex_to_transaction_id(transaction_id) if transaction_id else None
        ),
        name=name,
        type=type_filter,
    ):
        if isinstance(item, TableInfo):
            output_json({"type": "table", **table_info_to_dict(item)})
        elif isinstance(item, ViewInfo):
            output_json({"type": "view", **view_info_to_dict(item)})
        elif isinstance(item, FunctionInfo):
            output_json({"type": "function", **function_info_to_dict(item)})
        else:
            output_json({"type": "unknown", "name": getattr(item, "name", "unknown")})
