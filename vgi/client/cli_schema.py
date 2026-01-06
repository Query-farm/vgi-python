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

from vgi.catalog import FunctionInfo, TableInfo, ViewInfo
from vgi.client.cli_utils import (
    function_info_to_dict,
    hex_to_attach_id,
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
@click.argument("attach_id")
@click.option("--server", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex) for transactional read")
def schema_list(attach_id: str, server: str, transaction_id: str | None) -> None:
    """List schemas in a catalog.

    ATTACH_ID is the hex-encoded attach ID from catalog attach.

    """
    client = Client(server)
    for schema_info in client.schemas(
        attach_id=hex_to_attach_id(attach_id),
        transaction_id=(
            hex_to_transaction_id(transaction_id) if transaction_id else None
        ),
    ):
        output_json(schema_info_to_dict(schema_info))


@schema.command("get")
@click.argument("attach_id")
@click.argument("name")
@click.option("--server", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex) for transactional read")
def schema_get(
    attach_id: str, name: str, server: str, transaction_id: str | None
) -> None:
    """Get information about a schema.

    ATTACH_ID is the hex-encoded attach ID from catalog attach.
    NAME is the schema name.

    """
    client = Client(server)
    schema_info = client.schema_get(
        attach_id=hex_to_attach_id(attach_id),
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
@click.argument("attach_id")
@click.argument("name")
@click.option("--server", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option("--comment", help="Description of the schema")
@click.option("--tags", default="{}", help="Metadata tags as JSON object")
def schema_create(
    attach_id: str,
    name: str,
    server: str,
    transaction_id: str | None,
    comment: str | None,
    tags: str,
) -> None:
    """Create a new schema.

    ATTACH_ID is the hex-encoded attach ID from catalog attach.
    NAME is the name for the new schema.

    """
    tags_dict = parse_json_option(tags, "--tags")
    client = Client(server)
    client.schema_create(
        attach_id=hex_to_attach_id(attach_id),
        transaction_id=(
            hex_to_transaction_id(transaction_id) if transaction_id else None
        ),
        name=name,
        comment=comment,
        tags=tags_dict,
    )
    output_json({"status": "created", "name": name})


@schema.command("drop")
@click.argument("attach_id")
@click.argument("name")
@click.option("--server", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option("--ignore-not-found", is_flag=True, help="Don't error if not found")
@click.option("--cascade", is_flag=True, help="Drop contained tables and views")
def schema_drop(
    attach_id: str,
    name: str,
    server: str,
    transaction_id: str | None,
    ignore_not_found: bool,
    cascade: bool,
) -> None:
    """Drop a schema.

    ATTACH_ID is the hex-encoded attach ID from catalog attach.
    NAME is the name of the schema to drop.

    """
    client = Client(server)
    client.schema_drop(
        attach_id=hex_to_attach_id(attach_id),
        transaction_id=(
            hex_to_transaction_id(transaction_id) if transaction_id else None
        ),
        name=name,
        ignore_not_found=ignore_not_found,
        cascade=cascade,
    )
    output_json({"status": "dropped", "name": name})


@schema.command("contents")
@click.argument("attach_id")
@click.argument("name")
@click.option("--server", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex) for transactional read")
def schema_contents(
    attach_id: str, name: str, server: str, transaction_id: str | None
) -> None:
    """List contents of a schema (tables, views, functions).

    ATTACH_ID is the hex-encoded attach ID from catalog attach.
    NAME is the schema name.

    """
    client = Client(server)
    for item in client.schema_contents(
        attach_id=hex_to_attach_id(attach_id),
        transaction_id=(
            hex_to_transaction_id(transaction_id) if transaction_id else None
        ),
        name=name,
    ):
        if isinstance(item, TableInfo):
            output_json({"type": "table", **table_info_to_dict(item)})
        elif isinstance(item, ViewInfo):
            output_json({"type": "view", **view_info_to_dict(item)})
        elif isinstance(item, FunctionInfo):
            output_json({"type": "function", **function_info_to_dict(item)})
        else:
            output_json({"type": "unknown", "name": getattr(item, "name", "unknown")})
