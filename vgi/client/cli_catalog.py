"""Catalog CLI commands for VGI.

This module provides CLI commands for catalog operations, organized hierarchically:

catalog
├── list                    # List available catalogs
├── attach <name>           # Attach to a catalog
├── detach <attach_id>      # Detach from a catalog
├── create <name>           # Create a new catalog
├── drop <name>             # Drop a catalog
├── version <attach_id>     # Get catalog version
├── schema                  # Schema operations
│   ├── list/get/create/drop/contents
├── table                   # Table operations
│   ├── get/create/drop/rename/...
├── view                    # View operations
│   ├── get/create/drop/rename/...
└── transaction             # Transaction operations
    ├── begin/commit/rollback

"""

from __future__ import annotations

import click

from vgi.catalog import OnConflict
from vgi.client.cli_schema import schema
from vgi.client.cli_table import table
from vgi.client.cli_transaction import transaction
from vgi.client.cli_utils import (
    catalog_attach_result_to_dict,
    hex_to_attach_id,
    hex_to_transaction_id,
    output_json,
    parse_json_option,
)
from vgi.client.cli_view import view
from vgi.client.client import Client


@click.group()
def catalog() -> None:
    """Manage catalogs, schemas, tables, views, and transactions."""


@catalog.command("list")
@click.option("--server", required=True, help="VGI worker command")
def catalog_list(server: str) -> None:
    """List available catalogs."""
    client = Client(server)
    catalogs = client.catalogs()
    output_json(catalogs)


@catalog.command("attach")
@click.argument("name")
@click.option("--server", required=True, help="VGI worker command")
@click.option("--options", default="{}", help="Catalog options as JSON object")
def catalog_attach(name: str, server: str, options: str) -> None:
    """Attach to a catalog and return attach_id.

    NAME is the catalog name to attach to.

    """
    opts = parse_json_option(options, "--options")
    client = Client(server)
    result = client.catalog_attach(name=name, options=opts)
    output_json(catalog_attach_result_to_dict(result))


@catalog.command("detach")
@click.argument("attach_id")
@click.option("--server", required=True, help="VGI worker command")
def catalog_detach(attach_id: str, server: str) -> None:
    """Detach from a catalog.

    ATTACH_ID is the hex-encoded attach ID from catalog attach.

    """
    client = Client(server)
    client.catalog_detach(attach_id=hex_to_attach_id(attach_id))
    output_json({"status": "detached"})


@catalog.command("create")
@click.argument("name")
@click.option("--server", required=True, help="VGI worker command")
@click.option(
    "--on-conflict",
    type=click.Choice(["error", "ignore", "replace"]),
    default="error",
    help="Behavior if catalog already exists",
)
@click.option("--options", default="{}", help="Catalog options as JSON object")
def catalog_create(name: str, server: str, on_conflict: str, options: str) -> None:
    """Create a new catalog.

    NAME is the name for the new catalog.

    """
    opts = parse_json_option(options, "--options")
    client = Client(server)
    client.catalog_create(
        name=name,
        on_conflict=OnConflict(on_conflict),
        options=opts,
    )
    output_json({"status": "created", "name": name})


@catalog.command("drop")
@click.argument("name")
@click.option("--server", required=True, help="VGI worker command")
def catalog_drop(name: str, server: str) -> None:
    """Drop a catalog.

    NAME is the name of the catalog to drop.

    """
    client = Client(server)
    client.catalog_drop(name=name)
    output_json({"status": "dropped", "name": name})


@catalog.command("version")
@click.argument("attach_id")
@click.option("--server", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex) for transactional read")
def catalog_version(attach_id: str, server: str, transaction_id: str | None) -> None:
    """Get the current catalog version.

    ATTACH_ID is the hex-encoded attach ID from catalog attach.

    """
    client = Client(server)
    version = client.catalog_version(
        attach_id=hex_to_attach_id(attach_id),
        transaction_id=(
            hex_to_transaction_id(transaction_id) if transaction_id else None
        ),
    )
    output_json({"version": version, "attach_id": attach_id})


# Add nested subcommand groups
catalog.add_command(schema)
catalog.add_command(table)
catalog.add_command(view)
catalog.add_command(transaction)
