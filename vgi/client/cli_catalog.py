"""Catalog CLI commands for VGI.

This module provides CLI commands for catalog operations, organized hierarchically:

catalog
├── list                    # List available catalogs
├── attach <name>           # Attach to a catalog
├── detach <attach_id>      # Detach from a catalog
├── create <name>           # Create a new catalog
├── drop <name>             # Drop a catalog
├── version                 # Get catalog version (--attach-id or --catalog)
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
    bytes_to_hex,
    catalog_attach_result_to_dict,
    get_attach_id_from_options,
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
@click.option("--worker", "-w", required=True, help="VGI worker command")
def catalog_list(worker: str) -> None:
    """List available catalogs."""
    client = Client(worker)
    catalogs = client.catalogs()
    output_json(
        [
            {
                "name": c.name,
                "implementation_version": c.implementation_version,
                "data_version_spec": c.data_version_spec,
            }
            for c in catalogs
        ]
    )


@catalog.command("attach")
@click.argument("name")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--options", default="{}", help="Catalog options as JSON object")
def catalog_attach(name: str, worker: str, options: str) -> None:
    """Attach to a catalog and return attach_id.

    NAME is the catalog name to attach to.

    """
    opts = parse_json_option(options, "--options")
    client = Client(worker)
    result = client.catalog_attach(name=name, options=opts)
    output_json(catalog_attach_result_to_dict(result))


@catalog.command("detach")
@click.argument("attach_id")
@click.option("--worker", "-w", required=True, help="VGI worker command")
def catalog_detach(attach_id: str, worker: str) -> None:
    """Detach from a catalog.

    ATTACH_ID is the hex-encoded attach ID from catalog attach.

    """
    client = Client(worker)
    client.catalog_detach(attach_id=hex_to_attach_id(attach_id))
    output_json({"status": "detached"})


@catalog.command("create")
@click.argument("name")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option(
    "--on-conflict",
    type=click.Choice(["error", "ignore", "replace"]),
    default="error",
    help="Behavior if catalog already exists",
)
@click.option("--options", default="{}", help="Catalog options as JSON object")
def catalog_create(name: str, worker: str, on_conflict: str, options: str) -> None:
    """Create a new catalog.

    NAME is the name for the new catalog.

    """
    opts = parse_json_option(options, "--options")
    client = Client(worker)
    client.catalog_create(
        name=name,
        on_conflict=OnConflict(on_conflict),
        options=opts,
    )
    output_json({"status": "created", "name": name})


@catalog.command("drop")
@click.argument("name")
@click.option("--worker", "-w", required=True, help="VGI worker command")
def catalog_drop(name: str, worker: str) -> None:
    """Drop a catalog.

    NAME is the name of the catalog to drop.

    """
    client = Client(worker)
    client.catalog_drop(name=name)
    output_json({"status": "dropped", "name": name})


@catalog.command("version")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex) for transactional read")
def catalog_version(
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
) -> None:
    """Get the current catalog version."""
    client = Client(worker)
    opts = parse_json_option(attach_options, "--attach-options")
    resolved_attach_id, is_stateful = get_attach_id_from_options(client, attach_id, catalog_name, opts)
    if is_stateful and catalog_name:
        click.echo(
            "Warning: Using --catalog with a stateful catalog. Consider using --attach-id for session persistence.",
            err=True,
        )
    version = client.catalog_version(
        attach_id=resolved_attach_id,
        transaction_id=(hex_to_transaction_id(transaction_id) if transaction_id else None),
    )
    output_json({"version": version, "attach_id": bytes_to_hex(resolved_attach_id)})


# Add nested subcommand groups
catalog.add_command(schema)
catalog.add_command(table)
catalog.add_command(view)
catalog.add_command(transaction)
