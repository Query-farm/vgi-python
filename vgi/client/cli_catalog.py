# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Catalog CLI commands for VGI.

This module provides CLI commands for catalog operations, organized hierarchically:

catalog
├── list                    # List available catalogs
├── attach <name>           # Attach to a catalog
├── detach <attach_opaque_data>      # Detach from a catalog
├── create <name>           # Create a new catalog
├── drop <name>             # Drop a catalog
├── version                 # Get catalog version (--attach-opaque-data or --catalog)
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
    hex_to_attach_opaque_data,
    optional_transaction_opaque_data,
    output_json,
    parse_json_option,
    resolve_attach,
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
    """Attach to a catalog and return attach_opaque_data.

    NAME is the catalog name to attach to.

    """
    opts = parse_json_option(options, "--options")
    client = Client(worker)
    result = client.catalog_attach(
        name=name,
        options=opts,
        data_version_spec=None,
        implementation_version=None,
    )
    output_json(catalog_attach_result_to_dict(result))


@catalog.command("detach")
@click.argument("attach_opaque_data")
@click.option("--worker", "-w", required=True, help="VGI worker command")
def catalog_detach(attach_opaque_data: str, worker: str) -> None:
    """Detach from a catalog.

    ATTACH_ID is the hex-encoded attach ID from catalog attach.

    """
    client = Client(worker)
    client.catalog_detach(attach_opaque_data=hex_to_attach_opaque_data(attach_opaque_data))
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
@click.option("--attach-opaque-data", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-opaque-data", help="Transaction ID (hex) for transactional read")
def catalog_version(
    attach_opaque_data: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_opaque_data: str | None,
) -> None:
    """Get the current catalog version."""
    client, resolved_attach_opaque_data = resolve_attach(worker, attach_opaque_data, catalog_name, attach_options)
    version = client.catalog_version(
        attach_opaque_data=resolved_attach_opaque_data,
        transaction_opaque_data=(optional_transaction_opaque_data(transaction_opaque_data)),
    )
    output_json({"version": version, "attach_opaque_data": bytes_to_hex(resolved_attach_opaque_data)})


# Add nested subcommand groups
catalog.add_command(schema)
catalog.add_command(table)
catalog.add_command(view)
catalog.add_command(transaction)
