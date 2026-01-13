"""View CLI commands for VGI.

This module provides CLI commands for view operations:
- get: Get view info
- create: Create a new view
- drop: Drop a view
- rename: Rename a view
- comment: Update view comment

"""

from __future__ import annotations

import click

from vgi.catalog import OnConflict
from vgi.client.cli_utils import (
    get_attach_id_from_options,
    hex_to_transaction_id,
    output_json,
    parse_json_option,
    view_info_to_dict,
)
from vgi.client.client import Client


@click.group()
def view() -> None:
    """Manage views in a catalog."""


@view.command("get")
@click.argument("schema_name")
@click.argument("name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex) for transactional read")
def view_get(
    schema_name: str,
    name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
) -> None:
    """Get information about a view.

    SCHEMA_NAME is the schema containing the view.
    NAME is the view name.

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
    view_info = client.view_get(
        attach_id=resolved_attach_id,
        transaction_id=(
            hex_to_transaction_id(transaction_id) if transaction_id else None
        ),
        schema_name=schema_name,
        name=name,
    )
    if view_info:
        output_json(view_info_to_dict(view_info))
    else:
        output_json({"error": "not_found", "schema": schema_name, "name": name})


@view.command("create")
@click.argument("schema_name")
@click.argument("name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option("--definition", required=True, help="View definition SQL")
@click.option(
    "--on-conflict",
    type=click.Choice(["error", "ignore", "replace"]),
    default="error",
    help="Behavior if view already exists",
)
def view_create(
    schema_name: str,
    name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    definition: str,
    on_conflict: str,
) -> None:
    """Create a new view.

    SCHEMA_NAME is the schema to create the view in.
    NAME is the name for the new view.

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
    client.view_create(
        attach_id=resolved_attach_id,
        transaction_id=(
            hex_to_transaction_id(transaction_id) if transaction_id else None
        ),
        schema_name=schema_name,
        name=name,
        definition=definition,
        on_conflict=OnConflict(on_conflict),
    )
    output_json({"status": "created", "schema": schema_name, "name": name})


@view.command("drop")
@click.argument("schema_name")
@click.argument("name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option("--ignore-not-found", is_flag=True, help="Don't error if not found")
def view_drop(
    schema_name: str,
    name: str,
    attach_id: str | None,
    catalog_name: str | None,
    attach_options: str,
    worker: str,
    transaction_id: str | None,
    ignore_not_found: bool,
) -> None:
    """Drop a view.

    SCHEMA_NAME is the schema containing the view.
    NAME is the name of the view to drop.

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
    client.view_drop(
        attach_id=resolved_attach_id,
        transaction_id=(
            hex_to_transaction_id(transaction_id) if transaction_id else None
        ),
        schema_name=schema_name,
        name=name,
        ignore_not_found=ignore_not_found,
    )
    output_json({"status": "dropped", "schema": schema_name, "name": name})


@view.command("rename")
@click.argument("schema_name")
@click.argument("name")
@click.argument("new_name")
@click.option("--attach-id", help="Hex-encoded attach ID")
@click.option("--catalog", "catalog_name", help="Catalog name for auto-attach")
@click.option("--attach-options", default="{}", help="Attach options as JSON")
@click.option("--worker", "-w", required=True, help="VGI worker command")
@click.option("--transaction-id", help="Transaction ID (hex)")
@click.option("--ignore-not-found", is_flag=True, help="Don't error if not found")
def view_rename(
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
    """Rename a view.

    SCHEMA_NAME is the schema containing the view.
    NAME is the current view name.
    NEW_NAME is the new name for the view.

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
    client.view_rename(
        attach_id=resolved_attach_id,
        transaction_id=(
            hex_to_transaction_id(transaction_id) if transaction_id else None
        ),
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


@view.command("comment")
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
def view_comment(
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
    """Update or clear a view's comment.

    SCHEMA_NAME is the schema containing the view.
    NAME is the view name.

    Use --set to set a comment, --clear to remove it.

    """
    if comment_text is None and not clear:
        raise click.ClickException("Must specify either --set or --clear")
    if comment_text is not None and clear:
        raise click.ClickException("Cannot specify both --set and --clear")

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
    client.view_comment_set(
        attach_id=resolved_attach_id,
        transaction_id=(
            hex_to_transaction_id(transaction_id) if transaction_id else None
        ),
        schema_name=schema_name,
        name=name,
        comment=None if clear else comment_text,
        ignore_not_found=ignore_not_found,
    )
    status = "comment_cleared" if clear else "comment_set"
    output_json({"status": status, "schema": schema_name, "name": name})
