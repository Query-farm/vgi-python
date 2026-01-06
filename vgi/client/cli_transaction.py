"""Transaction CLI commands for VGI.

This module provides CLI commands for transaction operations:
- begin: Begin a new transaction
- commit: Commit a transaction
- rollback: Rollback a transaction

"""

from __future__ import annotations

import click

from vgi.client.cli_utils import (
    bytes_to_hex,
    hex_to_attach_id,
    hex_to_transaction_id,
    output_json,
)
from vgi.client.client import Client


@click.group()
def transaction() -> None:
    """Manage transactions in a catalog."""


@transaction.command("begin")
@click.argument("attach_id")
@click.option("--server", required=True, help="VGI worker command")
def transaction_begin(attach_id: str, server: str) -> None:
    """Begin a new transaction.

    ATTACH_ID is the hex-encoded attach ID from catalog attach.

    Returns a transaction_id that can be used with other catalog operations.

    """
    client = Client(server)
    result = client.transaction_begin(attach_id=hex_to_attach_id(attach_id))
    output_json(
        {
            "transaction_id": bytes_to_hex(result.transaction_id),
            "attach_id": attach_id,
        }
    )


@transaction.command("commit")
@click.argument("attach_id")
@click.argument("transaction_id")
@click.option("--server", required=True, help="VGI worker command")
def transaction_commit(attach_id: str, transaction_id: str, server: str) -> None:
    """Commit a transaction.

    ATTACH_ID is the hex-encoded attach ID from catalog attach.
    TRANSACTION_ID is the hex-encoded transaction ID from transaction begin.

    """
    client = Client(server)
    client.transaction_commit(
        attach_id=hex_to_attach_id(attach_id),
        transaction_id=hex_to_transaction_id(transaction_id),
    )
    output_json(
        {
            "status": "committed",
            "transaction_id": transaction_id,
            "attach_id": attach_id,
        }
    )


@transaction.command("rollback")
@click.argument("attach_id")
@click.argument("transaction_id")
@click.option("--server", required=True, help="VGI worker command")
def transaction_rollback(attach_id: str, transaction_id: str, server: str) -> None:
    """Rollback a transaction.

    ATTACH_ID is the hex-encoded attach ID from catalog attach.
    TRANSACTION_ID is the hex-encoded transaction ID from transaction begin.

    """
    client = Client(server)
    client.transaction_rollback(
        attach_id=hex_to_attach_id(attach_id),
        transaction_id=hex_to_transaction_id(transaction_id),
    )
    output_json(
        {
            "status": "rolled_back",
            "transaction_id": transaction_id,
            "attach_id": attach_id,
        }
    )
