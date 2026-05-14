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
    hex_to_attach_opaque_data,
    hex_to_transaction_opaque_data,
    output_json,
)
from vgi.client.client import Client


@click.group()
def transaction() -> None:
    """Manage transactions in a catalog."""


@transaction.command("begin")
@click.option("--attach-opaque-data", required=True, help="Hex-encoded attach ID")
@click.option("--worker", "-w", required=True, help="VGI worker command")
def transaction_begin(attach_opaque_data: str, worker: str) -> None:
    """Begin a new transaction.

    Returns a transaction_opaque_data that can be used with other catalog operations.

    """
    client = Client(worker)
    tx_id = client.catalog_transaction_begin(attach_opaque_data=hex_to_attach_opaque_data(attach_opaque_data))
    if tx_id is None:
        output_json({"error": "Catalog does not support transactions"})
        return
    output_json(
        {
            "transaction_opaque_data": bytes_to_hex(tx_id),
            "attach_opaque_data": attach_opaque_data,
        }
    )


@transaction.command("commit")
@click.argument("transaction_opaque_data")
@click.option("--attach-opaque-data", required=True, help="Hex-encoded attach ID")
@click.option("--worker", "-w", required=True, help="VGI worker command")
def transaction_commit(transaction_opaque_data: str, attach_opaque_data: str, worker: str) -> None:
    """Commit a transaction.

    TRANSACTION_ID is the hex-encoded transaction ID from transaction begin.

    """
    client = Client(worker)
    client.catalog_transaction_commit(
        attach_opaque_data=hex_to_attach_opaque_data(attach_opaque_data),
        transaction_opaque_data=hex_to_transaction_opaque_data(transaction_opaque_data),
    )
    output_json(
        {
            "status": "committed",
            "transaction_opaque_data": transaction_opaque_data,
            "attach_opaque_data": attach_opaque_data,
        }
    )


@transaction.command("rollback")
@click.argument("transaction_opaque_data")
@click.option("--attach-opaque-data", required=True, help="Hex-encoded attach ID")
@click.option("--worker", "-w", required=True, help="VGI worker command")
def transaction_rollback(transaction_opaque_data: str, attach_opaque_data: str, worker: str) -> None:
    """Rollback a transaction.

    TRANSACTION_ID is the hex-encoded transaction ID from transaction begin.

    """
    client = Client(worker)
    client.catalog_transaction_rollback(
        attach_opaque_data=hex_to_attach_opaque_data(attach_opaque_data),
        transaction_opaque_data=hex_to_transaction_opaque_data(transaction_opaque_data),
    )
    output_json(
        {
            "status": "rolled_back",
            "transaction_opaque_data": transaction_opaque_data,
            "attach_opaque_data": attach_opaque_data,
        }
    )
