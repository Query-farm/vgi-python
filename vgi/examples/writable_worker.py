"""Writable worker with transactional INSERT, UPDATE, DELETE support.

This worker exposes writable tables backed by a db-transactor subprocess.
It supports transactions — scan and write workers share the same DuckDB
transaction through the transactor.

Usage::

    vgi-writable-worker

Tables:
    writable_data — simple two-column table (id, name)
    writable_products — table with defaults, constraints, server-side modification
"""

from __future__ import annotations

import uuid

from vgi.catalog import (
    AttachId,
    Catalog,
    ReadOnlyCatalogInterface,
    Schema,
    Sql,
    Table,
    TransactionId,
)
from vgi.examples.writable_table import (
    WritableProductsInsert,
    WritableProductsScan,
    WritableTableDelete,
    WritableTableInsert,
    WritableTableScan,
    WritableTableUpdate,
    transactor_proxy,
)
from vgi.worker import Worker

_WRITABLE_CATALOG = Catalog(
    name="writable",
    default_schema="main",
    schemas=[
        Schema(
            name="main",
            comment="Writable tables backed by db-transactor",
            functions=[
                # Scan functions registered here for projection_pushdown metadata
                WritableTableScan,
                WritableProductsScan,
            ],
            tables=[
                Table(
                    name="writable_data",
                    function=WritableTableScan,
                    insert_function=WritableTableInsert,
                    update_function=WritableTableUpdate,
                    delete_function=WritableTableDelete,
                    comment="Simple writable table (id, name)",
                ),
                Table(
                    name="writable_products",
                    function=WritableProductsScan,
                    insert_function=WritableProductsInsert,
                    primary_key=(("product_id",),),
                    not_null=("product_id", "name"),
                    check=("price >= 0",),
                    defaults={
                        "price": 0.0,
                        "status": "draft",
                        "created_at": Sql("'server-assigned'"),
                    },
                    comment="Writable products with defaults, constraints, server-side modification",
                ),
            ],
        ),
    ],
)


class WritableCatalog(ReadOnlyCatalogInterface):
    """Catalog interface with transaction support for writable tables.

    Transactions are managed by the db-transactor subprocess. The transactor
    owns the single DuckDB connection and serializes all operations.
    """

    catalog = _WRITABLE_CATALOG
    _FIXED_ATTACH_ID = AttachId(b"writable-catalog-")
    supports_transactions = True

    def catalog_transaction_begin(self, *, attach_id: AttachId) -> TransactionId | None:
        """Begin a transaction via the transactor."""
        tx_id = TransactionId(uuid.uuid4().bytes)
        proxy = transactor_proxy._get_proxy()
        proxy.begin(tx_id=tx_id)
        return tx_id

    def catalog_transaction_commit(self, *, attach_id: AttachId, transaction_id: TransactionId) -> None:
        """Commit a transaction via the transactor."""
        proxy = transactor_proxy._get_proxy()
        proxy.commit(tx_id=transaction_id)

    def catalog_transaction_rollback(self, *, attach_id: AttachId, transaction_id: TransactionId) -> None:
        """Rollback a transaction via the transactor."""
        proxy = transactor_proxy._get_proxy()
        proxy.rollback(tx_id=transaction_id)


class WritableWorker(Worker):
    """Worker with transactional writable tables.

    Exposes writable_data and writable_products tables via the WritableCatalog.
    """

    catalog_interface = WritableCatalog
    catalog = _WRITABLE_CATALOG


def main() -> None:
    """Run the writable worker process."""
    WritableWorker.main()


if __name__ == "__main__":
    main()
