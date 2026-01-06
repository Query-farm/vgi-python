"""VGI client package for communicating with VGI workers.

This package provides:
- Client: A class for programmatic interaction with VGI workers
- ClientError: Exception raised by Client operations
- CatalogClient: A class for catalog operations on VGI workers
- CatalogClientError: Exception raised by CatalogClient operations
- OutputWriter: Helper for writing output in various formats
- main: CLI entry point

Usage (API):
    from vgi.client import Client, ClientError
    from vgi.arguments import Arguments

    with Client("./my_worker.py") as client:
        for batch in client.table_in_out_function(
            function_name="echo",
            arguments=Arguments(positional=[], named={}),
            input=input_batches,
        ):
            process(batch)

Usage (Catalog API):
    from vgi.client import CatalogClient

    client = CatalogClient("./my_worker")
    result = client.catalog_attach(name="my_catalog", options={})

Usage (CLI):
    vgi-client --input data.parquet --function echo
    vgi-client --input data.parquet --function sum_all_columns

"""

from vgi.client.catalog_client import CatalogClient, CatalogClientError
from vgi.client.cli import OutputWriter, main
from vgi.client.client import Client, ClientError

__all__ = [
    "CatalogClient",
    "CatalogClientError",
    "Client",
    "ClientError",
    "OutputWriter",
    "main",
]
