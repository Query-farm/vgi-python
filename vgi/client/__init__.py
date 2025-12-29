"""VGI client package for communicating with VGI workers.

This package provides:
- Client: A class for programmatic interaction with VGI workers
- ClientError: Exception raised by Client operations
- OutputWriter: Helper for writing output in various formats
- main: CLI entry point

Usage (API):
    from vgi.client import Client, ClientError
    from vgi.function import Arguments

    with Client("./my_worker.py") as client:
        for batch in client.table_in_out_function(
            function_name="echo",
            arguments=Arguments(positional=[], named={}),
            input=input_batches,
        ):
            process(batch)

Usage (CLI):
    vgi-client --input data.parquet --function echo
    vgi-client --input data.parquet --function sum_all_columns

"""

from vgi.client.cli import OutputWriter, main
from vgi.client.client import Client, ClientError

__all__ = [
    "Client",
    "ClientError",
    "OutputWriter",
    "main",
]
