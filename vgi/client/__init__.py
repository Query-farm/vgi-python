"""VGI client package for communicating with VGI workers.

This package provides:
- Client: A class for programmatic interaction with VGI workers, including
  both function invocation and catalog operations
- ClientError: Exception raised by Client function operations
- CatalogClientMixin: Mixin class providing catalog operations
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
    from vgi.client import Client

    client = Client("./my_worker")
    result = client.catalog_attach(
        name="my_catalog", options={}, data_version_spec=None, implementation_version=None
    )

Usage (CLI):
    vgi-client --input data.parquet --function echo
    vgi-client --input data.parquet --function sum_all_columns

"""

from typing import TYPE_CHECKING, Any

from vgi.client.catalog_mixin import CatalogClientMixin
from vgi.client.client import Client, ClientError

if TYPE_CHECKING:
    from vgi.client.cli import OutputWriter, main

__all__ = [
    "CatalogClientMixin",
    "Client",
    "ClientError",
    "OutputWriter",
    "main",
]


# Lazy-load the CLI surface. ``vgi.client.cli`` transitively imports
# ``pyarrow.parquet`` / ``pyarrow._s3fs`` / ``pyarrow._gcsfs`` etc., which add
# ~2 seconds to the cold import path. Programmatic users of ``Client`` don't
# need any of that; only the ``vgi-client`` CLI entry point does.
def __getattr__(name: str) -> Any:
    if name in {"OutputWriter", "main"}:
        from vgi.client import cli

        return getattr(cli, name)
    raise AttributeError(f"module 'vgi.client' has no attribute {name!r}")
