"""VGI (Vector Gateway Interface) - Apache Arrow-based protocol for DuckDB extensions.

VGI provides a framework for connecting DuckDB to external programs via
streaming Arrow IPC. User-defined functions run in worker subprocesses
and communicate with the database through stdin/stdout.

Key Modules:
    vgi.worker: Base Worker class for hosting functions
    vgi.client: Client for invoking worker functions
    vgi.table_in_out_function: TableInOutFunction base class for streaming functions
    vgi.function: Core data structures (CallData, BindResult, Arguments)
    vgi.table_function: Extended bind results with cardinality hints

Quick Start:
    See vgi.examples for ready-to-use example functions and workers.
"""


def hello() -> str:
    """Return a greeting string. Used for basic installation verification."""
    return "Hello from vgi-python!"
