"""VGI (Vector Gateway Interface) - Apache Arrow-based protocol for DuckDB extensions.

VGI provides a framework for connecting DuckDB to external programs via
streaming Arrow IPC. User-defined functions run in worker subprocesses
and communicate with the database through stdin/stdout.

Class Hierarchy:
    vgi.function.Function              - Base class (max_processes, invocation_id)
    vgi.table_function.Function        - Adds cardinality hints
    vgi.table_in_out_function.Function - Full streaming (process/finalize)

Key Modules:
    vgi.worker: Base Worker class for hosting functions
    vgi.client: Client for invoking worker functions
    vgi.table_in_out_function: Function base class for streaming functions
    vgi.function: Core data structures (FunctionRequest, FunctionOutputSpec, Arguments)
    vgi.table_function: Extended bind results with cardinality hints

Quick Start:
    See vgi.examples for ready-to-use example functions and workers.
"""

# Re-export commonly used classes for convenient imports
from vgi.function import Arguments, FunctionRequest, LogLevel, LogMessage
from vgi.table_in_out_function import Function, Output, OutputGenerator
from vgi.worker import Worker

__all__ = [
    "Arguments",
    "Function",
    "FunctionRequest",
    "LogLevel",
    "LogMessage",
    "Output",
    "OutputGenerator",
    "Worker",
    "hello",
]


def hello() -> str:
    """Return a greeting string. Used for basic installation verification."""
    return "Hello from vgi-python!"
