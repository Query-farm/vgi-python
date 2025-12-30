"""VGI (Vector Gateway Interface) - Apache Arrow-based protocol for DuckDB extensions.

VGI provides a framework for connecting DuckDB to external programs via
streaming Arrow IPC. User-defined functions run in worker subprocesses
and communicate with the database through stdin/stdout.

QUICK START (Simple API - Recommended)
--------------------------------------
For most use cases, use TableInOutFunction with callback methods:

    from vgi import TableInOutFunction, Invocation
    import pyarrow as pa

    class MyFunction(TableInOutFunction):
        def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
            # Transform each batch here
            return batch

For advanced streaming control, use TableInOutGeneratorFunction with generators:

    from vgi import TableInOutGeneratorFunction, Output, OutputGenerator, Invocation
    import pyarrow as pa

    class MyFunction(TableInOutGeneratorFunction):
        def process(self, batch: pa.RecordBatch) -> OutputGenerator:
            _ = yield None  # Required priming yield
            while True:
                yield Output(batch)  # Your transformation here
                batch = yield None
                if batch is None:
                    break

To create a worker that hosts functions:

    from vgi import Worker

    class MyWorker(Worker):
        registry = {"my_function": MyFunction}

    if __name__ == "__main__":
        MyWorker().run()

PUBLIC API
----------
Classes exported from this module:

    TableInOutFunction - Callback-based API (recommended)
    TableInOutGeneratorFunction       - Generator-based API (advanced)
    Output                   - Output batch from process()/finalize()
    OutputGenerator          - Type alias for process()/finalize()
    Invocation               - Function invocation request
    Arguments                - Positional and named arguments
    Worker                   - Base class for worker processes
    Level                    - Log severity enum
    Message                  - Log message for process()

ADDITIONAL MODULES
------------------
    vgi.client      - Client class for invoking functions on workers
    vgi.log         - Level and Message for function diagnostics
    vgi.ipc_utils   - RecordBatchState for distributed function state
    vgi.table_function - CardinalityInfo for row count hints

CLASS HIERARCHY
---------------
    vgi.function.Function                - Base (max_processes, invocation_id)
    └─ vgi.table_function.TableFunction  - Adds cardinality hints, projection
       └─ TableInOutGeneratorFunction             - Full streaming (process/finalize)
          └─ TableInOutFunction    - Callback API (transform/finish)

Examples
--------
See vgi.examples.table_in_out for example functions:
    - EchoFunction: Passthrough (no-op)
    - BufferInputFunction: Collect all input, emit on finalize
    - RepeatInputsFunction: Duplicate each batch N times
    - SumAllColumnsFunction: Aggregate numeric columns
    - SumAllColumnsFunctionDistributed: Parallel aggregation with state sharing

"""

# Re-export commonly used classes for convenient imports
from vgi.function import Arguments, Invocation
from vgi.log import Level, Message
from vgi.table_in_out_function import (
    Output,
    OutputGenerator,
    TableInOutFunction,
    TableInOutGeneratorFunction,
)
from vgi.worker import Worker

__all__ = [
    "Arguments",
    "Invocation",
    "Level",
    "Message",
    "Output",
    "OutputGenerator",
    "TableInOutGeneratorFunction",
    "TableInOutFunction",
    "Worker",
    "hello",
]


def hello() -> str:
    """Return a greeting string. Used for basic installation verification."""
    return "Hello from vgi-python!"
