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

For advanced streaming control, use TableInOutGeneratorFunction with the
@streaming decorator:

    from vgi import TableInOutGeneratorFunction, Output, StreamingGenerator, streaming
    import pyarrow as pa

    class MyFunction(TableInOutGeneratorFunction):
        @streaming
        def process(self, batch: pa.RecordBatch) -> StreamingGenerator:
            # No priming yield needed!
            while batch is not None:
                batch = yield Output(batch)  # Your transformation here

Or without the decorator (more verbose):

    from vgi import TableInOutGeneratorFunction, Output, OutputGenerator
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
        functions = [MyFunction]

    if __name__ == "__main__":
        MyWorker().run()

PUBLIC API
----------
Classes and functions exported from this module:

    TableInOutFunction       - Callback-based API (recommended)
    TableInOutGeneratorFunction - Generator-based API (advanced)
    ScalarFunction           - Scalar function with compute() (single-column output)
    ScalarFunctionGenerator  - Scalar function with generator protocol
    Output                   - Output batch from process()/finalize()
    OutputGenerator          - Type alias for process()/finalize()
    StreamingGenerator       - Type alias for @streaming decorated methods
    streaming                - Decorator to simplify generator methods
    Invocation               - Function invocation request
    Arguments                - Positional and named arguments
    Arg                      - Descriptor for declarative argument parsing
    Worker                   - Base class for worker processes
    Level                    - Log severity enum
    Message                  - Log message for process()
    FunctionTestClient       - In-process test client
    schema                   - Build schemas from keyword arguments
    schema_like              - Derive schemas with modifications

FUNCTION METADATA
-----------------
Functions can define a nested Meta class for introspection and registration:

    class MyFunction(TableInOutFunction):
        class Meta:
            name = "my_func"
            description = "Transform data"
            max_workers = 4
            categories = ["transform"]

        count = Arg[int](0, doc="Iteration count")

    # Access metadata
    meta = MyFunction.get_metadata()
    print(meta.name, meta.parameters)

See vgi.metadata for complete documentation on available Meta attributes.

SPECIALIZED PATTERNS
--------------------
For common use cases, use these specialized base classes:

    AggregationFunction - Reduce input to summary (sum, count, mean, etc.)
    FilterFunction      - Filter rows by boolean predicate
    MapFunction         - Transform columns row-by-row

ADDITIONAL MODULES
------------------
    vgi.client      - Client class for invoking functions on workers
    vgi.log         - Level and Message for function diagnostics
    vgi.ipc_utils   - RecordBatchState for distributed function state
    vgi.table_function - TableCardinality for row count hints

CLASS HIERARCHY
---------------
    vgi.function.Function                - Base (max_processes, invocation_id)
    ├─ vgi.table_function.TableFunctionBase - Adds cardinality hints, projection
    │  ├─ TableFunctionGenerator        - Generate output without input
    │  └─ TableInOutGeneratorFunction   - Full streaming (process/finalize)
    │     └─ TableInOutFunction         - Callback API (transform/finish)
    │        ├─ AggregationFunction     - Reduce to summary
    │        ├─ FilterFunction          - Row filtering
    │        └─ MapFunction             - Column transformation
    └─ ScalarFunctionGenerator          - Single-column output (1:1 rows)
       └─ ScalarFunction                - Callback API (compute)

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
from vgi.arguments import Arg, Arguments, ArgumentValidationError, TableInput
from vgi.function import Invocation
from vgi.log import Level, Message
from vgi.metadata import (
    FunctionExample,
    FunctionStability,
    FunctionType,
    OrderPreservation,
    ParameterInfo,
    ResolvedMetadata,
    TableInputValidationError,
    functions_to_arrow,
)
from vgi.scalar_function import (
    RowCountMismatchError,
    ScalarFunction,
    ScalarFunctionGenerator,
    ScalarOutputGenerator,
)
from vgi.schema_utils import schema, schema_like
from vgi.table_in_out_function import (
    Output,
    OutputGenerator,
    StreamingGenerator,
    TableInOutFunction,
    TableInOutGeneratorFunction,
    streaming,
)
from vgi.table_in_out_function_patterns import (
    AggregationFunction,
    FilterFunction,
    MapFunction,
)
from vgi.testing import FunctionTestClient
from vgi.worker import Worker

__all__ = [
    "AggregationFunction",
    "Arg",
    "ArgumentValidationError",
    "Arguments",
    "FilterFunction",
    "FunctionExample",
    "FunctionStability",
    "FunctionTestClient",
    "FunctionType",
    "Invocation",
    "Level",
    "MapFunction",
    "Message",
    "OrderPreservation",
    "Output",
    "OutputGenerator",
    "ParameterInfo",
    "ResolvedMetadata",
    "RowCountMismatchError",
    "ScalarFunction",
    "ScalarFunctionGenerator",
    "ScalarOutputGenerator",
    "StreamingGenerator",
    "TableInOutFunction",
    "TableInOutGeneratorFunction",
    "TableInput",
    "TableInputValidationError",
    "Worker",
    "functions_to_arrow",
    "hello",
    "schema",
    "schema_like",
    "streaming",
]


def hello() -> str:
    """Return a greeting string. Used for basic installation verification."""
    return "Hello from vgi-python!"
