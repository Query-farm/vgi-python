"""Example table function implementations using TableFunctionGenerator.

This module contains table functions that generate output without receiving input.
Each function demonstrates different patterns for generating data.

AVAILABLE FUNCTIONS
-------------------
ConstantColumnsFunction       - Demonstrates varargs with dynamic output schema
DoubleSequenceFunction        - Generates a sequence of floats 0.0..n-1
GeneratorExceptionFunction    - Demonstrates exception handling
LoggingGeneratorFunction      - Demonstrates log message emission
PartitionedSequenceFunction   - Demonstrates multi-worker parallel execution
ProjectedDataFunction         - Demonstrates projection pushdown
SequenceFunction              - Generates a sequence of integers 0..n-1
SettingsAwareFunction         - Demonstrates settings-aware output schema
"""

import struct
from typing import Annotated, Any, ClassVar, cast

import numpy as np
import pyarrow as pa

from vgi.arguments import Arg
from vgi.invocation import InitResult
from vgi.log import Level, Message
from vgi.metadata import FunctionExample
from vgi.table_function import (
    Output,
    OutputGenerator,
    TableCardinality,
    TableFunctionGenerator,
    TableFunctionInitInput,
)

__all__ = [
    "ConstantColumnsFunction",
    "DoubleSequenceFunction",
    "GeneratorExceptionFunction",
    "LoggingGeneratorFunction",
    "PartitionedSequenceFunction",
    "ProjectedDataFunction",
    "SequenceFunction",
    "SettingsAwareFunction",
]


class SequenceFunction(TableFunctionGenerator):
    """Generates a sequence of integers from 0 to n-1 with optional increment.

    USE CASE
    --------
    Generate test data, create row numbers, or produce a fixed sequence
    for joining or filtering. The increment parameter allows generating
    sequences like 0, 2, 4, 6, ... or 0, 10, 20, 30, ...

    SCHEMA
    ------
    Output: {"n": int64}

    PARALLELIZATION
    ---------------
    Single worker only (max_workers=1). Each worker would produce the full
    sequence, which is typically not desired.

    Example:
    -------
    SELECT * FROM sequence(5)
    Returns: [{"n": 0}, {"n": 1}, {"n": 2}, {"n": 3}, {"n": 4}]

    SELECT * FROM sequence(5, increment=2)
    Returns: [{"n": 0}, {"n": 2}, {"n": 4}, {"n": 6}, {"n": 8}]

    SELECT * FROM sequence(1000, 100)
    Returns: integers 0-999 in batches of 100 rows each

    """

    class Meta:
        """Metadata for SequenceFunction."""

        name = "sequence"
        description = "Generates a sequence of integers from 0 to n-1"
        categories = ["generator", "utility"]
        tags = {"category": "generator", "type": "utility"}
        max_workers = 1
        examples = [
            FunctionExample(
                sql="SELECT * FROM sequence(10)",
                description="Generate integers 0-9",
            ),
            FunctionExample(
                sql="SELECT * FROM sequence(1000, 100)",
                description="Generate integers 0-999 in batches of 100",
            ),
            FunctionExample(
                sql="SELECT * FROM sequence(5, increment=10)",
                description="Generate 0, 10, 20, 30, 40",
            ),
        ]

    count: Annotated[int, Arg(0, doc="Number of integers to generate", ge=0)]
    batch_size: Annotated[int, Arg(1, default=1000, doc="Batch size for output", ge=1)]
    increment: Annotated[
        int, Arg("increment", default=1, doc="Step between values", ge=1)
    ]

    @property
    def output_schema(self) -> pa.Schema:
        """Return output schema with single integer column."""
        return pa.schema([pa.field("n", pa.int64())])

    @property
    def cardinality(self) -> TableCardinality:
        """Return exact cardinality since we know the count."""
        return TableCardinality(estimate=self.count, max=self.count)

    def process(self) -> OutputGenerator:
        """Generate the sequence in batches."""
        remaining = self.count
        current_index = 0

        while remaining > 0:
            size = min(remaining, self.batch_size)
            # Generate: idx*increment, (idx+1)*increment, (idx+2)*increment, ...
            values = np.arange(
                current_index * self.increment,
                (current_index + size) * self.increment,
                self.increment,
                dtype=np.int64,
            )

            yield Output(
                pa.RecordBatch.from_pydict({"n": values}, schema=self.output_schema)
            )

            current_index += size
            remaining -= size


class DoubleSequenceFunction(TableFunctionGenerator):
    """Generates a sequence of floats from 0.0 to n-1 with optional increment.

    USE CASE
    --------
    Generate test data with floating-point values, create sequences for
    interpolation or sampling. The increment parameter allows generating
    sequences like 0.0, 0.5, 1.0, 1.5, ... or 0.0, 0.1, 0.2, 0.3, ...

    SCHEMA
    ------
    Output: {"n": float64}

    PARALLELIZATION
    ---------------
    Single worker only (max_workers=1). Each worker would produce the full
    sequence, which is typically not desired.

    Example:
    -------
    SELECT * FROM double_sequence(5)
    Returns: [{"n": 0.0}, {"n": 1.0}, {"n": 2.0}, {"n": 3.0}, {"n": 4.0}]

    SELECT * FROM double_sequence(5, increment=0.5)
    Returns: [{"n": 0.0}, {"n": 0.5}, {"n": 1.0}, {"n": 1.5}, {"n": 2.0}]

    SELECT * FROM double_sequence(1000, 100)
    Returns: floats 0.0-999.0 in batches of 100 rows each

    """

    class Meta:
        """Metadata for DoubleSequenceFunction."""

        name = "double_sequence"
        description = "Generates a sequence of floating-point numbers from 0 to n-1"
        categories = ["generator", "utility"]
        tags = {"category": "generator", "type": "utility"}
        max_workers = 1
        examples = [
            FunctionExample(
                sql="SELECT * FROM double_sequence(10)",
                description="Generate floats 0.0-9.0",
            ),
            FunctionExample(
                sql="SELECT * FROM double_sequence(1000, 100)",
                description="Generate floats 0.0-999.0 in batches of 100",
            ),
            FunctionExample(
                sql="SELECT * FROM double_sequence(5, increment=0.5)",
                description="Generate 0.0, 0.5, 1.0, 1.5, 2.0",
            ),
        ]

    count: Annotated[int, Arg(0, doc="Number of values to generate", ge=0)]
    batch_size: Annotated[int, Arg(1, default=1000, doc="Batch size for output", ge=1)]
    increment: Annotated[
        float, Arg("increment", default=1.0, doc="Step between values", gt=0.0)
    ]

    @property
    def output_schema(self) -> pa.Schema:
        """Return output schema with single float64 column."""
        return pa.schema([pa.field("n", pa.float64())])

    @property
    def cardinality(self) -> TableCardinality:
        """Return exact cardinality since we know the count."""
        return TableCardinality(estimate=self.count, max=self.count)

    def process(self) -> OutputGenerator:
        """Generate the sequence in batches."""
        remaining = self.count
        current_index = 0

        while remaining > 0:
            size = min(remaining, self.batch_size)
            # Generate: idx*increment, (idx+1)*increment, (idx+2)*increment, ...
            values = np.arange(
                current_index * self.increment,
                (current_index + size) * self.increment,
                self.increment,
                dtype=np.float64,
            )

            yield Output(
                pa.RecordBatch.from_pydict({"n": values}, schema=self.output_schema)
            )

            current_index += size
            remaining -= size


class GeneratorExceptionFunction(TableFunctionGenerator):
    """Function that raises an exception after generating some output.

    USE CASE
    --------
    Testing exception handling in the generator protocol.

    SCHEMA
    ------
    Output: {"n": int64}

    """

    class Meta:
        """Metadata for GeneratorExceptionFunction."""

        name = "generator_exception"
        description = "Raises an exception after N batches for testing"
        categories = ["testing"]
        tags = {"category": "testing", "type": "error-handling"}
        max_workers = 1

    fail_after: Annotated[int, Arg(0, doc="Number of batches before failure", ge=0)]

    @property
    def output_schema(self) -> pa.Schema:
        """Return output schema."""
        return pa.schema([pa.field("n", pa.int64())])

    def process(self) -> OutputGenerator:
        """Generate batches then raise an exception."""
        for i in range(self.fail_after):
            yield Output(
                pa.RecordBatch.from_pydict({"n": [i]}, schema=self.output_schema)
            )

        raise ValueError(f"Intentional failure after {self.fail_after} batches")


class LoggingGeneratorFunction(TableFunctionGenerator):
    """Function that emits log messages during generation.

    USE CASE
    --------
    Testing log message handling in the generator protocol.

    SCHEMA
    ------
    Output: {"n": int64}

    """

    class Meta:
        """Metadata for LoggingGeneratorFunction."""

        name = "logging_generator"
        description = "Emits log messages during generation"
        categories = ["testing"]
        max_workers = 1

    count: Annotated[int, Arg(0, doc="Number of values to generate", ge=0)]

    @property
    def output_schema(self) -> pa.Schema:
        """Return output schema."""
        return pa.schema([pa.field("n", pa.int64())])

    def process(self) -> OutputGenerator:
        """Generate values with logging."""
        yield Message(Level.INFO, f"Starting generation of {self.count} values")

        for i in range(self.count):
            yield Output(
                pa.RecordBatch.from_pydict({"n": [i]}, schema=self.output_schema)
            )

        yield Message(Level.INFO, "Generation complete")


class PartitionedSequenceFunction(TableFunctionGenerator):
    """Generates a partitioned sequence of integers for multi-worker execution.

    USE CASE
    --------
    Generate a sequence of values using a work queue pattern. The primary worker
    populates a queue with work chunks during initialization. All workers
    (including the primary) pull chunks from the queue and generate output.

    This is resilient to fewer workers launching than expected - all work
    will still be completed by the available workers.

    SCHEMA
    ------
    Output: {"n": int64}

    PARALLELIZATION
    ---------------
    Fully parallelizable using a shared work queue. Each worker pulls chunks
    atomically from the queue and generates values for that chunk.

    The union of all workers' output produces the complete sequence.

    Example:
    -------
    With count=3000 and CHUNK_SIZE=1000:
        Queue is populated with: [(0, 1000), (1000, 2000), (2000, 3000)]
        Workers pull chunks and generate values for each range.
        Combined output: [0, 1, 2, ..., 2999]

    With count=5 and increment=10:
        Combined output: [0, 10, 20, 30, 40]

    """

    class Meta:
        """Metadata for PartitionedSequenceFunction."""

        name = "partitioned_sequence"
        description = "Generates a partitioned sequence for multi-worker execution"
        categories = ["generator", "utility"]
        # No max_workers limit - fully parallelizable
        examples = [
            FunctionExample(
                sql="SELECT * FROM partitioned_sequence(100)",
                description="Generate 0-99 in parallel across workers",
            ),
            FunctionExample(
                sql="SELECT * FROM partitioned_sequence(5, increment=10)",
                description="Generate 0, 10, 20, 30, 40 in parallel",
            ),
        ]

    count: Annotated[int, Arg(0, doc="Total number of integers to generate", ge=0)]
    increment: Annotated[
        int, Arg("increment", default=1, doc="Step between values", ge=1)
    ]

    # Size of each work chunk in the queue
    CHUNK_SIZE: ClassVar[int] = 1000
    # Batch size for output within each chunk
    BATCH_SIZE: ClassVar[int] = 1000

    @property
    def output_schema(self) -> pa.Schema:
        """Return output schema with single integer column."""
        return pa.schema([pa.field("n", pa.int64())])

    @property
    def cardinality(self) -> TableCardinality:
        """Return cardinality estimate.

        Since work is distributed dynamically via queue, we can only provide
        the total count estimate, not per-worker estimates.
        """
        return TableCardinality(estimate=self.count, max=self.count)

    def initialize_global_state(self, init_input: pa.RecordBatch) -> InitResult:
        """Populate the work queue with sequence chunks."""
        # Parse init data and store in storage
        self.init_input = TableFunctionInitInput.deserialize(init_input)
        self.execution_identifier = self.storage.global_put(self.init_input.serialize())

        # Create work items for each chunk of the sequence
        work_items: list[bytes] = []
        for start_idx in range(0, self.count, self.CHUNK_SIZE):
            end_idx = min(start_idx + self.CHUNK_SIZE, self.count)
            # Pack as two unsigned 64-bit integers: (start_idx, end_idx)
            work_items.append(struct.pack(">QQ", start_idx, end_idx))

        # Always enqueue (even if empty) to register the invocation
        self.enqueue_work(work_items)

        return InitResult(self.execution_identifier)

    def process(self) -> OutputGenerator:
        """Generate values by pulling chunks from the work queue."""
        while True:
            # Atomically claim a work item from the queue
            work_data = self.dequeue_work()
            if work_data is None:
                break  # Queue empty, done

            # Unpack the index range (start_idx, end_idx)
            start_idx, end_idx = struct.unpack(">QQ", work_data)

            # Generate values for this chunk in batches
            current_idx = start_idx
            while current_idx < end_idx:
                batch_end_idx = min(current_idx + self.BATCH_SIZE, end_idx)
                # Generate values: idx * increment for each idx in range
                values = [
                    idx * self.increment for idx in range(current_idx, batch_end_idx)
                ]

                yield Output(
                    pa.RecordBatch.from_pydict({"n": values}, schema=self.output_schema)
                )

                current_idx = batch_end_idx


class ProjectedDataFunction(TableFunctionGenerator):
    """Generates data with 4 columns, supporting projection pushdown.

    USE CASE
    --------
    Demonstrates projection pushdown where the function only computes
    columns that are actually requested. This is useful for expensive
    column computations that can be skipped if the column isn't needed.

    SCHEMA
    ------
    Full output: {"id": int64, "name": string, "value": float64, "extra": int64}
    With projection, only the projected columns are included.

    PARALLELIZATION
    ---------------
    Single worker only (max_workers=1).

    Example:
    -------
    SELECT id, value FROM projected_data(10)  -- Only computes id and value
    Returns: 10 rows with id and value columns only

    """

    class Meta:
        """Metadata for ProjectedDataFunction."""

        name = "projected_data"
        description = "Generates data with 4 columns, supporting projection pushdown"
        categories = ["generator", "utility"]
        max_workers = 1
        examples = [
            FunctionExample(
                sql="SELECT * FROM projected_data(10)",
                description="Generate 10 rows with all 4 columns",
            ),
            FunctionExample(
                sql="SELECT id, value FROM projected_data(10)",
                description="Generate 10 rows with only id and value columns",
            ),
        ]

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]

    # Full schema with all 4 columns
    FULL_SCHEMA: pa.Schema = pa.schema(
        cast(
            list[tuple[str, pa.DataType]],
            [
                ("id", pa.int64()),
                ("name", pa.string()),
                ("value", pa.float64()),
                ("extra", pa.int64()),
            ],
        )
    )

    BATCH_SIZE: int = 1000

    @property
    def output_schema(self) -> pa.Schema:
        """Return the projected schema based on init_input."""
        return self.apply_projection(self.FULL_SCHEMA)

    @property
    def cardinality(self) -> TableCardinality:
        """Return exact cardinality since we know the count."""
        return TableCardinality(estimate=self.count, max=self.count)

    def _get_projected_column_indices(self) -> list[int]:
        """Get the column indices to generate.

        Returns indices from projection_ids if set, otherwise all columns.
        """
        if self.init_input and self.init_input.projection_ids is not None:
            return self.init_input.projection_ids
        return list(range(len(self.FULL_SCHEMA)))

    def process(self) -> OutputGenerator:
        """Generate data for only the projected columns."""
        projected_indices = self._get_projected_column_indices()
        output_schema = self.output_schema

        remaining = self.count
        current_id = 0

        while remaining > 0:
            batch_size = min(remaining, self.BATCH_SIZE)

            # Only compute columns that are projected
            columns: dict[str, list[int] | list[str] | list[float]] = {}

            for idx in projected_indices:
                field = self.FULL_SCHEMA.field(idx)
                if field.name == "id":
                    # Column 0: Sequential IDs
                    columns["id"] = list(range(current_id, current_id + batch_size))
                elif field.name == "name":
                    # Column 1: Names based on ID
                    columns["name"] = [
                        f"item_{i}" for i in range(current_id, current_id + batch_size)
                    ]
                elif field.name == "value":
                    # Column 2: Float values (ID * 1.5)
                    columns["value"] = [
                        float(i) * 1.5
                        for i in range(current_id, current_id + batch_size)
                    ]
                elif field.name == "extra":
                    # Column 3: Extra integer (ID squared)
                    columns["extra"] = [
                        i * i for i in range(current_id, current_id + batch_size)
                    ]

            yield Output(pa.RecordBatch.from_pydict(columns, schema=output_schema))

            current_id += batch_size
            remaining -= batch_size


class SettingsAwareFunction(TableFunctionGenerator):
    """Generates data demonstrating that settings are passed to functions.

    USE CASE
    --------
    Demonstrates how functions can declare required settings via
    Meta.required_settings and access them via self.settings or
    self.get_setting(). The output includes columns showing the actual
    setting values that were passed.

    This function uses three settings:
    - vgi_verbose_mode: bool - when true, adds a details column
    - greeting: str - a custom greeting message echoed in output
    - multiplier: int - multiplies the value column

    SCHEMA
    ------
    Base output: {"id": int64, "greeting": string, "value": float64}
    With vgi_verbose_mode="true": adds "details": string column

    PARALLELIZATION
    ---------------
    Single worker only (max_workers=1).

    Example:
    -------
    With settings={"vgi_verbose_mode": "true", "greeting": "Hi", "multiplier": "2"}:
    Returns: [{"id": 0, "greeting": "Hi", "value": 0.0, "details": "row_0"}, ...]

    """

    class Meta:
        """Metadata for SettingsAwareFunction."""

        name = "settings_aware"
        description = "Generates data demonstrating settings are passed"
        categories = ["generator", "settings"]
        max_workers = 1
        required_settings = ["vgi_verbose_mode", "greeting", "multiplier"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM settings_aware(5)",
                description="Generate 5 rows showing setting values",
            )
        ]

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]

    @property
    def output_schema(self) -> pa.Schema:
        """Return output schema based on vgi_verbose_mode setting.

        Always includes id, greeting (from setting), and value (multiplied).
        When vgi_verbose_mode is "true", includes an extra "details" column.
        """
        fields: list[pa.Field[pa.DataType]] = [
            pa.field("id", pa.int64()),
            pa.field("greeting", pa.string()),
            pa.field("value", pa.float64()),
        ]

        # Add details column if verbose mode is enabled
        if self.get_setting("vgi_verbose_mode") == "true":
            fields.append(pa.field("details", pa.string()))

        return pa.schema(fields)

    @property
    def cardinality(self) -> TableCardinality:
        """Return exact cardinality since we know the count."""
        return TableCardinality(estimate=self.count, max=self.count)

    def process(self) -> OutputGenerator:
        """Generate data based on settings."""
        verbose = self.get_setting("vgi_verbose_mode") == "true"
        greeting = self.get_setting("greeting") or "Hello"
        multiplier_str = self.get_setting("multiplier") or "1"
        multiplier = int(multiplier_str)
        output_schema = self.output_schema

        for i in range(self.count):
            data: dict[str, list[int] | list[float] | list[str]] = {
                "id": [i],
                "greeting": [greeting],
                "value": [float(i) * 2.5 * multiplier],
            }

            if verbose:
                data["details"] = [f"row_{i}"]

            yield Output(pa.RecordBatch.from_pydict(data, schema=output_schema))


class ConstantColumnsFunction(TableFunctionGenerator):
    """Generates a table with constant values in each column based on varargs.

    USE CASE
    --------
    Demonstrates varargs with AnyArrow type where the output schema is
    determined by the types of the values provided. Each vararg value
    becomes a column filled with that constant value for all rows.

    This shows how varargs can accept mixed types and produce a dynamic
    output schema based on the argument types.

    SCHEMA
    ------
    Output schema is dynamic based on the types of provided values.
    Column names are auto-generated as col_0, col_1, col_2, etc.

    Example: constant_columns(3, 42, 'hello', 3.14)
    Output schema: {"col_0": int64, "col_1": string, "col_2": double}

    PARALLELIZATION
    ---------------
    Single worker only (max_workers=1).

    Example:
    -------
    SELECT * FROM constant_columns(3, 42, 'hello')
    Returns: [{"col_0": 42, "col_1": "hello"},
              {"col_0": 42, "col_1": "hello"},
              {"col_0": 42, "col_1": "hello"}]

    SELECT * FROM constant_columns(2, 1, 2, 3, 'apple')
    Returns: [{"col_0": 1, "col_1": 2, "col_2": 3, "col_3": "apple"},
              {"col_0": 1, "col_1": 2, "col_2": 3, "col_3": "apple"}]

    """

    class Meta:
        """Metadata for ConstantColumnsFunction."""

        name = "constant_columns"
        description = "Generates rows with constant values from varargs"
        categories = ["generator", "utility"]
        max_workers = 1
        examples = [
            FunctionExample(
                sql="SELECT * FROM constant_columns(5, 42, 'hello')",
                description="Generate 5 rows with columns containing 42 and 'hello'",
            ),
            FunctionExample(
                sql="SELECT * FROM constant_columns(3, 1, 2, 3, 'test')",
                description="Generate 3 rows with 4 columns of mixed types",
            ),
        ]

    count: Annotated[int, Arg(0, doc="Number of rows to generate", ge=0)]
    values: Annotated[
        tuple[Any, ...],
        Arg(1, varargs=True, doc="Values to fill each column (at least one required)"),
    ]

    # Store Arrow scalars for type information
    _value_scalars: list[Any]

    BATCH_SIZE: int = 1000

    def bind(self) -> None:
        """Extract Arrow scalars from positional arguments for type info."""
        # Access raw Arrow scalars to preserve type information
        positional = self.invocation.arguments.positional
        # Filter to non-None scalars (varargs validation ensures no nulls)
        self._value_scalars = [s for s in positional[1:] if s is not None]

    @property
    def output_schema(self) -> pa.Schema:
        """Return output schema with one column per vararg, typed by value."""
        fields = [
            pa.field(f"col_{i}", scalar.type)
            for i, scalar in enumerate(self._value_scalars)
        ]
        return pa.schema(fields)

    @property
    def cardinality(self) -> TableCardinality:
        """Return exact cardinality since we know the count."""
        return TableCardinality(estimate=self.count, max=self.count)

    def process(self) -> OutputGenerator:
        """Generate rows with constant values in each column."""
        output_schema = self.output_schema
        remaining = self.count

        while remaining > 0:
            batch_size = min(remaining, self.BATCH_SIZE)

            # Create arrays filled with constant values
            arrays = [
                pa.array([scalar.as_py()] * batch_size, type=scalar.type)
                for scalar in self._value_scalars
            ]

            yield Output(pa.RecordBatch.from_arrays(arrays, schema=output_schema))

            remaining -= batch_size
