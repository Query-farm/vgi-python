"""Example table function implementations using TableFunctionGenerator.

This module contains table functions that generate output without receiving input.
Each function demonstrates different patterns for generating data.

AVAILABLE FUNCTIONS
-------------------
SequenceFunction              - Generates a sequence of integers 0..n-1
RangeFunction                 - Generates integers in a start..end range
ConstantTableFunction         - Returns a constant single-row table
RandomSampleFunction          - Generates random sample data (parallelizable)
GeneratorExceptionFunction    - Demonstrates exception handling
"""

import random
import struct
from typing import ClassVar, cast

import pyarrow as pa

from vgi.arguments import Arg
from vgi.function import InitResult
from vgi.log import Level, Message
from vgi.metadata import FunctionExample
from vgi.table_function import (
    CardinalityInfo,
    Output,
    OutputGenerator,
    TableFunctionGenerator,
    TableFunctionInitInput,
)

__all__ = [
    "SequenceFunction",
    "RangeFunction",
    "ConstantTableFunction",
    "RandomSampleFunction",
    "GeneratorExceptionFunction",
    "LoggingGeneratorFunction",
    "PartitionedRangeFunction",
    "ProjectedDataFunction",
]


class SequenceFunction(TableFunctionGenerator):
    """Generates a sequence of integers from 0 to n-1.

    USE CASE
    --------
    Generate test data, create row numbers, or produce a fixed sequence
    for joining or filtering.

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

    """

    class Meta:
        """Metadata for SequenceFunction."""

        name = "sequence"
        description = "Generates a sequence of integers from 0 to n-1"
        categories = ["generator", "utility"]
        max_workers = 1
        examples = [
            FunctionExample(
                sql="SELECT * FROM sequence(10)",
                description="Generate integers 0-9",
            )
        ]

    count: int = Arg[int](0, doc="Number of integers to generate", ge=0)  # type: ignore[assignment]

    # Batch size for chunking output
    BATCH_SIZE: ClassVar[int] = 1000

    @property
    def output_schema(self) -> pa.Schema:
        """Return output schema with single integer column."""
        return pa.schema([pa.field("n", pa.int64())])

    def cardinality(self) -> CardinalityInfo:
        """Return exact cardinality since we know the count."""
        return CardinalityInfo(estimate=self.count, max=self.count)

    def process(self) -> OutputGenerator:
        """Generate the sequence in batches."""
        remaining = self.count
        current = 0

        while remaining > 0:
            batch_size = min(remaining, self.BATCH_SIZE)
            values = list(range(current, current + batch_size))

            yield Output(
                pa.RecordBatch.from_pydict({"n": values}, schema=self.output_schema)
            )

            current += batch_size
            remaining -= batch_size


class RangeFunction(TableFunctionGenerator):
    """Generates integers in a range [start, end) with optional step.

    USE CASE
    --------
    Generate a range of values similar to Python's range() function.
    Useful for creating test data or generating join keys.

    SCHEMA
    ------
    Output: {"value": int64}

    PARALLELIZATION
    ---------------
    Single worker only. For parallel range generation, use RangePartitionFunction.

    Example:
    -------
    SELECT * FROM range(10, 20, 2)
    Returns: [{"value": 10}, {"value": 12}, {"value": 14}, {"value": 16}, {"value": 18}]

    """

    class Meta:
        """Metadata for RangeFunction."""

        name = "range"
        description = "Generates integers in a range [start, end) with optional step"
        categories = ["generator", "utility"]
        max_workers = 1
        examples = [
            FunctionExample(
                sql="SELECT * FROM range(0, 100, 10)",
                description="Generate 0, 10, 20, ..., 90",
            )
        ]

    start: int = Arg[int](0, doc="Start of range (inclusive)")  # type: ignore[assignment]
    end: int = Arg[int](1, doc="End of range (exclusive)")  # type: ignore[assignment]
    step: int = Arg[int](2, default=1, doc="Step between values", ge=1)  # type: ignore[assignment]

    BATCH_SIZE: ClassVar[int] = 1000

    @property
    def output_schema(self) -> pa.Schema:
        """Return output schema with single integer column."""
        return pa.schema([pa.field("value", pa.int64())])

    def cardinality(self) -> CardinalityInfo:
        """Return cardinality based on range parameters."""
        if self.end <= self.start:
            count = 0
        else:
            count = (self.end - self.start + self.step - 1) // self.step
        return CardinalityInfo(estimate=count, max=count)

    def process(self) -> OutputGenerator:
        """Generate the range in batches."""
        current = self.start

        while current < self.end:
            # Calculate batch values
            values = []
            batch_end = min(current + self.BATCH_SIZE * self.step, self.end)

            while current < batch_end:
                values.append(current)
                current += self.step

            if values:
                yield Output(
                    pa.RecordBatch.from_pydict(
                        {"value": values}, schema=self.output_schema
                    )
                )


class ConstantTableFunction(TableFunctionGenerator):
    """Returns a constant single-row table with a specified value.

    USE CASE
    --------
    Testing, providing configuration values, or creating a single-row
    lookup table.

    SCHEMA
    ------
    Output: {"value": int64}

    PARALLELIZATION
    ---------------
    Single worker (returns exactly one row).

    Example:
    -------
    SELECT * FROM constant_table(42)
    Returns: [{"value": 42}]

    """

    class Meta:
        """Metadata for ConstantTableFunction."""

        name = "constant_table"
        description = "Returns a single-row table with a constant value"
        categories = ["generator", "utility"]
        max_workers = 1
        examples = [
            FunctionExample(
                sql="SELECT * FROM constant_table(42)",
                description="Return a table with one row containing 42",
            )
        ]

    value: int = Arg[int](0, doc="The constant value to return")  # type: ignore[assignment]

    @property
    def output_schema(self) -> pa.Schema:
        """Return output schema with single integer column."""
        return pa.schema([pa.field("value", pa.int64())])

    def cardinality(self) -> CardinalityInfo:
        """Return cardinality of exactly one row."""
        return CardinalityInfo(estimate=1, max=1)

    def process(self) -> OutputGenerator:
        """Emit a single batch with one row."""
        yield Output(
            pa.RecordBatch.from_pydict(
                {"value": [self.value]}, schema=self.output_schema
            )
        )


class RandomSampleFunction(TableFunctionGenerator):
    """Generates random sample data.

    USE CASE
    --------
    Generate random test data for benchmarking, testing, or simulation.
    Each parallel worker generates its own random sample, making this
    suitable for parallel execution.

    SCHEMA
    ------
    Output: {"id": int64, "value": float64}

    PARALLELIZATION
    ---------------
    Fully parallelizable. Each worker generates `count` rows independently,
    so total output is count * num_workers. Use max_workers to control.

    Example:
    -------
    SELECT * FROM random_sample(1000, 42)
    Returns: 1000 rows with random id and value columns

    """

    class Meta:
        """Metadata for RandomSampleFunction."""

        name = "random_sample"
        description = "Generates random sample data"
        categories = ["generator", "testing"]
        # No max_workers limit - fully parallelizable
        examples = [
            FunctionExample(
                sql="SELECT * FROM random_sample(1000, 42)",
                description="Generate 1000 random rows with seed 42",
            )
        ]

    count: int = Arg[int](0, doc="Number of rows to generate", ge=0)  # type: ignore[assignment]
    seed: int = Arg[int](1, default=None, doc="Random seed for reproducibility")  # type: ignore[assignment]

    BATCH_SIZE: ClassVar[int] = 10000

    @property
    def output_schema(self) -> pa.Schema:
        """Return output schema with id and value columns."""
        fields: list[tuple[str, pa.DataType]] = [
            ("id", pa.int64()),
            ("value", pa.float64()),
        ]
        return pa.schema(fields)

    def cardinality(self) -> CardinalityInfo:
        """Return cardinality estimate."""
        return CardinalityInfo(estimate=self.count, max=self.count)

    def setup(self) -> None:
        """Initialize random number generator with seed."""
        if self.seed is not None:
            random.seed(self.seed)

    def process(self) -> OutputGenerator:
        """Generate random data in batches."""
        remaining = self.count
        next_id = 0

        while remaining > 0:
            batch_size = min(remaining, self.BATCH_SIZE)

            ids = list(range(next_id, next_id + batch_size))
            values = [random.random() for _ in range(batch_size)]

            yield Output(
                pa.RecordBatch.from_pydict(
                    {"id": ids, "value": values},
                    schema=self.output_schema,
                )
            )

            next_id += batch_size
            remaining -= batch_size


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
        max_workers = 1

    fail_after: int = Arg[int](0, doc="Number of batches before failure", ge=0)  # type: ignore[assignment]

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

    count: int = Arg[int](0, doc="Number of values to generate", ge=0)  # type: ignore[assignment]

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


class PartitionedRangeFunction(TableFunctionGenerator):
    """Generates a partitioned range of integers for multi-worker execution.

    USE CASE
    --------
    Generate a range of values using a work queue pattern. The primary worker
    populates a queue with work chunks during initialization. All workers
    (including the primary) pull chunks from the queue and generate output.

    This is resilient to fewer workers launching than expected - all work
    will still be completed by the available workers.

    SCHEMA
    ------
    Output: {"value": int64}

    PARALLELIZATION
    ---------------
    Fully parallelizable using a shared work queue. Each worker pulls chunks
    atomically from the queue and generates values for that chunk.

    The union of all workers' output produces the complete range [0, count).

    Example:
    -------
    With count=3000 and CHUNK_SIZE=1000:
        Queue is populated with: [(0, 1000), (1000, 2000), (2000, 3000)]
        Workers pull chunks and generate values for each range.
        Combined output: [0, 1, 2, ..., 2999]

    """

    class Meta:
        """Metadata for PartitionedRangeFunction."""

        name = "partitioned_range"
        description = "Generates a partitioned range for multi-worker execution"
        categories = ["generator", "utility"]
        # No max_workers limit - fully parallelizable
        examples = [
            FunctionExample(
                sql="SELECT * FROM partitioned_range(100)",
                description="Generate 0-99 in parallel across workers",
            )
        ]

    count: int = Arg[int](0, doc="Total number of integers to generate", ge=0)  # type: ignore[assignment]

    # Size of each work chunk in the queue
    CHUNK_SIZE: ClassVar[int] = 1000
    # Batch size for output within each chunk
    BATCH_SIZE: ClassVar[int] = 1000

    @property
    def output_schema(self) -> pa.Schema:
        """Return output schema with single integer column."""
        return pa.schema([pa.field("value", pa.int64())])

    def cardinality(self) -> CardinalityInfo:
        """Return cardinality estimate.

        Since work is distributed dynamically via queue, we can only provide
        the total count estimate, not per-worker estimates.
        """
        return CardinalityInfo(estimate=self.count, max=self.count)

    def perform_init(self, init_input: pa.RecordBatch) -> InitResult:
        """Populate the work queue with range chunks."""
        # Parse init data and store in init_storage
        self.init_data = TableFunctionInitInput.deserialize(init_input)
        self.init_identifier = self.init_storage.create(self.init_data.serialize())

        # Create work items for each chunk of the range
        work_items: list[bytes] = []
        for start in range(0, self.count, self.CHUNK_SIZE):
            end = min(start + self.CHUNK_SIZE, self.count)
            # Pack as two unsigned 64-bit integers: (start, end)
            work_items.append(struct.pack(">QQ", start, end))

        if work_items:
            self.enqueue_work(work_items)

        return InitResult(self.init_identifier)

    def process(self) -> OutputGenerator:
        """Generate values by pulling chunks from the work queue."""
        while True:
            # Atomically claim a work item from the queue
            work_data = self.dequeue_work()
            if work_data is None:
                break  # Queue empty, done

            # Unpack the range (start, end)
            start, end = struct.unpack(">QQ", work_data)

            # Generate values for this chunk in batches
            current = start
            while current < end:
                batch_end = min(current + self.BATCH_SIZE, end)
                values = list(range(current, batch_end))

                yield Output(
                    pa.RecordBatch.from_pydict(
                        {"value": values}, schema=self.output_schema
                    )
                )

                current = batch_end


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

    count: int = Arg[int](0, doc="Number of rows to generate", ge=0)  # type: ignore[assignment]

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
        """Return the projected schema based on init_data."""
        return self.apply_projection(self.FULL_SCHEMA)

    def cardinality(self) -> CardinalityInfo:
        """Return exact cardinality since we know the count."""
        return CardinalityInfo(estimate=self.count, max=self.count)

    def _get_projected_column_indices(self) -> list[int]:
        """Get the column indices to generate.

        Returns indices from projection_ids if set, otherwise all columns.
        """
        if self.init_data and self.init_data.projection_ids is not None:
            return self.init_data.projection_ids
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
