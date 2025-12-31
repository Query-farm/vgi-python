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
from typing import ClassVar

import pyarrow as pa
import structlog

from vgi.arguments import Arg
from vgi.function import Invocation
from vgi.log import Level, Message
from vgi.metadata import FunctionExample
from vgi.table_function import (
    CardinalityInfo,
    Output,
    OutputGenerator,
    TableFunctionGenerator,
)

__all__ = [
    "SequenceFunction",
    "RangeFunction",
    "ConstantTableFunction",
    "RandomSampleFunction",
    "GeneratorExceptionFunction",
    "LoggingGeneratorFunction",
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
        """Always returns exactly one row."""
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
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("value", pa.float64()),
            ]
        )

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
