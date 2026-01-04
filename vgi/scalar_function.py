"""Scalar functions: per-row transforms with single-column output.

Scalar functions are the simplest function type in VGI. They transform each
input row into exactly one output value, producing a single column of results.

Key characteristics:
- **1:1 row mapping**: Output has exactly the same number of rows as input
- **Single column output**: Output schema has exactly one column named "result"
- **No finalization**: All processing happens in compute(), no finish() phase

Common use cases:
- Mathematical operations: multiply, add, abs
- String transforms: upper, lower, concat, trim
- Type conversions: cast, parse
- Field extraction: get nested values, parse JSON fields

This module provides two base classes:

    ScalarFunction (recommended)
        Simple callback-based API. Override output_type and compute().

    ScalarFunctionGenerator (advanced)
        Generator-based API for fine-grained control over logging.
        Override output_schema and process().

Example::

    class DoubleValue(ScalarFunction):
        column = Arg[str](0, doc="Column to double")

        @property
        def output_type(self) -> pa.DataType:
            return self.input_schema.field(self.column).type

        def compute(self, batch: pa.RecordBatch) -> pa.Array:
            return pc.multiply(batch.column(self.column), 2)

"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any, final

import pyarrow as pa
import structlog

import vgi.function
import vgi.log
from vgi.function import SchemaValidationError
from vgi.table_function import Output, ProtocolOutput

__all__ = [
    "ScalarFunctionGenerator",
    "ScalarFunction",
    "Output",
    "ScalarOutputGenerator",
    "ProtocolInput",
    "RowCountMismatchError",
]


class RowCountMismatchError(Exception):
    """Raised when scalar function output row count doesn't match input.

    Scalar functions must produce exactly one output row for each input row.
    This error indicates the compute() method returned an array with the
    wrong number of elements.

    Attributes:
        input_rows: Number of rows in the input batch.
        output_rows: Number of rows in the output batch.
        function_name: Name of the function that produced the mismatch.

    """

    def __init__(
        self,
        message: str,
        *,
        input_rows: int | None = None,
        output_rows: int | None = None,
        function_name: str = "",
    ) -> None:
        """Initialize with row count details.

        Args:
            message: Base error message.
            input_rows: Number of input rows.
            output_rows: Number of output rows.
            function_name: Name of the function class.

        """
        self.input_rows = input_rows
        self.output_rows = output_rows
        self.function_name = function_name

        if input_rows is not None and output_rows is not None:
            full_message = self._build_detailed_message(
                message, input_rows, output_rows
            )
        else:
            full_message = message

        super().__init__(full_message)

    def _build_detailed_message(
        self, base_message: str, input_rows: int, output_rows: int
    ) -> str:
        """Build a detailed, helpful error message."""
        lines = [base_message, ""]

        if self.function_name:
            lines.append(f"  Function: {self.function_name}")

        lines.append(f"  Input rows:  {input_rows}")
        lines.append(f"  Output rows: {output_rows}")

        # Provide specific guidance based on the mismatch type
        lines.append("")
        if output_rows < input_rows:
            lines.append("  Problem: Output has fewer rows than input.")
            lines.append("")
            lines.append("  Possible causes:")
            lines.append("    - compute() is filtering rows (not allowed in scalar)")
            lines.append("    - compute() is aggregating (not allowed in scalar)")
            lines.append("    - Bug in array construction")
            lines.append("")
            lines.append("  Scalar functions require 1:1 row mapping.")
            lines.append("  For filtering or aggregation, use a table function.")
        else:
            lines.append("  Problem: Output has more rows than input.")
            lines.append("")
            lines.append("  Possible causes:")
            lines.append("    - compute() is expanding rows (not allowed in scalar)")
            lines.append("    - compute() is unnesting arrays")
            lines.append("    - Bug in array construction")
            lines.append("")
            lines.append("  Scalar functions require 1:1 row mapping.")
            lines.append("  For row expansion (1→N), use a table function.")

        return "\n".join(lines)


# Generator type for scalar function output.
# Must yield Output or Message (never None) since scalars always produce output.
ScalarOutputGenerator = Generator[vgi.log.Message | Output, pa.RecordBatch | None, None]


@dataclass(frozen=True, slots=True)
class ProtocolInput:
    """Input sent to the scalar function generator via send().

    Contains an input batch and optional metadata. The scalar function
    processes each batch and returns an output batch with the same row count.

    Attributes:
        batch: The input RecordBatch to process.
        metadata: Optional metadata from the IPC stream.

    """

    batch: pa.RecordBatch
    metadata: pa.KeyValueMetadata | None = None


@dataclass(frozen=True, slots=True)
class _ScalarOutputComplete:
    """Internal: Output with guaranteed non-None batch for scalar functions."""

    batch: pa.RecordBatch
    log_message: vgi.log.Message | None = None

    @classmethod
    def from_process_result(
        cls,
        source: vgi.log.Message | Output,
        empty_batch: pa.RecordBatch,
    ) -> _ScalarOutputComplete:
        """Create from user's yield value.

        Args:
            source: What the user yielded (Output or Message).
            empty_batch: Empty batch to substitute when yielding Message.

        Returns:
            Normalized output with guaranteed non-None batch.

        """
        if isinstance(source, vgi.log.Message):
            return cls(batch=empty_batch, log_message=source)
        # source is Output
        return cls(
            batch=source.batch if source.batch is not None else empty_batch,
        )


class ScalarFunctionGenerator(vgi.function.Function[vgi.function.FunctionInitInput]):
    """Generator-based base class for scalar functions.

    This is the advanced API for scalar functions. For most use cases,
    use ScalarFunction instead, which provides a simpler compute() callback.

    Scalar functions have these constraints:
    - **1:1 row mapping**: Output row count must equal input row count
    - **Single column**: Output schema has exactly one column
    - **No finalization**: Processing ends when input is exhausted

    Override process() to implement the generator protocol:

        def process(self, batch: pa.RecordBatch) -> ScalarOutputGenerator:
            _ = yield Output(self.empty_output_batch)  # Priming yield
            while True:
                # Optional: yield log messages
                yield Message(Level.INFO, f"Processing {batch.num_rows} rows")

                result_array = compute_result(batch)
                output_batch = pa.RecordBatch.from_arrays(
                    [result_array], schema=self.output_schema
                )
                batch = yield Output(output_batch)
                if batch is None:
                    break

    Methods to Override
    -------------------
    output_schema : pa.Schema (property)
        Define the single-column output schema.

    process(batch) : ScalarOutputGenerator
        Generator that processes batches. Must yield Output or Message.

    setup() : None
        Called before processing. Acquire resources here.

    teardown() : None
        Called after processing. Release resources here.

    Available Attributes
    --------------------
    self.invocation : Invocation
        The complete invocation request with function name and arguments.

    self.input_schema : pa.Schema
        Schema of input batches (from invocation).

    self.output_schema : pa.Schema
        Schema of output batches (single column).

    self.empty_output_batch : pa.RecordBatch
        Empty batch conforming to output_schema, useful for priming yields.

    """

    InitDataCls = vgi.function.FunctionInitInput

    def __init__(
        self,
        invocation: vgi.function.Invocation,
        logger: structlog.stdlib.BoundLogger,
    ):
        """Initialize the scalar function with invocation data and logger."""
        super().__init__(invocation=invocation, logger=logger)
        if invocation.input_schema is None:
            raise ValueError(
                f"{type(self).__name__} requires an input schema, but none was "
                f"provided. ScalarFunction processes input batches and requires "
                f"input_schema to be set in the Invocation."
            )
        # Validate single-column output at construction
        if len(self.output_schema) != 1:
            cols = [f.name for f in self.output_schema]
            raise SchemaValidationError(
                f"ScalarFunction must have exactly 1 output column, "
                f"but output_schema has {len(self.output_schema)} columns.\n\n"
                f"  Columns found: {cols}\n\n"
                f"  Scalar functions transform each input row to a single value.\n"
                f"  For multiple output columns, use a table function instead."
            )

    # input_schema property and _validate_input_schema inherited from Function

    @final
    def _validate_row_count(
        self, output_batch: pa.RecordBatch, input_batch: pa.RecordBatch
    ) -> None:
        """Validate that output row count matches input row count."""
        if output_batch.num_rows != input_batch.num_rows:
            raise RowCountMismatchError(
                "Scalar function output must have same row count as input.",
                input_rows=input_batch.num_rows,
                output_rows=output_batch.num_rows,
                function_name=type(self).__name__,
            )

    @final
    def _process_and_validate(
        self,
        generator: ScalarOutputGenerator,
        input_batch: pa.RecordBatch,
    ) -> _ScalarOutputComplete:
        """Process a batch and validate schemas and row count.

        Args:
            generator: The user's process() generator.
            input_batch: The input RecordBatch to process.

        Returns:
            _ScalarOutputComplete with validated output batch.

        Raises:
            SchemaValidationError: If input or output batch schema doesn't match.
            RowCountMismatchError: If output row count doesn't match input.

        """
        self._validate_input_schema(input_batch)
        result: _ScalarOutputComplete = _ScalarOutputComplete.from_process_result(
            generator.send(input_batch),
            self.empty_output_batch,
        )
        self._validate_output_schema(result.batch)
        # Validate row count for actual output (not log messages)
        if result.log_message is None:
            self._validate_row_count(result.batch, input_batch)
        return result

    @final
    def _process_with_exception_handling(
        self,
        generator: ScalarOutputGenerator,
        input_batch: pa.RecordBatch,
    ) -> _ScalarOutputComplete:
        """Process a batch with exception handling.

        Wraps _process_and_validate to catch exceptions and convert them
        to _ScalarOutputComplete with an error log message.
        """
        try:
            return self._process_and_validate(generator, input_batch)
        except Exception as e:
            return _ScalarOutputComplete(
                batch=self.empty_output_batch,
                log_message=vgi.log.Message.from_exception(e),
            )

    @final
    def _should_terminate(self, result: _ScalarOutputComplete) -> bool:
        """Check if processing should terminate due to an exception."""
        return (
            result.log_message is not None
            and result.log_message.level == vgi.log.Level.EXCEPTION
        )

    @abstractmethod
    def process(self, batch: pa.RecordBatch) -> ScalarOutputGenerator:
        """Process input batches.

        Override this method to implement your scalar transformation.

        Args:
            batch: First input batch (subsequent batches via yield return).

        Yields:
            Output: Batch with same row count as input.
            Message: Log message (input will be re-sent).

        """
        ...

    @final
    def run(self) -> Generator[ProtocolOutput, ProtocolInput | None, None]:
        """Run the scalar function protocol. Do not override.

        This generator implements the SETUP -> DATA -> TEARDOWN lifecycle.
        The generator is closed by the caller when input is exhausted.

        Protocol:
            - Caller primes with next() or send(None)
            - Caller sends ProtocolInput for each batch
            - When input exhausted, caller closes the generator
        """
        # Priming yield - caller calls next() or send(None)
        input: ProtocolInput | None = yield ProtocolOutput(batch=None)
        if input is None:
            raise ValueError("Expected ProtocolInput, got None")

        # Acquire resources before processing
        self.setup()

        generator = self.process(input.batch)
        # Prime the process() generator past the initial yield
        generator.send(None)

        try:
            # DATA phase - process batches until generator is closed
            while True:
                result = self._process_with_exception_handling(generator, input.batch)

                input = yield ProtocolOutput(
                    batch=result.batch,
                    log_message=result.log_message,
                )
                if input is None:
                    raise ValueError("Expected ProtocolInput, got None")
                if self._should_terminate(result):
                    return
        finally:
            generator.close()
            # Release resources after processing completes
            self.teardown()


class ScalarFunction(ScalarFunctionGenerator):
    """Base class for scalar functions using the compute() callback.

    This is the recommended API for scalar functions. Override output_type
    to define the output column type, and compute() to transform each batch.

    Scalar functions transform input rows to output values with 1:1 mapping.
    The output is always a single column named "result".

    Methods to Override
    -------------------
    output_type : pa.DataType (property)
        Return the Arrow type for the output column.

    compute(batch) : pa.Array
        Transform the input batch to a single output array.
        Must return an array with exactly batch.num_rows elements.

    setup() : None
        Called before processing. Acquire resources here.

    teardown() : None
        Called after processing. Release resources here.

    Logging
    -------
    Call self.log(level, message) from compute() to emit log messages:

        def compute(self, batch: pa.RecordBatch) -> pa.Array:
            self.log(Level.INFO, f"Processing {batch.num_rows} rows")
            return pc.multiply(batch.column("x"), 2)

    Example:
    -------
    A function that doubles the values in a specified column:

        class DoubleColumn(ScalarFunction):
            column = Arg[str](0, doc="Column to double")

            @property
            def output_type(self) -> pa.DataType:
                return self.input_schema.field(self.column).type

            def compute(self, batch: pa.RecordBatch) -> pa.Array:
                return pc.multiply(batch.column(self.column), 2)

    Available Attributes
    --------------------
    self.invocation : Invocation
        The complete invocation request with function name and arguments.

    self.input_schema : pa.Schema
        Schema of input batches.

    self.output_schema : pa.Schema
        Schema of output batches (single column named "result").

    self.empty_output_batch : pa.RecordBatch
        Empty batch conforming to output_schema.

    """

    _pending_messages: list[vgi.log.Message]

    def __init__(
        self,
        invocation: vgi.function.Invocation,
        logger: structlog.stdlib.BoundLogger,
    ):
        """Initialize the scalar function."""
        # Initialize pending messages before super().__init__ because
        # output_schema property may be accessed during init
        self._pending_messages = []
        super().__init__(invocation=invocation, logger=logger)

    def log(self, level: vgi.log.Level, message: str) -> None:
        """Queue a log message to be emitted with the output.

        Messages are yielded before the compute() result.

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR).
            message: Log message text.

        Example:
            def compute(self, batch: pa.RecordBatch) -> pa.Array:
                self.log(Level.INFO, f"Processing {batch.num_rows} rows")
                return pc.multiply(batch.column(self.column), 2)

        """
        self._pending_messages.append(vgi.log.Message(level=level, message=message))

    @property
    @abstractmethod
    def output_type(self) -> pa.DataType:
        """Return the Arrow type for the output column.

        Override this property to specify the output column type.

        Example:
            @property
            def output_type(self) -> pa.DataType:
                return pa.int64()

            # Or derive from input:
            @property
            def output_type(self) -> pa.DataType:
                return self.input_schema.field(self.column).type

        """
        ...

    @property
    @final
    def output_schema(self) -> pa.Schema:
        """Return single-column output schema. Do not override."""
        return pa.schema([pa.field("result", self.output_type)])

    @abstractmethod
    def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
        """Compute output array from input batch.

        Override this method to implement your scalar transformation.

        Args:
            batch: Input RecordBatch.

        Returns:
            Array with exactly batch.num_rows elements.

        Example:
            def compute(self, batch: pa.RecordBatch) -> pa.Array[Any]:
                return pc.multiply(batch.column("x"), 2)

        """
        ...

    @final
    def _yield_pending_messages(self) -> ScalarOutputGenerator:
        """Yield all pending log messages. Helper for process()."""
        while self._pending_messages:
            msg = self._pending_messages.pop(0)
            _ = yield msg

    @final
    def process(self, batch: pa.RecordBatch) -> ScalarOutputGenerator:
        """Convert compute() to generator protocol. Do not override.

        This method implements the generator protocol by calling your compute()
        method for each input batch.
        """
        # Priming yield
        _ = yield Output(self.empty_output_batch)

        while True:
            result = self.compute(batch)

            # Yield any pending log messages first
            yield from self._yield_pending_messages()

            # Create output batch from result array
            output = pa.RecordBatch.from_arrays([result], schema=self.output_schema)
            received = yield Output(output)

            if received is None:
                break
            batch = received
