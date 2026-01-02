"""Base classes for scalar functions that transform input batches to single-column.

Scalar functions transform input batches to single-column output.

Scalar functions receive input batches and produce output batches where:
1. Output row count must exactly match input row count (1:1 mapping)
2. Output schema has exactly one column

This module provides:
- ScalarFunctionGenerator: Generator-based base class (like TableInOutGeneratorFunction)
- ScalarFunction: Callback-based API with compute() method (like TableInOutFunction)

Class Hierarchy:
    TableFunctionBase (vgi.table_function)
        └── ScalarFunctionGenerator  (generator protocol, validates row count)
                └── ScalarFunction   (callback API with compute())

ScalarFunctionGenerator is useful for functions that need full generator control
including yielding log messages. For most use cases, use ScalarFunction with its
simpler compute() method.
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
import vgi.table_function
from vgi.table_function import RowCountMismatchError, SchemaValidationError

__all__ = [
    "ScalarFunctionGenerator",
    "ScalarFunction",
    "Output",
    "OutputGenerator",
    "ProtocolInput",
]


# Protocol types - reuse Output/OutputGenerator from table_in_out_function
from vgi.table_in_out_function import (  # noqa: E402
    Output,
    OutputGenerator,
    ProtocolOutput,
    _OutputStatus,
)


@dataclass(frozen=True, slots=True)
class ProtocolInput:
    """Input sent to the scalar function generator via send().

    This is a simplified version of table_in_out_function.ProtocolInput
    without finalization support, since scalar functions don't have a
    finalize phase.

    Attributes:
        batch: The input RecordBatch to process.
        metadata: Optional metadata from the IPC stream.

    """

    batch: pa.RecordBatch
    metadata: pa.KeyValueMetadata | None = None


@dataclass(frozen=True, slots=True)
class _ScalarOutputComplete:
    """Internal: Output with guaranteed non-None batch for scalar functions.

    Similar to _OutputComplete in table_in_out_function, but tracks the input
    batch for row count validation.
    """

    batch: pa.RecordBatch
    has_more: bool = False
    log_message: vgi.log.Message | None = None

    @classmethod
    def from_process_result(
        cls,
        source: vgi.log.Message | Output | None,
        empty_batch: pa.RecordBatch,
    ) -> _ScalarOutputComplete:
        """Create from user's yield value.

        Args:
            source: What the user yielded (Output, Message, or None).
            empty_batch: Empty batch to substitute when needed.

        Returns:
            Normalized output with guaranteed non-None batch.

        """
        if source is None:
            return cls(batch=empty_batch)
        if isinstance(source, vgi.log.Message):
            return cls(batch=empty_batch, has_more=True, log_message=source)
        # source is Output
        return cls(
            batch=source.batch if source.batch is not None else empty_batch,
            has_more=source.has_more,
        )


class ScalarFunctionGenerator(vgi.table_function.TableFunctionBase):
    """Base class for scalar functions with generator protocol.

    Scalar functions transform input batches to single-column output with
    1:1 row mapping. Unlike TableInOutGeneratorFunction, scalar functions:
    - Have no finalize() phase
    - Must produce exactly one output row per input row
    - Must have exactly one column in output_schema

    Override process() for full generator control. Can yield Output or Message:

        def process(self, batch: pa.RecordBatch) -> OutputGenerator:
            _ = yield None  # Priming yield
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

    METHODS TO OVERRIDE
    -------------------
    output_schema -> pa.Schema (property)
        Override to define the single-column output schema.

    process(batch: pa.RecordBatch) -> OutputGenerator
        Generator that processes input batches. Must yield Output with
        batch.num_rows matching input batch.num_rows.

    setup() -> None
        Called before processing starts. Default: no-op.

    teardown() -> None
        Called after processing completes. Default: no-op.

    AVAILABLE ATTRIBUTES
    --------------------
    self.invocation: Invocation   - The complete invocation request
    self.input_schema: pa.Schema  - Input schema (from invocation)
    self.output_schema: pa.Schema - Property returning the output schema
    self.empty_output_batch       - Empty batch conforming to output_schema
    """

    def __init__(
        self,
        invocation: vgi.function.Invocation,
        logger: structlog.stdlib.BoundLogger,
    ):
        """Initialize the scalar function with invocation data and logger."""
        super().__init__(invocation=invocation, logger=logger)
        if invocation.in_out_function_input_schema is None:
            raise ValueError(
                f"{type(self).__name__} requires an input schema, but none was "
                f"provided. ScalarFunction processes input batches and requires "
                f"in_out_function_input_schema to be set in the Invocation."
            )
        # Validate single-column output at construction
        if len(self.output_schema) != 1:
            raise SchemaValidationError(
                f"ScalarFunction must have exactly 1 output column, "
                f"got {len(self.output_schema)}: {self.output_schema}"
            )

    @property
    def input_schema(self) -> pa.Schema:
        """Return the input schema from the invocation."""
        # Validated as non-None in __init__
        assert self.invocation.in_out_function_input_schema is not None
        return self.invocation.in_out_function_input_schema

    def teardown(self) -> None:
        """Release resources after processing completes.

        Override to release resources acquired in setup().
        Always called, even if an error occurred during processing.
        """
        pass

    @final
    def _validate_input_schema(self, batch: pa.RecordBatch) -> None:
        """Validate that a batch conforms to the expected input schema."""
        if batch.schema != self.input_schema:
            raise SchemaValidationError(
                f"Input batch schema does not match expected input_schema. "
                f"Expected: {self.input_schema}, got: {batch.schema}"
            )

    @final
    def _validate_row_count(
        self, output_batch: pa.RecordBatch, input_batch: pa.RecordBatch
    ) -> None:
        """Validate that output row count matches input row count."""
        if output_batch.num_rows != input_batch.num_rows:
            raise RowCountMismatchError(
                f"ScalarFunction output must have same row count as input. "
                f"Input: {input_batch.num_rows}, Output: {output_batch.num_rows}"
            )

    @final
    def _process_and_validate(
        self,
        generator: OutputGenerator,
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
        # Only validate row count for actual output, not log messages
        if result.log_message is None and result.batch.num_rows > 0:
            self._validate_row_count(result.batch, input_batch)
        return result

    @final
    def _process_with_exception_handling(
        self,
        generator: OutputGenerator,
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
    def process(self, batch: pa.RecordBatch) -> OutputGenerator:
        """Process input batches.

        Override this method to implement your scalar transformation.
        The generator must yield Output with batch.num_rows matching
        input batch.num_rows.

        Args:
            batch: First input batch (subsequent batches via yield return).

        Yields:
            Output: Batch with same row count as input.
            Message: Log message (input will be re-sent).
            None: No output (ready for next batch).

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
        input: ProtocolInput | None = yield ProtocolOutput(
            batch=None, status=_OutputStatus.NEED_MORE_INPUT
        )
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

                # Determine status based on result
                has_more_output = result.has_more or result.log_message is not None
                if has_more_output:
                    status = _OutputStatus.HAVE_MORE_OUTPUT
                else:
                    status = _OutputStatus.NEED_MORE_INPUT

                input = yield ProtocolOutput(
                    batch=result.batch,
                    status=status,
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
    """Simplified base class using compute() callback instead of generators.

    This class provides a simpler API for scalar functions. Instead of
    implementing process() as a generator, you override compute() as a
    regular method that returns a single Array.

    METHODS TO OVERRIDE
    -------------------
    output_type -> pa.DataType (property)
        Return the Arrow type for the output column.

    compute(batch) -> pa.Array
        Transform the input batch to a single output array.
        Must return an array with exactly batch.num_rows elements.

    output_name -> str (property, optional)
        Return the name of the output column. Default: "result"

    LOGGING
    -------
    Call self.log(level, message) from compute() to emit log messages:

        def compute(self, batch: pa.RecordBatch) -> pa.Array:
            self.log(Level.INFO, f"Processing {batch.num_rows} rows")
            return pc.multiply(batch.column("x"), 2)

    Example:
    -------
        class DoubleColumn(ScalarFunction):
            column = Arg[str](0, doc="Column to double")

            @property
            def output_type(self) -> pa.DataType:
                return self.input_schema.field(self.column).type

            def compute(self, batch: pa.RecordBatch) -> pa.Array:
                return pc.multiply(batch.column(self.column), 2)

    """

    # Message queue for log() method (same pattern as TableInOutFunction)
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
    def output_name(self) -> str:
        """Return the name of the output column. Override to customize."""
        return "result"

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
        return pa.schema([pa.field(self.output_name, self.output_type)])

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
    def _yield_pending_messages(self) -> OutputGenerator:
        """Yield all pending log messages. Helper for process()."""
        while self._pending_messages:
            msg = self._pending_messages.pop(0)
            _ = yield msg

    @final
    def process(self, batch: pa.RecordBatch) -> OutputGenerator:
        """Convert compute() to generator protocol. Do not override.

        This method implements the generator protocol by calling your compute()
        method for each input batch.
        """
        _ = yield None  # Priming yield

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
