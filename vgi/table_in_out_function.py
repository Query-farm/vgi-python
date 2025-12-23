"""Framework for implementing streaming table-in-table-out functions.

This module provides the base class and decorator for creating functions that
transform Arrow RecordBatch streams. Functions receive batches via a generator
protocol and can buffer, filter, transform, or aggregate data.

Protocol Overview:
    1. BIND: Function is instantiated and returns output schema + generator
    2. DATA: Input batches are sent via generator.send(), outputs are yielded
    3. FINALIZE: Signal sent to flush any buffered data

Key Components:
    TableInOutFunction: Base class to subclass for custom functions.
    @table_in_out_function: Decorator that wraps the class into a callable.
    ProcessResult: Return type for process_batch() with batch and has_more flag.
    FunctionInput/FunctionOutput: Protocol messages for the generator.
    OutputStatus: Enum indicating generator state after each yield.

Quick Start:
    @table_in_out_function
    class MyFunction(TableInOutFunction):
        def process_batch(self, batch, is_finalize):
            if is_finalize:
                return ProcessResult(None)
            # Transform batch here
            return ProcessResult(transformed_batch)

See TableInOutFunction docstring for comprehensive documentation and examples.
"""

from collections.abc import Callable, Generator
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from typing import ClassVar, cast, final

import pyarrow as pa

import vgi.function
import vgi.table_function

__all__ = [
    "SchemaValidationError",
    "OutputStatus",
    "FunctionInput",
    "FunctionOutput",
    "ProcessResult",
    "TableInOutFunction",
    "TableInOutFunctionCallable",
    "BindResult",
    "table_in_out_function",
]


class OutputStatus(Enum):
    """Status returned with each FunctionOutput to indicate the generator's state.

    Values:
        NEED_MORE_INPUT: Ready for the next input batch (DATA phase).
        HAVE_MORE_OUTPUT: Call send() again to get more output from the current
            input, or to retrieve a log message.
        FINISHED: Processing complete, no more output will be produced.
    """

    NEED_MORE_INPUT = "NEED_MORE_INPUT"
    HAVE_MORE_OUTPUT = "HAVE_MORE_OUTPUT"
    FINISHED = "FINISHED"


@dataclass(frozen=True, slots=True)
class FunctionInput:
    """Input sent to the generator via send().

    Attributes:
        batch: The input RecordBatch to process.
        metadata: Optional metadata; used to signal the FINALIZE phase.
    """

    # pa.KeyValueMetadata uses bytes so we define the finalize signal as bytes
    _FINALIZE_SIGNAL: ClassVar[bytes] = b"FINALIZE"

    batch: pa.RecordBatch
    metadata: pa.KeyValueMetadata | None = None

    @property
    def is_finalize(self) -> bool:
        """Check if this input signals the FINALIZE phase."""
        return (
            self.metadata is not None
            and self.metadata.get("type") == self._FINALIZE_SIGNAL
        )

    @classmethod
    def create_finalize(cls, batch: pa.RecordBatch) -> "FunctionInput":
        """Create a FunctionInput that signals the FINALIZE phase.

        This is only sent once so there is no benefit to caching it.
        """
        return cls(
            batch=batch, metadata=pa.KeyValueMetadata({"type": cls._FINALIZE_SIGNAL})
        )


@dataclass(frozen=True, slots=True)
class FunctionOutput:
    """Output yielded by the generator after each send().

    Attributes:
        batch: The output RecordBatch. None only for the initial priming yield;
            during normal operation, None batches are replaced with empty batches.
        status: The generator's state after this yield (see OutputStatus).
        log_message: Optional log or error message associated with this output.
    """

    batch: pa.RecordBatch | None
    status: OutputStatus | None
    log_message: vgi.function.LogMessage | None = None

    def metadata(self, call_data: vgi.function.CallData) -> pa.KeyValueMetadata | None:
        """Create metadata for this output based on the status.

        Args:
            call_data: The CallData for this function invocation, passed through
                to LogMessage.add_to_metadata() for correlation information.

        Returns:
            KeyValueMetadata containing status and optional log message fields,
            or None if status is None (only for the initial priming yield).
        """
        if self.status is None:
            return None

        metadata_dict = {"status": self.status.value}

        if self.log_message is not None:
            metadata_dict = self.log_message.add_to_metadata(call_data, metadata_dict)

        return pa.KeyValueMetadata(metadata_dict)

    @classmethod
    def from_process_result(
        cls, process_result: "ProcessResultComplete", in_finalize_phase: bool
    ) -> "FunctionOutput":
        """Create a FunctionOutput from a ProcessResult and status.

        Args:
            process_result: The result from process_batch().
            in_finalize_phase: Whether we are in the FINALIZE phase.
        """
        has_more_output = (
            process_result.has_more or process_result.log_message is not None
        )

        if has_more_output:
            status = OutputStatus.HAVE_MORE_OUTPUT
        elif in_finalize_phase:
            status = OutputStatus.FINISHED
        else:
            status = OutputStatus.NEED_MORE_INPUT
        return cls(
            batch=process_result.batch,
            status=status,
            log_message=process_result.log_message,
        )


@dataclass(frozen=True, slots=True)
class BindResult(vgi.table_function.TableFunctionBindResult):
    """Complete bind result for TableInOutFunction, including the processing generator.

    Extends TableFunctionBindResult with the generator that implements the
    streaming DATA -> FINALIZE protocol. The caller interacts with the function
    by sending FunctionInput objects and receiving FunctionOutput objects.

    Attributes:
        output_schema: Arrow schema for output batches (inherited).
        max_processes: Parallelization hint (inherited).
        call_identifier: Unique call ID (inherited).
        cardinality: Optional row count estimates (inherited).
        generator: The generator implementing the streaming protocol.
            - Must be primed with next() before use
            - Accepts FunctionInput via send()
            - Yields FunctionOutput with batch and status

    Usage:
        call_data = vgi.function.CallData(
            function_name="my_function",
            arguments=[],
            in_schema=input_schema,
        )
        bind_result = MyFunction(call_data)
        next(bind_result.generator)  # Prime
        output = bind_result.generator.send(FunctionInput(batch=data))
    """

    generator: Generator[FunctionOutput, FunctionInput, None]


class SchemaValidationError(Exception):
    """Raised when a batch schema doesn't match the expected schema.

    This error is raised by the framework during input/output validation.
    It indicates a programming error where a batch doesn't conform to the
    declared schema.
    """


@dataclass(frozen=True, slots=True)
class ProcessResult:
    """Result returned by process_batch().

    Attributes:
        batch: The output RecordBatch, or None to emit an empty batch.
        has_more: If True, process_batch will be called again with the same input.
            Use this to produce multiple output batches from a single input.
        log_message: Optional log message to send with this output. When present,
            the framework emits an empty batch with the log message before
            continuing (or terminating if the log level is EXCEPTION).

    Examples:
        # Normal processing - emit one batch per input
        ProcessResult(transformed_batch)

        # Emit multiple batches from one input
        ProcessResult(first_batch, has_more=True)  # Will be called again
        ProcessResult(second_batch, has_more=False)  # Done with this input
    """

    batch: pa.RecordBatch | None
    has_more: bool = False
    log_message: vgi.function.LogMessage | None = None


@dataclass(frozen=True, slots=True)
class ProcessResultComplete(ProcessResult):
    """A ProcessResult with a guaranteed non-None batch.

    Used internally by the framework to ensure the generator always yields
    a valid RecordBatch. When a ProcessResult has a None batch or contains
    a log message, an empty batch is substituted.

    Attributes:
        batch: Always a valid RecordBatch (never None).
        has_more: Inherited from ProcessResult; set to True when a log message
            is present to ensure the message is delivered.
        log_message: Inherited from ProcessResult.
    """

    batch: pa.RecordBatch

    @classmethod
    def from_process_result(
        cls, source: ProcessResult, empty_batch: pa.RecordBatch
    ) -> "ProcessResultComplete":
        """Create a ProcessResultComplete from a ProcessResult.

        Args:
            source: The original ProcessResult from process_batch().
            empty_batch: An empty batch to use when source.batch is None
                or when a log message is present.

        Returns:
            A ProcessResultComplete with a guaranteed non-None batch.
        """
        return cls(
            batch=empty_batch
            if source.batch is None or source.log_message is not None
            else source.batch,
            has_more=True if source.log_message is not None else source.has_more,
            log_message=source.log_message,
        )


class TableInOutFunction(vgi.table_function.TableFunction):
    """Base class for streaming table functions that transform Arrow RecordBatches.

    OVERVIEW
    --------
    Subclass this to create table functions that receive a stream of input batches
    and produce a stream of output batches. The framework handles all protocol state
    management, including exception handling - you only implement the data
    transformation logic.

    LIFECYCLE
    ---------
    1. BIND: The decorator calls _output_schema() and returns a BindResult
       containing the output schema, cardinality info, and generator.

    2. DATA: Your process_batch(batch, is_finalize=False) is called for each input
       batch. Return ProcessResult(batch, has_more). If has_more=True, you'll be
       called again with the same input to produce more output.

    3. FINALIZE: Your process_batch(batch, is_finalize=True) is called repeatedly
       after all input until has_more=False. The batch will be an empty batch.
       Return buffered/aggregated results. Set has_more=True to emit multiple batches.

    METHODS TO OVERRIDE
    -------------------
    _output_schema() -> pa.Schema
        Called lazily when output_schema property is first accessed. Use this to:
        - Inspect self.input_schema to see what columns are available
        - Decide what columns your output will have
        - Initialize any processing state (accumulators, buffers, etc.)
        - Return the output schema
        Default: returns self.input_schema unchanged (passthrough)

    process_batch(batch, is_finalize) -> ProcessResult
        Called for each input batch during DATA phase (is_finalize=False), and
        called repeatedly during FINALIZE phase (is_finalize=True) until has_more
        is False. Returns a ProcessResult with:
        - batch: A RecordBatch conforming to output_schema, or None for empty
        - has_more: If True, you will be called again with the SAME input batch to
          produce more output. Set False when done with this input/finalization.
        - log_message: Optional LogMessage for logging or error reporting
        Default: returns ProcessResult(batch) during DATA (only valid when input/output
        schemas match), returns ProcessResult(None) during FINALIZE

    AVAILABLE ATTRIBUTES
    --------------------
    self.arguments: list[Any]     - Arguments passed to the function
    self.input_schema: pa.Schema  - Schema of incoming batches
    self.output_schema: pa.Schema - Cached property calling _output_schema()

    HELPER PROPERTIES/METHODS
    -------------------------
    self.empty_output_batch: pa.RecordBatch
        Returns an empty batch conforming to output_schema (cached). Use when you
        need to signal "no output for this input" - return ProcessResult(None) is
        equivalent.

    self.empty_input_batch() -> pa.RecordBatch
        Returns an empty batch conforming to input_schema. Useful for creating the
        finalize signal: FunctionInput.create_finalize(self.empty_input_batch())

    CALLER PROTOCOL
    ---------------
    To use a decorated TableInOutFunction, the caller must:

    1. Create the bind result:
       call_data = vgi.function.CallData(
           function_name="my_function",
           arguments=[],
           in_schema=input_schema,
       )
       bind_result = MyFunction(call_data)

    2. Prime the generator:
       next(bind_result.generator)  # Returns FunctionOutput(batch=None, status=None)

    3. Send inputs and receive outputs in a loop:
       output = bind_result.generator.send(FunctionInput(batch=input_batch))
       # Check output.status:
       #   - OutputStatus.NEED_MORE_INPUT: Send next input batch
       #   - OutputStatus.HAVE_MORE_OUTPUT: Call send() again (input is ignored)

    4. Signal finalization:
       output = bind_result.generator.send(FunctionInput.create_finalize(empty_batch))
       # Check output.status:
       #   - OutputStatus.HAVE_MORE_OUTPUT: Call send() again to get more output
       #   - OutputStatus.FINISHED: Stop iteration

    EXAMPLES
    --------
    Note: Examples below assume `import pyarrow.compute as pc`

    Example 1: Passthrough (no transformation)
    ------------------------------------------
    @table_in_out_function
    class PassthroughFunction(TableInOutFunction):
        pass  # Default behavior passes everything through

    Example 2: Filter rows (1:1 mapping, possibly fewer rows)
    ---------------------------------------------------------
    @table_in_out_function
    class FilterPositiveFunction(TableInOutFunction):
        def process_batch(self, batch, is_finalize):
            if is_finalize:
                return ProcessResult(None)
            mask = pc.greater(batch.column("value"), 0)
            return ProcessResult(pc.filter(batch, mask))

    Example 3: Transform schema (different output columns)
    ------------------------------------------------------
    @table_in_out_function
    class AddComputedColumnFunction(TableInOutFunction):
        def _output_schema(self) -> pa.Schema:
            # Add a new column to the output schema
            return pa.schema(list(self.input_schema) + [
                pa.field("doubled", pa.int64())
            ])

        def process_batch(self, batch, is_finalize):
            if is_finalize:
                return ProcessResult(None)
            doubled = pc.multiply(batch.column("value"), 2)
            return ProcessResult(pa.RecordBatch.from_arrays(
                list(batch.columns) + [doubled],
                schema=self.output_schema
            ))

    Example 4: Aggregation (buffer inputs, emit on finalize)
    --------------------------------------------------------
    @table_in_out_function
    class SumAllColumnsFunction(TableInOutFunction):
        def _output_schema(self) -> pa.Schema:
            # Build output schema from numeric input columns
            self.sums: dict[str, pa.Scalar] = {}
            output_fields = []
            for field in self.input_schema:
                if pa.types.is_integer(field.type):
                    out_type = pa.int64()
                elif pa.types.is_floating(field.type):
                    out_type = pa.float64()
                else:
                    continue  # Skip non-numeric columns
                output_fields.append(pa.field(field.name, out_type))
                self.sums[field.name] = pa.scalar(0, type=out_type)
            return pa.schema(output_fields)

        def process_batch(self, batch, is_finalize):
            if is_finalize:
                # Emit final sums as a single row
                return ProcessResult(pa.RecordBatch.from_pydict(
                    {name: [val] for name, val in self.sums.items()},
                    schema=self.output_schema,
                ))
            # Accumulate sums, emit nothing yet
            for name in self.sums:
                col_sum = pc.sum(batch.column(name))
                if col_sum.is_valid:
                    self.sums[name] = pc.add(self.sums[name], col_sum)
            return ProcessResult(None)

    Example 5: Explode (one input produces multiple outputs)
    --------------------------------------------------------
    @table_in_out_function
    class RepeatFunction(TableInOutFunction):
        def __init__(self, call_data: vgi.function.CallData):
            super().__init__(call_data)
            self.repeat_count = self.arguments[0] if self.arguments else 2
            self.current_repeat = 0

        def process_batch(self, batch, is_finalize):
            if is_finalize:
                return ProcessResult(None)
            self.current_repeat += 1
            has_more = self.current_repeat < self.repeat_count
            if not has_more:
                self.current_repeat = 0  # Reset for next input batch
            return ProcessResult(batch, has_more)

    Example 6: Buffer and emit on finalize (multiple output batches)
    ----------------------------------------------------------------
    @table_in_out_function
    class BufferFunction(TableInOutFunction):
        def __init__(self, call_data: vgi.function.CallData):
            super().__init__(call_data)
            self.buffered: list[pa.RecordBatch] = []
            self.finalize_index = 0

        def process_batch(self, batch, is_finalize):
            if is_finalize:
                if self.finalize_index < len(self.buffered):
                    out = self.buffered[self.finalize_index]
                    self.finalize_index += 1
                    has_more = self.finalize_index < len(self.buffered)
                    return ProcessResult(out, has_more)
                return ProcessResult(None)
            self.buffered.append(batch)
            return ProcessResult(None)
    """

    def __init__(self, call_data: vgi.function.CallData):
        super().__init__(call_data)
        self.arguments = call_data.arguments
        if call_data.in_schema is None:
            raise ValueError("TableInOutFunction requires a non-null input schema")
        self.input_schema = call_data.in_schema

    @final
    @cached_property
    def output_schema(self) -> pa.Schema:
        """Output schema, computed lazily via _output_schema() on first access."""
        return self._output_schema()

    @final
    @cached_property
    def empty_output_batch(self) -> pa.RecordBatch:
        """Return an empty batch conforming to output_schema. Cached."""
        return pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in self.output_schema],
            schema=self.output_schema,
        )

    def _output_schema(self) -> pa.Schema:
        """Return the output schema. Called lazily via the output_schema property.

        Override to transform the schema or initialize processing state.
        Default: returns input_schema unchanged (passthrough).
        """
        return self.input_schema

    @final
    def empty_input_batch(self) -> pa.RecordBatch:
        """Return an empty batch conforming to input_schema.

        Useful for creating the finalize signal:
            FunctionInput.create_finalize(self.empty_input_batch())
        """
        return pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in self.input_schema],
            schema=self.input_schema,
        )

    @final
    def _validate_input_schema(self, batch: pa.RecordBatch) -> None:
        """Validate that a batch conforms to the expected input schema."""
        if batch.schema != self.input_schema:
            raise SchemaValidationError(
                f"Input batch schema does not match expected input_schema. "
                f"Expected: {self.input_schema}, got: {batch.schema}"
            )

    @final
    def _validate_output_schema(self, batch: pa.RecordBatch) -> None:
        """Validate that a batch conforms to the expected output schema."""
        if batch.schema != self.output_schema:
            raise SchemaValidationError(
                f"Output batch schema does not match expected output_schema. "
                f"Expected: {self.output_schema}, got: {batch.schema}"
            )

    @final
    def _process_and_validate(
        self, batch: pa.RecordBatch, is_finalize: bool
    ) -> ProcessResultComplete:
        """Process a batch and validate both input and output schemas.

        Validates the input batch schema, calls process_batch(), converts
        the result to ProcessResultComplete, and validates the output schema.

        Args:
            batch: The input RecordBatch to process.
            is_finalize: Whether this is a finalization call.

        Returns:
            ProcessResultComplete with validated output batch.

        Raises:
            SchemaValidationError: If input or output batch schema doesn't match.
        """
        self._validate_input_schema(batch)
        result = ProcessResultComplete.from_process_result(
            self.process_batch(batch, is_finalize), self.empty_output_batch
        )
        self._validate_output_schema(result.batch)
        return result

    @final
    def _process_with_exception_handling(
        self, batch: pa.RecordBatch, is_finalize: bool
    ) -> ProcessResultComplete:
        """Process a batch with exception handling.

        Wraps _process_and_validate to catch exceptions and convert them
        to ProcessResultComplete with an error log message.
        """
        try:
            return self._process_and_validate(batch, is_finalize=is_finalize)
        except Exception as e:
            return ProcessResultComplete(
                batch=self.empty_output_batch,
                log_message=vgi.function.LogMessage.from_exception(e),
            )

    @final
    def _should_terminate(self, result: ProcessResultComplete) -> bool:
        """Check if processing should terminate due to an exception."""
        return (
            result.log_message is not None
            and result.log_message.level == vgi.function.LogLevel.EXCEPTION
        )

    def process_batch(self, batch: pa.RecordBatch, is_finalize: bool) -> ProcessResult:
        """Process an input batch or handle finalization.

        Args:
            batch: The input RecordBatch to process. During finalize, this will
                   be an empty batch conforming to the input schema.
            is_finalize: True when called during FINALIZE phase.

        Returns:
            A ProcessResult with:
            - batch: A RecordBatch conforming to output_schema, or None
            - has_more: If True, will be called again with the same input

        Default: returns ProcessResult(batch) during DATA (only valid when input/output
        schemas match), returns ProcessResult(None) during FINALIZE.
        """
        if is_finalize:
            return ProcessResult(None)
        return ProcessResult(batch)

    @final
    def run(self) -> Generator[FunctionOutput, FunctionInput, None]:
        """Run the function protocol. Do not override.

        This generator implements the DATA* -> FINALIZE lifecycle:

        1. DATA: Receives input batches via send(), calls process_batch()
           with is_finalize=False, and yields outputs. Continues until
           caller sends metadata with type="FINALIZE".

        2. FINALIZE: Calls process_batch() with is_finalize=True repeatedly
           until has_more=False, then yields FINISHED status.
        """
        # Prime the generator - caller must call next() first
        function_input: FunctionInput = yield FunctionOutput(batch=None, status=None)

        # DATA phase
        while not function_input.is_finalize:
            result = self._process_with_exception_handling(
                function_input.batch, is_finalize=False
            )
            function_input = yield FunctionOutput.from_process_result(
                result, in_finalize_phase=False
            )
            if self._should_terminate(result):
                return

        # FINALIZE phase
        while True:
            result = self._process_with_exception_handling(
                function_input.batch, is_finalize=True
            )
            if result.has_more:
                function_input = yield FunctionOutput.from_process_result(
                    result, in_finalize_phase=True
                )
                if self._should_terminate(result):
                    return
            else:
                yield FunctionOutput.from_process_result(result, in_finalize_phase=True)
                return


# Type alias for decorated table function callables
type TableInOutFunctionCallable = Callable[[vgi.function.CallData], BindResult]


def table_in_out_function(cls: type[TableInOutFunction]) -> TableInOutFunctionCallable:
    """Decorator to convert a TableInOutFunction class into a callable.

    The decorated class becomes a callable that accepts a CallData object
    and returns a BindResult containing the output schema and generator.

    Usage:
        @table_in_out_function
        class MyFunction(TableInOutFunction):
            def process_batch(self, batch, is_finalize):
                ...

        # Create CallData and call the decorated function:
        call_data = vgi.function.CallData(
            function_name="my_function",
            arguments=[],
            in_schema=input_schema,
        )
        bind_result = MyFunction(call_data)  # Returns BindResult
        next(bind_result.generator)  # Prime the generator
        output = bind_result.generator.send(FunctionInput(batch=data))
    """

    def wrapper(call_data: vgi.function.CallData) -> BindResult:
        fn = cls(call_data)
        return BindResult(
            output_schema=fn.output_schema,
            max_processes=fn.max_processes(),
            cardinality=fn.cardinality(),
            call_identifier=fn.call_identifier(),
            generator=fn.run(),
        )

    return cast(TableInOutFunctionCallable, wrapper)
