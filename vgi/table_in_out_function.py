"""Framework for implementing streaming table-in-table-out functions.

This module provides the base class for creating functions that transform
Arrow RecordBatch streams. Functions receive batches via a generator protocol
and can buffer, filter, transform, or aggregate data.

Protocol Overview:
    1. BIND: Function is instantiated and returns output schema + generator
    2. DATA: Input batches are sent via generator.send(), outputs are yielded
    3. FINALIZE: Signal sent to flush any buffered data

Key Components:
    TableInOutFunction: Base class to subclass for custom functions.
    ProcessResult: Return type for process_batches() with batch and has_more flag.
    ProcessInput: Input type sent to process_batches() with batch and is_finalize flag.
    FunctionInput/FunctionOutput: Protocol messages for the generator.
    OutputStatus: Enum indicating generator state after each yield.

Quick Start:
    from collections.abc import Generator

    class MyFunction(TableInOutFunction):
        def process_batches(
            self,
        ) -> Generator[ProcessResult, ProcessInput | None, None]:
            _ = yield ProcessResult(None)  # Priming yield
            while True:
                input = yield ProcessResult(None)
                if input is None:
                    raise ValueError("Expected ProcessInput, got None")
                if input.is_finalize:
                    break
                # Transform input.batch here
                yield ProcessResult(transformed_batch)

See TableInOutFunction docstring for comprehensive documentation and examples.
"""

from collections.abc import Generator
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from typing import ClassVar, final

import pyarrow as pa
import structlog

import vgi.function
import vgi.table_function

__all__ = [
    "SchemaValidationError",
    "OutputStatus",
    "FunctionInput",
    "FunctionOutput",
    "ProcessResult",
    "TableInOutFunction",
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
            process_result: The result from process_batches().
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


class SchemaValidationError(Exception):
    """Raised when a batch schema doesn't match the expected schema.

    This error is raised by the framework during input/output validation.
    It indicates a programming error where a batch doesn't conform to the
    declared schema.
    """


@dataclass(frozen=True, slots=True)
class ProcessResult:
    """Result yielded by process_batches() generator.

    Attributes:
        batch: The output RecordBatch, or None to emit an empty batch.
        has_more: If True, the generator will receive another send() call.
            Use this to produce multiple output batches from a single input.
        log_message: Optional log message to send with this output. When present,
            the framework emits an empty batch with the log message before
            continuing (or terminating if the log level is EXCEPTION).

    Examples:
        # Normal processing - emit one batch per input
        yield ProcessResult(transformed_batch)

        # Emit multiple batches from one input
        yield ProcessResult(first_batch, has_more=True)  # Will receive send()
        yield ProcessResult(second_batch, has_more=False)  # Done with this input
    """

    batch: pa.RecordBatch | None
    has_more: bool = False
    log_message: vgi.function.LogMessage | None = None


@dataclass(frozen=True, slots=True)
class ProcessInput:
    batch: pa.RecordBatch
    is_finalize: bool


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
            source: The original ProcessResult from process_batches().
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
    transformation logic via the process_batches() generator.

    LIFECYCLE
    ---------
    1. BIND: The class is instantiated and output_schema property is accessed to
       get the output schema.

    2. DATA: Your process_batches() generator receives ProcessInput objects with
       is_finalize=False. Yield ProcessResult(batch, has_more) for each input.
       If has_more=True, you'll receive another send() with the same logical input.

    3. FINALIZE: Your generator receives ProcessInput with is_finalize=True.
       Yield buffered/aggregated results. Set has_more=True to emit multiple batches.

    METHODS TO OVERRIDE
    -------------------
    output_schema -> pa.Schema (property)
        Override to define the output schema. Use this to:
        - Inspect self.input_schema to see what columns are available
        - Decide what columns your output will have
        Default: returns self.input_schema unchanged (passthrough)

    process_batches() -> Generator[ProcessResult, ProcessInput | None, None]
        Generator that processes input batches. Must:
        1. Yield an initial ProcessResult(None) for priming
        2. Loop receiving ProcessInput via yield, yielding ProcessResult
        3. Check input.is_finalize to detect finalization phase
        4. Exit the generator when done processing

        The ProcessInput contains:
        - batch: The input RecordBatch to process
        - is_finalize: True when no more input batches are coming

        The ProcessResult contains:
        - batch: A RecordBatch conforming to output_schema, or None for empty
        - has_more: If True, you will receive another send() call
        - log_message: Optional LogMessage for logging or error reporting

        Default: passes input batches through unchanged (passthrough)

    AVAILABLE ATTRIBUTES
    --------------------
    self.arguments: Arguments     - Arguments passed to the function
    self.input_schema: pa.Schema  - Schema of incoming batches
    self.output_schema: pa.Schema - Property returning the output schema

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
    To use a TableInOutFunction, the caller must:

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
    Note: Examples below assume:
        from collections.abc import Generator
        import pyarrow.compute as pc

    Example 1: Passthrough (no transformation)
    ------------------------------------------
    class PassthroughFunction(TableInOutFunction):
        pass  # Default process_batches passes everything through

    Example 2: Filter rows (1:1 mapping, possibly fewer rows)
    ---------------------------------------------------------
    class FilterPositiveFunction(TableInOutFunction):
        def process_batches(
            self,
        ) -> Generator[ProcessResult, ProcessInput | None, None]:
            _ = yield ProcessResult(None)  # Priming yield
            while True:
                input = yield ProcessResult(None)
                if input is None:
                    raise ValueError("Expected ProcessInput")
                if input.is_finalize:
                    break
                mask = pc.greater(input.batch.column("value"), 0)
                yield ProcessResult(pc.filter(input.batch, mask))

    Example 3: Transform schema (different output columns)
    ------------------------------------------------------
    class AddComputedColumnFunction(TableInOutFunction):
        @property
        def output_schema(self) -> pa.Schema:
            return pa.schema(list(self.input_schema) + [
                pa.field("doubled", pa.int64())
            ])

        def process_batches(
            self,
        ) -> Generator[ProcessResult, ProcessInput | None, None]:
            _ = yield ProcessResult(None)
            while True:
                input = yield ProcessResult(None)
                if input is None:
                    raise ValueError("Expected ProcessInput")
                if input.is_finalize:
                    break
                doubled = pc.multiply(input.batch.column("value"), 2)
                yield ProcessResult(pa.RecordBatch.from_arrays(
                    list(input.batch.columns) + [doubled],
                    schema=self.output_schema
                ))

    Example 4: Aggregation (buffer inputs, emit on finalize)
    --------------------------------------------------------
    class SumAllColumnsFunction(TableInOutFunction):
        @property
        def output_schema(self) -> pa.Schema:
            output_fields = []
            for field in self.input_schema:
                if pa.types.is_integer(field.type):
                    output_fields.append(pa.field(field.name, pa.int64()))
                elif pa.types.is_floating(field.type):
                    output_fields.append(pa.field(field.name, pa.float64()))
            return pa.schema(output_fields)

        def process_batches(
            self,
        ) -> Generator[ProcessResult, ProcessInput | None, None]:
            sums: dict[str, pa.Scalar] = {
                f.name: pa.scalar(0, type=f.type) for f in self.output_schema
            }
            _ = yield ProcessResult(None)

            while True:
                input = yield ProcessResult(None)
                if input is None:
                    raise ValueError("Expected ProcessInput")
                if input.is_finalize:
                    # Emit final sums as a single row
                    yield ProcessResult(pa.RecordBatch.from_pydict(
                        {name: [val] for name, val in sums.items()},
                        schema=self.output_schema,
                    ))
                    break
                # Accumulate sums
                for name in sums:
                    col_sum = pc.sum(input.batch.column(name))
                    if col_sum.is_valid:
                        sums[name] = pc.add(sums[name], col_sum)

    Example 5: Explode (one input produces multiple outputs)
    --------------------------------------------------------
    class RepeatFunction(TableInOutFunction):
        def __init__(self, call_data, logger):
            super().__init__(call_data, logger)
            self.repeat_count = self.arguments.positional[0].as_py()

        def process_batches(
            self,
        ) -> Generator[ProcessResult, ProcessInput | None, None]:
            _ = yield ProcessResult(None)
            while True:
                input = yield ProcessResult(None)
                if input is None:
                    raise ValueError("Expected ProcessInput")
                if input.is_finalize:
                    break
                # Emit the same batch repeat_count times
                for i in range(self.repeat_count):
                    has_more = i < self.repeat_count - 1
                    yield ProcessResult(input.batch, has_more)

    Example 6: Buffer and emit on finalize (multiple output batches)
    ----------------------------------------------------------------
    class BufferFunction(TableInOutFunction):
        def process_batches(
            self,
        ) -> Generator[ProcessResult, ProcessInput | None, None]:
            buffered: list[pa.RecordBatch] = []
            _ = yield ProcessResult(None)

            while True:
                input = yield ProcessResult(None)
                if input is None:
                    raise ValueError("Expected ProcessInput")
                if input.is_finalize:
                    break
                buffered.append(input.batch)

            # Emit all buffered batches during finalize
            for i, batch in enumerate(buffered):
                has_more = i < len(buffered) - 1
                yield ProcessResult(batch, has_more)
    """

    def __init__(
        self, call_data: vgi.function.CallData, logger: structlog.stdlib.BoundLogger
    ):
        super().__init__(call_data=call_data, logger=logger)
        self.arguments = call_data.arguments
        if call_data.in_schema is None:
            raise ValueError("TableInOutFunction requires a non-null input schema")
        self.input_schema = call_data.in_schema

    @final
    @cached_property
    def empty_output_batch(self) -> pa.RecordBatch:
        """Return an empty batch conforming to output_schema. Cached."""
        return pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in self.output_schema],
            schema=self.output_schema,
        )

    @property
    def output_schema(self) -> pa.Schema:
        """Return the output schema. Not cached since it can change
        if the init data is available or not.

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
        self,
        generator: Generator[ProcessResult, ProcessInput, None],
        batch: pa.RecordBatch,
        is_finalize: bool,
    ) -> ProcessResultComplete:
        """Process a batch and validate both input and output schemas.

        Validates the input batch schema, calls process_batch(), converts
        the result to ProcessResultComplete, and validates the output schema.

        Args:
            init_data: The global initialization data.
            batch: The input RecordBatch to process.
            is_finalize: Whether this is a finalization call.

        Returns:
            ProcessResultComplete with validated output batch.

        Raises:
            SchemaValidationError: If input or output batch schema doesn't match.
        """
        self._validate_input_schema(batch)
        result = ProcessResultComplete.from_process_result(
            generator.send(ProcessInput(batch=batch, is_finalize=is_finalize)),
            self.empty_output_batch,
        )
        self._validate_output_schema(result.batch)
        return result

    @final
    def _process_with_exception_handling(
        self,
        generator: Generator[ProcessResult, ProcessInput, None],
        batch: pa.RecordBatch,
        is_finalize: bool,
    ) -> ProcessResultComplete:
        """Process a batch with exception handling.

        Wraps _process_and_validate to catch exceptions and convert them
        to ProcessResultComplete with an error log message.
        """
        try:
            return self._process_and_validate(generator, batch, is_finalize=is_finalize)
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

    def process_batches(
        self,
    ) -> Generator[ProcessResult, ProcessInput | None, None]:
        """Generator that processes input batches through the function.

        Yields ProcessResult for each input batch during DATA phase
        and during FINALIZE phase until has_more is False.

        Returns:
            Generator yielding ProcessResult objects.
        """
        # Initial priming value
        _ = yield ProcessResult(None)

        result = ProcessResult(None)
        while True:
            input = yield result
            if input is None:
                raise ValueError("Expected ProcessInput, got None")
            result = ProcessResult(input.batch, has_more=False)

    @final
    def run(
        self, fn_log: structlog.stdlib.BoundLogger
    ) -> Generator[FunctionOutput, FunctionInput | None, None]:
        """Run the function protocol. Do not override.

        This generator implements the DATA* -> FINALIZE lifecycle:

        1. DATA: Receives input batches via send(), calls process_batch()
           with is_finalize=False, and yields outputs. Continues until
           caller sends metadata with type="FINALIZE".

        2. FINALIZE: Calls process_batch() with is_finalize=True repeatedly
           until has_more=False, then yields FINISHED status.
        """
        generator = self.process_batches()
        generator.send(None)

        # Prime the generator - caller must call next() first
        _ = yield FunctionOutput(batch=None, status=None)

        input: FunctionInput | None = yield FunctionOutput(batch=None, status=None)
        if input is None:
            raise ValueError("Expected FunctionInput, got None")

        generator.send(ProcessInput(self.empty_input_batch(), is_finalize=False))

        assert input is not None
        # DATA phase
        while not input.is_finalize:
            result = self._process_with_exception_handling(
                generator, input.batch, is_finalize=False
            )
            input = yield FunctionOutput.from_process_result(
                result, in_finalize_phase=False
            )
            if input is None:
                raise ValueError("Expected FunctionInput, got None")
            if self._should_terminate(result):
                return

        # FINALIZE phase
        while True:
            result = self._process_with_exception_handling(
                generator, input.batch, is_finalize=True
            )
            if result.has_more:
                input = yield FunctionOutput.from_process_result(
                    result, in_finalize_phase=True
                )
                if input is None:
                    raise ValueError("Expected FunctionInput, got None")
                if self._should_terminate(result):
                    return
            else:
                yield FunctionOutput.from_process_result(result, in_finalize_phase=True)
                return
