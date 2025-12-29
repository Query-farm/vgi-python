"""Framework for implementing streaming table-in-table-out functions.

This module provides the base class for creating functions that transform
Arrow RecordBatch streams. Functions receive batches via a generator protocol
and can buffer, filter, transform, or aggregate data.

Protocol Overview:
    1. BIND: Function is instantiated and returns output schema
    2. DATA: Input batches are sent via process() generator, outputs are yielded
    3. FINALIZE: The finalize() generator is called to flush buffered data

Key Components:
    Function: Base class to subclass for custom functions.
    Output: Return type for process()/finalize() with batch and has_more flag.
    OutputGenerator: Type alias for the process()/finalize() return type.
    ProtocolInput/ProtocolOutput: Protocol messages for the run() generator.

Quick Start (Recommended Pattern):
    The process() method uses a generator pattern. Always use this explicit
    loop structure for clarity:

    class MyFunction(Function):
        def process(self, batch: pa.RecordBatch) -> OutputGenerator:
            # 1. REQUIRED: Priming yield (framework advances past this)
            _ = yield None

            # 2. Process batches in a loop
            while True:
                # Transform batch here
                result = transform(batch)

                # 3. Yield output
                yield Output(result)

                # 4. Get next batch (returns None when input exhausted)
                batch = yield None
                if batch is None:
                    break

        # Optional: override finalize() only if you need to emit final results
        def finalize(self) -> OutputGenerator | None:
            _ = yield None  # Priming yield
            yield Output(final_batch)

    IMPORTANT: The explicit `while True` / `if batch is None: break` pattern
    is recommended over the compact `while batch := (yield ...)` form for
    clarity and to avoid common mistakes.

Logging:
    Functions can emit log messages by yielding LogMessage directly or via
    Output.log_message. When a LogMessage is yielded, an empty batch
    is sent with the message in metadata, and the current input is re-sent:

        from vgi.function import LogLevel, LogMessage

        def process(self, batch: pa.RecordBatch) -> OutputGenerator:
            _ = yield None
            while True:
                yield LogMessage(LogLevel.INFO, f"Processing {batch.num_rows} rows")
                yield Output(transformed_batch)
                batch = yield None
                if batch is None:
                    break

See Function docstring for comprehensive documentation and examples.
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
    "ProtocolInput",
    "ProtocolOutput",
    "Output",
    "OutputGenerator",
    "Function",
]


class _OutputStatus(Enum):
    """Status returned with each ProtocolOutput to indicate the generator's state.

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
class ProtocolInput:
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
    def create_finalize(cls, batch: pa.RecordBatch) -> "ProtocolInput":
        """Create a ProtocolInput that signals the FINALIZE phase.

        This is only sent once so there is no benefit to caching it.
        """
        return cls(
            batch=batch, metadata=pa.KeyValueMetadata({"type": cls._FINALIZE_SIGNAL})
        )


@dataclass(frozen=True, slots=True)
class ProtocolOutput:
    """Output yielded by the generator after each send().

    Attributes:
        batch: The output RecordBatch. None only for the initial priming yield;
            during normal operation, None batches are replaced with empty batches.
        status: The generator's state after this yield (see _OutputStatus).
        log_message: Optional log or error message associated with this output.

    """

    batch: pa.RecordBatch | None
    status: _OutputStatus | None
    log_message: vgi.function.LogMessage | None = None

    def metadata(
        self, invocation: vgi.function.FunctionRequest
    ) -> pa.KeyValueMetadata | None:
        """Create metadata for this output based on the status.

        Args:
            invocation: The FunctionRequest for this function invocation, passed through
                to LogMessage.add_to_metadata() for correlation information.

        Returns:
            KeyValueMetadata containing status and optional log message fields,
            or None if status is None (only for the initial priming yield).

        """
        if self.status is None:
            return None

        metadata_dict = {"status": self.status.value}

        if self.log_message is not None:
            metadata_dict = self.log_message.add_to_metadata(invocation, metadata_dict)

        return pa.KeyValueMetadata(metadata_dict)

    @classmethod
    def from_process_result(
        cls, process_result: "_OutputComplete", in_finalize_phase: bool
    ) -> "ProtocolOutput":
        """Create a ProtocolOutput from an Output and status.

        Args:
            process_result: The result from process() or finalize().
            in_finalize_phase: Whether we are in the FINALIZE phase.

        """
        continue_from_current_input_output = (
            process_result.continue_from_current_input
            or process_result.log_message is not None
        )

        if continue_from_current_input_output:
            status = _OutputStatus.HAVE_MORE_OUTPUT
        elif in_finalize_phase:
            status = _OutputStatus.FINISHED
        else:
            status = _OutputStatus.NEED_MORE_INPUT
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
class Output:
    """Output yielded by process() and finalize() generators.

    Attributes:
        batch: The output RecordBatch, or None to emit an empty batch.
        continue_from_current_input: If True, the generator will receive another
            send() call. Use this to produce multiple output batches from a
            single input.
        log_message: Optional log message to send with this output. When present,
            the framework substitutes an empty batch for this yield, attaches
            the log message to the metadata, and sets continue_from_current_input
            to True so the generator receives another send() to continue.
            If the log level is EXCEPTION, processing terminates instead.

    Examples:
        # Normal processing - emit one batch per input
        yield Output(transformed_batch)

        # Emit multiple batches from one input
        yield Output(first_batch, continue_from_current_input=True)  # more coming
        yield Output(second_batch, continue_from_current_input=False)  # done

        # Emit a log message with processing
        yield Output(batch, log_message=LogMessage(LogLevel.INFO, "Done"))

    """

    batch: pa.RecordBatch | None
    continue_from_current_input: bool = False
    log_message: vgi.function.LogMessage | None = None


# Type alias for process() and finalize() return type.
# Receives: pa.RecordBatch in process(), None in finalize().
# Yields:
#   - None: No output for this input (ready for next batch)
#   - Output: Output batch with optional continue_from_current_input flag
#   - LogMessage: Emit a log message; input will be re-sent after logging
OutputGenerator = Generator[
    vgi.function.LogMessage | Output | None, pa.RecordBatch | None, None
]


@dataclass(frozen=True, slots=True)
class _OutputComplete(Output):
    """An Output with a guaranteed non-None batch.

    Used internally by the framework to ensure the generator always yields
    a valid RecordBatch. When an Output has a None batch or contains
    a log message, an empty batch is substituted.

    Attributes:
        batch: Always a valid RecordBatch (never None).
        continue_from_current_input: Inherited from Output; set to True when a
            log message is present to ensure the message is delivered.
        log_message: Inherited from Output.

    """

    batch: pa.RecordBatch

    @classmethod
    def from_process_result(
        cls,
        source: vgi.function.LogMessage | Output | None,
        empty_batch: pa.RecordBatch,
    ) -> "_OutputComplete":
        """Create an OutputComplete from an Output.

        Args:
            source: The original Output from process() or finalize().
            empty_batch: An empty batch to use when source.batch is None
                or when a log message is present.

        Returns:
            An OutputComplete with a guaranteed non-None batch.

        """
        if source is None:
            return cls(
                batch=empty_batch, continue_from_current_input=False, log_message=None
            )
        if isinstance(source, vgi.function.LogMessage):
            return cls(
                batch=empty_batch, continue_from_current_input=True, log_message=source
            )

        return cls(
            batch=empty_batch
            if source.batch is None or source.log_message is not None
            else source.batch,
            continue_from_current_input=True
            if source.log_message is not None
            else source.continue_from_current_input,
            log_message=source.log_message,
        )


class Function(vgi.table_function.Function):
    """Base class for streaming table functions that transform Arrow RecordBatches.

    This class handles functions that receive arguments and a streaming table input,
    producing Arrow RecordBatches as output.

    OVERVIEW
    --------
    Subclass this to create table functions that receive a stream of input batches
    and produce a stream of output batches. The framework handles all protocol state
    management, including exception handling - you only implement the data
    transformation logic via the process() and finalize() generators.

    LIFECYCLE
    ---------
    1. BIND: The class is instantiated and output_schema property is accessed to
       get the output schema.

    2. DATA: Your process() generator receives RecordBatch objects via yield.
       Yield Output(batch, continue_from_current_input) for each input.
       If continue_from_current_input=True, you'll receive another send().

    3. FINALIZE: Your finalize() generator is called to emit buffered/aggregated
       results. Set continue_from_current_input=True to emit multiple batches.

    METHODS TO OVERRIDE
    -------------------
    output_schema -> pa.Schema (property)
        Override to define the output schema. Use this to:
        - Inspect self.input_schema to see what columns are available
        - Decide what columns your output will have
        Default: returns self.input_schema unchanged (passthrough)

    process(batch: pa.RecordBatch) -> OutputGenerator
        Generator that processes input batches during the DATA phase.

        The first batch is passed as a parameter. Subsequent batches are
        received via yield. Must:
        1. Yield None for priming (value is discarded)
        2. Loop processing batches, yielding Output or LogMessage
        3. Receive subsequent batches via yield (returns None when done)

        Input:
        - batch parameter: The first input batch
        - yield return value: Subsequent batches, or None when finalize begins

        Yield options:
        - Output: Batch with optional continue_from_current_input and log_message
        - LogMessage: Emit a log message directly (input will be re-sent)
        - None: No output, ready for next batch

        The Output contains:
        - batch: A RecordBatch conforming to output_schema, or None for empty
        - continue_from_current_input: If True, you will receive another send() call
        - log_message: Optional LogMessage for logging or error reporting

        Default: passes input batches through unchanged (passthrough)

    finalize() -> OutputGenerator | None
        Generator that emits final output during the FINALIZE phase.
        Return None (default) if no finalization is needed.
        If returning a generator, it must:
        1. Yield None for priming (value is discarded)
        2. Yield Output for each output batch
        3. Set continue_from_current_input=True to emit multiple batches

        Default: returns None (no finalization output)

    AVAILABLE ATTRIBUTES
    --------------------
    self.arguments: Arguments     - Arguments passed to the function
    self.input_schema: pa.Schema  - Schema of incoming batches
    self.output_schema: pa.Schema - Property returning the output schema

    HELPER PROPERTIES/METHODS
    -------------------------
    self.empty_output_batch: pa.RecordBatch
        Returns an empty batch conforming to output_schema (cached). Use when you
        need to signal "no output for this input" - return Output(None) is
        equivalent.

    CALLER PROTOCOL
    ---------------
    To use a Function, the caller must:

    1. Create the bind result:
       invocation = vgi.function.FunctionRequest(
           function_name="my_function",
           arguments=vgi.function.Arguments(positional=[], named={}),
           in_out_function_input_schema=input_schema,
           correlation_id="",
           invocation_id=None,
       )
       bind_result = MyFunction(invocation, logger)

    2. Prime the generator:
       next(bind_result.run())  # Returns ProtocolOutput(batch=None, status=None)

    3. Send inputs and receive outputs in a loop:
       output = generator.send(ProtocolInput(batch=input_batch))
       # Check output.status:
       #   - _OutputStatus.NEED_MORE_INPUT: Send next input batch
       #   - _OutputStatus.HAVE_MORE_OUTPUT: Call send() again (input is ignored)

    4. Signal finalization:
       output = generator.send(ProtocolInput.create_finalize(empty_batch))
       # Check output.status:
       #   - _OutputStatus.HAVE_MORE_OUTPUT: Call send() again to get more output
       #   - _OutputStatus.FINISHED: Stop iteration

    """

    def __init__(
        self,
        invocation: vgi.function.FunctionRequest,
        logger: structlog.stdlib.BoundLogger,
    ):
        """Initialize the function with invocation data and logger."""
        super().__init__(invocation=invocation, logger=logger)
        self.arguments = invocation.arguments
        if invocation.in_out_function_input_schema is None:
            raise ValueError("Function requires a non-null input schema")
        self.input_schema = invocation.in_out_function_input_schema

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
        """Return the output schema (default: passthrough input schema)."""
        return self.input_schema

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
        generator: OutputGenerator,
        batch: pa.RecordBatch | None,
    ) -> _OutputComplete:
        """Process a batch and validate both input and output schemas.

        Validates the input batch schema, sends it to the generator, converts
        the result to OutputComplete, and validates the output schema.

        Args:
            generator: The user's process() or finalize() generator.
            batch: The input RecordBatch to process, or None during finalize.

        Returns:
            OutputComplete with validated output batch.

        Raises:
            SchemaValidationError: If input or output batch schema doesn't match.

        """
        if batch is not None:
            self._validate_input_schema(batch)
        result: _OutputComplete = _OutputComplete.from_process_result(
            generator.send(batch),
            self.empty_output_batch,
        )
        self._validate_output_schema(result.batch)
        return result

    @final
    def _process_with_exception_handling(
        self,
        generator: OutputGenerator,
        batch: pa.RecordBatch | None,
    ) -> _OutputComplete:
        """Process a batch with exception handling.

        Wraps _process_and_validate to catch exceptions and convert them
        to OutputComplete with an error log message.
        """
        try:
            return self._process_and_validate(generator, batch)
        except Exception as e:
            return _OutputComplete(
                batch=self.empty_output_batch,
                log_message=vgi.function.LogMessage.from_exception(e),
            )

    @final
    def _should_terminate(self, result: _OutputComplete) -> bool:
        """Check if processing should terminate due to an exception."""
        return (
            result.log_message is not None
            and result.log_message.level == vgi.function.LogLevel.EXCEPTION
        )

    def process(self, batch: pa.RecordBatch) -> OutputGenerator:
        """Process input batches during the DATA phase.

        Receives pa.RecordBatch objects via yield. Yield None, Output,
        or LogMessage to control output and logging behavior.

        Yield options:
            None: No output for this input, ready for next batch.
            Output: Batch with optional continue_from_current_input and log_message.
            LogMessage: Emit log message directly; current input will be re-sent.

        When yielding LogMessage directly, the framework sends an empty batch
        with the log in metadata and re-sends the current input batch. The
        re-sent value is returned by the yield expression but is typically
        discarded since the original batch is still in scope.

        Returns:
            Generator yielding None, Output, or LogMessage objects.

        """
        # Initial priming yield
        _ = yield None

        while batch := (yield Output(batch)):
            pass

    def finalize(self) -> OutputGenerator | None:
        """Finalize processing and produce any remaining output.

        Override this method to emit buffered or aggregated results after all
        input batches have been processed. Return None (default) if no
        finalization is needed.

        Returns:
            Generator yielding Output or LogMessage objects during
            finalization, or None if no finalization output is needed.

        """
        return None

    @final
    def run(self) -> Generator[ProtocolOutput, ProtocolInput | None, None]:
        """Run the function protocol. Do not override.

        This generator implements the DATA* -> FINALIZE lifecycle:

        1. DATA: Receives input batches via send(), yields outputs. Continues
           until caller sends metadata with type="FINALIZE".

        2. FINALIZE: Calls finalize() generator, yields outputs until
           continue_from_current_input=False, then yields FINISHED status.

        Protocol:
            - Caller primes with next() or send(None)
            - Caller sends ProtocolInput for each batch
            - Caller sends ProtocolInput with is_finalize=True to end
        """
        # Priming yield - caller calls next() or send(None)
        input: ProtocolInput | None = yield ProtocolOutput(batch=None, status=None)
        if input is None:
            raise ValueError("Expected ProtocolInput, got None")

        generator = self.process(input.batch)
        # Prime process() generator past both yields (priming + first data yield)
        generator.send(None)

        # DATA phase
        while not input.is_finalize:
            result = self._process_with_exception_handling(generator, input.batch)
            input = yield ProtocolOutput.from_process_result(
                result, in_finalize_phase=False
            )
            if input is None:
                raise ValueError("Expected ProtocolInput, got None")
            if self._should_terminate(result):
                return

        finalize_generator = self.finalize()

        # If no finalize generator, just emit FINISHED
        if finalize_generator is None:
            yield ProtocolOutput(
                batch=self.empty_output_batch, status=_OutputStatus.FINISHED
            )
            return

        finalize_generator.send(None)

        # FINALIZE phase - send None to signal finalize
        while True:
            result = self._process_with_exception_handling(finalize_generator, None)
            if result.continue_from_current_input:
                input = yield ProtocolOutput.from_process_result(
                    result, in_finalize_phase=True
                )
                if input is None:
                    raise ValueError("Expected ProtocolInput, got None")
                if self._should_terminate(result):
                    return
            else:
                yield ProtocolOutput.from_process_result(result, in_finalize_phase=True)
                return
