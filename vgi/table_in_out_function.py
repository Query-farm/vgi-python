"""Framework for implementing streaming table-in-table-out functions.

This module provides the base class for creating functions that transform
Arrow RecordBatch streams. Functions receive batches via a generator protocol
and can buffer, filter, transform, or aggregate data.

Protocol Overview:
    1. BIND: Function is instantiated and returns output schema
    2. DATA: Input batches are sent via process() generator, outputs are yielded
    3. FINALIZE: The finalize() generator is called to flush buffered data

Key Components:
    TableInOutGenerator: Base class to subclass for custom functions.
    Output: Return type for process()/finalize() with batch and has_more flag.
    OutputGenerator: Type alias for the process()/finalize() return type.
    ProtocolInput/ProtocolOutput: Protocol messages for the run() generator.

Quick Start (Recommended Pattern):
    The process() method uses a generator pattern. Always use this explicit
    loop structure for clarity:

    class MyFunction(TableInOutGenerator):
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
    Functions can emit log messages by yielding Message directly. When a
    Message is yielded, an empty batch is sent with the message in metadata,
    and the current input is re-sent:

        from vgi.log import Level, Message

        def process(self, batch: pa.RecordBatch) -> OutputGenerator:
            _ = yield None
            while True:
                yield Message(Level.INFO, f"Processing {batch.num_rows} rows")
                yield Output(transformed_batch)
                batch = yield None
                if batch is None:
                    break

See TableInOutGenerator docstring for comprehensive documentation and examples.
"""

from collections.abc import Callable, Generator
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from typing import ClassVar, final

import pyarrow as pa
import structlog

import vgi.function
import vgi.ipc_utils
import vgi.log
import vgi.table_function
from vgi.output_complete import OutputComplete
from vgi.protocol_types import ProtocolInput as ProtocolInputBase

__all__ = [
    "ProtocolInput",
    "ProtocolOutput",
    "Output",
    "OutputGenerator",
    "StreamingGenerator",
    "streaming",
    "TableInOutGenerator",
    "TableInOutFunction",
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


@dataclass(frozen=True)
class ProtocolInput(ProtocolInputBase):
    """Input sent to the generator via send().

    Extends ProtocolInputBase with finalize phase signaling for table-in-out
    functions.

    Attributes:
        batch: The input RecordBatch to process.
        metadata: Optional metadata; used to signal the FINALIZE phase.

    """

    # pa.KeyValueMetadata uses bytes so we define signals as bytes
    _FINALIZE_SIGNAL: ClassVar[bytes] = b"FINALIZE"

    @property
    def is_finalize(self) -> bool:
        """Check if this input signals the FINALIZE phase."""
        return (
            self.metadata is not None
            and self.metadata.get(b"type") == self._FINALIZE_SIGNAL
        )

    @classmethod
    def create_finalize(cls, batch: pa.RecordBatch) -> "ProtocolInput":
        """Create a ProtocolInput that signals the FINALIZE phase.

        This is only sent once so there is no benefit to caching it.
        """
        return cls(
            batch=batch, metadata=pa.KeyValueMetadata({b"type": cls._FINALIZE_SIGNAL})
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
    status: _OutputStatus
    log_message: vgi.log.Message | None = None

    def metadata(
        self, invocation: vgi.invocation.Invocation
    ) -> pa.KeyValueMetadata | None:
        """Create metadata for this output based on the status.

        Args:
            invocation: The Invocation for this function invocation, passed through
                to Message.add_to_metadata() for correlation information.

        Returns:
            KeyValueMetadata containing status and optional log message fields.

        """
        metadata_dict: dict[str, str] = {"status": self.status.value}

        if self.log_message is not None:
            metadata_dict = self.log_message.add_to_metadata(invocation, metadata_dict)

        return pa.KeyValueMetadata(
            {k.encode(): v.encode() for k, v in metadata_dict.items()}
        )

    @classmethod
    def from_process_result(
        cls, process_result: "OutputComplete", in_finalize_phase: bool
    ) -> "ProtocolOutput":
        """Create a ProtocolOutput from an Output and status.

        Args:
            process_result: The result from process() or finalize().
            in_finalize_phase: Whether we are in the FINALIZE phase.

        """
        has_more_output = (
            process_result.has_more or process_result.log_message is not None
        )

        if has_more_output:
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


@dataclass(frozen=True, slots=True)
class Output(vgi.table_function.Output):
    """Output yielded by process() and finalize() generators.

    Attributes:
        batch: The output RecordBatch, or None to emit an empty batch.
        has_more: If True, the generator will receive another send() call.
            Use this to produce multiple output batches from a single input.

    Examples:
        # Normal processing - emit one batch per input
        yield Output(transformed_batch)

        # Emit multiple batches from one input
        yield Output(first_batch, has_more=True)  # more coming
        yield Output(second_batch)  # done (has_more=False is default)

        # For logging, yield Message directly (not via Output):
        yield Message(Level.INFO, "Processing started")
        yield Output(transformed_batch)

    """

    has_more: bool = False


# Type alias for process() and finalize() return type.
# Receives: pa.RecordBatch in process(), None in finalize().
# Yields:
#   - Output: Batch with optional has_more flag
#   - Message: Log message; input will be re-sent after logging
#   - None: No output for this input (ready for next batch)
OutputGenerator = Generator[
    vgi.log.Message | Output | None, pa.RecordBatch | None, None
]

# Type alias for the simplified streaming function signature.
# Used by the @streaming decorator.
StreamingGenerator = Generator[Output | vgi.log.Message, pa.RecordBatch | None, None]


def streaming[T](
    method: Callable[[T, pa.RecordBatch], StreamingGenerator],
) -> Callable[[T, pa.RecordBatch], OutputGenerator]:
    """Simplify generator-based process() methods by eliminating priming yield.

    Eliminates the required priming yield while keeping the same send/yield pattern.
    The decorated method receives the first batch as a parameter and subsequent
    batches via yield expressions.

    The decorated method:
    - Receives the first batch as a parameter
    - Gets subsequent batches via: batch = yield Output(...)
    - Gets None when input is exhausted
    - Does NOT need the priming yield

    Args:
        method: A method that takes (self, first_batch) and yields Output objects,
            receiving subsequent batches via yield.

    Returns:
        A wrapped method compatible with the VGI generator protocol.

    Examples:
        Using the @streaming decorator (recommended for new code):

        ```python
        class MyFunction(TableInOutGenerator):
            @streaming
            def process(self, batch: pa.RecordBatch) -> StreamingGenerator:
                # No priming yield needed!
                while batch is not None:
                    batch = yield Output(batch)
        ```

        Equivalent without decorator (more verbose):

        ```python
        class MyFunction(TableInOutGenerator):
            def process(self, batch: pa.RecordBatch) -> OutputGenerator:
                _ = yield None  # Required priming yield

                while True:
                    yield Output(batch)
                    batch = yield None
                    if batch is None:
                        break
        ```

        With logging:

        ```python
        class LoggingFunction(TableInOutGenerator):
            @streaming
            def process(self, batch: pa.RecordBatch) -> StreamingGenerator:
                while batch is not None:
                    yield Message(Level.INFO, f"Processing {batch.num_rows} rows")
                    batch = yield Output(batch)
        ```

        Aggregation pattern:

        ```python
        class SumFunction(TableInOutGenerator):
            @streaming
            def process(self, batch: pa.RecordBatch) -> StreamingGenerator:
                while batch is not None:
                    self.total += pc.sum(batch.column(0)).as_py()
                    batch = yield Output(self.empty_output_batch)
                # Note: for aggregations, also implement finalize()
        ```

    """

    @wraps(method)
    def wrapper(self: T, first_batch: pa.RecordBatch) -> OutputGenerator:
        # Priming yield (required by VGI protocol, handled by decorator)
        _ = yield None

        # Create and run user's generator
        user_gen = method(self, first_batch)

        try:
            # Get first output from user
            output = next(user_gen)
        except StopIteration:
            return

        while True:
            # Yield output and receive next batch from VGI
            next_batch = yield output

            if next_batch is None:
                # Signal end of input - close user's generator cleanly
                user_gen.close()
                return

            try:
                # Send next batch to user and get their next output
                output = user_gen.send(next_batch)
            except StopIteration:
                return

    return wrapper


class TableInOutGenerator(vgi.table_function.TableFunctionBase):
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
       Yield Output(batch, has_more) for each input.
       If has_more=True, you'll receive another send().

    3. FINALIZE: Your finalize() generator is called to emit buffered/aggregated
       results. Set has_more=True to emit multiple batches.

    4. TEARDOWN: The teardown() method is called for resource cleanup.

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
        2. Loop processing batches, yielding Output or Message
        3. Receive subsequent batches via yield (returns None when done)

        Input:
        - batch parameter: The first input batch
        - yield return value: Subsequent batches, or None when finalize begins

        Yield options:
        - Output: Batch with optional has_more flag
        - Message: Emit a log message directly (input will be re-sent)
        - None: No output, ready for next batch

        The Output contains:
        - batch: A RecordBatch conforming to output_schema, or None for empty
        - has_more: If True, you will receive another send() call

        Default: passes input batches through unchanged (passthrough)

    finalize() -> OutputGenerator | None
        Generator that emits final output during the FINALIZE phase.
        Return None (default) if no finalization is needed.
        If returning a generator, it must:
        1. Yield None for priming (value is discarded)
        2. Yield Output for each output batch
        3. Set has_more=True to emit multiple batches

        Default: returns None (no finalization output)

    setup() -> None
        Called before processing starts, after init_input is available.
        Override to acquire resources like database connections, file handles,
        or external service clients. Default: no-op.

    teardown() -> None
        Called after processing completes on every worker. Override to release
        resources acquired in setup(). Called on primary worker after finalize(),
        on secondary workers after process() (no finalize). Always called, even
        if an error occurred. Default: no-op.

    AVAILABLE ATTRIBUTES
    --------------------
    self.invocation: Invocation   - The complete invocation request
    self.input_schema: pa.Schema  - Input schema (from invocation)
    self.output_schema: pa.Schema - Property returning the output schema

    Access arguments via self.invocation.arguments or use Arg descriptors.

    HELPER PROPERTIES/METHODS
    -------------------------
    self.empty_output_batch: pa.RecordBatch
        Returns an empty batch conforming to output_schema (cached). Use when you
        need to signal "no output for this input" - return Output(None) is
        equivalent.

    RESOURCE MANAGEMENT
    -------------------
    Functions can use setup/teardown for resource cleanup:

        class MyDbFunction(TableInOutGenerator):
            def setup(self) -> None:
                self.conn = sqlite3.connect("my.db")

            def teardown(self) -> None:
                self.conn.close()

            def process(self, batch: pa.RecordBatch) -> OutputGenerator:
                _ = yield None
                while True:
                    # Use self.conn safely - guaranteed to be cleaned up
                    self.conn.execute(...)
                    yield Output(batch)
                    batch = yield None
                    if batch is None:
                        break

    CALLER PROTOCOL
    ---------------
    To use a TableInOutGenerator, the caller must:

    1. Create the bind result:
       invocation = vgi.invocation.Invocation(
           function_name="my_function",
           arguments=vgi.function.Arguments(positional=[], named={}),
           input_schema=input_schema,
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
        invocation: vgi.invocation.Invocation,
        logger: structlog.stdlib.BoundLogger,
    ):
        """Initialize the function with invocation data and logger."""
        super().__init__(invocation=invocation, logger=logger)
        if invocation.input_schema is None:
            raise ValueError(
                f"{type(self).__name__} requires an input schema, but none was "
                f"provided. TableInOutGenerator processes input batches and "
                f"requires input_schema to be set in the Invocation. "
                f"If your function generates output without input, inherit from "
                f"TableFunctionGenerator instead."
            )

    # input_schema property inherited from Function

    def teardown(self) -> None:
        """Release resources after processing completes.

        Override to release resources acquired in setup(). This is called:
        - On the primary worker: after finalize() completes
        - On secondary workers: after process() completes (no finalize)

        Always called, even if an error occurred during processing.

        """
        pass

    @property
    def output_schema(self) -> pa.Schema:
        """Return the output schema (default: passthrough input schema)."""
        return self.input_schema

    # _validate_input_schema inherited from Function

    @final
    def _process_and_validate(
        self,
        generator: OutputGenerator,
        batch: pa.RecordBatch | None,
    ) -> OutputComplete:
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
        result: OutputComplete = OutputComplete.from_process_result(
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
    ) -> OutputComplete:
        """Process a batch with exception handling.

        Wraps _process_and_validate to catch exceptions and convert them
        to OutputComplete with an error log message.
        """
        try:
            return self._process_and_validate(generator, batch)
        except Exception as e:
            return self._create_error_output(e)

    # _should_terminate inherited from Function

    def process(self, batch: pa.RecordBatch) -> OutputGenerator:
        """Process input batches during the DATA phase.

        Receives pa.RecordBatch objects via yield. Yield None, Output,
        or Message to control output and logging behavior.

        Yield options:
            None: No output for this input, ready for next batch.
            Output: Batch with optional has_more flag.
            Message: Emit log message directly; current input will be re-sent.

        When yielding Message directly, the framework sends an empty batch
        with the Message in the RecordBatch's metadata and re-sends the
        current input batch. The re-sent value is returned by the yield
        expression but is typically discarded since the original batch is
        still in scope.

        Returns:
            Generator yielding None, Output, or Message objects.

        """
        _ = yield None  # Priming yield

        while True:
            received = yield Output(batch)
            if received is None:
                break
            batch = received

    def finalize(self) -> OutputGenerator | None:
        """Finalize processing and produce any remaining output.

        Override this method to emit buffered or aggregated results after all
        input batches have been processed. Return None (default) if no
        finalization is needed.

        Returns:
            Generator yielding Output or Message objects during
            finalization, or None if no finalization output is needed.

        """
        return None

    @final
    def run(self) -> Generator[ProtocolOutput, ProtocolInput | None, None]:
        """Run the function protocol. Do not override.

        This generator implements the SETUP -> DATA -> FINALIZE -> TEARDOWN lifecycle:

        1. SETUP: Calls setup() for resource acquisition.

        2. DATA: Receives input batches via send(), yields outputs. Continues
           until caller sends metadata with type="FINALIZE".

        3. FINALIZE: Calls finalize() generator, yields outputs until
           has_more=False, then yields FINISHED status.

        4. TEARDOWN: Calls teardown() for resource cleanup (always, even on error).

        Protocol:
            - Caller primes with next() or send(None)
            - Caller sends ProtocolInput for each batch
            - Caller sends ProtocolInput with is_finalize=True to end
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

            # Close the process generator before finalize. This allows functions
            # to catch GeneratorExit for cleanup (e.g., saving state in
            # distributed functions) before finalize() aggregates results.
            generator.close()

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
                if result.has_more:
                    input = yield ProtocolOutput.from_process_result(
                        result, in_finalize_phase=True
                    )
                    if input is None:
                        raise ValueError("Expected ProtocolInput, got None")
                    if self._should_terminate(result):
                        return
                else:
                    yield ProtocolOutput.from_process_result(
                        result, in_finalize_phase=True
                    )
                    return
        finally:
            # Ensure the process generator is closed when run() is closed.
            # This allows functions to catch GeneratorExit for cleanup (e.g.,
            # saving state in distributed functions).
            generator.close()
            # Release resources after processing completes
            self.teardown()


class TableInOutFunction(TableInOutGenerator):
    """Simplified base class using callbacks instead of generators.

    This class provides a simpler API for common use cases where you don't need
    the full power of generators. Instead of implementing process() and finalize()
    as generators, you override transform() and optionally finish() as regular methods.

    METHODS TO OVERRIDE
    -------------------
    transform(batch) -> pa.RecordBatch | list[pa.RecordBatch]
        Called for each input batch. Return a single transformed batch,
        or a list of batches if you need multiple outputs per input.
        Default: returns input batch unchanged (passthrough).

    finish() -> list[pa.RecordBatch]
        Called after all input is processed. Return a list of final batches,
        or an empty list if no finalization is needed.
        Default: returns empty list.

    output_schema -> pa.Schema (property)
        Override to define the output schema if different from input.
        Default: returns input schema (passthrough).

    LOGGING
    -------
    Call self.log(level, message) from transform() to emit log messages:

        def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
            self.log(Level.INFO, f"Processing {batch.num_rows} rows")
            return batch

    DISTRIBUTED PROCESSING
    ----------------------
    For parallel aggregations, override save_state() and load_states():

    save_state() -> RecordBatchState | None
        Called on each worker before finalize. Return partial state to save.
        Default: returns None (no state).

    load_states(states: list[RecordBatchState]) -> None
        Called on primary worker with all worker states before finish().
        Default: no-op.

    Examples
    --------
    Passthrough (no-op):

        class Echo(TableInOutFunction):
            pass  # Default transform() returns batch unchanged

    Transform each batch:

        class DoubleValues(TableInOutFunction):
            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                doubled = pc.multiply(batch.column(0), 2)
                return batch.set_column(0, batch.schema[0].name, doubled)

    Multiple outputs per input:

        class TripleOutput(TableInOutFunction):
            def transform(self, batch: pa.RecordBatch) -> list[pa.RecordBatch]:
                return [batch, batch, batch]  # Emit 3 copies

    Aggregation with logging:

        class SumColumn(TableInOutFunction):
            def __init__(self, invocation, logger):
                super().__init__(invocation, logger)
                self.total = 0

            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([("sum", pa.int64())])

            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                self.log(Level.INFO, f"Processing {batch.num_rows} rows")
                self.total += pc.sum(batch.column(0)).as_py()
                return self.empty_output_batch

            def finish(self) -> list[pa.RecordBatch]:
                self.log(Level.INFO, f"Final sum: {self.total}")
                return [pa.RecordBatch.from_pydict(
                    {"sum": [self.total]},
                    schema=self.output_schema
                )]

            class Meta:
                max_workers = 1  # Single-process aggregation

    Distributed aggregation (parallel workers):

        class DistributedSum(TableInOutFunction):
            def __init__(self, invocation, logger):
                super().__init__(invocation, logger)
                self.total = 0

            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([("sum", pa.int64())])

            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                self.total += pc.sum(batch.column(0)).as_py()
                return self.empty_output_batch

            def save_state(self) -> RecordBatchState:
                return RecordBatchState(batch=pa.RecordBatch.from_pydict(
                    {"partial_sum": [self.total]},
                    schema=self.output_schema
                ))

            def load_states(self, states: list[RecordBatchState]) -> None:
                table = pa.Table.from_batches([s.batch for s in states])
                self.total = pc.sum(table.column(0)).as_py()

            def finish(self) -> list[pa.RecordBatch]:
                return [pa.RecordBatch.from_pydict(
                    {"sum": [self.total]},
                    schema=self.output_schema
                )]

    """

    def __init__(
        self,
        invocation: vgi.invocation.Invocation,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        """Initialize the function."""
        super().__init__(invocation=invocation, logger=logger)
        self._pending_messages: list[vgi.log.Message] = []

    def log(self, level: vgi.log.Level, message: str) -> None:
        """Queue a log message to be emitted.

        Call this from transform() or finish() to emit log messages. Messages
        are queued and emitted after each batch is processed.

        Args:
            level: Log severity (Level.INFO, Level.WARN, Level.ERROR, etc.)
            message: The log message text.

        Examples:
            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                self.log(Level.INFO, f"Processing {batch.num_rows} rows")
                return batch

        """
        self._pending_messages.append(vgi.log.Message(level=level, message=message))

    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch | list[pa.RecordBatch]:
        """Transform a single input batch.

        Override this method to implement your transformation logic. This is called
        once for each input batch.

        Args:
            batch: Input RecordBatch to transform.

        Returns:
            Either:
            - A single pa.RecordBatch: The transformed output
            - A list of pa.RecordBatch: Multiple outputs from this input
            - self.empty_output_batch: To emit nothing for this input

        Examples:
            # Simple transformation
            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                return batch  # Passthrough

            # Multiple outputs
            def transform(self, batch: pa.RecordBatch) -> list[pa.RecordBatch]:
                return [batch, batch]  # Emit twice

            # Accumulate without output (for aggregations)
            def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch:
                self.buffer.append(batch)
                return self.empty_output_batch

        """
        return batch

    def finish(self) -> list[pa.RecordBatch]:
        """Return final batches after all input is processed.

        Override this method to emit results after all input batches have been
        processed. This is useful for aggregations, sorting, or any operation
        that needs to see all data before producing output.

        Returns:
            List of pa.RecordBatch to emit as final output.
            Return an empty list if no finalization output is needed.

        Examples:
            # No finalization needed
            def finish(self) -> list[pa.RecordBatch]:
                return []

            # Emit aggregation result
            def finish(self) -> list[pa.RecordBatch]:
                return [pa.RecordBatch.from_pydict(
                    {"total": [self.total]},
                    schema=self.output_schema
                )]

            # Emit buffered batches
            def finish(self) -> list[pa.RecordBatch]:
                return self.buffered_batches

        """
        return []

    def save_state(self) -> vgi.ipc_utils.RecordBatchState | None:
        """Save partial state before finalize (for distributed processing).

        Override this method to return partial state that will be collected
        from all workers and passed to load_states() on the primary worker.

        Returns:
            RecordBatchState containing partial results, or None if no state.

        Examples:
            def save_state(self) -> RecordBatchState:
                return RecordBatchState(batch=pa.RecordBatch.from_pydict(
                    {"partial_sum": [self.total]},
                    schema=self.output_schema
                ))

        """
        return None

    def load_states(self, states: list[vgi.ipc_utils.RecordBatchState]) -> None:
        """Load and merge states from all workers (for distributed processing).

        Override this method to combine partial states from all workers.
        Called on the primary worker before finish().

        Args:
            states: List of RecordBatchState from all workers (including self).

        Examples:
            def load_states(self, states: list[RecordBatchState]) -> None:
                table = pa.Table.from_batches([s.batch for s in states])
                self.total = pc.sum(table.column("partial_sum")).as_py()

        """
        pass

    @final
    def _yield_pending_messages(self) -> OutputGenerator:
        """Yield all pending log messages. Helper for process/finalize."""
        while self._pending_messages:
            msg = self._pending_messages.pop(0)
            _ = yield msg

    @final
    def process(self, batch: pa.RecordBatch) -> OutputGenerator:
        """Process input batches by calling transform(). Do not override.

        This method implements the generator protocol by calling your transform()
        method for each input batch. The generator boilerplate is handled for you.

        """
        _ = yield None  # Priming yield

        try:
            while True:
                result = self.transform(batch)

                # Yield any pending log messages first
                yield from self._yield_pending_messages()

                # Handle single batch or list of batches
                if isinstance(result, list):
                    for i, output_batch in enumerate(result):
                        is_last = i == len(result) - 1
                        if is_last:
                            # Last batch: receive next input
                            received = yield Output(output_batch, has_more=False)
                        else:
                            # More batches: caller re-sends same input, we ignore it
                            _ = yield Output(output_batch, has_more=True)
                else:
                    # Single batch: yield output and receive next input
                    received = yield Output(result)

                if received is None:
                    break
                batch = received
        except GeneratorExit:
            # Save state for distributed processing before generator closes
            state = self.save_state()
            if state is not None:
                self.store_state(state)
            raise

    @final
    def finalize(self) -> OutputGenerator | None:
        """Emit final batches by calling finish(). Do not override.

        This method implements the generator protocol by calling your finish()
        method and yielding the returned batches.

        """
        # Collect states from all workers for distributed processing
        # Only attempt if execution_identifier is set (indicates distributed mode)
        if self.execution_identifier is not None:
            states = self.collect_states(vgi.ipc_utils.RecordBatchState)
            if states:
                self.load_states(states)

        # Call finish() and collect any log messages
        batches = self.finish()

        # Check if we have anything to yield
        has_messages = len(self._pending_messages) > 0
        if not batches and not has_messages:
            return None

        def _finalize_generator() -> OutputGenerator:
            _ = yield None  # Priming yield

            # Yield any pending log messages first
            yield from self._yield_pending_messages()

            # Yield output batches
            for i, batch in enumerate(batches):
                is_last = i == len(batches) - 1
                yield Output(batch, has_more=not is_last)

        return _finalize_generator()
