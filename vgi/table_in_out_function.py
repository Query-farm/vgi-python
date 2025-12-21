"""Base function framework for implementing streaming table functions.

This module provides the scaffolding for creating table functions that follow
the standard protocol: DATA* -> FINALIZE (BIND happens via the decorator)
"""

from collections.abc import Callable, Generator
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from typing import Any, ClassVar, cast

import pyarrow as pa

__all__ = [
    "SchemaValidationError",
    "OutputStatus",
    "FunctionInput",
    "FunctionOutput",
    "ProcessResult",
    "TableInOutFunction",
    "TableInOutFunctionCallable",
    "TableInOutFunctionBindResult",
    "table_in_out_function",
]


@dataclass(frozen=True, slots=True)
class TableInOutFunctionBindResult:
    """Result returned by the bind() method of TableInOutFunction.

    Attributes:
        output_schema: The schema of output RecordBatches.
    """

    output_schema: pa.Schema
    cardinality_estimate: int | None
    cardinality_max: int | None
    generator: Generator["FunctionOutput", "FunctionInput", None]


class SchemaValidationError(Exception):
    """Raised when a batch schema doesn't match the expected schema."""


class OutputStatus(Enum):
    """Status returned with each FunctionOutput to indicate the generator's state.

    NEED_MORE_INPUT: Ready for the next input batch (DATA phase).
    HAVE_MORE_OUTPUT: Call send() again to get more output from the current input.
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
        status: The generator's state after this yield.
    """

    batch: pa.RecordBatch | None
    status: OutputStatus | None


@dataclass(frozen=True, slots=True)
class ProcessResult:
    """Result returned by process_batch().

    Attributes:
        batch: The output RecordBatch, or None to emit an empty batch.
        has_more: If True, process_batch will be called again with the same input.
    """

    batch: pa.RecordBatch | None
    has_more: bool = False


class TableInOutFunction:
    """Base class for streaming table functions that transform Arrow RecordBatches.

    OVERVIEW
    --------
    Subclass this to create table functions that receive a stream of input batches
    and produce a stream of output batches. The framework handles all protocol state
    management - you only implement the data transformation logic.

    LIFECYCLE
    ---------
    1. BIND: The decorator calls bind() and returns the output schema along with
       the generator. The caller receives (schema, generator) from the decorator.

    2. DATA: Your process_batch(batch, is_finalize=False) is called for each input
       batch. Return ProcessResult(batch, has_more). If has_more=True, you'll be
       called again with the same input to produce more output.

    3. FINALIZE: Your process_batch(batch, is_finalize=True) is called repeatedly
       after all input until has_more=False. The batch will be an empty batch.
       Return buffered/aggregated results. Set has_more=True to emit multiple batches.

    METHODS TO OVERRIDE
    -------------------
    bind() -> pa.Schema
        Called lazily when output_schema is first accessed. Use this to:
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
        Default: returns ProcessResult(batch) during DATA (only valid when input/output
        schemas match), returns ProcessResult(None) during FINALIZE

    AVAILABLE ATTRIBUTES
    --------------------
    self.arguments: list[Any]     - Arguments passed to the function
    self.input_schema: pa.Schema  - Schema of incoming batches (available in bind())
    self.output_schema: pa.Schema - Cached property that calls bind() on first access

    HELPER METHODS
    --------------
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

    1. Create the schema and generator:
       schema, gen = MyFunction(arguments, input_schema)

    2. Prime the generator:
       next(gen)  # Returns FunctionOutput(batch=None, status=None)

    3. Send inputs and receive outputs in a loop:
       output = gen.send(FunctionInput(batch=input_batch))
       # Check output.status:
       #   - OutputStatus.NEED_MORE_INPUT: Send next input batch
       #   - OutputStatus.HAVE_MORE_OUTPUT: Call send() again (input is ignored)

    4. Signal finalization:
       output = gen.send(FunctionInput.create_finalize(empty_batch))
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
        def bind(self) -> pa.Schema:
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
        def bind(self) -> pa.Schema:
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
        def __init__(self, arguments, input_schema):
            super().__init__(arguments, input_schema)
            self.repeat_count = arguments[0] if arguments else 2
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
        def __init__(self, arguments, input_schema):
            super().__init__(arguments, input_schema)
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

    def __init__(self, arguments: list[Any], input_schema: pa.Schema):
        self.arguments = arguments
        self.input_schema = input_schema

    @cached_property
    def output_schema(self) -> pa.Schema:
        """Output schema, computed lazily by calling bind() on first access."""
        return self.bind()

    @cached_property
    def empty_output_batch(self) -> pa.RecordBatch:
        """Return an empty batch conforming to output_schema. Cached."""
        return pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in self.output_schema],
            schema=self.output_schema,
        )

    def empty_input_batch(self) -> pa.RecordBatch:
        """Return an empty batch conforming to input_schema.

        Useful for creating the finalize signal:
            FunctionInput.create_finalize(self.empty_input_batch())
        """
        return pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in self.input_schema],
            schema=self.input_schema,
        )

    def bind(self) -> pa.Schema:
        """Called during initialization. Return the output schema.

        Override to transform the schema or initialize state.
        Default: passthrough input schema.
        """
        return self.input_schema

    def _validate_input_schema(self, batch: pa.RecordBatch) -> None:
        """Validate that a batch conforms to the expected input schema."""
        if batch.schema != self.input_schema:
            raise SchemaValidationError(
                f"Input batch schema does not match expected input_schema. "
                f"Expected: {self.input_schema}, got: {batch.schema}"
            )

    def _validate_output_schema(self, batch: pa.RecordBatch) -> None:
        """Validate that a batch conforms to the expected output schema."""
        if batch.schema != self.output_schema:
            raise SchemaValidationError(
                f"Output batch schema does not match expected output_schema. "
                f"Expected: {self.output_schema}, got: {batch.schema}"
            )

    def _process_and_validate(
        self, batch: pa.RecordBatch, is_finalize: bool
    ) -> tuple[pa.RecordBatch, bool]:
        """Process a batch and validate the output schema.

        Returns:
            A tuple of (output_batch, has_more). The output_batch is guaranteed
            to be non-None and conform to output_schema.
        """
        self._validate_input_schema(batch)
        result = self.process_batch(batch, is_finalize)
        output_batch = (
            result.batch if result.batch is not None else self.empty_output_batch
        )
        self._validate_output_schema(output_batch)
        return output_batch, result.has_more

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

        # Process DATA batches
        while True:
            if function_input.is_finalize:
                break
            if function_input.batch is None:
                raise ValueError("DATA input must have a batch")

            output_batch, has_more = self._process_and_validate(
                function_input.batch, is_finalize=False
            )
            function_input = yield FunctionOutput(
                batch=output_batch,
                status=OutputStatus.HAVE_MORE_OUTPUT
                if has_more
                else OutputStatus.NEED_MORE_INPUT,
            )

        # FINALIZE: keep yielding until no more output
        while True:
            output_batch, has_more = self._process_and_validate(
                function_input.batch, is_finalize=True
            )
            if has_more:
                function_input = yield FunctionOutput(
                    batch=output_batch, status=OutputStatus.HAVE_MORE_OUTPUT
                )
            else:
                yield FunctionOutput(batch=output_batch, status=OutputStatus.FINISHED)
                return


# Type alias for decorated table function callables
type TableInOutFunctionCallable = Callable[
    [list[Any], Any], TableInOutFunctionBindResult
]


def table_in_out_function(cls: type[TableInOutFunction]) -> TableInOutFunctionCallable:
    """Decorator to convert a TableInOutFunction class into a callable.

    Usage:
        @table_in_out_function
        class MyFunction(TableInOutFunction):
            def process_batch(self, batch, is_finalize):
                ...

        # Returns a TableInOutFunctionBindResult when called:
        bind_result = MyFunction(arguments, input_schema)
        next(bind_result.generator)  # Prime the generator
        output = bind_result.generator.send(FunctionInput(batch=data))  # Process data
    """

    def wrapper(
        arguments: list[Any], input_schema: Any
    ) -> TableInOutFunctionBindResult:
        fn = cls(arguments, input_schema)
        return TableInOutFunctionBindResult(
            output_schema=fn.output_schema,
            cardinality_estimate=None,
            cardinality_max=None,
            generator=fn.run(),
        )

    return cast(TableInOutFunctionCallable, wrapper)
