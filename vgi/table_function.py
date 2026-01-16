"""Base classes for table functions with cardinality hints and generator support.

This module provides:
- Output: Simple output container (batch only, no has_more)
- OutputGenerator: Generator type alias for simple process() methods
- OutputSpec: OutputSpec subclass with cardinality support
- ProtocolOutput: Protocol-level output with optional log messages
- SchemaValidationError: Exception for schema mismatches
- TableCardinality: Row count estimates for query optimization
- TableFunctionBase: Base class with cardinality, schema validation, and lifecycle
- TableFunctionGenerator: Generator-based base class with simple run() loop

Class Hierarchy:
    Function (vgi.function)
        └── TableFunctionBase
                └── TableFunctionGenerator  (simple generator, no input via send)
                └── TableInOutGenerator (full protocol with input batches)

TableFunctionGenerator is useful for functions that don't need to receive
input batches via yield - they just produce output batches in a loop until done.
For functions that transform input batches, use TableInOutGenerator.
"""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass
from typing import Any, Self, final

import pyarrow as pa
import structlog

import vgi.function
import vgi.ipc_utils
import vgi.log
from vgi.output_complete import OutputComplete

__all__ = [
    "TableCardinality",
    "TableFunctionInitInput",
    "Output",
    "OutputGenerator",
    "OutputSpec",
    "ProtocolOutput",
    "TableFunctionBase",
    "TableFunctionGenerator",
]


@dataclass(frozen=True, slots=True)
class TableCardinality:
    """Cardinality hints for query optimization.

    Provides optional row count estimates that can help query planners make
    better decisions about join ordering, memory allocation, and parallelization.

    Attributes:
        estimate: Estimated number of output rows, or None if unknown.
        max: Maximum possible output rows, or None if unbounded.

    Example:
        # Function that filters ~10% of rows, with known input size
        TableCardinality(estimate=1000, max=10000)

        # Aggregation that always produces exactly one row
        TableCardinality(estimate=1, max=1)

        # Unknown output size
        TableCardinality(estimate=None, max=None)

    """

    estimate: int | None
    max: int | None


@dataclass(frozen=True, slots=True)
class OutputSpec(vgi.function.OutputSpec):
    """Extended bind result for table functions with cardinality information.

    Extends OutputSpec with optional cardinality estimates that help query
    planners optimize execution strategies.

    Attributes:
        cardinality: Optional row count estimates for query optimization.
            None indicates no cardinality information is available.

    """

    cardinality: TableCardinality | None = None

    def serialize_schema(self) -> pa.Schema:
        """Extend parent schema with cardinality fields."""
        return (
            super(OutputSpec, self)
            .serialize_schema()
            .append(pa.field("cardinality_estimated", pa.int64(), nullable=True))
            .append(pa.field("cardinality_max", pa.int64(), nullable=True))
        )

    def serialize_dict(self) -> dict[str, Any]:
        """Extend parent dict with cardinality values."""
        return super(OutputSpec, self).serialize_dict() | {
            "cardinality_estimated": (
                self.cardinality.estimate if self.cardinality else None
            ),
            "cardinality_max": (self.cardinality.max if self.cardinality else None),
        }


@dataclass(frozen=True, slots=True)
class Output:
    """Output yielded by process().

    Attributes:
        batch: The output RecordBatch, or None to emit an empty batch.

    Examples:
        # Normal processing - emit one batch per input
        yield Output(transformed_batch)

        # For logging, yield Message directly (not via Output):
        yield Message(Level.INFO, "Processing started")
        yield Output(transformed_batch)

    """

    batch: pa.RecordBatch | None


# Type alias for process() return type.
# Receives: pa.RecordBatch in process()
# Yields:
#   - Output: Batch
#   - Message: Log message
OutputGenerator = Generator[vgi.log.Message | Output, None, None]


@dataclass(frozen=True, slots=True)
class ProtocolOutput:
    """Output yielded by the generator after each send().

    Attributes:
        batch: The output RecordBatch. None batches are replaced with empty batches.
        log_message: Optional log or error message associated with this output.

    """

    batch: pa.RecordBatch | None
    log_message: vgi.log.Message | None = None

    def metadata(self, invocation: vgi.invocation.Invocation) -> pa.KeyValueMetadata:
        """Create metadata for this output based on the status.

        Args:
            invocation: The Invocation for this function invocation, passed through
                to Message.add_to_metadata() for correlation information.

        Returns:
            KeyValueMetadata containing protocol state, status and optional log
            message fields.

        """
        # Start with protocol state (required for VGI protocol)
        protocol_key = vgi.ipc_utils.PROTOCOL_STATE_KEY
        protocol_state = vgi.ipc_utils.ProtocolState.OUTPUT.encode()
        metadata_dict: dict[bytes, bytes] = {protocol_key: protocol_state}

        # Add log message metadata if present
        if self.log_message is not None:
            for k, v in self.log_message.add_to_metadata(invocation, {}).items():
                metadata_dict[k.encode()] = v.encode()

        return pa.KeyValueMetadata(metadata_dict)

    @classmethod
    def from_process_result(cls, process_result: OutputComplete) -> ProtocolOutput:
        """Create a ProtocolOutput from an Output and status.

        Args:
            process_result: The result from process() or finalize().

        """
        return cls(
            batch=process_result.batch,
            log_message=process_result.log_message,
        )


@dataclass(frozen=True, slots=True)
class TableFunctionInitInput(vgi.function.FunctionInitInput):
    """Input sent to initialize global state for a TableFunction.

    Inherits traceparent and tracestate from FunctionInitInput.

    Attributes:
        projection_ids: Optional list of column indices to project, or None for all.

    Note:
        For parallel execution, functions should use the work queue pattern
        via enqueue_work() and dequeue_work() methods on the Function base class
        instead of static partitioning.

    """

    projection_ids: list[int] | None = None

    @classmethod
    def _schema_fields(cls) -> list[pa.Field[Any]]:
        """Return Arrow schema fields for this class only."""
        return [
            pa.field("projection_ids", pa.list_(pa.int32()), nullable=True),
        ]

    def _to_dict(self) -> dict[str, Any]:
        """Return dict of this class's own field values."""
        return {
            "projection_ids": self.projection_ids,
        }

    @classmethod
    def _from_values(cls, values: dict[str, Any]) -> Self:
        """Create instance from dict, including parent fields."""
        return cls(
            traceparent=values.get("traceparent"),
            tracestate=values.get("tracestate"),
            projection_ids=values.get("projection_ids"),
        )


class TableFunctionBase(vgi.function.Function[TableFunctionInitInput]):
    """Base class for table functions with cardinality and schema validation.

    Extends Function with:
    - Cardinality hints for query optimization
    - Projection pushdown support

    This class is not meant to be used directly. Subclass either:
    - TableFunctionGenerator: For simple generators that produce output
    - TableInOutGenerator: For functions that transform input batches

    Attributes:
        init_input: TableFunctionInitInput with projection info (set after init)
        empty_output_batch: Cached empty batch conforming to output_schema

    See Also:
        TableFunctionGenerator: Simple generator base class
        TableInOutGenerator: Full streaming with input batches

    """

    # InitInputType inferred from generic parameter Function[TableFunctionInitInput]
    init_input: TableFunctionInitInput | None = None

    def __init__(
        self,
        *,
        invocation: vgi.invocation.Invocation,
        logger: structlog.stdlib.BoundLogger,
    ):
        """Initialize the table function with call data.

        Args:
            invocation: Complete invocation request including function name,
                arguments, and input schema.
            logger: Logger instance for structured logging.

        """
        super().__init__(invocation=invocation, logger=logger)

    @property
    def cardinality(self) -> TableCardinality | None:
        """Optional cardinality estimate for the output.

        Override to provide row count estimates that help query planners
        make better decisions about join ordering and memory allocation.

        Returns:
            TableCardinality with estimate and/or max, or None if unknown.

        """
        return None

    def apply_projection(self, schema: pa.Schema) -> pa.Schema:
        """Apply any projection specified in the init data to the schema.

        Args:
            schema: Original output schema before projection.

        Returns:
            Projected schema according to init data, or original if no projection.

        """
        if self.init_input and self.init_input.projection_ids is not None:
            projected_fields = []
            for proj_id in self.init_input.projection_ids:
                field = schema.field(proj_id)
                projected_fields.append(field)
            return pa.schema(projected_fields)
        return schema


class TableFunctionGenerator(TableFunctionBase):
    """Generator-based table function with simple run() lifecycle.

    This base class provides a simplified generator protocol where the process()
    method yields Output objects without receiving input batches via send().
    The run() method handles the SETUP -> DATA -> TEARDOWN lifecycle.

    Use this class for functions that:
    - Generate output without transforming input batches
    - Produce a fixed sequence of output batches
    - Don't need the full DATA/FINALIZE protocol

    For functions that transform input batches, use TableInOutGenerator.

    LIFECYCLE
    ---------
    1. SETUP: setup() is called for resource acquisition
    2. DATA: process() generator yields Output objects via send(None)
    3. TEARDOWN: teardown() is called for cleanup (always, even on error)

    METHODS TO OVERRIDE
    -------------------
    process() -> OutputGenerator
        Generator that yields Output objects. Each yield produces one output
        batch. The generator receives None via send() (no input batches).
        Default: empty generator (no output)

    output_schema -> pa.Schema (property)
        Must be implemented by subclasses.

    setup() -> None
        Optional: Acquire resources before processing.

    teardown() -> None
        Optional: Release resources after processing.

    Example:
        class CountFunction(TableFunctionGenerator):
            @property
            def output_schema(self) -> pa.Schema:
                return pa.schema([("n", pa.int64())])

            def process(self) -> OutputGenerator:
                for i in range(10):
                    yield Output(pa.RecordBatch.from_pydict(
                        {"n": [i]}, schema=self.output_schema
                    ))

    """

    @final
    def _process_and_validate(self, generator: OutputGenerator) -> OutputComplete:
        """Process a batch and validate the output schema.

        Converts the result of the generator to OutputComplete, and
        validates the output schema.

        Args:
            generator: The user's process() or finalize() generator.

        Returns:
            OutputComplete with validated output batch.

        Raises:
            SchemaValidationError: If output batch schema doesn't match.

        """
        result: OutputComplete = OutputComplete.from_process_result(
            generator.send(None),
            self.empty_output_batch,
        )
        self._validate_output_schema(result.batch)
        return result

    @final
    def _process_with_exception_handling(
        self,
        generator: OutputGenerator,
    ) -> OutputComplete:
        """Process a batch with exception handling.

        Wraps _process_and_validate to catch exceptions and convert them
        to OutputComplete with an error log message.

        Note: StopIteration is re-raised, not caught, since it signals
        the generator is exhausted (not an error condition).
        """
        try:
            return self._process_and_validate(generator)
        except StopIteration:
            raise
        except Exception as e:
            return self._create_error_output(e)

    # _should_terminate inherited from Function

    def process(self) -> OutputGenerator:
        """Process batches during the DATA phase.

        Yield Output or Message to control output and logging behavior.

        Yield options:
            Output: Batch
            Message: Emit log message directly; current input will be re-sent.

        When yielding Message directly, the framework sends an empty batch
        with the log information in metadata.

        Returns:
            Generator yielding Output or Message objects.

        """
        if False:
            yield

    @final
    def run(self) -> Generator[ProtocolOutput, None, None]:
        """Run the function protocol. Do not override.

        This generator implements the SETUP -> DATA -> TEARDOWN lifecycle:

        1. SETUP: Calls setup() for resource acquisition.

        2. DATA: Produces output batches via send(None). Continues
           until the process() generator is exhausted.

        3. TEARDOWN: Calls teardown() for resource cleanup (always, even on error).
        """
        # Acquire resources before processing
        self.setup()

        generator = self.process()

        try:
            sent_batch: bool = False
            # DATA phase - iterate until generator is exhausted
            while True:
                try:
                    result = self._process_with_exception_handling(generator)
                    sent_batch = True
                except StopIteration:
                    if not sent_batch:
                        # If no batches were sent, emit an empty batch to
                        # signal completion, otherwise the client may
                        # block indefinitely waiting for data.
                        result = OutputComplete(
                            batch=self.empty_output_batch, log_message=None
                        )
                        yield ProtocolOutput.from_process_result(result)
                    break
                yield ProtocolOutput.from_process_result(result)
                if self._should_terminate(result):
                    break
        finally:
            # Ensure the process generator is closed when run() is closed.
            # This allows functions to catch GeneratorExit for cleanup (e.g.,
            # saving state in distributed functions).
            generator.close()
            # Release resources after processing completes
            self.teardown()
