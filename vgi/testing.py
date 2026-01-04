"""Testing utilities for VGI functions.

This module provides FunctionTestClient, a lightweight in-process client for testing
VGI functions without the overhead of subprocess communication.

QUICK START
-----------
Use FunctionTestClient to test functions directly without spawning workers:

    from vgi.testing import FunctionTestClient
    from vgi.function import Arguments
    from my_functions import MyFunction
    import pyarrow as pa

    # Create input batches
    batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})

    with FunctionTestClient(MyFunction) as client:
        outputs = list(client.table_in_out_function(
            input=iter([batch]),
            arguments=Arguments(),
        ))
        print(outputs)

        # Check captured log messages
        for log in client.logs:
            print(f"{log.level}: {log.message}")

DECLARATIVE TESTING
-------------------
Use helper functions for concise, declarative test specifications:

    from vgi.testing import assert_function_output, batch

    # Simple passthrough test
    assert_function_output(
        function=EchoFunction,
        input=[batch(x=[1, 2, 3])],
        expected=[batch(x=[1, 2, 3])],
    )

    # Aggregation test
    assert_function_output(
        function=SumFunction,
        input=[batch(a=[1, 2], b=[3, 4]), batch(a=[5], b=[6])],
        expected=[batch(a=[8], b=[13])],
    )

    # Test with arguments
    assert_function_output(
        function=RepeatFunction,
        args=(3,),  # Positional args
        input=[batch(x=[1])],
        expected=[batch(x=[1]), batch(x=[1]), batch(x=[1])],
    )

    # Test with log assertions
    assert_function_logs(
        function=LoggingFunction,
        input=[batch(x=[1, 2, 3])],
        expected_logs=[
            {"level": Level.INFO, "message_contains": "Processing"},
        ],
    )

FEATURES
--------
- No subprocess overhead - runs function directly in process
- Full protocol support including HAVE_MORE_OUTPUT, FINALIZE
- Log message capture via client.logs
- Projection support via projection_ids parameter
- Distributed state support (save_state/load_states) for single-process testing
- Declarative test helpers: batch(), assert_function_output(), assert_function_logs()

"""

import uuid
from collections.abc import Callable, Generator, Iterator
from typing import Any, cast

import pyarrow as pa
import structlog
import structlog.stdlib

from vgi.function import Arguments, Invocation, InvocationType
from vgi.log import Level, Message
from vgi.scalar_function import (
    ProtocolInput as ScalarProtocolInput,
)
from vgi.scalar_function import (
    ScalarFunction,
    ScalarFunctionGenerator,
)
from vgi.table_function import (
    ProtocolOutput as TableProtocolOutput,
)
from vgi.table_function import (
    TableFunctionGenerator,
    TableFunctionInitInput,
)
from vgi.table_in_out_function import (
    ProtocolInput,
    ProtocolOutput,
    TableInOutFunction,
    TableInOutGeneratorFunction,
    _OutputStatus,
)

__all__ = [
    "FunctionTestClient",
    "FunctionTestClientError",
    "TableFunctionTestClient",
    "ScalarFunctionTestClient",
    "batch",
    "assert_function_output",
    "assert_function_logs",
    "run_function",
    "run_table_function",
    "assert_table_function_output",
    "run_scalar_function",
    "assert_scalar_function_output",
]


class FunctionTestClientError(Exception):
    """Error raised by FunctionTestClient operations."""


class FunctionTestClient:
    """In-process client for testing VGI functions.

    Provides the same interface as Client but runs functions directly in the
    current process without subprocess overhead. Ideal for unit tests.

    Example:
        with FunctionTestClient(MyFunction) as client:
            outputs = list(client.table_in_out_function(
                input=iter([batch]),
                arguments=Arguments(),
            ))

    Attributes:
        logs: List of log messages emitted during the last function call.

    """

    def __init__(
        self,
        function_class: type[TableInOutGeneratorFunction] | type[TableInOutFunction],
    ) -> None:
        """Initialize the TestClient.

        Args:
            function_class: The function class to test (not an instance).

        """
        self.function_class = function_class
        self.logs: list[Message] = []
        self._logger: structlog.stdlib.BoundLogger = structlog.get_logger().bind(
            component="test_client"
        )

    def __enter__(self) -> "FunctionTestClient":
        """Enter context manager."""
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
        """Exit context manager."""
        pass

    def table_in_out_function(
        self,
        *,
        input: Iterator[pa.RecordBatch],
        arguments: Arguments | None = None,
        bind_result_callback: Callable[[pa.RecordBatch], None] | None = None,
        projection_ids: list[int] | None = None,
    ) -> Generator[pa.RecordBatch, None, None]:
        """Call the function with the given input data.

        This method implements the VGI streaming protocol directly in-process,
        without any IPC or subprocess communication.

        Args:
            input: Iterator yielding input RecordBatches.
            arguments: Arguments container with positional and named arguments.
            bind_result_callback: Optional callback invoked with the bind result.
            projection_ids: Optional list of column indices to project.

        Yields:
            Output RecordBatches from the function.

        Raises:
            FunctionTestClientError: If the function raises an exception.

        """
        # Clear logs from previous invocation
        self.logs = []

        if arguments is None:
            arguments = Arguments()

        # Get first batch to determine input schema
        try:
            first_batch = next(input)
        except StopIteration:
            # No input batches - nothing to process
            return

        input_schema = first_batch.schema

        # Create invocation
        invocation_id = uuid.uuid4().bytes
        invocation = Invocation(
            function_name=self.function_class.__name__,
            input_schema=input_schema,
            function_type=InvocationType.TABLE,
            correlation_id="test",
            invocation_id=invocation_id,
            arguments=arguments,
        )

        # Instantiate function
        func = self.function_class(invocation=invocation, logger=self._logger)

        # Create bind result for callback
        if bind_result_callback is not None:
            bind_batch = pa.RecordBatch.from_pylist(
                [
                    {
                        "output_schema": func.output_schema.serialize().to_pybytes(),
                        "max_processes": func.max_processes(),
                        "invocation_id": invocation_id,
                    }
                ],
                schema=pa.schema(
                    cast(
                        list[tuple[str, pa.DataType]],
                        [
                            ("output_schema", pa.binary()),
                            ("max_processes", pa.int64()),
                            ("invocation_id", pa.binary()),
                        ],
                    )
                ),
            )
            bind_result_callback(bind_batch)

        # Perform init with TableFunctionInitInput
        init_input = TableFunctionInitInput(projection_ids=projection_ids)
        init_batch = pa.RecordBatch.from_arrays(
            [pa.array([init_input.projection_ids], type=pa.list_(pa.int32()))],
            schema=pa.schema([pa.field("projection_ids", pa.list_(pa.int32()))]),
        )
        init_result = func.perform_init(init_batch)

        # If this is a secondary worker scenario, retrieve init
        if init_result.global_init_identifier is not None:
            func.retrieve_init(init_result)

        # Get the run generator
        generator: Generator[ProtocolOutput, ProtocolInput | None, None] = func.run()

        # Prime the generator
        try:
            priming_output = next(generator)
            assert priming_output.status == _OutputStatus.NEED_MORE_INPUT
        except StopIteration:
            return

        # Create empty batch for finalize
        empty_batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in input_schema],
            schema=input_schema,
        )

        # Process first batch
        yield from self._process_batch(generator, first_batch, empty_batch)

        # Process remaining batches
        for batch in input:
            yield from self._process_batch(generator, batch, empty_batch)

        # Finalize
        yield from self._finalize(generator, empty_batch)

    def _process_batch(
        self,
        generator: Generator[ProtocolOutput, ProtocolInput | None, None],
        batch: pa.RecordBatch,
        empty_batch: pa.RecordBatch,
    ) -> Generator[pa.RecordBatch, None, None]:
        """Process a single input batch, handling HAVE_MORE_OUTPUT."""
        while True:
            try:
                output = generator.send(ProtocolInput(batch=batch))
            except StopIteration:
                return

            # Capture log message if present
            if output.log_message is not None:
                self.logs.append(output.log_message)
                # Check for exception
                if output.log_message.level.name == "EXCEPTION":
                    raise FunctionTestClientError(output.log_message.message)

            # Yield output batch if it has rows
            if output.batch is not None and output.batch.num_rows > 0:
                yield output.batch

            # Check status
            if output.status == _OutputStatus.HAVE_MORE_OUTPUT:
                # Re-send the same batch to get more output
                continue
            elif output.status == _OutputStatus.NEED_MORE_INPUT:
                # Ready for next input batch
                break
            elif output.status == _OutputStatus.FINISHED:
                # Should not happen during data phase
                return
            else:
                raise FunctionTestClientError(f"Unexpected status: {output.status}")

    def _finalize(
        self,
        generator: Generator[ProtocolOutput, ProtocolInput | None, None],
        empty_batch: pa.RecordBatch,
    ) -> Generator[pa.RecordBatch, None, None]:
        """Send finalize signal and collect final outputs."""
        while True:
            try:
                output = generator.send(ProtocolInput.create_finalize(empty_batch))
            except StopIteration:
                return

            # Capture log message if present
            if output.log_message is not None:
                self.logs.append(output.log_message)
                # Check for exception
                if output.log_message.level.name == "EXCEPTION":
                    raise FunctionTestClientError(output.log_message.message)

            # Yield output batch if it has rows
            if output.batch is not None and output.batch.num_rows > 0:
                yield output.batch

            # Check status
            if output.status == _OutputStatus.HAVE_MORE_OUTPUT:
                continue
            elif output.status == _OutputStatus.FINISHED:
                return
            else:
                msg = f"Unexpected finalize status: {output.status}"
                raise FunctionTestClientError(msg)


class TableFunctionTestClient:
    """In-process client for testing TableFunctionGenerator functions.

    Unlike FunctionTestClient (for TableInOut functions), this client is for
    table functions that generate output without receiving input batches.

    Example:
        with TableFunctionTestClient(SequenceFunction) as client:
            outputs = list(client.table_function(
                arguments=Arguments(positional=(pa.scalar(10),)),
            ))

    Attributes:
        logs: List of log messages emitted during the last function call.

    """

    def __init__(
        self,
        function_class: type[TableFunctionGenerator],
    ) -> None:
        """Initialize the TableFunctionTestClient.

        Args:
            function_class: The table function class to test (not an instance).

        """
        self.function_class = function_class
        self.logs: list[Message] = []
        self._logger: structlog.stdlib.BoundLogger = structlog.get_logger().bind(
            component="table_test_client"
        )

    def __enter__(self) -> "TableFunctionTestClient":
        """Enter context manager."""
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
        """Exit context manager."""
        pass

    def table_function(
        self,
        *,
        arguments: Arguments | None = None,
        projection_ids: list[int] | None = None,
    ) -> Generator[pa.RecordBatch, None, None]:
        """Call the table function with the given arguments.

        Args:
            arguments: Arguments container with positional and named arguments.
            projection_ids: Optional list of column indices to project.

        Yields:
            Output RecordBatches from the function.

        Raises:
            FunctionTestClientError: If the function raises an exception.

        """
        # Clear logs from previous invocation
        self.logs = []

        if arguments is None:
            arguments = Arguments()

        # Create invocation (no input schema for table functions)
        invocation_id = uuid.uuid4().bytes
        invocation = Invocation(
            function_name=self.function_class.__name__,
            input_schema=None,
            function_type=InvocationType.TABLE,
            correlation_id="test",
            invocation_id=invocation_id,
            arguments=arguments,
        )

        # Instantiate function
        func = self.function_class(invocation=invocation, logger=self._logger)

        # Perform init with TableFunctionInitInput
        init_input = TableFunctionInitInput(projection_ids=projection_ids)
        init_batch = pa.RecordBatch.from_arrays(
            [pa.array([init_input.projection_ids], type=pa.list_(pa.int32()))],
            schema=pa.schema([pa.field("projection_ids", pa.list_(pa.int32()))]),
        )
        init_result = func.perform_init(init_batch)

        # If this is a secondary worker scenario, retrieve init
        if init_result.global_init_identifier is not None:
            func.retrieve_init(init_result)

        # Get the run generator (no priming needed for TableFunctionGenerator)
        generator: Generator[TableProtocolOutput, None, None] = func.run()

        # Collect all outputs
        try:
            for output in generator:
                # Capture log message if present
                if output.log_message is not None:
                    self.logs.append(output.log_message)
                    # Check for exception
                    if output.log_message.level == Level.EXCEPTION:
                        raise FunctionTestClientError(output.log_message.message)

                # Yield output batch if it has rows
                if output.batch is not None and output.batch.num_rows > 0:
                    yield output.batch
        except StopIteration:
            pass


# =============================================================================
# Declarative Test Helpers
# =============================================================================


def batch(__schema: pa.Schema | None = None, **columns: list[Any]) -> pa.RecordBatch:
    """Create a RecordBatch from column data.

    A convenience function for creating test batches with minimal boilerplate.

    Args:
        __schema: Optional explicit schema. If not provided, schema is inferred.
        **columns: Column names mapped to lists of values.

    Returns:
        A RecordBatch containing the specified data.

    Examples:
        # Simple batch with inferred types
        b = batch(x=[1, 2, 3], y=["a", "b", "c"])

        # Batch with explicit schema
        b = batch(
            pa.schema([("x", pa.int64()), ("y", pa.string())]),
            x=[1, 2, 3],
            y=["a", "b", "c"],
        )

        # Empty batch with schema (for edge case testing)
        b = batch(pa.schema([("x", pa.int64())]), x=[])

    """
    if __schema is not None:
        return pa.RecordBatch.from_pydict(columns, schema=__schema)
    return pa.RecordBatch.from_pydict(columns)


def run_function(
    function: type[TableInOutGeneratorFunction] | type[TableInOutFunction],
    input_batches: list[pa.RecordBatch],
    args: tuple[Any, ...] | None = None,
    kwargs: dict[str, Any] | None = None,
    projection_ids: list[int] | None = None,
) -> tuple[list[pa.RecordBatch], list[Message]]:
    """Run a function and return outputs and logs.

    A convenience wrapper around FunctionTestClient for simple test cases.

    Args:
        function: The function class to test.
        input_batches: List of input RecordBatches.
        args: Optional positional arguments as a tuple.
        kwargs: Optional named arguments as a dict.
        projection_ids: Optional list of column indices to project.

    Returns:
        Tuple of (output_batches, log_messages).

    Example:
        outputs, logs = run_function(
            MyFunction,
            [batch(x=[1, 2, 3])],
            args=(42,),
            kwargs={"name": "test"},
        )

    """
    # Build Arguments from args/kwargs
    positional: tuple[pa.Scalar[Any], ...] = ()
    named: dict[str, pa.Scalar[Any]] = {}

    if args:
        positional = tuple(pa.scalar(a) for a in args)
    if kwargs:
        named = {k: pa.scalar(v) for k, v in kwargs.items()}

    arguments = Arguments(positional=positional, named=named)

    with FunctionTestClient(function) as client:
        outputs = list(
            client.table_in_out_function(
                input=iter(input_batches),
                arguments=arguments,
                projection_ids=projection_ids,
            )
        )
        return outputs, client.logs


def assert_function_output(
    function: type[TableInOutGeneratorFunction] | type[TableInOutFunction],
    input: list[pa.RecordBatch],
    expected: list[pa.RecordBatch],
    args: tuple[Any, ...] | None = None,
    kwargs: dict[str, Any] | None = None,
    projection_ids: list[int] | None = None,
    check_order: bool = True,
    msg: str | None = None,
) -> list[Message]:
    """Assert that a function produces expected output batches.

    Runs the function with the given input and compares output to expected batches.
    Returns captured log messages for additional assertions.

    Args:
        function: The function class to test.
        input: List of input RecordBatches.
        expected: List of expected output RecordBatches.
        args: Optional positional arguments as a tuple.
        kwargs: Optional named arguments as a dict.
        projection_ids: Optional list of column indices to project.
        check_order: If True, order of output batches must match. Default True.
        msg: Optional custom assertion message prefix.

    Returns:
        List of log messages captured during execution.

    Raises:
        AssertionError: If output doesn't match expected.
        FunctionTestClientError: If the function raises an exception.

    Examples:
        # Simple echo test
        assert_function_output(
            EchoFunction,
            input=[batch(x=[1, 2, 3])],
            expected=[batch(x=[1, 2, 3])],
        )

        # Aggregation test
        assert_function_output(
            SumFunction,
            input=[batch(a=[1, 2]), batch(a=[3, 4])],
            expected=[batch(a=[10])],
        )

        # With arguments
        assert_function_output(
            RepeatFunction,
            input=[batch(x=[1])],
            expected=[batch(x=[1]), batch(x=[1]), batch(x=[1])],
            args=(3,),
        )

        # Capture logs for additional assertions
        logs = assert_function_output(
            LoggingFunction,
            input=[batch(x=[1, 2, 3])],
            expected=[batch(x=[1, 2, 3])],
        )
        assert any("Processing" in log.message for log in logs)

    """
    outputs, logs = run_function(
        function=function,
        input_batches=input,
        args=args,
        kwargs=kwargs,
        projection_ids=projection_ids,
    )

    prefix = f"{msg}: " if msg else ""

    # Check batch count
    if len(outputs) != len(expected):
        actual_rows = [o.num_rows for o in outputs]
        expected_rows = [e.num_rows for e in expected]
        raise AssertionError(
            f"{prefix}Expected {len(expected)} output batches, got {len(outputs)}. "
            f"Output rows: {actual_rows}, Expected rows: {expected_rows}"
        )

    # Compare batches
    if check_order:
        for i, (actual, exp) in enumerate(zip(outputs, expected, strict=True)):
            if not actual.equals(exp):
                raise AssertionError(
                    f"{prefix}Batch {i} mismatch.\n"
                    f"Expected:\n{exp.to_pydict()}\n"
                    f"Got:\n{actual.to_pydict()}"
                )
    else:
        # Convert to sets of dicts for unordered comparison
        actual_dicts = [b.to_pydict() for b in outputs]
        expected_dicts = [b.to_pydict() for b in expected]

        for exp_dict in expected_dicts:
            if exp_dict not in actual_dicts:
                raise AssertionError(
                    f"{prefix}Expected batch not found in output.\n"
                    f"Expected:\n{exp_dict}\n"
                    f"Actual outputs:\n{actual_dicts}"
                )

    return logs


def assert_function_logs(
    function: type[TableInOutGeneratorFunction] | type[TableInOutFunction],
    input: list[pa.RecordBatch],
    expected_logs: list[dict[str, Any]],
    args: tuple[Any, ...] | None = None,
    kwargs: dict[str, Any] | None = None,
    msg: str | None = None,
) -> list[pa.RecordBatch]:
    """Assert that a function emits expected log messages.

    Runs the function and verifies log messages match expectations.
    Returns output batches for additional assertions.

    Args:
        function: The function class to test.
        input: List of input RecordBatches.
        expected_logs: List of log expectations. Each dict can contain:
            - level: Expected Level enum value
            - message: Exact message match
            - message_contains: Substring that must be in message
            - message_startswith: Prefix that message must start with
        args: Optional positional arguments as a tuple.
        kwargs: Optional named arguments as a dict.
        msg: Optional custom assertion message prefix.

    Returns:
        List of output RecordBatches.

    Raises:
        AssertionError: If logs don't match expectations.
        FunctionTestClientError: If the function raises an exception.

    Examples:
        # Check for specific log level and message
        assert_function_logs(
            MyFunction,
            input=[batch(x=[1, 2, 3])],
            expected_logs=[
                {"level": Level.INFO, "message_contains": "Processing"},
                {"level": Level.DEBUG, "message_startswith": "Completed"},
            ],
        )

        # Get output batches for additional verification
        outputs = assert_function_logs(
            MyFunction,
            input=[batch(x=[1, 2, 3])],
            expected_logs=[{"level": Level.INFO}],
        )
        assert outputs[0].num_rows == 3

    """
    outputs, logs = run_function(
        function=function,
        input_batches=input,
        args=args,
        kwargs=kwargs,
    )

    prefix = f"{msg}: " if msg else ""

    # Check each expected log pattern
    for i, expectation in enumerate(expected_logs):
        # Find matching log
        found = False
        for log in logs:
            if _log_matches(log, expectation):
                found = True
                break

        if not found:
            log_summary = [
                f"  - {log.level.name}: {log.message[:50]}..."
                if len(log.message) > 50
                else f"  - {log.level.name}: {log.message}"
                for log in logs
            ]
            raise AssertionError(
                f"{prefix}Expected log pattern {i} not found: {expectation}\n"
                f"Actual logs:\n" + "\n".join(log_summary)
            )

    return outputs


def _log_matches(log: Message, expectation: dict[str, Any]) -> bool:
    """Check if a log message matches an expectation dict."""
    return not (
        ("level" in expectation and log.level != expectation["level"])
        or ("message" in expectation and log.message != expectation["message"])
        or (
            "message_contains" in expectation
            and expectation["message_contains"] not in log.message
        )
        or (
            "message_startswith" in expectation
            and not log.message.startswith(expectation["message_startswith"])
        )
    )


# =============================================================================
# Table Function Helpers (for TableFunctionGenerator)
# =============================================================================


def run_table_function(
    function: type[TableFunctionGenerator],
    args: tuple[Any, ...] | None = None,
    kwargs: dict[str, Any] | None = None,
    projection_ids: list[int] | None = None,
) -> tuple[list[pa.RecordBatch], list[Message]]:
    """Run a table function and return outputs and logs.

    A convenience wrapper around TableFunctionTestClient for simple test cases.
    Unlike run_function(), this is for TableFunctionGenerator which generates
    output without receiving input batches.

    Args:
        function: The table function class to test.
        args: Optional positional arguments as a tuple.
        kwargs: Optional named arguments as a dict.
        projection_ids: Optional list of column indices to project.

    Returns:
        Tuple of (output_batches, log_messages).

    Example:
        outputs, logs = run_table_function(
            SequenceFunction,
            args=(10,),  # Generate 10 numbers
        )
        assert len(outputs) == 1
        assert outputs[0].num_rows == 10

    """
    # Build Arguments from args/kwargs
    positional: tuple[pa.Scalar[Any], ...] = ()
    named: dict[str, pa.Scalar[Any]] = {}

    if args:
        positional = tuple(pa.scalar(a) for a in args)
    if kwargs:
        named = {k: pa.scalar(v) for k, v in kwargs.items()}

    arguments = Arguments(positional=positional, named=named)

    with TableFunctionTestClient(function) as client:
        outputs = list(
            client.table_function(
                arguments=arguments,
                projection_ids=projection_ids,
            )
        )
        return outputs, client.logs


def assert_table_function_output(
    function: type[TableFunctionGenerator],
    expected: list[pa.RecordBatch],
    args: tuple[Any, ...] | None = None,
    kwargs: dict[str, Any] | None = None,
    projection_ids: list[int] | None = None,
    check_order: bool = True,
    msg: str | None = None,
) -> list[Message]:
    """Assert that a table function produces expected output batches.

    Runs the function with the given arguments and compares output to expected.
    Returns captured log messages for additional assertions.

    Args:
        function: The table function class to test.
        expected: List of expected output RecordBatches.
        args: Optional positional arguments as a tuple.
        kwargs: Optional named arguments as a dict.
        projection_ids: Optional list of column indices to project.
        check_order: If True, order of output batches must match. Default True.
        msg: Optional custom assertion message prefix.

    Returns:
        List of log messages captured during execution.

    Raises:
        AssertionError: If output doesn't match expected.
        FunctionTestClientError: If the function raises an exception.

    Examples:
        # Sequence test
        assert_table_function_output(
            SequenceFunction,
            args=(5,),
            expected=[batch(n=[0, 1, 2, 3, 4])],
        )

        # Constant table test
        assert_table_function_output(
            ConstantTableFunction,
            args=(42,),
            expected=[batch(value=[42])],
        )

    """
    outputs, logs = run_table_function(
        function=function,
        args=args,
        kwargs=kwargs,
        projection_ids=projection_ids,
    )

    prefix = f"{msg}: " if msg else ""

    # Check batch count
    if len(outputs) != len(expected):
        actual_rows = [o.num_rows for o in outputs]
        expected_rows = [e.num_rows for e in expected]
        raise AssertionError(
            f"{prefix}Expected {len(expected)} output batches, got {len(outputs)}. "
            f"Output rows: {actual_rows}, Expected rows: {expected_rows}"
        )

    # Compare batches
    if check_order:
        for i, (actual, exp) in enumerate(zip(outputs, expected, strict=True)):
            if not actual.equals(exp):
                raise AssertionError(
                    f"{prefix}Batch {i} mismatch.\n"
                    f"Expected:\n{exp.to_pydict()}\n"
                    f"Got:\n{actual.to_pydict()}"
                )
    else:
        # Convert to sets of dicts for unordered comparison
        actual_dicts = [b.to_pydict() for b in outputs]
        expected_dicts = [b.to_pydict() for b in expected]

        for exp_dict in expected_dicts:
            if exp_dict not in actual_dicts:
                raise AssertionError(
                    f"{prefix}Expected batch not found in output.\n"
                    f"Expected:\n{exp_dict}\n"
                    f"Actual outputs:\n{actual_dicts}"
                )

    return logs


# =============================================================================
# Scalar Function Test Client and Helpers
# =============================================================================


class ScalarFunctionTestClient:
    """In-process client for testing ScalarFunction and ScalarFunctionGenerator.

    Scalar functions transform input batches to single-column output with 1:1
    row mapping. Unlike TableInOut functions, scalar functions have no finalize
    phase.

    Example:
        with ScalarFunctionTestClient(DoubleColumnFunction) as client:
            outputs = list(client.scalar_function(
                input=iter([batch]),
                arguments=Arguments(positional=(pa.scalar("x"),)),
            ))

    Attributes:
        logs: List of log messages emitted during the last function call.

    """

    def __init__(
        self,
        function_class: type[ScalarFunctionGenerator] | type[ScalarFunction],
    ) -> None:
        """Initialize the ScalarFunctionTestClient.

        Args:
            function_class: The scalar function class to test (not an instance).

        """
        self.function_class = function_class
        self.logs: list[Message] = []
        self._logger: structlog.stdlib.BoundLogger = structlog.get_logger().bind(
            component="scalar_test_client"
        )

    def __enter__(self) -> "ScalarFunctionTestClient":
        """Enter context manager."""
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
        """Exit context manager."""
        pass

    def scalar_function(
        self,
        *,
        input: Iterator[pa.RecordBatch],
        arguments: Arguments | None = None,
        bind_result_callback: Callable[[pa.RecordBatch], None] | None = None,
    ) -> Generator[pa.RecordBatch, None, None]:
        """Call the scalar function with the given input data.

        This method implements the VGI scalar function protocol directly in-process,
        without any IPC or subprocess communication.

        Args:
            input: Iterator yielding input RecordBatches.
            arguments: Arguments container with positional and named arguments.
            bind_result_callback: Optional callback invoked with the bind result.

        Yields:
            Output RecordBatches from the function (single-column).

        Raises:
            FunctionTestClientError: If the function raises an exception.

        """
        # Clear logs from previous invocation
        self.logs = []

        if arguments is None:
            arguments = Arguments()

        # Get first batch to determine input schema
        try:
            first_batch = next(input)
        except StopIteration:
            # No input batches - nothing to process
            return

        input_schema = first_batch.schema

        # Create invocation
        invocation_id = uuid.uuid4().bytes
        invocation = Invocation(
            function_name=self.function_class.__name__,
            input_schema=input_schema,
            function_type=InvocationType.SCALAR,
            correlation_id="test",
            invocation_id=invocation_id,
            arguments=arguments,
        )

        # Instantiate function
        func = self.function_class(invocation=invocation, logger=self._logger)

        # Create bind result for callback
        if bind_result_callback is not None:
            bind_batch = pa.RecordBatch.from_pylist(
                [
                    {
                        "output_schema": func.output_schema.serialize().to_pybytes(),
                        "max_processes": func.max_processes(),
                        "invocation_id": invocation_id,
                    }
                ],
                schema=pa.schema(
                    cast(
                        list[tuple[str, pa.DataType]],
                        [
                            ("output_schema", pa.binary()),
                            ("max_processes", pa.int64()),
                            ("invocation_id", pa.binary()),
                        ],
                    )
                ),
            )
            bind_result_callback(bind_batch)

        # Get the run generator
        generator = func.run()

        # Prime the generator
        try:
            next(generator)  # Priming output is discarded
        except StopIteration:
            return

        # Process first batch
        yield from self._process_scalar_batch(generator, first_batch)

        # Process remaining batches
        for batch in input:
            yield from self._process_scalar_batch(generator, batch)

        # No finalize for scalar functions - just close
        generator.close()

    def _process_scalar_batch(
        self,
        generator: Generator[TableProtocolOutput, ScalarProtocolInput | None, None],
        batch: pa.RecordBatch,
    ) -> Generator[pa.RecordBatch, None, None]:
        """Process a single input batch, handling log messages."""
        while True:
            try:
                output = generator.send(ScalarProtocolInput(batch=batch))
            except StopIteration:
                return

            # Capture log message if present
            if output.log_message is not None:
                self.logs.append(output.log_message)
                # Check for exception
                if output.log_message.level == Level.EXCEPTION:
                    raise FunctionTestClientError(output.log_message.message)
                # Re-send the same batch to get actual output after log
                continue

            # Yield output batch if it has rows
            if output.batch is not None and output.batch.num_rows > 0:
                yield output.batch

            # No log message means we're done with this batch
            break


def run_scalar_function(
    function: type[ScalarFunctionGenerator] | type[ScalarFunction],
    input_batches: list[pa.RecordBatch],
    args: tuple[Any, ...] | None = None,
    kwargs: dict[str, Any] | None = None,
) -> tuple[list[pa.RecordBatch], list[Message]]:
    """Run a scalar function and return outputs and logs.

    A convenience wrapper around ScalarFunctionTestClient for simple test cases.

    Args:
        function: The scalar function class to test.
        input_batches: List of input RecordBatches.
        args: Optional positional arguments as a tuple.
        kwargs: Optional named arguments as a dict.

    Returns:
        Tuple of (output_batches, log_messages).

    Example:
        outputs, logs = run_scalar_function(
            DoubleColumnFunction,
            [batch(x=[1, 2, 3])],
            args=("x",),
        )
        assert outputs[0].to_pydict() == {"result": [2, 4, 6]}

    """
    # Build Arguments from args/kwargs
    positional: tuple[pa.Scalar[Any], ...] = ()
    named: dict[str, pa.Scalar[Any]] = {}

    if args:
        positional = tuple(pa.scalar(a) for a in args)
    if kwargs:
        named = {k: pa.scalar(v) for k, v in kwargs.items()}

    arguments = Arguments(positional=positional, named=named)

    with ScalarFunctionTestClient(function) as client:
        outputs = list(
            client.scalar_function(
                input=iter(input_batches),
                arguments=arguments,
            )
        )
        return outputs, client.logs


def assert_scalar_function_output(
    function: type[ScalarFunctionGenerator] | type[ScalarFunction],
    input: list[pa.RecordBatch],
    expected: list[pa.RecordBatch],
    args: tuple[Any, ...] | None = None,
    kwargs: dict[str, Any] | None = None,
    check_order: bool = True,
    msg: str | None = None,
) -> list[Message]:
    """Assert that a scalar function produces expected output batches.

    Runs the function with the given input and compares output to expected batches.
    Returns captured log messages for additional assertions.

    Args:
        function: The scalar function class to test.
        input: List of input RecordBatches.
        expected: List of expected output RecordBatches.
        args: Optional positional arguments as a tuple.
        kwargs: Optional named arguments as a dict.
        check_order: If True, order of output batches must match. Default True.
        msg: Optional custom assertion message prefix.

    Returns:
        List of log messages captured during execution.

    Raises:
        AssertionError: If output doesn't match expected.
        FunctionTestClientError: If the function raises an exception.

    Examples:
        # Double column test
        assert_scalar_function_output(
            DoubleColumnFunction,
            input=[batch(x=[1, 2, 3])],
            expected=[batch(result=[2, 4, 6])],
            args=("x",),
        )

        # Add columns test
        assert_scalar_function_output(
            AddColumnsFunction,
            input=[batch(a=[1, 2], b=[10, 20])],
            expected=[batch(result=[11, 22])],
            args=("a", "b"),
        )

    """
    outputs, logs = run_scalar_function(
        function=function,
        input_batches=input,
        args=args,
        kwargs=kwargs,
    )

    prefix = f"{msg}: " if msg else ""

    # Check batch count
    if len(outputs) != len(expected):
        actual_rows = [o.num_rows for o in outputs]
        expected_rows = [e.num_rows for e in expected]
        raise AssertionError(
            f"{prefix}Expected {len(expected)} output batches, got {len(outputs)}. "
            f"Output rows: {actual_rows}, Expected rows: {expected_rows}"
        )

    # Compare batches
    if check_order:
        for i, (actual, exp) in enumerate(zip(outputs, expected, strict=True)):
            if not actual.equals(exp):
                raise AssertionError(
                    f"{prefix}Batch {i} mismatch.\n"
                    f"Expected:\n{exp.to_pydict()}\n"
                    f"Got:\n{actual.to_pydict()}"
                )
    else:
        # Convert to sets of dicts for unordered comparison
        actual_dicts = [b.to_pydict() for b in outputs]
        expected_dicts = [b.to_pydict() for b in expected]

        for exp_dict in expected_dicts:
            if exp_dict not in actual_dicts:
                raise AssertionError(
                    f"{prefix}Expected batch not found in output.\n"
                    f"Expected:\n{exp_dict}\n"
                    f"Actual outputs:\n{actual_dicts}"
                )

    return logs
