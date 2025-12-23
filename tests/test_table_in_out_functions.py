"""Tests for table-in/table-out functions using the Client interface."""

import pyarrow as pa
import pytest

from vgi.client import Client
from vgi.function import Arguments


@pytest.fixture
def example_worker() -> str:
    """Return the path to the example worker."""
    return "vgi-example-worker"


@pytest.fixture
def simple_batches() -> list[pa.RecordBatch]:
    """Create simple test batches with integer and string columns."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("value", pa.int64()),
            pa.field("name", pa.string()),
        ]
    )
    batch1 = pa.RecordBatch.from_pydict(
        {"id": [1, 2], "value": [10, 20], "name": ["a", "b"]},
        schema=schema,
    )
    batch2 = pa.RecordBatch.from_pydict(
        {"id": [3, 4], "value": [30, 40], "name": ["c", "d"]},
        schema=schema,
    )
    return [batch1, batch2]


@pytest.fixture
def numeric_batches() -> list[pa.RecordBatch]:
    """Create test batches with only numeric columns for sum tests."""
    schema = pa.schema(
        [
            pa.field("a", pa.int32()),
            pa.field("b", pa.float64()),
        ]
    )
    batch1 = pa.RecordBatch.from_pydict(
        {"a": [1, 2, 3], "b": [1.5, 2.5, 3.0]},
        schema=schema,
    )
    batch2 = pa.RecordBatch.from_pydict(
        {"a": [4, 5], "b": [4.0, 5.0]},
        schema=schema,
    )
    return [batch1, batch2]


class TestEchoFunction:
    """Tests for the echo function (passthrough)."""

    def test_echo_preserves_data(
        self, example_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Echo should return the same data it receives."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="echo",
                    arguments=Arguments(positional=[], named={}),
                    input=iter(simple_batches),
                )
            )

        # Should have same number of data batches plus finalize batch
        assert len(output_batches) == len(simple_batches) + 1

        # Data batches should match input
        for _i, (input_batch, output_batch) in enumerate(
            zip(simple_batches, output_batches[:-1], strict=False)
        ):
            assert output_batch.schema == input_batch.schema
            assert output_batch.num_rows == input_batch.num_rows
            assert output_batch.to_pydict() == input_batch.to_pydict()

        # Finalize batch should be empty
        assert output_batches[-1].num_rows == 0

    def test_echo_preserves_schema(
        self, example_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Echo should preserve the input schema exactly."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="echo",
                    arguments=Arguments(positional=[], named={}),
                    input=iter(simple_batches),
                )
            )

        assert output_batches[0].schema == simple_batches[0].schema


class TestBufferInputFunction:
    """Tests for the buffer_input function (collect then emit)."""

    def test_buffer_emits_on_finalize(
        self, example_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Buffer should emit all batches during finalization."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="buffer_input",
                    arguments=Arguments(positional=[], named={}),
                    input=iter(simple_batches),
                )
            )

        # During data phase, buffer returns empty batches
        # During finalize, it returns all buffered batches
        # So we expect: empty, empty, batch1, batch2
        total_input_rows = sum(b.num_rows for b in simple_batches)
        total_output_rows = sum(b.num_rows for b in output_batches)
        assert total_output_rows == total_input_rows

    def test_buffer_preserves_order(
        self, example_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Buffer should emit batches in the order they were received."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="buffer_input",
                    arguments=Arguments(positional=[], named={}),
                    input=iter(simple_batches),
                )
            )

        # Filter to non-empty batches (the actual buffered data)
        non_empty = [b for b in output_batches if b.num_rows > 0]
        assert len(non_empty) == len(simple_batches)

        for input_batch, output_batch in zip(simple_batches, non_empty, strict=True):
            assert output_batch.to_pydict() == input_batch.to_pydict()


class TestRepeatInputsFunction:
    """Tests for the repeat_inputs function (explosion)."""

    def test_repeat_custom_count(
        self, example_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Should respect custom repeat count argument."""
        repeat_count = 3
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="repeat_inputs",
                    arguments=Arguments(positional=[repeat_count], named={}),
                    input=iter(simple_batches),
                )
            )

        total_input_rows = sum(b.num_rows for b in simple_batches)
        total_output_rows = sum(b.num_rows for b in output_batches)
        assert total_output_rows == total_input_rows * repeat_count

    def test_repeat_single_time(
        self, example_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Repeat count of 1 should act like echo."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="repeat_inputs",
                    arguments=Arguments(positional=[1], named={}),
                    input=iter(simple_batches),
                )
            )

        total_input_rows = sum(b.num_rows for b in simple_batches)
        total_output_rows = sum(b.num_rows for b in output_batches)
        assert total_output_rows == total_input_rows


class TestSumAllColumnsFunction:
    """Tests for the sum_all_columns function (aggregation)."""

    def test_sum_numeric_columns(
        self, example_worker: str, numeric_batches: list[pa.RecordBatch]
    ) -> None:
        """Should sum all numeric columns across all batches."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="sum_all_columns",
                    arguments=Arguments(positional=[], named={}),
                    input=iter(numeric_batches),
                )
            )

        # Should get empty batches during data phase, then single row on finalize
        non_empty = [b for b in output_batches if b.num_rows > 0]
        assert len(non_empty) == 1

        result = non_empty[0].to_pydict()
        # a: 1+2+3+4+5 = 15
        assert result["a"] == [15]
        # b: 1.5+2.5+3.0+4.0+5.0 = 16.0
        assert result["b"] == [16.0]

    def test_sum_excludes_non_numeric(
        self, example_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Should exclude non-numeric columns from output."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="sum_all_columns",
                    arguments=Arguments(positional=[], named={}),
                    input=iter(simple_batches),
                )
            )

        non_empty = [b for b in output_batches if b.num_rows > 0]
        assert len(non_empty) == 1

        result = non_empty[0]
        # Should only have numeric columns (id, value), not string (name)
        assert "id" in result.schema.names
        assert "value" in result.schema.names
        assert "name" not in result.schema.names

    def test_sum_promotes_types(
        self, example_worker: str, numeric_batches: list[pa.RecordBatch]
    ) -> None:
        """Should promote int32 to int64 and float32 to float64."""
        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="sum_all_columns",
                    arguments=Arguments(positional=[], named={}),
                    input=iter(numeric_batches),
                )
            )

        non_empty = [b for b in output_batches if b.num_rows > 0]
        result_schema = non_empty[0].schema

        # int32 input -> int64 output
        assert result_schema.field("a").type == pa.int64()
        # float64 stays float64
        assert result_schema.field("b").type == pa.float64()


class TestClientLifecycle:
    """Tests for Client start/stop behavior."""

    def test_context_manager(self, example_worker: str) -> None:
        """Client should work as a context manager."""
        with Client(example_worker) as client:
            assert client._proc is not None
        # After context exit, process should be cleaned up
        assert client._proc is None


class TestWorkerStderrCapture:
    """Tests for capturing worker stderr output."""

    def test_captures_worker_stderr(
        self, example_worker: str, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Should capture stderr output from the worker process."""
        with Client(example_worker) as client:
            # The example worker uses structlog which writes to stderr
            list(
                client.table_in_out_function(
                    function_name="echo",
                    arguments=Arguments(positional=[], named={}),
                    input=iter(simple_batches),
                )
            )
            stderr_output = client.get_worker_stderr()

        # Worker should have written some log output to stderr
        assert isinstance(stderr_output, str)
        # The example worker logs startup info
        assert len(stderr_output) > 0

    def test_stderr_available_on_error(
        self, simple_batches: list[pa.RecordBatch]
    ) -> None:
        """Should be able to access stderr after an error occurs."""
        # Use a worker script that writes to stderr then fails
        worker_script = (
            'python -c "'
            "import sys; "
            "sys.stderr.write('Debug: worker starting\\n'); "
            "sys.stderr.write('Error: something went wrong\\n'); "
            "sys.stderr.flush(); "
            'sys.exit(1)"'
        )

        client = Client(worker_script)
        client.start()

        # Give the process a moment to write stderr and exit
        import time

        time.sleep(0.1)

        stderr_output = client.get_worker_stderr()
        assert "Debug: worker starting" in stderr_output
        assert "Error: something went wrong" in stderr_output

        client.stop()

    def test_stderr_empty_initially(self, example_worker: str) -> None:
        """Stderr buffer should be empty before worker writes anything."""
        client = Client(example_worker)
        # Before start, buffer should be empty
        assert client.get_worker_stderr() == ""
