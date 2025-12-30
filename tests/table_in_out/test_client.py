"""Tests for Client lifecycle, edge cases, and stderr capture."""

import time

import pyarrow as pa

from vgi.client import Client


class TestClientLifecycle:
    """Tests for Client start/stop behavior."""

    def test_context_manager(self, example_worker: str) -> None:
        """Client should work as a context manager."""
        with Client(example_worker) as client:
            assert client._proc is not None
        # After context exit, process should be cleaned up
        assert client._proc is None


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_empty_batch(self, example_worker: str) -> None:
        """Empty batch (zero rows) should process correctly."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
        empty_batch = pa.RecordBatch.from_pydict(
            {"id": [], "value": []}, schema=schema
        )

        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="echo",
                    input=iter([empty_batch]),
                )
            )

        # Should complete without error
        assert len(output_batches) >= 1
        # All output should have zero rows
        total_rows = sum(b.num_rows for b in output_batches)
        assert total_rows == 0

    def test_empty_batch_with_aggregation(self, example_worker: str) -> None:
        """Aggregation with empty batch should handle zero rows."""
        schema = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.float64())])
        empty_batch = pa.RecordBatch.from_pydict({"a": [], "b": []}, schema=schema)

        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="sum_all_columns",
                    input=iter([empty_batch]),
                )
            )

        # Should complete without error
        assert len(output_batches) >= 1
        # Aggregation should produce a result (sums of zero elements)
        non_empty = [b for b in output_batches if b.num_rows > 0]
        assert len(non_empty) == 1
        result = non_empty[0].to_pydict()
        # Sum of empty column should be 0
        assert result["a"] == [0]
        assert result["b"] == [0.0]

    def test_single_row_batch(self, example_worker: str) -> None:
        """Single row batch should process correctly."""
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
        single_row_batch = pa.RecordBatch.from_pydict(
            {"id": [1], "value": [100]}, schema=schema
        )

        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="echo",
                    input=iter([single_row_batch]),
                )
            )

        non_empty = [b for b in output_batches if b.num_rows > 0]
        assert len(non_empty) == 1
        assert non_empty[0].to_pydict() == {"id": [1], "value": [100]}

    def test_large_batch_count(self, example_worker: str) -> None:
        """Many small batches should process correctly."""
        schema = pa.schema([pa.field("id", pa.int64())])
        batches = [
            pa.RecordBatch.from_pydict({"id": [i]}, schema=schema) for i in range(50)
        ]

        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="echo",
                    input=iter(batches),
                )
            )

        total_output_rows = sum(b.num_rows for b in output_batches)
        assert total_output_rows == 50


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
                    input=iter(simple_batches),
                )
            )
            stderr_output = client.get_worker_stderr()

        # Worker should have written some log output to stderr
        assert isinstance(stderr_output, str)
        # The example worker logs startup info
        assert len(stderr_output) > 0

    def test_stderr_available_on_error(self) -> None:
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
