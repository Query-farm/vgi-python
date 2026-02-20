"""Tests for Client lifecycle, edge cases, and stderr capture."""

from __future__ import annotations

import time

import pyarrow as pa
import pytest

from tests.conftest import assert_single_result, assert_total_rows, make_schema
from vgi.client import Client
from vgi.client.client import ClientError


class TestClientLifecycle:
    """Tests for Client start/stop behavior."""

    def test_context_manager(self, example_worker: str) -> None:
        """Client should work as a context manager."""
        with Client(example_worker) as client:
            assert client._primary is not None
        # After context exit, process should be cleaned up
        assert client._primary is None

    def test_start_when_already_started_raises(self, example_worker: str) -> None:
        """Starting an already-started client should raise ClientError."""
        client = Client(example_worker)
        client.start()
        try:
            with pytest.raises(ClientError, match="already started"):
                client.start()
        finally:
            client.stop()

    def test_stop_when_not_started_raises(self, example_worker: str) -> None:
        """Stopping a client that wasn't started should raise ClientError."""
        client = Client(example_worker)
        with pytest.raises(ClientError, match="not started"):
            client.stop()

    def test_table_in_out_function_not_started_raises(self, example_worker: str) -> None:
        """Calling table_in_out_function before start should raise ClientError."""
        client = Client(example_worker)
        schema = make_schema([pa.field("id", pa.int64())])
        batch = pa.RecordBatch.from_pydict({"id": [1]}, schema=schema)

        with pytest.raises(ClientError, match="not started"):
            list(
                client.table_in_out_function(
                    function_name="echo",
                    input=iter([batch]),
                )
            )


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_empty_iterator_raises(self, example_worker: str) -> None:
        """Empty iterator (no batches) should raise ClientError."""
        with (
            Client(example_worker) as client,
            pytest.raises(ClientError, match="requires at least one input batch"),
        ):
            list(
                client.table_in_out_function(
                    function_name="echo",
                    input=iter([]),
                )
            )

    def test_empty_batch(self, example_worker: str) -> None:
        """Empty batch (zero rows) should process correctly."""
        schema = make_schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
        empty_batch = pa.RecordBatch.from_pydict({"id": [], "value": []}, schema=schema)

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
        assert_total_rows(output_batches, 0)

    def test_empty_batch_with_aggregation(self, example_worker: str) -> None:
        """Aggregation with empty batch should handle zero rows."""
        schema = make_schema([pa.field("a", pa.int64()), pa.field("b", pa.float64())])
        empty_batch = pa.RecordBatch.from_pydict({"a": [], "b": []}, schema=schema)

        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="sum_all_columns",
                    input=iter([empty_batch]),
                )
            )

        # Aggregation should produce a result (sums of zero elements)
        assert_single_result(output_batches, {"a": [0], "b": [0.0]})

    def test_single_row_batch(self, example_worker: str) -> None:
        """Single row batch should process correctly."""
        schema = make_schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
        single_row_batch = pa.RecordBatch.from_pydict({"id": [1], "value": [100]}, schema=schema)

        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="echo",
                    input=iter([single_row_batch]),
                )
            )

        assert_single_result(output_batches, {"id": [1], "value": [100]})

    def test_large_batch_count(self, example_worker: str) -> None:
        """Many small batches should process correctly."""
        schema = make_schema([pa.field("id", pa.int64())])
        batches = [pa.RecordBatch.from_pydict({"id": [i]}, schema=schema) for i in range(50)]

        with Client(example_worker) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="echo",
                    input=iter(batches),
                )
            )

        assert_total_rows(output_batches, 50)


class TestMultiWorkerEdgeCases:
    """Tests for edge cases with multiple workers.

    These tests expose timeout/hang bugs when:
    - Processing parquet with one batch of zero rows
    - Additional workers spawned but don't receive batches
    """

    def test_zero_row_batch_single_worker(self, example_worker: str) -> None:
        """Baseline: zero-row batch with max_workers=1 should complete quickly."""
        schema = make_schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
        zero_row_batch = pa.RecordBatch.from_pydict({"id": [], "value": []}, schema=schema)

        with Client(example_worker, worker_limit=1) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="echo",
                    input=iter([zero_row_batch]),
                )
            )

        # Should complete without hanging
        assert len(output_batches) >= 1
        assert_total_rows(output_batches, 0)

    def test_zero_row_batch_forced_multiple_workers(self, example_worker: str) -> None:
        """Zero-row batch with max_workers=4 should complete without hanging."""
        schema = make_schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
        zero_row_batch = pa.RecordBatch.from_pydict({"id": [], "value": []}, schema=schema)

        # Force 4 workers even though there's only one batch with zero rows
        with Client(example_worker, worker_limit=4) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="echo",
                    input=iter([zero_row_batch]),
                )
            )

        # Should complete without hanging
        assert len(output_batches) >= 1
        assert_total_rows(output_batches, 0)

    def test_single_batch_multiple_workers(self, example_worker: str) -> None:
        """Single normal batch with max_workers=4 should complete without hanging."""
        schema = make_schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
        single_batch = pa.RecordBatch.from_pydict({"id": [1, 2, 3], "value": [10, 20, 30]}, schema=schema)

        # Force 4 workers even though there's only 1 batch
        with Client(example_worker, worker_limit=4) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="echo",
                    input=iter([single_batch]),
                )
            )

        # Should complete without hanging and return correct data
        assert_total_rows(output_batches, 3)

    def test_fewer_batches_than_workers(self, example_worker: str) -> None:
        """2 batches with max_workers=4 should complete without hanging."""
        schema = make_schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
        batch1 = pa.RecordBatch.from_pydict({"id": [1, 2], "value": [10, 20]}, schema=schema)
        batch2 = pa.RecordBatch.from_pydict({"id": [3, 4, 5], "value": [30, 40, 50]}, schema=schema)

        # Force 4 workers even though there are only 2 batches
        with Client(example_worker, worker_limit=4) as client:
            output_batches = list(
                client.table_in_out_function(
                    function_name="echo",
                    input=iter([batch1, batch2]),
                )
            )

        # Should complete without hanging and return correct data (5 rows total)
        assert_total_rows(output_batches, 5)


class TestWorkerStderrCapture:
    """Tests for capturing worker stderr output."""

    def test_captures_worker_stderr(self, example_worker: str, simple_batches: list[pa.RecordBatch]) -> None:
        """Should capture stderr output from the worker process."""
        with Client(example_worker, pool=None) as client:
            # The example worker uses logging which writes to stderr
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

        client = Client(worker_script, pool=None)
        client.start()

        # Poll for stderr content with timeout instead of fixed sleep
        # The worker exits quickly, but the stderr drain thread needs time
        timeout = 2.0
        poll_interval = 0.05
        elapsed = 0.0
        stderr_output = ""

        while elapsed < timeout:
            stderr_output = client.get_worker_stderr()
            # Check if we have the expected content
            if "Debug: worker starting" in stderr_output:
                break
            time.sleep(poll_interval)
            elapsed += poll_interval

        assert "Debug: worker starting" in stderr_output
        assert "Error: something went wrong" in stderr_output

        client.stop()

    def test_stderr_empty_initially(self, example_worker: str) -> None:
        """Stderr buffer should be empty before worker writes anything."""
        client = Client(example_worker)
        # Before start, buffer should be empty
        assert client.get_worker_stderr() == ""
