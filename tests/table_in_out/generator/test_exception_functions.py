# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for exception handling functions."""

import pyarrow as pa
import pytest

from vgi.client import Client, ClientError


class TestExceptionProcessFunction:
    """Tests for exception_process function (raises during process)."""

    def test_exception_process_raises_client_error(
        self, fixture_worker: str, numeric_batches: list[pa.RecordBatch]
    ) -> None:
        """Should raise ClientError when exception occurs during process()."""
        # Need at least 2 batches to trigger the exception
        # (raises on batch_count % 2 == 0, i.e. the 2nd batch)
        # Must use worker_limit=1 so both batches go to the same worker
        with (
            Client(fixture_worker, worker_limit=1) as client,
            pytest.raises(ClientError) as exc_info,
        ):
            list(
                client.table_buffering_function(
                    function_name="exception_process",
                    schema_name="main",
                    input=iter(numeric_batches),
                )
            )

        # Verify the error message contains the expected text
        assert "Intentional exception on batch" in str(exc_info.value)
        assert "ValueError" in str(exc_info.value)

    def test_exception_process_includes_traceback(
        self, fixture_worker: str, numeric_batches: list[pa.RecordBatch]
    ) -> None:
        """Exception should include traceback in the error message."""
        # Must use worker_limit=1 so both batches go to the same worker
        with (
            Client(fixture_worker, worker_limit=1) as client,
            pytest.raises(ClientError) as exc_info,
        ):
            list(
                client.table_buffering_function(
                    function_name="exception_process",
                    schema_name="main",
                    input=iter(numeric_batches),
                )
            )

        # Traceback should be included in the error
        error_message = str(exc_info.value)
        assert "Traceback" in error_message


class TestExceptionFinalizeFunction:
    """Tests for exception_finalize function (raises during finalize)."""

    def test_exception_finalize_raises_client_error(
        self, fixture_worker: str, numeric_batches: list[pa.RecordBatch]
    ) -> None:
        """Should raise ClientError when exception occurs during finalize()."""
        with (
            Client(fixture_worker) as client,
            pytest.raises(ClientError) as exc_info,
        ):
            list(
                client.table_buffering_function(
                    function_name="exception_finalize",
                    schema_name="main",
                    input=iter(numeric_batches),
                )
            )

        # Verify the error message contains the expected text
        assert "Intentional exception during finalize()" in str(exc_info.value)
        assert "ValueError" in str(exc_info.value)

    def test_exception_finalize_includes_traceback(
        self, fixture_worker: str, numeric_batches: list[pa.RecordBatch]
    ) -> None:
        """Exception should include traceback in the error message."""
        with (
            Client(fixture_worker) as client,
            pytest.raises(ClientError) as exc_info,
        ):
            list(
                client.table_buffering_function(
                    function_name="exception_finalize",
                    schema_name="main",
                    input=iter(numeric_batches),
                )
            )

        # Traceback should be included in the error
        error_message = str(exc_info.value)
        assert "Traceback" in error_message

    def test_exception_finalize_after_successful_processing(
        self, fixture_worker: str, numeric_batches: list[pa.RecordBatch]
    ) -> None:
        """Process phase should complete successfully before finalize fails."""
        # The function inherits from SumAllColumnsFunction, so process() should work
        # but finalize() should raise. We can't easily verify process completed
        # since the generator fails before returning, but the exception message
        # confirms it's during finalize.
        with (
            Client(fixture_worker) as client,
            pytest.raises(ClientError) as exc_info,
        ):
            list(
                client.table_buffering_function(
                    function_name="exception_finalize",
                    schema_name="main",
                    input=iter(numeric_batches),
                )
            )

        # The error should specifically mention finalize
        assert "finalize()" in str(exc_info.value)
