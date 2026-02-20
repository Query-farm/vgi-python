"""Tests for improved exception handling in VGI.

This module tests that exceptions are properly propagated from workers to clients
with full traceback information preserved. Uses the subprocess-based Client to
test the actual IPC exception handling paths.
"""

# ruff: noqa: SIM117
# SIM117: Nested with statements are needed for pytest.raises context

from __future__ import annotations

import pyarrow as pa
import pytest

from vgi.arguments import Arguments
from vgi.client.client import Client, ClientError

# =============================================================================
# Tests for Bind-Time Exception Handling
# =============================================================================


class TestBindExceptionHandling:
    """Tests for bind-time exception handling."""

    def test_unknown_function_raises_client_error(self) -> None:
        """Test that calling an unknown function raises ClientError."""
        with Client("vgi-example-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                list(
                    client.table_function(
                        function_name="nonexistent_function_xyz",
                        arguments=Arguments(),
                    )
                )

            assert "nonexistent_function_xyz" in str(exc_info.value)
            assert "Worker Exception" in str(exc_info.value)

    def test_missing_required_setting_raises_client_error(self) -> None:
        """Test that missing required settings raise ClientError."""
        with Client("vgi-example-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                list(
                    client.table_function(
                        function_name="settings_aware",
                        arguments=Arguments(positional=(pa.scalar(3),)),
                    )
                )

            # Error should contain the missing setting name
            assert "vgi_verbose_mode" in str(exc_info.value)


# =============================================================================
# Tests for Processing Exception Handling
# =============================================================================


class TestProcessingExceptionHandling:
    """Tests for exceptions during processing phase."""

    def test_invalid_argument_type_raises_client_error(self) -> None:
        """Test that invalid argument types are caught and reported."""
        with Client("vgi-example-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                list(
                    client.table_function(
                        function_name="sequence",
                        arguments=Arguments(positional=(pa.scalar("not_a_number"),)),
                    )
                )

            # Should get a type-related error
            assert "Worker Exception" in str(exc_info.value)


# =============================================================================
# Tests for Exception Traceback Preservation
# =============================================================================


class TestExceptionTracebackPreservation:
    """Tests to verify that exception tracebacks are preserved."""

    def test_bind_exception_has_traceback(self) -> None:
        """Verify bind exceptions include traceback information."""
        with Client("vgi-example-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                list(
                    client.table_function(
                        function_name="settings_aware",
                        arguments=Arguments(positional=(pa.scalar(3),)),
                    )
                )

            error_message = str(exc_info.value)
            # Should contain traceback keyword
            assert "Traceback" in error_message

    def test_unknown_function_has_traceback(self) -> None:
        """Verify unknown function errors include traceback information."""
        with Client("vgi-example-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                list(
                    client.table_function(
                        function_name="nonexistent_function",
                        arguments=Arguments(),
                    )
                )

            error_message = str(exc_info.value)
            # Should contain traceback
            assert "Traceback" in error_message


# =============================================================================
# Tests for Multi-Worker Exception Handling
# =============================================================================


class TestMultiWorkerExceptionHandling:
    """Tests for exception handling in multi-worker scenarios."""

    def test_bind_exception_in_primary_worker(self) -> None:
        """Test that bind exceptions in primary worker are propagated."""
        with Client("vgi-example-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                list(
                    client.table_function(
                        function_name="settings_aware",
                        arguments=Arguments(positional=(pa.scalar(3),)),
                    )
                )

            # Should get a proper error message
            error_message = str(exc_info.value)
            assert "Worker Exception" in error_message
            assert "vgi_verbose_mode" in error_message


# =============================================================================
# Tests for Table-In-Out Function Exception Handling
# =============================================================================


class TestTableInOutExceptionHandling:
    """Tests for exception handling in table-in-out functions."""

    def test_invalid_column_reference_raises_error(self) -> None:
        """Test that referencing invalid column raises ClientError."""
        input_batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})

        with Client("vgi-example-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                list(
                    client.table_in_out_function(
                        function_name="multiply_column",
                        input=iter([input_batch]),
                        arguments=Arguments(positional=(pa.scalar("nonexistent_column"),)),
                    )
                )

            # Should get an error about the missing column
            assert "Worker Exception" in str(exc_info.value)


# =============================================================================
# Tests for Scalar Function Exception Handling
# =============================================================================


class TestScalarExceptionHandling:
    """Tests for exception handling in scalar functions."""

    def test_invalid_column_name_raises_error(self) -> None:
        """Test that referencing invalid column raises ClientError."""
        input_batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})

        with Client("vgi-example-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                list(
                    client.scalar_function(
                        function_name="add",
                        input=iter([input_batch]),
                        arguments=Arguments(
                            positional=(
                                pa.scalar("nonexistent_col1"),
                                pa.scalar("nonexistent_col2"),
                            )
                        ),
                    )
                )

            # Should get an error about the missing column
            assert "Worker Exception" in str(exc_info.value)


# =============================================================================
# Tests for Error Message Content
# =============================================================================


class TestErrorMessageContent:
    """Tests for verifying error message content and format."""

    def test_worker_exception_prefix(self) -> None:
        """Test that errors have 'Worker Exception' prefix."""
        with Client("vgi-example-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                list(
                    client.table_function(
                        function_name="nonexistent_function",
                        arguments=Arguments(),
                    )
                )

            assert "Worker Exception" in str(exc_info.value)

    def test_exception_type_in_message(self) -> None:
        """Test that exception type is included in error message."""
        with Client("vgi-example-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                list(
                    client.table_function(
                        function_name="settings_aware",
                        arguments=Arguments(positional=(pa.scalar(3),)),
                    )
                )

            # Should mention the exception type somewhere
            error_msg = str(exc_info.value)
            assert "ValueError" in error_msg

    def test_original_exception_message_preserved(self) -> None:
        """Test that the original exception message is preserved."""
        with Client("vgi-example-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                list(
                    client.table_function(
                        function_name="settings_aware",
                        arguments=Arguments(positional=(pa.scalar(3),)),
                    )
                )

            # The original message about missing setting should appear
            assert "vgi_verbose_mode" in str(exc_info.value)
