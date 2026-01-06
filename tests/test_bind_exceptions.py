"""Tests for bind-time exception handling.

These tests verify that exceptions raised during the bind phase
(function instantiation, output_schema access, argument validation)
are properly caught by the worker and reported to the client.
"""

import pyarrow as pa
import pytest

from vgi.arguments import Arguments
from vgi.client.client import Client, ClientError


class TestBindExceptionHandling:
    """Test that bind-time exceptions are properly handled and reported."""

    def test_missing_required_setting_raises_client_error(self) -> None:
        """Missing required setting during bind should raise ClientError."""
        with Client("vgi-example-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                list(
                    client.table_function(
                        function_name="settings_aware",
                        arguments=Arguments(positional=(pa.scalar(3),)),
                        # No settings provided
                    )
                )

            # Error should contain the missing setting name
            assert "vgi_verbose_mode" in str(exc_info.value)

    def test_bind_exception_contains_traceback(self) -> None:
        """Bind-time exceptions should include traceback information."""
        with Client("vgi-example-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                list(
                    client.table_function(
                        function_name="settings_aware",
                        arguments=Arguments(positional=(pa.scalar(3),)),
                    )
                )

            error_message = str(exc_info.value)
            # Should contain traceback
            assert "Traceback" in error_message
            # Should contain the exception type
            assert "ValueError" in error_message

    def test_bind_exception_contains_worker_exception_prefix(self) -> None:
        """Bind-time exceptions should have 'Worker Exception' prefix."""
        with Client("vgi-example-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                list(
                    client.table_function(
                        function_name="settings_aware",
                        arguments=Arguments(positional=(pa.scalar(3),)),
                    )
                )

            error_message = str(exc_info.value)
            assert "Worker Exception" in error_message

    def test_unknown_function_raises_error(self) -> None:
        """Calling unknown function should raise error.

        Note: Unknown function errors happen before the bind phase try-except,
        so they result in worker exit rather than a ClientError with traceback.
        The error appears in worker stderr.
        """
        with Client("vgi-example-worker") as client:
            with pytest.raises((EOFError, pa.ArrowInvalid)):
                # Unknown function causes worker to exit with error
                list(
                    client.table_function(
                        function_name="nonexistent_function",
                        arguments=Arguments(positional=()),
                    )
                )

            # The worker stderr should contain the error about unknown function
            stderr = client.get_worker_stderr()
            assert "nonexistent_function" in stderr

    def test_argument_mismatch_raises_error(self) -> None:
        """Wrong number of arguments should raise error during bind."""
        with Client("vgi-example-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                # sequence expects 1 argument but we provide none
                list(
                    client.table_function(
                        function_name="sequence",
                        arguments=Arguments(positional=()),
                    )
                )

            error_message = str(exc_info.value)
            # Should indicate argument matching failed
            is_worker_exception = "Worker Exception" in error_message
            mentions_argument = "argument" in error_message.lower()
            assert is_worker_exception or mentions_argument
