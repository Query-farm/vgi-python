# Copyright 2025, 2026 Query Farm LLC - https://query.farm

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
        with Client("vgi-fixture-worker") as client:
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
        with Client("vgi-fixture-worker") as client:
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

    def test_bind_exception_leads_with_user_message(self) -> None:
        """The first line of a ClientError is the user's exception, not VGI framing.

        We used to prefix with ``Worker Exception:``; the prefix was noise that
        pushed the actionable exception down a line. The contract now is:
        ``str(e)`` starts with ``{error_type}: {error_message}`` (the same
        thing ``RpcError.__init__`` sets), and optional sections (remote
        traceback, worker stderr) follow after a blank line.
        """
        with Client("vgi-fixture-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                list(
                    client.table_function(
                        function_name="settings_aware",
                        arguments=Arguments(positional=(pa.scalar(3),)),
                    )
                )

            error_message = str(exc_info.value)
            first_line = error_message.split("\n", 1)[0]
            # First line is "ValueError: <message>" (or similar user error)
            # — NOT framed with "Worker Exception:" or any VGI wrapper.
            assert not first_line.startswith("Worker Exception")
            assert ":" in first_line  # standard "Type: message" shape
            # Remote traceback, when present, is a later section.
            assert "Traceback" in error_message

    def test_unknown_function_raises_client_error(self) -> None:
        """Calling unknown function should raise ClientError with helpful message."""
        with Client("vgi-fixture-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                list(
                    client.table_function(
                        function_name="nonexistent_function",
                        arguments=Arguments(positional=()),
                    )
                )

            error_message = str(exc_info.value)
            # Should contain the function name
            assert "nonexistent_function" in error_message
            # Should indicate it's unknown
            assert "Unknown function" in error_message

    def test_argument_mismatch_raises_error(self) -> None:
        """Wrong number of arguments should raise error during bind."""
        with Client("vgi-fixture-worker") as client:
            with pytest.raises(ClientError) as exc_info:
                # sequence expects 1 argument but we provide none
                list(
                    client.table_function(
                        function_name="sequence",
                        arguments=Arguments(positional=()),
                    )
                )

            error_message = str(exc_info.value)
            # Should mention missing/required argument or matching failure
            error_lower = error_message.lower()
            assert any(term in error_lower for term in ["argument", "required", "missing", "match"]), (
                f"Expected argument-related error, got: {error_message}"
            )
