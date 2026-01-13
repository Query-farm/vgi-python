"""Tests for worker exit handling in Client and catalog operations.

These tests verify that the Client and CatalogClientMixin properly detect
and report when a worker process terminates unexpectedly (before reading
input or producing output).
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from vgi.arguments import Arguments
from vgi.client.catalog_mixin import CatalogClientError
from vgi.client.client import Client, ClientError


def _make_test_batch() -> pa.RecordBatch:
    """Create a simple test batch for invoking functions."""
    return pa.RecordBatch.from_pydict({"x": [1, 2, 3]})


class TestClientWorkerExitHandling:
    """Tests for worker exit handling in Client class."""

    def test_client_raises_error_when_worker_exits_immediately(self) -> None:
        """Client raises ClientError when worker exits before accepting input."""
        # Use 'exit 1' which immediately terminates without reading stdin
        client = Client("exit 1")

        # Worker exits before sending any output - detected either via BrokenPipeError
        # or via early exit check when reading fails
        with pytest.raises(ClientError):
            client.start()
            # Try to invoke a function - this should fail because the worker
            # exited before we could write to it or before sending output
            list(
                client.table_in_out_function(
                    function_name="test",
                    arguments=Arguments(),
                    input=iter([_make_test_batch()]),
                )
            )

    def test_client_raises_error_with_useful_message(self) -> None:
        """Client error message includes useful debugging information."""
        # Use a specific exit code
        client = Client("exit 42")

        try:
            client.start()
            list(
                client.table_in_out_function(
                    function_name="test",
                    arguments=Arguments(),
                    input=iter([_make_test_batch()]),
                )
            )
            pytest.fail("Expected ClientError to be raised")
        except ClientError as e:
            # Verify the error message contains useful information
            # Either includes exit code (BrokenPipeError path) or describes
            # what operation failed (read-failure path)
            err_str = str(e)
            has_useful_info = (
                "42" in err_str  # Exit code from BrokenPipeError path
                or "bind_result" in err_str  # Read failure path
                or "terminated" in err_str  # BrokenPipeError message
            )
            assert has_useful_info, f"Expected useful error info, got: {e}"


class TestCatalogMixinWorkerExitHandling:
    """Tests for worker exit handling in CatalogClientMixin."""

    def test_catalog_invoke_raises_error_when_worker_exits(self) -> None:
        """Catalog invoke raises CatalogClientError when worker exits."""
        # Use 'exit 1' which immediately terminates without reading stdin
        client = Client("exit 1")

        # Worker exits - detected either via BrokenPipeError (shows "terminated
        # unexpectedly") or via read failure (shows "Failed to read catalog result")
        with pytest.raises(CatalogClientError):
            # catalogs() uses _catalog_invoke internally
            list(client.catalogs())

    def test_catalog_invoke_error_includes_useful_info(self) -> None:
        """Catalog error message includes useful debugging information."""
        client = Client("exit 42")

        try:
            list(client.catalogs())
            pytest.fail("Expected CatalogClientError to be raised")
        except CatalogClientError as e:
            # Error should mention either "terminated unexpectedly" (BrokenPipeError)
            # or "catalog result" (read failure) and include exit code
            err_str = str(e)
            assert "42" in err_str or "catalog" in err_str.lower(), (
                f"Expected useful error info, got: {e}"
            )
