# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for VGI_WORKER_DEBUG env var and stderr enrichment in error messages."""

from __future__ import annotations

import os
import sys
from unittest.mock import patch

import pyarrow as pa
import pytest

from vgi.arguments import Arguments
from vgi.client.client import Client, ClientError


def _stderr_worker(*lines: str) -> list[str]:
    """Argv for a worker that writes ``lines`` to stderr and exits non-zero.

    An argv list rather than a command string: the ``-c`` body carries spaces and
    quotes, which no single quoting convention survives on both POSIX and Windows.
    """
    body = "import sys; " + "".join(f"sys.stderr.write({line!r} + chr(10)); " for line in lines)
    return [sys.executable, "-c", body + "sys.stderr.flush(); sys.exit(1)"]


class TestWorkerDebugEnvVar:
    """Tests for VGI_WORKER_DEBUG environment variable behavior."""

    def test_env_var_enables_passthrough_stderr(self) -> None:
        """VGI_WORKER_DEBUG=1 should set passthrough_stderr=True."""
        with patch.dict(os.environ, {"VGI_WORKER_DEBUG": "1"}):
            client = Client("dummy-worker")
        assert client.passthrough_stderr is True

    def test_env_var_true_enables_passthrough_stderr(self) -> None:
        """VGI_WORKER_DEBUG=true should set passthrough_stderr=True."""
        with patch.dict(os.environ, {"VGI_WORKER_DEBUG": "true"}):
            client = Client("dummy-worker")
        assert client.passthrough_stderr is True

    def test_env_var_yes_enables_passthrough_stderr(self) -> None:
        """VGI_WORKER_DEBUG=yes should set passthrough_stderr=True."""
        with patch.dict(os.environ, {"VGI_WORKER_DEBUG": "YES"}):
            client = Client("dummy-worker")
        assert client.passthrough_stderr is True

    def test_no_env_var_defaults_to_false(self) -> None:
        """Without VGI_WORKER_DEBUG, passthrough_stderr defaults to False."""
        with patch.dict(os.environ, {}, clear=True):
            client = Client("dummy-worker")
        assert client.passthrough_stderr is False

    def test_explicit_passthrough_stderr_without_env_var(self) -> None:
        """Explicit passthrough_stderr=True works without env var."""
        with patch.dict(os.environ, {}, clear=True):
            client = Client("dummy-worker", passthrough_stderr=True)
        assert client.passthrough_stderr is True


class TestStderrInErrorMessages:
    """Tests for stderr content in ClientError messages."""

    def test_error_includes_stderr_on_worker_failure(self) -> None:
        """Error messages should include stderr when worker fails (non-pooled)."""
        worker_script = _stderr_worker("Debug: bind starting", "Error: function not found")

        client = Client(worker_script, pool=None)
        with pytest.raises(ClientError) as exc_info:
            client.start()
            list(
                client.table_function(
                    function_name="nonexistent",
                    schema_name="main",
                    arguments=Arguments(),
                )
            )
        client.stop()

        error_msg = str(exc_info.value)
        assert "Worker stderr" in error_msg
        assert "Error: function not found" in error_msg

    def test_error_no_stderr_section_when_passthrough(self) -> None:
        """Error messages should NOT include 'Worker stderr:' when passthrough is enabled."""
        worker_script = _stderr_worker("Debug info")

        client = Client(worker_script, passthrough_stderr=True, pool=None)
        with pytest.raises(ClientError) as exc_info:
            client.start()
            list(
                client.table_function(
                    function_name="nonexistent",
                    schema_name="main",
                    arguments=Arguments(),
                )
            )
        client.stop()

        error_msg = str(exc_info.value)
        assert "Worker stderr" not in error_msg

    def test_stderr_enrichment_on_table_in_out_function(self) -> None:
        """table_in_out_function errors should include stderr."""
        worker_script = _stderr_worker("worker log line")

        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})
        client = Client(worker_script, pool=None)
        with pytest.raises(ClientError) as exc_info:
            client.start()
            list(
                client.table_in_out_function(
                    function_name="nonexistent",
                    schema_name="main",
                    input=iter([batch]),
                )
            )
        client.stop()

        assert "Worker stderr" in str(exc_info.value)

    def test_stderr_enrichment_on_scalar_function(self) -> None:
        """scalar_function errors should include stderr."""
        worker_script = _stderr_worker("scalar debug")

        batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})
        client = Client(worker_script, pool=None)
        with pytest.raises(ClientError) as exc_info:
            client.start()
            list(
                client.scalar_function(
                    function_name="nonexistent",
                    schema_name="main",
                    input=iter([batch]),
                )
            )
        client.stop()

        assert "Worker stderr" in str(exc_info.value)
