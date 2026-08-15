# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for VGI_WORKER_DEBUG env var and stderr enrichment in error messages."""

from __future__ import annotations

import os
import sys
import time
from typing import Any
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

    def test_stderr_survives_a_late_drainer(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Enrichment must not race the drainer thread that fills its buffer.

        ``_stderr_buffer`` is filled by a daemon thread otherwise joined only in
        ``stop()``, so before the fix an error raised the instant a worker died
        could be built from an empty buffer — losing exactly the traceback the
        enrichment exists to surface. Delaying the drainer reproduces under a
        scheduler what a loaded machine produces on its own; 50ms was enough to
        lose the output entirely.
        """
        real_drain = Client._drain_stderr

        def late_drain(self: Client, stderr: Any) -> None:
            time.sleep(0.25)
            real_drain(self, stderr)

        monkeypatch.setattr(Client, "_drain_stderr", late_drain)

        client = Client(_stderr_worker("Error: function not found"), pool=None)
        with pytest.raises(ClientError) as exc_info:
            client.start()
            list(client.table_function(function_name="nonexistent", schema_name="main", arguments=Arguments()))
        client.stop()

        assert "Error: function not found" in str(exc_info.value)

    def test_a_live_worker_does_not_stall_the_error_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Only a *dead* worker's drainer is waited on.

        A running worker holds its stderr pipe open, so its drainer never
        returns; waiting on that would stall every ordinary error — a bind
        rejection from a perfectly healthy worker — until the timeout.
        """
        elapsed: list[float] = []
        real_enrich = Client._client_error_with_stderr

        def timed_enrich(self: Client, error: ClientError) -> ClientError:
            started = time.monotonic()
            try:
                return real_enrich(self, error)
            finally:
                elapsed.append(time.monotonic() - started)

        monkeypatch.setattr(Client, "_client_error_with_stderr", timed_enrich)

        with Client("vgi-fixture-worker", pool=None) as client:
            with pytest.raises(ClientError):
                list(client.table_function(function_name="no_such_function", schema_name="main", arguments=Arguments()))
            assert client._primary is not None  # noqa: SLF001 - a started client always has one
            proc = client._primary.proc  # noqa: SLF001 - the point is that it is still running
            assert proc is not None
            assert proc.poll() is None, "probe is only meaningful while the worker is alive"
            assert client._stderr_threads[0].is_alive(), "the drainer should still be blocked on the open pipe"  # noqa: SLF001

        assert elapsed, "the error should have gone through stderr enrichment"
        assert max(elapsed) < Client.STDERR_DRAIN_TIMEOUT / 4, (
            f"enrichment waited {max(elapsed):.3f}s on a worker that is still running"
        )

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
