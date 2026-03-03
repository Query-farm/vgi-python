"""Tests for the vgi-serve CLI and programmatic API."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import textwrap
import time

import pyarrow as pa
import pytest

from vgi.scalar_function import ScalarFunction
from vgi.serve import create_app, load_worker_class
from vgi.worker import Worker

# ---------------------------------------------------------------------------
# Fixture workers for testing
# ---------------------------------------------------------------------------


class _DoubleFunc(ScalarFunction):
    class Meta:
        name = "double"

    def compute(self, x: pa.Int64Array) -> pa.Int64Array:
        return pa.compute.multiply(x, 2)


class _SingleWorker(Worker):
    """Worker with one function — for auto-discover tests."""

    functions = [_DoubleFunc]


class _AnotherWorker(Worker):
    """Second worker in same module — for multiple-workers error test."""

    functions = [_DoubleFunc]


# ---------------------------------------------------------------------------
# Tests: load_worker_class
# ---------------------------------------------------------------------------


class TestLoadWorkerClass:
    """Tests for load_worker_class()."""

    def test_module_colon_classname(self) -> None:
        """module:ClassName loads the exact class."""
        cls = load_worker_class("vgi.examples.worker:ExampleWorker")
        from vgi.examples.worker import ExampleWorker

        assert cls is ExampleWorker

    def test_auto_discover(self) -> None:
        """Bare module auto-discovers the single Worker subclass."""
        cls = load_worker_class("vgi.examples.worker")
        from vgi.examples.worker import ExampleWorker

        assert cls is ExampleWorker

    def test_file_path(self, tmp_path: object) -> None:
        """./file.py loads from a file path."""
        p = os.path.join(str(tmp_path), "my_worker.py")
        with open(p, "w") as f:
            f.write(
                textwrap.dedent("""\
                from vgi.worker import Worker
                from vgi.scalar_function import ScalarFunction
                import pyarrow as pa

                class Dbl(ScalarFunction):
                    class Meta:
                        name = "dbl"
                    def compute(self, x: pa.Int64Array) -> pa.Int64Array:
                        return pa.compute.multiply(x, 2)

                class FileWorker(Worker):
                    functions = [Dbl]
                """)
            )
        cls = load_worker_class(p)
        assert cls.__name__ == "FileWorker"
        assert issubclass(cls, Worker)

    def test_file_path_with_classname(self, tmp_path: object) -> None:
        """./file.py:ClassName loads a specific class from a file."""
        p = os.path.join(str(tmp_path), "multi.py")
        with open(p, "w") as f:
            f.write(
                textwrap.dedent("""\
                from vgi.worker import Worker

                class WorkerA(Worker):
                    functions = []

                class WorkerB(Worker):
                    functions = []
                """)
            )
        cls = load_worker_class(f"{p}:WorkerB")
        assert cls.__name__ == "WorkerB"

    def test_no_worker_exits(self, tmp_path: object) -> None:
        """Module with no Worker subclass exits with error."""
        p = os.path.join(str(tmp_path), "empty.py")
        with open(p, "w") as f:
            f.write("x = 1\n")
        with pytest.raises(SystemExit):
            load_worker_class(p)

    def test_multiple_workers_exits(self, tmp_path: object) -> None:
        """Module with multiple Workers (no :Class) exits with error."""
        p = os.path.join(str(tmp_path), "multi.py")
        with open(p, "w") as f:
            f.write(
                textwrap.dedent("""\
                from vgi.worker import Worker

                class WorkerA(Worker):
                    functions = []

                class WorkerB(Worker):
                    functions = []
                """)
            )
        with pytest.raises(SystemExit):
            load_worker_class(p)

    def test_bad_classname_exits(self) -> None:
        """module:NonExistent exits with error."""
        with pytest.raises(SystemExit):
            load_worker_class("vgi.examples.worker:NonExistent")

    def test_not_a_worker_exits(self) -> None:
        """module:NotAWorker exits with error."""
        with pytest.raises(SystemExit):
            load_worker_class("vgi.examples.worker:ExampleWorker.Settings")

    def test_bad_module_exits(self) -> None:
        """Non-existent module exits with error."""
        with pytest.raises(SystemExit):
            load_worker_class("nonexistent_module_xyz_123")

    def test_bad_file_exits(self) -> None:
        """Non-existent file exits with error."""
        with pytest.raises(SystemExit):
            load_worker_class("./no_such_file.py")

    def test_excludes_imported_workers(self, tmp_path: object) -> None:
        """Auto-discover excludes Worker subclasses imported from elsewhere."""
        p = os.path.join(str(tmp_path), "importer.py")
        with open(p, "w") as f:
            f.write(
                textwrap.dedent("""\
                from vgi.examples.worker import ExampleWorker  # noqa: F401
                from vgi.worker import Worker

                class LocalWorker(Worker):
                    functions = []
                """)
            )
        cls = load_worker_class(p)
        assert cls.__name__ == "LocalWorker"


# ---------------------------------------------------------------------------
# Tests: create_app
# ---------------------------------------------------------------------------


class TestCreateApp:
    """Tests for create_app()."""

    def test_returns_falcon_app(self) -> None:
        """create_app() returns a Falcon WSGI app."""
        import falcon

        app = create_app(_SingleWorker)
        assert isinstance(app, falcon.App)

    def test_custom_prefix(self) -> None:
        """Custom prefix is used in the app."""
        app = create_app(_SingleWorker, prefix="/api")
        # The app should have routes under /api — verify it's a Falcon app
        import falcon

        assert isinstance(app, falcon.App)

    def test_describe_disabled(self) -> None:
        """describe=False skips the worker page route."""
        app = create_app(_SingleWorker, describe=False)
        import falcon

        assert isinstance(app, falcon.App)


# ---------------------------------------------------------------------------
# Tests: CLI integration
# ---------------------------------------------------------------------------


def _free_port() -> int:
    """Allocate an available localhost TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class TestCLI:
    """Integration tests for the vgi-serve CLI."""

    def test_bad_reference_exits(self) -> None:
        """Bad worker reference prints error and exits non-zero."""
        result = subprocess.run(
            [sys.executable, "-m", "vgi.serve", "nonexistent_module_xyz"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode != 0
        assert "Error" in result.stderr

    def test_http_mode_starts_and_responds(self) -> None:
        """HTTP mode starts and serves requests."""
        port = _free_port()
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "vgi.serve",
                "vgi.examples.worker:ExampleWorker",
                "--http",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            # Wait for PORT: line
            assert proc.stdout is not None
            port_line = proc.stdout.readline()
            assert port_line.strip() == f"PORT:{port}"

            # Give server a moment to start
            time.sleep(0.5)

            # Hit the worker description page
            import urllib.request

            url = f"http://127.0.0.1:{port}/vgi/worker"
            with urllib.request.urlopen(url, timeout=5) as resp:
                body = resp.read()
                assert resp.status == 200
                assert b"ExampleWorker" in body
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_port_env_var(self) -> None:
        """$PORT env var is respected when --port is not given."""
        port = _free_port()
        env = os.environ.copy()
        env["PORT"] = str(port)
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "vgi.serve",
                "vgi.examples.worker:ExampleWorker",
                "--http",
                "--host",
                "127.0.0.1",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        try:
            assert proc.stdout is not None
            port_line = proc.stdout.readline()
            assert port_line.strip() == f"PORT:{port}"
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_auto_discover_module(self) -> None:
        """vgi-serve with bare module auto-discovers the worker."""
        port = _free_port()
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "vgi.serve",
                "vgi.examples.worker",
                "--http",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            assert proc.stdout is not None
            port_line = proc.stdout.readline()
            assert port_line.strip() == f"PORT:{port}"
        finally:
            proc.terminate()
            proc.wait(timeout=5)
