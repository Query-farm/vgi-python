# Copyright 2025, 2026 Query Farm LLC - https://query.farm

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
from vgi.serve import (
    _resolve_describe,
    _resolve_otel_config,
    _resolve_signing_key,
    create_app,
    load_worker_class,
)
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
        cls = load_worker_class("vgi._test_fixtures.worker:ExampleWorker")
        from vgi._test_fixtures.worker import ExampleWorker

        assert cls is ExampleWorker

    def test_auto_discover(self) -> None:
        """Bare module auto-discovers the single Worker subclass."""
        cls = load_worker_class("vgi._test_fixtures.worker")
        from vgi._test_fixtures.worker import ExampleWorker

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
            load_worker_class("vgi._test_fixtures.worker:NonExistent")

    def test_not_a_worker_exits(self) -> None:
        """module:NotAWorker exits with error."""
        with pytest.raises(SystemExit):
            load_worker_class("vgi._test_fixtures.worker:ExampleWorker.Settings")

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
                from vgi._test_fixtures.worker import ExampleWorker  # noqa: F401
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

    def test_signing_key_passed(self) -> None:
        """Explicit signing_key is accepted without warning."""
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            app = create_app(_SingleWorker, signing_key=b"test-secret-key-1234")

        import falcon

        assert isinstance(app, falcon.App)


# ---------------------------------------------------------------------------
# Tests: env var helpers
# ---------------------------------------------------------------------------


class TestResolveSigningKey:
    """Tests for _resolve_signing_key()."""

    def test_unset_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No env var returns None."""
        monkeypatch.delenv("VGI_SIGNING_KEY", raising=False)
        assert _resolve_signing_key() is None

    def test_empty_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Empty string returns None."""
        monkeypatch.setenv("VGI_SIGNING_KEY", "")
        assert _resolve_signing_key() is None

    def test_value_returns_bytes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Non-empty value returns UTF-8 encoded bytes."""
        monkeypatch.setenv("VGI_SIGNING_KEY", "my-secret")
        assert _resolve_signing_key() == b"my-secret"


class TestResolveDescribe:
    """Tests for _resolve_describe()."""

    def test_unset_uses_cli(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No env var passes through CLI value."""
        monkeypatch.delenv("VGI_ENABLE_DESCRIBE", raising=False)
        assert _resolve_describe(True) is True
        assert _resolve_describe(False) is False

    def test_env_true_overrides(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Truthy env values enable describe regardless of CLI."""
        for val in ("1", "true", "yes", "True", "YES"):
            monkeypatch.setenv("VGI_ENABLE_DESCRIBE", val)
            assert _resolve_describe(False) is True

    def test_env_false_overrides(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Falsy env values disable describe regardless of CLI."""
        for val in ("0", "false", "no", "False", "NO"):
            monkeypatch.setenv("VGI_ENABLE_DESCRIBE", val)
            assert _resolve_describe(True) is False


# ---------------------------------------------------------------------------
# Tests: _resolve_otel_config
# ---------------------------------------------------------------------------


class TestResolveOtelConfig:
    """Tests for _resolve_otel_config()."""

    def test_unset_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No env var returns None."""
        monkeypatch.delenv("VGI_OTEL_ENABLED", raising=False)
        assert _resolve_otel_config() is None

    def test_falsy_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Falsy values return None."""
        for val in ("0", "false", "no", ""):
            monkeypatch.setenv("VGI_OTEL_ENABLED", val)
            assert _resolve_otel_config() is None

    def test_enabled_returns_otel_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Truthy value returns OtelConfig with defaults."""
        from vgi_rpc.otel import OtelConfig

        monkeypatch.setenv("VGI_OTEL_ENABLED", "1")
        monkeypatch.delenv("VGI_OTEL_CUSTOM_ATTRIBUTES", raising=False)
        monkeypatch.delenv("VGI_OTEL_CLAIM_ATTRIBUTES", raising=False)
        monkeypatch.delenv("VGI_OTEL_DISABLE_TRACING", raising=False)
        monkeypatch.delenv("VGI_OTEL_DISABLE_METRICS", raising=False)
        result = _resolve_otel_config()
        assert isinstance(result, OtelConfig)
        assert result.enable_tracing is True
        assert result.enable_metrics is True
        assert dict(result.custom_attributes) == {}
        assert dict(result.claim_attributes) == {}

    def test_custom_attributes_parsed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Custom attributes are parsed from comma-separated key=value pairs."""
        monkeypatch.setenv("VGI_OTEL_ENABLED", "1")
        monkeypatch.setenv("VGI_OTEL_CUSTOM_ATTRIBUTES", "deployment=prod,region=us-east-1")
        monkeypatch.delenv("VGI_OTEL_CLAIM_ATTRIBUTES", raising=False)
        monkeypatch.delenv("VGI_OTEL_DISABLE_TRACING", raising=False)
        monkeypatch.delenv("VGI_OTEL_DISABLE_METRICS", raising=False)
        result = _resolve_otel_config()
        assert dict(result.custom_attributes) == {"deployment": "prod", "region": "us-east-1"}

    def test_custom_attributes_malformed_exits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Malformed custom attributes (missing =) exit with error."""
        monkeypatch.setenv("VGI_OTEL_ENABLED", "1")
        monkeypatch.setenv("VGI_OTEL_CUSTOM_ATTRIBUTES", "bad_entry")
        with pytest.raises(SystemExit):
            _resolve_otel_config()

    def test_claim_attributes_parsed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Claim attributes are parsed from comma-separated pairs."""
        monkeypatch.setenv("VGI_OTEL_ENABLED", "1")
        monkeypatch.setenv("VGI_OTEL_CLAIM_ATTRIBUTES", "tenant_id=rpc.vgi_rpc.auth.claim.tenant_id")
        monkeypatch.delenv("VGI_OTEL_CUSTOM_ATTRIBUTES", raising=False)
        monkeypatch.delenv("VGI_OTEL_DISABLE_TRACING", raising=False)
        monkeypatch.delenv("VGI_OTEL_DISABLE_METRICS", raising=False)
        result = _resolve_otel_config()
        assert dict(result.claim_attributes) == {"tenant_id": "rpc.vgi_rpc.auth.claim.tenant_id"}

    def test_claim_attributes_malformed_exits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Malformed claim attributes (missing =) exit with error."""
        monkeypatch.setenv("VGI_OTEL_ENABLED", "1")
        monkeypatch.setenv("VGI_OTEL_CLAIM_ATTRIBUTES", "no_equals_sign")
        with pytest.raises(SystemExit):
            _resolve_otel_config()

    def test_disable_tracing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """VGI_OTEL_DISABLE_TRACING disables tracing."""
        monkeypatch.setenv("VGI_OTEL_ENABLED", "1")
        monkeypatch.setenv("VGI_OTEL_DISABLE_TRACING", "1")
        monkeypatch.delenv("VGI_OTEL_CUSTOM_ATTRIBUTES", raising=False)
        monkeypatch.delenv("VGI_OTEL_CLAIM_ATTRIBUTES", raising=False)
        monkeypatch.delenv("VGI_OTEL_DISABLE_METRICS", raising=False)
        result = _resolve_otel_config()
        assert result.enable_tracing is False
        assert result.enable_metrics is True

    def test_disable_metrics(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """VGI_OTEL_DISABLE_METRICS disables metrics."""
        monkeypatch.setenv("VGI_OTEL_ENABLED", "1")
        monkeypatch.setenv("VGI_OTEL_DISABLE_METRICS", "yes")
        monkeypatch.delenv("VGI_OTEL_CUSTOM_ATTRIBUTES", raising=False)
        monkeypatch.delenv("VGI_OTEL_CLAIM_ATTRIBUTES", raising=False)
        monkeypatch.delenv("VGI_OTEL_DISABLE_TRACING", raising=False)
        result = _resolve_otel_config()
        assert result.enable_tracing is True
        assert result.enable_metrics is False

    def test_empty_attributes_returns_empty_dict(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Empty string for attributes results in empty dict."""
        monkeypatch.setenv("VGI_OTEL_ENABLED", "1")
        monkeypatch.setenv("VGI_OTEL_CUSTOM_ATTRIBUTES", "")
        monkeypatch.setenv("VGI_OTEL_CLAIM_ATTRIBUTES", "")
        monkeypatch.delenv("VGI_OTEL_DISABLE_TRACING", raising=False)
        monkeypatch.delenv("VGI_OTEL_DISABLE_METRICS", raising=False)
        result = _resolve_otel_config()
        assert dict(result.custom_attributes) == {}
        assert dict(result.claim_attributes) == {}

    def test_import_error_exits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Missing otel extra exits with helpful message."""
        from unittest.mock import patch

        monkeypatch.setenv("VGI_OTEL_ENABLED", "1")
        # Remove cached module so the import in _resolve_otel_config actually fires
        monkeypatch.delitem(sys.modules, "vgi_rpc.otel", raising=False)
        with patch.dict("sys.modules", {"vgi_rpc.otel": None}), pytest.raises(SystemExit):
            _resolve_otel_config()


class TestCreateAppOtel:
    """Tests for create_app() with otel_config."""

    def test_otel_config_accepted(self) -> None:
        """create_app accepts otel_config parameter."""
        import falcon
        from vgi_rpc.otel import OtelConfig

        app = create_app(
            _SingleWorker,
            otel_config=OtelConfig(custom_attributes={"env": "test"}),
        )
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
                "vgi._test_fixtures.worker:ExampleWorker",
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

            url = f"http://127.0.0.1:{port}/"
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
                "vgi._test_fixtures.worker:ExampleWorker",
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
                "vgi._test_fixtures.worker",
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

    def test_describe_env_var_disables_worker_page(self) -> None:
        """VGI_ENABLE_DESCRIBE=0 disables the worker description page."""
        import urllib.error
        import urllib.request

        port = _free_port()
        env = os.environ.copy()
        env["VGI_ENABLE_DESCRIBE"] = "0"
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "vgi.serve",
                "vgi._test_fixtures.worker:ExampleWorker",
                "--http",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
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

            time.sleep(0.5)

            # Worker page should not be served (404 or 405, not 200)
            url = f"http://127.0.0.1:{port}/worker"
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(url, timeout=5)
            assert exc_info.value.code in (404, 405)
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_signing_key_env_var(self) -> None:
        """VGI_SIGNING_KEY suppresses the random key warning."""
        port = _free_port()
        env = os.environ.copy()
        env["VGI_SIGNING_KEY"] = "test-key-for-ci"
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "vgi.serve",
                "vgi._test_fixtures.worker:ExampleWorker",
                "--http",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
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
            assert proc.stderr is not None
            stderr = proc.stderr.read()
            assert "No signing_key provided" not in stderr
