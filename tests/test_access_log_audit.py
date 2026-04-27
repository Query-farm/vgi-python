"""End-to-end access log conformance test over HTTP transport.

Starts the example HTTP server with --log-format json, exercises every function
type via DuckDB SQL, captures server log output, and validates conformance via
``vgi_rpc.access_log_conformance``.

Can also validate a pre-existing log file from ``run_http_integration.sh``.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import textwrap
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pytest
from vgi_rpc.access_log_conformance import validate_access_logs

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_DEFAULT_DUCKDB = str(Path.home() / "Development" / "vgi" / "build" / "release" / "duckdb")
DUCKDB_BINARY = os.environ.get("DUCKDB_BINARY", _DEFAULT_DUCKDB)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _free_port() -> int:
    """Allocate an available localhost TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@dataclass
class ServerProcess:
    """HTTP server subprocess handle with connection info."""

    proc: subprocess.Popen[str]
    port: int
    log_file: Path
    base_url: str


@contextmanager
def _start_http_server(
    *,
    port: int | None = None,
) -> Iterator[ServerProcess]:
    """Start the example HTTP server with JSON logging, wait for ready, yield, shut down."""
    if port is None:
        port = _free_port()

    log_path = Path(tempfile.mktemp(suffix=".log", prefix="vgi-audit-"))

    cmd = [
        sys.executable,
        "-m",
        "vgi._test_fixtures.http_server",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--log-format",
        "json",
    ]

    log_fh = Path(log_path).open("w")  # noqa: SIM115
    proc = subprocess.Popen(
        cmd,
        env=os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=log_fh,
        text=True,
    )

    base_url = f"http://127.0.0.1:{port}"
    server = ServerProcess(proc=proc, port=port, log_file=log_path, base_url=base_url)

    try:
        _wait_for_server(base_url)
        yield server
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        log_fh.close()


def _wait_for_server(base_url: str, timeout: float = 30) -> None:
    """Wait until the HTTP server responds."""
    from vgi_rpc.http import http_capabilities

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            http_capabilities(base_url=base_url)
            return
        except Exception:
            time.sleep(0.25)
    raise TimeoutError(f"Timed out waiting for HTTP server at {base_url}")


def _run_duckdb_sql(sql: str) -> subprocess.CompletedProcess[str]:
    """Write SQL to a temp file and run it through DuckDB."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sql)
        sql_file = f.name

    try:
        return subprocess.run(
            [DUCKDB_BINARY, "-f", sql_file],
            capture_output=True,
            text=True,
            timeout=120,
        )
    finally:
        os.unlink(sql_file)


def _parse_access_logs(log_file: Path) -> list[dict[str, object]]:
    """Parse JSON access log entries from a server log file."""
    entries: list[dict[str, object]] = []
    with open(log_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                if isinstance(entry, dict) and entry.get("logger") == "vgi_rpc.access":
                    entries.append(entry)
            except json.JSONDecodeError:
                continue
    return entries


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


_skip_no_duckdb = pytest.mark.skipif(
    shutil.which(DUCKDB_BINARY) is None,
    reason=f"DuckDB binary '{DUCKDB_BINARY}' not found (set DUCKDB_BINARY env var)",
)


@pytest.fixture(scope="module")
def http_server() -> Iterator[ServerProcess]:
    """Start the example HTTP server for the test module."""
    with _start_http_server() as server:
        yield server


@_skip_no_duckdb
class TestAccessLogConformance:
    """Run DuckDB queries over HTTP, capture logs, validate conformance."""

    def test_all_function_types(self, http_server: ServerProcess) -> None:
        """Exercise catalog, table, scalar, table-in-out, and error operations."""
        sql = textwrap.dedent(f"""\
            ATTACH 'example' AS example (TYPE vgi, LOCATION '{http_server.base_url}');

            -- Catalog operations
            SELECT count(*) FROM information_schema.tables WHERE table_catalog = 'example';

            -- Table functions
            SELECT count(*) FROM example.sequence(10);
            SELECT count(*) FROM example.data.numbers;
            SELECT count(*) FROM example.data.departments;

            -- Scalar functions
            SELECT example.double(x) FROM (VALUES (1), (2), (3)) t(x);
            SELECT example.upper_case(s) FROM (VALUES ('hello')) t(s);

            -- Table-in-out functions
            SELECT count(*) FROM example.echo((SELECT * FROM example.sequence(5)));

            DETACH example;
        """)

        # Clear log
        with open(http_server.log_file, "w"):
            pass

        result = _run_duckdb_sql(sql)
        time.sleep(0.5)

        access_logs = _parse_access_logs(http_server.log_file)
        assert len(access_logs) > 0, f"No access log entries found. DuckDB stderr: {result.stderr[:500]}"

        violations = validate_access_logs(access_logs)

        if violations:
            # Print details for debugging
            for v in violations:
                entry = access_logs[v.entry_index] if v.entry_index < len(access_logs) else {}
                print(f"  VIOLATION: entry {v.entry_index} method={v.method} path={v.path} — {v.message}")
                print(f"    keys: {sorted(entry.keys())}")

        assert violations == [], f"{len(violations)} conformance violations in {len(access_logs)} entries"

    def test_error_cases(self, http_server: ServerProcess) -> None:
        """Verify error entries have error_message."""
        sql = textwrap.dedent(f"""\
            ATTACH 'example' AS example (TYPE vgi, LOCATION '{http_server.base_url}');
            SELECT * FROM example.generator_exception();
            DETACH example;
        """)

        with open(http_server.log_file, "w"):
            pass

        _run_duckdb_sql(sql)
        time.sleep(0.5)

        access_logs = _parse_access_logs(http_server.log_file)
        error_entries = [e for e in access_logs if e.get("status") == "error"]

        # The generator_exception function raises during execution.
        # Verify error entries have error_message if any errors were captured.
        for entry in error_entries:
            assert entry.get("error_message"), (
                f"Error entry for method={entry.get('method')} missing error_message. Keys: {sorted(entry.keys())}"
            )


class TestConformanceFromIntegrationLog:
    """Validate a log file produced by run_http_integration.sh."""

    def test_integration_log(self) -> None:
        """Read the HTTP integration test log and validate conformance."""
        log_path = Path("/tmp/vgi-http-test-server.log")
        if not log_path.exists():
            pytest.skip("No integration test log found — run HTTP integration tests first")

        access_logs = _parse_access_logs(log_path)
        if not access_logs:
            pytest.skip("No vgi_rpc.access entries in integration log")

        violations = validate_access_logs(access_logs)
        assert violations == [], (
            f"{len(violations)} conformance violations in {len(access_logs)} entries.\n"
            + "\n".join(f"  {v.method} {v.path}: {v.message}" for v in violations[:20])
        )
