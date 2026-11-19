# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for VGI_IDLE_EXIT_SECONDS: vgi-serve --http exits cleanly when idle."""

from __future__ import annotations

import importlib.util
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from vgi import idle_exit
from vgi.idle_exit import (
    ActivityTracker,
    DirectoryActivity,
    IdleMiddleware,
    IdleWatcher,
    idle_exit_seconds,
    is_exempt,
)

# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


class TestIdleExitSeconds:
    """Parsing VGI_IDLE_EXIT_SECONDS."""

    def test_unset_or_empty_is_off(self) -> None:
        """No variable (or an empty one) disables idle exit."""
        assert idle_exit_seconds({}) is None
        assert idle_exit_seconds({"VGI_IDLE_EXIT_SECONDS": " "}) is None

    def test_number(self) -> None:
        """Integers and decimals are seconds."""
        assert idle_exit_seconds({"VGI_IDLE_EXIT_SECONDS": "120"}) == 120.0
        assert idle_exit_seconds({"VGI_IDLE_EXIT_SECONDS": "0.5"}) == 0.5

    @pytest.mark.parametrize("raw", ["0", "-3", "abc", "nan", "inf"])
    def test_rejects_non_positive_or_junk(self, raw: str) -> None:
        """Anything but a positive finite number is a configuration error."""
        with pytest.raises(ValueError, match="VGI_IDLE_EXIT_SECONDS"):
            idle_exit_seconds({"VGI_IDLE_EXIT_SECONDS": raw})


class TestTracker:
    """In-flight counting and quiet time."""

    def test_quiet_time_and_in_flight(self) -> None:
        """Quiet time runs from the last begin/end; a request in flight is never idle."""
        clock = _Clock()
        t = ActivityTracker(clock=clock)
        clock.now += 10
        assert t.idle_for() == 10
        t.begin()
        clock.now += 1000
        assert t.idle_for() is None
        t.end()
        clock.now += 3
        assert t.idle_for() == 3

    def test_concurrent_requests(self) -> None:
        """Idle only once every in-flight request has finished."""
        clock = _Clock()
        t = ActivityTracker(clock=clock)
        t.begin()
        t.begin()
        t.end()
        assert t.idle_for() is None
        t.end()
        assert t.idle_for() == 0
        t.end()  # an extra end never goes negative
        t.begin()
        assert t.idle_for() is None

    def test_state_file(self, tmp_path: Path) -> None:
        """With a state file, the in-flight count is published for a parent."""
        f = tmp_path / "123"
        t = ActivityTracker(state_file=str(f))
        assert f.read_text() == "0"
        t.begin()
        assert f.read_text() == "1"
        t.end()
        assert f.read_text() == "0"


def _start_response(status: str, headers: list[tuple[str, str]], exc_info: Any = None) -> Callable[[bytes], None]:
    return lambda _b: None


class TestMiddleware:
    """The WSGI wrapper."""

    def test_health_is_exempt(self) -> None:
        """/health (HEAD or GET, any prefix) does not count as activity."""
        assert is_exempt({"PATH_INFO": "/health", "REQUEST_METHOD": "HEAD"})
        assert is_exempt({"PATH_INFO": "/vgi/health", "REQUEST_METHOD": "GET"})
        assert not is_exempt({"PATH_INFO": "/vgi.v3/catalog_attach", "REQUEST_METHOD": "POST"})
        clock = _Clock()
        t = ActivityTracker(clock=clock)
        app = IdleMiddleware(lambda e, s: [b"ok"], t)
        clock.now += 50
        app({"PATH_INFO": "/health", "REQUEST_METHOD": "HEAD"}, _start_response)
        assert t.idle_for() == 50

    def test_in_flight_until_body_closed(self) -> None:
        """A request stays in flight until the server closes its response iterable."""
        clock = _Clock()
        t = ActivityTracker(clock=clock)
        closed: list[bool] = []

        class Body:
            def __iter__(self) -> Iterator[bytes]:
                yield b"a"
                yield b"b"

            def close(self) -> None:
                closed.append(True)

        app = IdleMiddleware(lambda e, s: Body(), t)
        body = app({"PATH_INFO": "/vgi.v3/x", "REQUEST_METHOD": "POST"}, _start_response)
        assert list(body) == [b"a", b"b"]
        assert t.idle_for() is None
        clock.now += 5
        body.close()  # type: ignore[attr-defined]
        body.close()  # type: ignore[attr-defined]  # idempotent
        assert closed == [True]
        assert t.idle_for() == 0

    def test_app_raising_ends_the_request(self) -> None:
        """An application error doesn't leave the request counted as in flight."""
        t = ActivityTracker()

        def boom(e: dict[str, Any], s: Any) -> list[bytes]:
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError):
            IdleMiddleware(boom, t)({"PATH_INFO": "/x", "REQUEST_METHOD": "POST"}, _start_response)
        assert t.idle_for() is not None


class TestDirectoryActivity:
    """The granian parent's view of its children."""

    def test_aggregates_children(self, tmp_path: Path) -> None:
        """Busy if any live child has a request in flight; else quiet since the newest mtime."""
        me = str(os.getpid())
        view = DirectoryActivity(str(tmp_path), started=time.time() - 100)
        q = view.idle_for()
        assert q is not None and q >= 99
        (tmp_path / me).write_text("1")
        assert view.idle_for() is None
        (tmp_path / me).write_text("0")
        q = view.idle_for()
        assert q is not None and q < 5

    def test_ignores_dead_children(self, tmp_path: Path) -> None:
        """A crashed child's stale in-flight count does not keep the server up."""
        proc = subprocess.run([sys.executable, "-c", "import os; print(os.getpid())"], capture_output=True, text=True)
        dead = proc.stdout.strip()
        (tmp_path / dead).write_text("3")
        (tmp_path / "junk.tmp").write_text("9")
        assert DirectoryActivity(str(tmp_path), started=time.time() - 100).idle_for() is not None


class TestWatcher:
    """The timer."""

    def test_fires_once_when_quiet(self) -> None:
        """on_idle runs once the source has been quiet for the period."""
        fired = threading.Event()
        t = ActivityTracker()
        w = IdleWatcher(t, 0.2, fired.set, poll=0.02)
        w.start()
        assert fired.wait(3)
        w.join(1)
        assert not w.is_alive()

    def test_activity_postpones(self) -> None:
        """Requests keep resetting the timer; stop() ends it without firing."""
        fired = threading.Event()
        t = ActivityTracker()
        w = IdleWatcher(t, 0.3, fired.set, poll=0.02)
        w.start()
        for _ in range(10):
            t.begin()
            t.end()
            time.sleep(0.05)
        assert not fired.is_set()
        w.stop()
        w.join(1)
        assert not fired.is_set()

    def test_default_poll(self) -> None:
        """The poll interval is a quarter of the period, clamped to [50 ms, 5 s]."""
        t = ActivityTracker()
        assert IdleWatcher(t, 120, lambda: None).poll == 5.0
        assert IdleWatcher(t, 2, lambda: None).poll == 0.5
        assert IdleWatcher(t, 0.01, lambda: None).poll == 0.05

    def test_state_directory(self) -> None:
        """The private directory is exported to children and removed afterwards."""
        with idle_exit.state_directory() as d:
            assert os.environ[idle_exit.STATE_DIR_ENV] == d
            assert os.path.isdir(d)
            tracker = idle_exit.child_tracker()
            assert tracker is not None
            assert os.path.exists(os.path.join(d, str(os.getpid())))
        assert idle_exit.STATE_DIR_ENV not in os.environ
        assert not os.path.exists(d)
        assert idle_exit.child_tracker() is None


# ---------------------------------------------------------------------------
# vgi-serve end to end
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _serve(port: int, idle: str, *extra: str) -> subprocess.Popen[str]:
    env = {**os.environ, "VGI_IDLE_EXIT_SECONDS": idle}
    env.pop("VGI_ORCHARD_HOSTED", None)
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
            *extra,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    assert proc.stdout is not None
    assert proc.stdout.readline().strip() == f"PORT:{port}"
    return proc


def _get(port: int, path: str, method: str = "GET", wait: float = 10) -> int:
    deadline = time.monotonic() + wait
    while True:
        try:
            req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method)
            with urllib.request.urlopen(req, timeout=5) as resp:
                resp.read()
                return int(resp.status)
        except urllib.error.HTTPError as e:
            return int(e.code)
        except OSError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.1)


_SERVERS = [
    pytest.param((), id="waitress"),
    pytest.param(
        ("--server", "granian", "--http-workers", "2"),
        id="granian",
        marks=[
            pytest.mark.skipif(importlib.util.find_spec("granian") is None, reason="granian extra not installed"),
            pytest.mark.skipif(sys.platform == "win32", reason="granian idle exit is refused on Windows"),
        ],
    ),
]


@pytest.mark.skipif(sys.platform != "win32", reason="Windows only")
def test_pid_alive_does_not_signal_on_windows() -> None:
    """The liveness probe never sends CTRL_C_EVENT (signal 0) to the console."""
    assert idle_exit._pid_alive(os.getpid())
    proc = subprocess.run([sys.executable, "-c", "import os; print(os.getpid())"], capture_output=True, text=True)
    assert not idle_exit._pid_alive(int(proc.stdout.strip()))


@pytest.mark.parametrize("extra", _SERVERS)
def test_serve_exits_cleanly_when_idle(extra: tuple[str, ...]) -> None:
    """Requests keep the server up; health checks don't; quiet for the period exits 0."""
    port = _free_port()
    proc = _serve(port, "2", *extra)
    try:
        # granian announces its port before its workers listen, and a cold macOS
        # runner can take well over 10 s to start them.
        _get(port, "/", wait=60)
        # Busy for well over the period: still serving.
        end = time.monotonic() + 4
        while time.monotonic() < end:
            assert _get(port, "/?format=json") == 200
            time.sleep(0.3)
        assert proc.poll() is None
        # Health checks only: they must not keep it alive. One attempt per probe:
        # _get retries for 10 s, which would count the time after a clean exit.
        started = time.monotonic()
        exited_at: float | None = None
        while time.monotonic() - started < 15:
            if proc.poll() is not None:
                exited_at = time.monotonic()
                break
            try:
                req = urllib.request.Request(f"http://127.0.0.1:{port}/health", method="HEAD")
                with urllib.request.urlopen(req, timeout=2) as resp:
                    resp.read()
            except OSError:
                pass  # shutting down or gone; poll() above decides
            time.sleep(0.3)
        code = proc.wait(timeout=15)
        quiet = (exited_at or time.monotonic()) - started
        assert code == 0, proc.stderr.read() if proc.stderr else ""
        assert 1.5 <= quiet <= 12
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


def test_serve_rejects_bad_idle_value() -> None:
    """A malformed VGI_IDLE_EXIT_SECONDS stops startup with a message."""
    env = {**os.environ, "VGI_IDLE_EXIT_SECONDS": "soon"}
    env.pop("VGI_ORCHARD_HOSTED", None)
    result = subprocess.run(
        [sys.executable, "-m", "vgi.serve", "vgi._test_fixtures.worker:ExampleWorker", "--http", "--port", "0"],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )
    assert result.returncode != 0
    assert "VGI_IDLE_EXIT_SECONDS" in result.stderr
