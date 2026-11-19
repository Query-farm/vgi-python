# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Idle exit for ``vgi-serve --http``: stop the server after a quiet period.

A hosting platform that starts a machine on the next request (Fly.io's
autostart, for one) can let the worker decide when it is idle. With
``VGI_IDLE_EXIT_SECONDS`` set, the HTTP server exits with status 0 once it
has had no request in flight and received no request for that many seconds.

What counts as activity: every request except ``/health``. Health checks are
exempt so a platform probe can never keep an idle worker alive.

Design. The serving process owns the timer; the request path only reports.

- **waitress** (one process): the WSGI middleware updates an in-process
  :class:`ActivityTracker`. When the watcher thread sees the quiet period
  elapse it calls ``_thread.interrupt_main()``; waitress catches the
  ``KeyboardInterrupt``, drains its task queue and returns, so ``vgi-serve``
  returns normally and the exit status is 0.
- **granian** (a parent and forked worker processes): each child's middleware
  also writes its state to one small file per process in a private directory
  the parent created (``<pid>``: in-flight count; the file's mtime is the last
  activity). The parent's watcher aggregates those files, ignoring files of
  processes that no longer exist (a crashed child must not hold the server up
  with a stale in-flight count). On idle it sends itself ``SIGTERM``, which is
  granian's own graceful-shutdown signal: the workers finish and ``serve()``
  returns, and ``vgi-serve`` exits 0.

Files rather than shared memory because granian builds each worker from an
import path: the environment is the only channel parent and child share (see
``vgi.serve.export_serve_config``), and a directory path travels through it.
"""

from __future__ import annotations

import _thread
import contextlib
import logging
import os
import signal
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from typing import Any

_logger = logging.getLogger("vgi.idle_exit")

IDLE_EXIT_ENV = "VGI_IDLE_EXIT_SECONDS"
"""Seconds of no activity after which the HTTP server exits (status 0)."""

STATE_DIR_ENV = "VGI_IDLE_STATE_DIR"
"""Private: the directory granian children report activity into (set by the parent)."""

_EXEMPT_PATH_SUFFIX = "/health"


def idle_exit_seconds(environ: dict[str, str] | None = None) -> float | None:
    """Read ``VGI_IDLE_EXIT_SECONDS``.

    Args:
        environ: The environment to read; defaults to ``os.environ``.

    Returns:
        The idle period in seconds, or ``None`` when unset or empty.

    Raises:
        ValueError: The value is not a positive number.

    """
    raw = (environ if environ is not None else os.environ).get(IDLE_EXIT_ENV, "").strip()
    if not raw:
        return None
    try:
        seconds = float(raw)
    except ValueError:
        msg = f"{IDLE_EXIT_ENV} must be a positive number of seconds, got {raw!r}"
        raise ValueError(msg) from None
    if not seconds > 0 or seconds != seconds or seconds == float("inf"):
        msg = f"{IDLE_EXIT_ENV} must be a positive number of seconds, got {raw!r}"
        raise ValueError(msg)
    return seconds


def is_exempt(environ: dict[str, Any]) -> bool:
    """Whether a request does not count as activity (health checks).

    Args:
        environ: The WSGI environ of the request.

    Returns:
        True for any request to a path ending in ``/health``.

    """
    path = str(environ.get("PATH_INFO") or "")
    return path == _EXEMPT_PATH_SUFFIX or path.endswith(_EXEMPT_PATH_SUFFIX)


class ActivityTracker:
    """In-flight requests and the time of the last one, for one process."""

    def __init__(self, *, clock: Callable[[], float] = time.monotonic, state_file: str | None = None) -> None:
        """Start idle, with the last activity at construction time.

        Args:
            clock: Monotonic clock (seconds); injectable for tests.
            state_file: When set, every change is also written to this file (the
                in-flight count as text; the file's mtime is the last activity) so a
                parent process can aggregate it.

        """
        self._clock = clock
        self._lock = threading.Lock()
        self._in_flight = 0
        self._last = clock()
        self._state_file = state_file
        self._publish()

    def begin(self) -> None:
        """Record a request starting."""
        with self._lock:
            self._in_flight += 1
            self._last = self._clock()
            self._publish()

    def end(self) -> None:
        """Record a request finishing (its response fully sent or abandoned)."""
        with self._lock:
            self._in_flight = max(0, self._in_flight - 1)
            self._last = self._clock()
            self._publish()

    def idle_for(self) -> float | None:
        """Seconds since the last activity, or ``None`` while a request is in flight.

        Returns:
            The quiet time so far, or ``None`` when not idle.

        """
        with self._lock:
            if self._in_flight:
                return None
            return self._clock() - self._last

    def _publish(self) -> None:
        if self._state_file is None:
            return
        tmp = f"{self._state_file}.tmp"
        try:
            with open(tmp, "w", encoding="ascii") as f:
                f.write(str(self._in_flight))
            os.replace(tmp, self._state_file)
        except OSError:  # pragma: no cover - the directory vanished at shutdown
            _logger.debug("idle state write failed", exc_info=True)


class _TrackedBody:
    """A response iterable that reports the request finished when closed."""

    def __init__(self, body: Iterable[bytes], tracker: ActivityTracker) -> None:
        self._body = body
        self._tracker = tracker
        self._closed = False

    def __iter__(self) -> Iterator[bytes]:
        return iter(self._body)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            close = getattr(self._body, "close", None)
            if close is not None:
                close()
        finally:
            self._tracker.end()


class IdleMiddleware:
    """WSGI middleware that reports every non-health request to a tracker.

    A request is in flight from the call until the server closes the response
    iterable (PEP 3333 requires servers to call ``close``), so a long streamed
    response keeps the worker awake for as long as it is being sent.
    """

    def __init__(self, app: Callable[..., Any], tracker: ActivityTracker) -> None:
        """Wrap *app*.

        Args:
            app: The WSGI application to wrap.
            tracker: Where activity is recorded.

        """
        self.app = app
        self.tracker = tracker

    def __call__(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        """Serve one request, counting it unless it is a health check.

        Args:
            environ: The WSGI environ.
            start_response: The WSGI start_response callable.

        Returns:
            The wrapped application's response iterable.

        """
        if is_exempt(environ):
            result: Iterable[bytes] = self.app(environ, start_response)
            return result
        self.tracker.begin()
        try:
            body = self.app(environ, start_response)
        except BaseException:
            self.tracker.end()
            raise
        return _TrackedBody(body, self.tracker)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # pragma: no cover - exists, owned by someone else
        return True
    return True


class DirectoryActivity:
    """Aggregate the per-process state files granian children write."""

    def __init__(self, directory: str, *, started: float | None = None) -> None:
        """Read from *directory*.

        Args:
            directory: The directory holding one ``<pid>`` file per child.
            started: Wall-clock time the server started (counts as activity, so a
                fresh server is never idle before its first full period).

        """
        self.directory = directory
        self.started = time.time() if started is None else started

    def idle_for(self) -> float | None:
        """Seconds since the last activity in any live child, or ``None`` if one is busy.

        Returns:
            The quiet time so far, or ``None`` when not idle.

        """
        last = self.started
        try:
            names = os.listdir(self.directory)
        except OSError:
            return time.time() - last
        for name in names:
            if not name.isdigit():
                continue
            path = os.path.join(self.directory, name)
            try:
                mtime = os.stat(path).st_mtime
                with open(path, encoding="ascii") as f:
                    in_flight = int(f.read().strip() or "0")
            except (OSError, ValueError):
                continue
            if not _pid_alive(int(name)):
                continue
            if in_flight > 0:
                return None
            last = max(last, mtime)
        return time.time() - last


class IdleWatcher(threading.Thread):
    """Poll an activity source and call *on_idle* once the quiet period elapses."""

    def __init__(
        self,
        source: ActivityTracker | DirectoryActivity,
        seconds: float,
        on_idle: Callable[[], None],
        *,
        poll: float | None = None,
    ) -> None:
        """Prepare the watcher; call ``start()`` to run it.

        Args:
            source: Anything with ``idle_for() -> float | None``.
            seconds: The quiet period.
            on_idle: Called once, from this thread.
            poll: Seconds between checks; defaults to a quarter of the period, at
                most 5 s and at least 50 ms.

        """
        super().__init__(name="vgi-idle-exit", daemon=True)
        self.source = source
        self.seconds = seconds
        self.on_idle = on_idle
        self.poll = poll if poll is not None else min(5.0, max(0.05, seconds / 4))
        self._stop_event = threading.Event()

    def stop(self) -> None:
        """Stop watching without calling *on_idle*."""
        self._stop_event.set()

    def run(self) -> None:
        """Check every ``poll`` seconds until idle (then call *on_idle*) or stopped."""
        while not self._stop_event.wait(self.poll):
            quiet = self.source.idle_for()
            if quiet is not None and quiet >= self.seconds:
                _logger.info("idle_exit seconds=%.0f quiet=%.1f", self.seconds, quiet)
                self.on_idle()
                return


def interrupt_main() -> None:
    """Stop a waitress server running in the main thread (it exits cleanly on KeyboardInterrupt)."""
    _thread.interrupt_main()


def terminate_self() -> None:
    """Ask this process to shut down gracefully (granian's parent handles SIGTERM)."""
    os.kill(os.getpid(), signal.SIGTERM)


def child_tracker() -> ActivityTracker | None:
    """The tracker for a granian child, when the parent enabled idle exit.

    Returns:
        A tracker writing ``<dir>/<pid>``, or ``None`` when idle exit is off.

    """
    directory = os.environ.get(STATE_DIR_ENV)
    if not directory:
        return None
    return ActivityTracker(state_file=os.path.join(directory, str(os.getpid())))


@contextlib.contextmanager
def state_directory() -> Iterator[str]:
    """A private directory for children's state files, exported as ``VGI_IDLE_STATE_DIR``.

    Yields:
        The directory path; it is removed (and the variable unset) on exit.

    """
    import shutil
    import tempfile

    directory = tempfile.mkdtemp(prefix="vgi-idle-")
    os.environ[STATE_DIR_ENV] = directory
    try:
        yield directory
    finally:
        os.environ.pop(STATE_DIR_ENV, None)
        shutil.rmtree(directory, ignore_errors=True)
