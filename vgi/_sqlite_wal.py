# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Open a SQLite file in WAL mode safely when several processes open it at once.

Pooled workers routinely open the same brand-new database file at the same
moment. Three things go wrong without care:

* ``PRAGMA journal_mode=WAL`` on a fresh rollback-journal file needs an
  exclusive lock, and when two connections try it together SQLite returns
  ``SQLITE_BUSY`` ("database is locked") *without* calling the busy handler
  (waiting could deadlock) — so ``sqlite3.connect(timeout=...)`` does not
  cover it. :func:`enable_wal` retries it with a bounded backoff.
* Every statement must wait for another process's lock rather than fail, so
  ``busy_timeout`` is set before anything else.
* Concurrent first-time schema setup can hit the read-to-write lock upgrade
  that SQLite also reports as an immediate ``SQLITE_BUSY``.
  :func:`immediate_transaction` takes the write lock up front with
  ``BEGIN IMMEDIATE`` (which does honour ``busy_timeout``), so setup
  serializes.
"""

from __future__ import annotations

import contextlib
import sqlite3
import time
from collections.abc import Iterator
from typing import Any

__all__ = ["BUSY_TIMEOUT_S", "connect_wal", "enable_wal", "immediate_transaction"]

#: How long any statement waits for another connection's lock, in seconds.
BUSY_TIMEOUT_S = 30.0

_BUSY_CODES = frozenset({sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED})


def _is_busy(exc: sqlite3.OperationalError) -> bool:
    code = getattr(exc, "sqlite_errorcode", None)
    if isinstance(code, int):
        return (code & 0xFF) in _BUSY_CODES
    message = str(exc)
    return "database is locked" in message or "database table is locked" in message


def enable_wal(conn: sqlite3.Connection, *, timeout: float = BUSY_TIMEOUT_S) -> None:
    """Set ``busy_timeout``, then switch ``conn`` to WAL, retrying on ``SQLITE_BUSY``.

    Raises the last ``sqlite3.OperationalError`` once ``timeout`` seconds have
    passed, or at once for any error other than busy/locked.
    """
    conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
    deadline = time.monotonic() + timeout
    delay = 0.005
    while True:
        try:
            conn.execute("PRAGMA journal_mode=WAL").fetchall()
            return
        except sqlite3.OperationalError as exc:
            if not _is_busy(exc) or time.monotonic() >= deadline:
                raise
        time.sleep(delay)
        delay = min(delay * 2, 0.1)


def connect_wal(database: str, *, timeout: float = BUSY_TIMEOUT_S, **kwargs: Any) -> sqlite3.Connection:
    """``sqlite3.connect`` plus :func:`enable_wal`; extra keyword arguments go to ``connect``."""
    conn: sqlite3.Connection = sqlite3.connect(database, timeout=timeout, **kwargs)
    try:
        enable_wal(conn, timeout=timeout)
    except BaseException:
        conn.close()
        raise
    return conn


@contextlib.contextmanager
def immediate_transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Run the block in ``BEGIN IMMEDIATE`` … ``COMMIT``, rolling back on error.

    Taking the write lock up front makes concurrent schema setup serialize on
    it instead of failing. Works whatever ``conn.isolation_level`` is.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        with contextlib.suppress(sqlite3.Error):
            conn.rollback()
        raise
    conn.commit()
