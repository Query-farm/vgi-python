"""Several processes opening one fresh SQLite storage file at the same instant.

Each worker process of a pooled VGI worker builds its own storage object, and on
a fresh host they all point at a database file that does not exist yet. Turning
a new file into WAL needs an exclusive lock that SQLite refuses with
``SQLITE_BUSY`` without consulting the busy handler, so without a retry some
openers failed at once with "database is locked".
"""

from __future__ import annotations

import multiprocessing
import sqlite3
import traceback
from multiprocessing.synchronize import Barrier
from pathlib import Path
from typing import Any

import pytest

from vgi.catalog.storage import CatalogStorageSqlite
from vgi.function_storage import FunctionStorageSqlite

_PROCESSES = 12
_ROUNDS = 3

_STORAGES: dict[str, Any] = {
    "function": FunctionStorageSqlite,
    "catalog": CatalogStorageSqlite,
}


def _open(kind: str, db_path: str, barrier: Barrier) -> str | None:
    """Open the storage once everyone is ready, and use it; return a traceback on failure."""
    try:
        barrier.wait(timeout=60)
        storage = _STORAGES[kind](db_path)
        if kind == "function":
            storage.state_counter_add(b"s", b"ns", b"k", 1)
            storage.close()
        else:
            storage.attach_put(b"attach-" + str(multiprocessing.current_process().pid).encode(), "cat", {})
        return None
    except BaseException:
        return traceback.format_exc()


def _schema(db_path: str) -> tuple[int, list[tuple[str, str, str | None]]]:
    with sqlite3.connect(db_path) as conn:
        version = conn.execute("PRAGMA schema_version").fetchone()[0]
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"
        objects = conn.execute(
            "SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
    return version, objects


@pytest.mark.parametrize("kind", sorted(_STORAGES))
def test_concurrent_open_of_fresh_db(kind: str, tmp_path: Path) -> None:
    """Every process succeeds and the schema matches one built by a lone opener, created exactly once."""
    reference = str(tmp_path / "reference.db")
    _STORAGES[kind](reference)
    expected = _schema(reference)

    ctx = multiprocessing.get_context("spawn")
    with ctx.Manager() as manager, ctx.Pool(_PROCESSES) as pool:
        for round_no in range(_ROUNDS):
            db_path = str(tmp_path / f"shared-{round_no}.db")
            barrier = manager.Barrier(_PROCESSES)
            errors = [e for e in pool.starmap(_open, [(kind, db_path, barrier)] * _PROCESSES) if e is not None]
            assert errors == [], f"{len(errors)}/{_PROCESSES} openers failed; first:\n{errors[0]}"
            # schema_version counts schema changes: equal to a lone opener's
            # means the tables were created exactly once, never dropped and
            # re-created by a racing opener.
            assert _schema(db_path) == expected
            with sqlite3.connect(db_path) as conn:
                if kind == "function":
                    assert conn.execute("SELECT n FROM function_counter").fetchall() == [(_PROCESSES,)]
                else:
                    assert conn.execute("SELECT COUNT(*) FROM catalog_attachments").fetchone()[0] == _PROCESSES
