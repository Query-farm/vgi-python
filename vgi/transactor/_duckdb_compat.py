# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Type-safe shim for VGI's ``subcursor()`` extension to duckdb-python.

The VGI fork of duckdb-python adds ``DuckDBPyConnection.subcursor()`` so
callers can issue reads inside an open write transaction. That change has
not yet been merged into haybarn or upstreamed to duckdb — only local fork
builds provide it. The upstream type stubs don't know about it, so we cast
through a small Protocol here rather than scatter ``# type: ignore``
across the codebase.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    import duckdb


class _SupportsSubcursor(Protocol):
    def subcursor(self) -> duckdb.DuckDBPyConnection: ...


def subcursor(conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """Return a read cursor that shares ``conn``'s transaction context."""
    return cast(_SupportsSubcursor, conn).subcursor()
