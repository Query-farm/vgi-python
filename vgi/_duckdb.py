# Copyright 2026 Query Farm LLC - https://query.farm

"""DuckDB-compatible engine resolution: prefer ``haybarn``, fall back to ``duckdb``.

vgi does not hard-depend on either engine. Code that needs an in-process
engine (expression-filter evaluation, the transactor, the statistics
fixtures) resolves it through this module, which prefers Query Farm's
``haybarn`` distribution when installed and falls back to stock ``duckdb``.

Install one via the extras: ``pip install 'vgi-python[haybarn]'`` (preferred) or
``pip install 'vgi-python[duckdb]'``.
"""

from __future__ import annotations

import importlib
from types import ModuleType
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

#: Engine modules in preference order. Both expose the duckdb-python API
#: (``connect()``, ``DuckDBPyConnection``, ...).
_ENGINE_MODULES = ("haybarn", "duckdb")

_engine: ModuleType | None = None


def engine_module() -> ModuleType:
    """Return the resolved engine module (``haybarn`` preferred, ``duckdb`` fallback).

    The result is cached for the life of the process.

    Returns:
        The resolved engine module (``haybarn`` if available, else ``duckdb``).

    Raises:
        ImportError: if neither engine is installed.

    """
    global _engine
    if _engine is None:
        for name in _ENGINE_MODULES:
            try:
                _engine = importlib.import_module(name)
                break
            except ImportError:
                continue
        else:
            raise ImportError(
                "No DuckDB-compatible engine is installed. Install 'haybarn' (preferred) "
                "or 'duckdb', e.g. `pip install 'vgi-python[haybarn]'` or `pip install 'vgi-python[duckdb]'`."
            )
    return _engine


def connect(*args: Any, **kwargs: Any) -> duckdb.DuckDBPyConnection:
    """Open a connection via the resolved engine's ``connect()``.

    Typed as ``duckdb.DuckDBPyConnection`` for the type-checker; at runtime
    the object is whichever engine module won resolution (haybarn's
    connection class implements the same API).
    """
    return cast("duckdb.DuckDBPyConnection", engine_module().connect(*args, **kwargs))


def arrow_table(result: Any, *args: Any) -> pa.Table:
    """Return ``result.to_arrow_table(*args)``, which DuckDB 2.0 types as optional.

    Args:
        result: An executed DuckDB connection or relation.
        *args: Passed to ``to_arrow_table`` (the batch size).

    Returns:
        The result set as an Arrow table.

    Raises:
        RuntimeError: the statement produced no result set.
    """
    table = result.to_arrow_table(*args)
    if table is None:
        raise RuntimeError("the statement produced no result set")
    return cast("pa.Table", table)


def arrow_reader(result: Any, **kwargs: Any) -> pa.RecordBatchReader:
    """Return ``result.to_arrow_reader(**kwargs)``, which DuckDB 2.0 types as optional.

    Args:
        result: An executed DuckDB connection or relation.
        **kwargs: Passed to ``to_arrow_reader`` (``batch_size``).

    Returns:
        A reader over the result set.

    Raises:
        RuntimeError: the statement produced no result set.
    """
    reader = result.to_arrow_reader(**kwargs)
    if reader is None:
        raise RuntimeError("the statement produced no result set")
    return cast("pa.RecordBatchReader", reader)
