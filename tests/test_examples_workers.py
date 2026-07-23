# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""End-to-end tests for the documentation example workers in ``examples/``.

The docs embed these files via pymdownx snippets, so ``find_examples`` never
executes them. This module is the source of truth that they actually run:

- every ``examples/*.py`` module imports cleanly, and
- the scalar / table / table-in-out workers serve real results over the
  subprocess transport, and the aggregate worker accumulates correctly.

Workers are spawned with the current interpreter (``sys.executable``) so the
already-installed ``vgi`` is used — no ``uv run`` dependency re-resolution.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pytest

from vgi.client import Client

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"

# Make the example modules importable by name (the framework re-imports a
# function's defining module during __init_subclass__).
if str(EXAMPLES_DIR) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_DIR))

ALL_EXAMPLES = sorted(p.name for p in EXAMPLES_DIR.glob("*.py"))


def _spawn(script: str) -> Client:
    """Return a subprocess Client for an example worker, run with this interpreter."""
    return Client(f"{sys.executable} {EXAMPLES_DIR / script}", pool=None)


@pytest.mark.parametrize("filename", ALL_EXAMPLES, ids=ALL_EXAMPLES)
def test_example_imports(filename: str) -> None:
    """Every example module imports without error."""
    __import__(filename.removesuffix(".py"))


def test_calc_scalar_worker() -> None:
    """The stage-1 scalar-only tutorial worker doubles each input row."""
    with _spawn("calc_scalar_worker.py") as client:
        batch = pa.record_batch({"value": pa.array([21, 5], type=pa.int64())})
        out = list(client.scalar_function(function_name="double", schema_name="main", input=iter([batch])))
    assert [v for b in out for v in b.column(0).to_pylist()] == [42, 10]


def test_calc_worker_scalar_and_table() -> None:
    """The full tutorial worker serves both the scalar and the table function."""
    with _spawn("calc_worker.py") as client:
        values = pa.record_batch({"value": pa.array([21], type=pa.int64())})
        scalar = list(client.scalar_function(function_name="double", schema_name="main", input=iter([values])))
        assert scalar[0].column(0).to_pylist() == [42]

    with _spawn("calc_worker.py") as client:
        from vgi.arguments import Arguments

        rows = list(
            client.table_function(
                function_name="series",
                schema_name="main",
                arguments=Arguments(positional=(pa.scalar(3),)),
            )
        )
    assert [v for b in rows for v in b.column("n").to_pylist()] == [0, 1, 2]


def test_series_streaming_worker() -> None:
    """The stateful streaming generator emits the full range across chunked calls."""
    from vgi.arguments import Arguments

    with _spawn("series_streaming_worker.py") as client:
        rows = list(
            client.table_function(
                function_name="series",
                schema_name="main",
                arguments=Arguments(positional=(pa.scalar(5),)),
            )
        )
    assert [v for b in rows for v in b.column("n").to_pylist()] == [0, 1, 2, 3, 4]


def test_row_count_worker_buffering() -> None:
    """The buffering worker counts every input row across batches and emits one total."""
    with _spawn("row_count_worker.py") as client:
        batches = [
            pa.record_batch({"x": pa.array([1, 2, 3], type=pa.int64())}),
            pa.record_batch({"x": pa.array([4, 5], type=pa.int64())}),
        ]
        out = list(client.table_buffering_function(function_name="row_count", schema_name="main", input=iter(batches)))
    assert [v for b in out for v in b.column("count").to_pylist()] == [5]


def test_greeting_scalar_worker_string_example() -> None:
    """The string-scalar example (used in the function-patterns guide) still serves."""
    with _spawn("greeting_scalar_worker.py") as client:
        batch = pa.record_batch({"name": pa.array(["Alice", "Bob"])})
        out = list(client.scalar_function(function_name="greeting", schema_name="main", input=iter([batch])))
    assert [v for b in out for v in b.column(0).to_pylist()] == ["Hello, Alice!", "Hello, Bob!"]


def test_filter_worker_table_in_out() -> None:
    """The table-in-out worker keeps only rows whose value is positive."""
    with _spawn("filter_worker.py") as client:
        batch = pa.record_batch({"value": pa.array([-2, 5, 0, 9, -1], type=pa.int64())})
        out = list(
            client.table_in_out_function(function_name="filter_positive", schema_name="main", input=iter([batch]))
        )
    kept = [v for b in out for v in b.column("value").to_pylist()]
    assert kept == [5, 9]


def test_sum_worker_aggregate_phases() -> None:
    """The aggregate worker accumulates per-group totals through its phases.

    Aggregates are driven by DuckDB's GROUP BY (no direct Client entry point),
    so we exercise update -> combine -> finalize directly.
    """
    import sum_worker as m

    states: dict[int, object] = {0: m.SumState(), 1: m.SumState()}
    m.Sum.update(states, pa.array([0, 0, 1, 1, 1], type=pa.int64()), pa.array([10, 5, 1, 2, 3], type=pa.int64()))
    assert states[0].total == 15  # type: ignore[attr-defined]
    assert states[1].total == 6  # type: ignore[attr-defined]

    merged = m.Sum.combine(m.SumState(total=15), m.SumState(total=100), params=None)
    assert merged.total == 115

    out = m.Sum.finalize(pa.array([0, 1], type=pa.int64()), states, params=None)
    assert out.column("result").to_pylist() == [15, 6]
