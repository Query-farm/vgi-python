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
from typing import cast

import pyarrow as pa
import pytest

from vgi.client import Client
from vgi.table_function import OutputCollector, ProcessParams

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


def test_batch_index_worker_reports_input_order() -> None:
    """The batch-index example sees a distinct index for every sunk batch."""
    with _spawn("batch_index_worker.py") as client:
        batches = [
            pa.record_batch({"x": pa.array([1, 2, 3], type=pa.int64())}),
            pa.record_batch({"x": pa.array([4, 5], type=pa.int64())}),
            pa.record_batch({"x": pa.array([6], type=pa.int64())}),
        ]
        out = list(
            client.table_buffering_function(function_name="batch_indexes", schema_name="main", input=iter(batches))
        )
    indexes = cast("list[int]", [v for b in out for v in b.column("batch_index").to_pylist()])
    rows = cast("list[int]", [v for b in out for v in b.column("rows").to_pylist()])
    # One report row per input batch, ordered by index, and none defaulted to -1.
    assert indexes == sorted(indexes)
    assert len(set(indexes)) == 3
    assert min(indexes) >= 0
    assert sum(rows) == 6


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

    # combine()/finalize() in this example never read params; the real ones
    # receive the ProcessParams the worker built for the aggregate.
    no_params = cast("ProcessParams[None]", None)

    states: dict[int, m.SumState] = {0: m.SumState(), 1: m.SumState()}
    m.Sum.update(states, pa.array([0, 0, 1, 1, 1], type=pa.int64()), pa.array([10, 5, 1, 2, 3], type=pa.int64()))
    assert states[0].total == 15
    assert states[1].total == 6

    merged = m.Sum.combine(m.SumState(total=15), m.SumState(total=100), params=no_params)
    assert merged.total == 115

    out = m.Sum.finalize(pa.array([0, 1], type=pa.int64()), states, params=no_params)
    assert out.column("result").to_pylist() == [15, 6]


def test_cache_worker_advertises_and_revalidates() -> None:
    """The caching worker serves rows, and answers a conditional request with 0 rows.

    The ``vgi.cache.*`` metadata itself is consumed by the C++ extension, not by
    the Python Client, so what is asserted here is the worker-side contract: the
    normal path emits the table, and a matching ``if_none_match`` produces the
    304-equivalent empty batch instead of re-streaming it.
    """
    import cache_worker as m

    from vgi.arguments import Arguments

    with _spawn("cache_worker.py") as client:
        rows = list(
            client.table_function(function_name="rates", schema_name="main", arguments=Arguments(positional=[]))
        )
    assert [v for b in rows for v in b.column("currency").to_pylist()] == ["EUR", "GBP", "JPY"]

    # The advertised metadata renders to the keys the extension reads by string.
    advertised = m.CacheControl(ttl=300, etag=m._DATA_VERSION, revalidatable=True).to_metadata()
    assert advertised["vgi.cache.ttl"] == "300"
    assert advertised["vgi.cache.etag"] == m._DATA_VERSION
    assert advertised["vgi.cache.revalidatable"] == "1"
    assert advertised["vgi.cache.scope"] == "catalog"

    # not_modified is what makes the 0-row reply mean "keep what you have".
    assert m.CacheControl(not_modified=True).to_metadata()["vgi.cache.not_modified"] == "1"


def test_copy_format_worker_advertises_both_directions() -> None:
    """The COPY worker's catalog advertises a reader AND a writer for ``tsvlite``."""
    import copy_format_worker as m

    interface = m.TsvWorker()._get_catalog_interface()()
    formats = interface.copy_from_formats(attach_opaque_data=b"", transaction_opaque_data=None)
    by_direction = {f.direction: f for f in formats}

    assert by_direction["from"].format_name == "tsvlite"
    assert by_direction["from"].handler == "read_tsv_lite"
    # Deliberately NOT "tsvlite": the extension registers COPY functions by name
    # alone, so a reader and a writer sharing one name silently loses the writer.
    assert by_direction["to"].format_name == "tsvlite_out"
    assert by_direction["to"].handler == "write_tsv_lite"


def test_copy_format_worker_round_trips_a_file(tmp_path: Path) -> None:
    """The reader parses TSV into the target schema; the writer writes it back.

    Driven directly rather than through ``COPY``, so the test covers the worker
    halves without needing an engine whose extension registers both directions.
    """
    import copy_format_worker as m

    source = tmp_path / "people.tsv"
    source.write_text("skipme\nalice\t30\nbob\t41\ncarol\t\n", encoding="utf-8")
    schema = pa.schema([("name", pa.string()), ("age", pa.int64())])

    emitted: list[pa.RecordBatch] = []

    class _Out:
        def emit(self, batch: pa.RecordBatch) -> None:
            emitted.append(batch)

        def finish(self) -> None:
            pass

    m.ReadTsvLite.read(
        path=str(source),
        options=m.ReadOptions(skip_rows=1),
        expected_schema=schema,
        params=cast("ProcessParams[m.ReadOptions]", None),
        out=cast("OutputCollector", _Out()),
    )

    assert len(emitted) == 1
    assert emitted[0].schema == schema
    assert emitted[0].to_pydict() == {"name": ["alice", "bob", "carol"], "age": [30, 41, None]}
