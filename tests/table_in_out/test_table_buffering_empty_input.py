# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""A table-buffering function whose input is empty still runs init, combine and finalize.

The DuckDB extension (vgi 63eb257) now drives the whole lifecycle when a
table-buffering input is empty at runtime: the TABLE_BUFFERING init, then
``table_buffering_combine`` with an EMPTY ``state_ids`` list, then finalize. These
drive that exact sequence (no ``process`` call at all) through the Python client,
mirroring ``test/sql/integration/table_in_out/table_buffering_empty_input.test``.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from vgi.client import Client, ClientError

_SCHEMA = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.int64())])


def _run(worker: str, function_name: str) -> list[pa.RecordBatch]:
    """Invoke *function_name* with zero input batches, so combine sees no state_ids."""
    with Client(worker) as client:
        return list(
            client.table_buffering_function(
                function_name=function_name,
                schema_path=["main"],
                input=iter(()),
                input_schema=_SCHEMA,
            )
        )


def _rows(batches: list[pa.RecordBatch]) -> list[dict[str, object]]:
    return [row for batch in batches for row in batch.to_pylist()]


class TestEmptyInputRunsTheLifecycle:
    """combine([]) and finalize with no sink states."""

    def test_reduction_answers_with_its_zero_row(self, fixture_worker: str) -> None:
        """``sum_all_columns`` emits ``0 0`` for empty input, not zero rows."""
        assert _rows(_run(fixture_worker, "sum_all_columns")) == [{"a": 0, "b": 0}]

    def test_function_with_nothing_to_say_returns_no_rows(self, fixture_worker: str) -> None:
        """``buffer_input`` still returns no rows."""
        assert _rows(_run(fixture_worker, "buffer_input")) == []

    def test_combine_errors_surface_on_empty_input(self, fixture_worker: str) -> None:
        """Combine really runs: its exception reaches the caller."""
        with pytest.raises(ClientError, match="Intentional exception during combine"):
            _run(fixture_worker, "crash_on_combine")
