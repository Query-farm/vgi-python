"""Reproducer for the projection-pushdown bug seen in vgi-kafka.

Symptom (from vgi-kafka kafka_consume + DuckDB ``SELECT count(*) FROM
topics.<topic>``):

    ValueError: Target schema's field names are not matching the record
    batch's field names: ['topic', 'partition', 'offset', ...12 cols...],
    ['topic']

The worker emitted a 1-column batch matching ``params.output_schema``
(which the framework had projected from the catalog's table schema down
to just the ``topic`` column for a count(*) query). But the
``OutputCollector`` cast-on-emit targeted the 12-column ``FIXED_SCHEMA``
from bind time, raising the ValueError.

This module reproduces the exact code path:

  * A wide ``FIXED_SCHEMA`` declared at bind time (``proj_repro_strict``).
  * ``projection_pushdown = True``.
  * Process emits batches matching ``params.output_schema``.

The driver test calls the function via ``Client.table_function`` with
explicit ``projection_ids``. That path mirrors what DuckDB's planner
does after pushing down a projection to the C++ extension.

Run via ``pytest tests/test_projection_repro.py``. Failures here are
the bug; passing means the projection-pushdown plumbing through init →
process → emit is consistent.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from vgi.arguments import Arguments
from vgi.client import Client

# All twelve column indices for the wide reproducer schema.
_ALL_INDICES = list(range(12))
# Single-col projections — these are the most likely to trigger schema
# mismatches because the emitted batch has just 1 column while the
# bind-time output_schema has 12.
_SINGLE_COL_PROJECTIONS = [[i] for i in range(12)]
# Empty projection — represents ``SELECT count(*)`` where DuckDB needs
# zero data columns.
_EMPTY_PROJECTION = []


# ---------------------------------------------------------------------------
# Direct table-function calls (no catalog routing)
# ---------------------------------------------------------------------------


class TestProjReproStrictDirect:
    """Driven calls to ``proj_repro_strict`` with explicit projection_ids.

    Each call mirrors what would happen if DuckDB pushed down the
    corresponding projection to a normal ``SELECT col_x FROM
    proj_repro_strict(N)`` query.
    """

    def test_no_projection_returns_all_columns(self) -> None:
        """Without projection_ids, all 12 columns come back."""
        with Client("vgi-fixture-projection-repro-worker", worker_limit=1) as client:
            outputs = list(
                client.table_function(
                    function_name="proj_repro_strict",
                    arguments=Arguments(positional=(pa.scalar(5),)),
                )
            )
        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 5
        assert table.num_columns == 12

    @pytest.mark.parametrize("projection_ids", _SINGLE_COL_PROJECTIONS)
    def test_single_col_projection(self, projection_ids: list[int]) -> None:
        """Each single-column projection returns exactly 1 column.

        Bug surfaces here when the framework's emit-cast targets the
        12-column FIXED_SCHEMA instead of the 1-column projected
        schema.
        """
        with Client("vgi-fixture-projection-repro-worker", worker_limit=1) as client:
            outputs = list(
                client.table_function(
                    function_name="proj_repro_strict",
                    arguments=Arguments(positional=(pa.scalar(5),)),
                    projection_ids=projection_ids,
                )
            )
        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 5
        assert table.num_columns == 1, (
            f"expected 1 column for projection_ids={projection_ids}, "
            f"got {table.num_columns}: {table.schema.names}"
        )

    def test_empty_projection_count_star(self) -> None:
        """Empty projection (count(*) shape) preserves the row count.

        ``SELECT count(*) FROM ...`` pushes down ``projection_ids=[]``
        meaning "no data columns needed, just row count". The framework
        must preserve the row count even when the output schema is
        empty — DuckDB's count(*) needs N, not 0.
        """
        with Client("vgi-fixture-projection-repro-worker", worker_limit=1) as client:
            outputs = list(
                client.table_function(
                    function_name="proj_repro_strict",
                    arguments=Arguments(positional=(pa.scalar(5),)),
                    projection_ids=_EMPTY_PROJECTION,
                )
            )
        # batch list may be non-empty even with 0 columns, since we want
        # to preserve row count for count(*).
        total_rows = sum(b.num_rows for b in outputs)
        assert total_rows == 5
        # Every emitted batch should have 0 columns (the projected schema).
        for b in outputs:
            assert b.num_columns == 0, (
                f"expected 0-column batch for projection_ids=[], "
                f"got {b.num_columns}: {b.schema.names}"
            )

    def test_two_col_projection_works(self) -> None:
        """Two-column projection ``[0, 2]`` still passes on this fixture.

        Sanity check that mirrors the existing
        ``test_projection_enforcement`` coverage on the canonical
        ``projected_data`` fixture.
        """
        with Client("vgi-fixture-projection-repro-worker", worker_limit=1) as client:
            outputs = list(
                client.table_function(
                    function_name="proj_repro_strict",
                    arguments=Arguments(positional=(pa.scalar(5),)),
                    projection_ids=[0, 2],
                )
            )
        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 5
        assert table.schema.names == ["topic", "offset"]

    def test_all_columns_projection(self) -> None:
        """Projecting every column should be equivalent to no projection."""
        with Client("vgi-fixture-projection-repro-worker", worker_limit=1) as client:
            outputs = list(
                client.table_function(
                    function_name="proj_repro_strict",
                    arguments=Arguments(positional=(pa.scalar(5),)),
                    projection_ids=_ALL_INDICES,
                )
            )
        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 5
        assert table.num_columns == 12


# ---------------------------------------------------------------------------
# A worker that emits the FULL schema regardless of projection. The
# framework should either handle this gracefully (e.g. by projecting
# server-side) or raise a clear, actionable error — not the confusing
# "different schema" cast error.
# ---------------------------------------------------------------------------


class TestProjReproFullSchema:
    """Driven calls to a worker that always emits the full 12-col schema.

    This is what a naive worker would do if it forgot to observe
    ``params.output_schema``. The framework should project the batch
    server-side rather than fail with an opaque cast error.
    """

    def test_full_emit_projected_to_all_null_column_stays_null(self) -> None:
        """Worker emits 12 cols; projection picks ``value_schema_id`` (all NULL).

        Reproduces a bug observed in vgi-kafka: a column declared
        ``pa.int32()`` nullable that the worker fills with all-None
        comes back from a projected scan with non-null values. The
        projected wire batch carries one column (``value_schema_id``)
        but DuckDB / the framework reads it back from a different
        position than the worker wrote, so unrelated bytes appear in
        the int32 cells.

        ``proj_repro_full_schema`` builds the full WIDE_SCHEMA per
        ``_build_row_dict``, which sets ``value_schema_id=None`` for
        every row. After projection, the resulting column must still
        be all-NULL.
        """
        # value_schema_id is index 10 in WIDE_SCHEMA.
        value_schema_id_idx = 10
        with Client("vgi-fixture-projection-repro-worker", worker_limit=1) as client:
            outputs = list(
                client.table_function(
                    function_name="proj_repro_full_schema",
                    arguments=Arguments(positional=(pa.scalar(8),)),
                    projection_ids=[value_schema_id_idx],
                )
            )
        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 8
        assert table.num_columns == 1
        assert table.schema.names == ["value_schema_id"]
        values = table.column("value_schema_id").to_pylist()
        assert values == [None] * 8, (
            f"projected all-NULL int32 column came back with non-null "
            f"values: {values} — projection plumbing is mis-mapping "
            f"column positions between emit and read."
        )

    def test_full_emit_with_projection_does_not_cast_crash(self) -> None:
        """Worker emitting 12 cols against a 1-col projection lands cleanly.

        The framework should produce *some* deterministic outcome —
        either accept and project server-side, or raise a clear
        ValueError/TypeError. Hitting the ``Target schema's field names
        are not matching`` cast error means the projection-emit
        handshake is broken.
        """
        with Client("vgi-fixture-projection-repro-worker", worker_limit=1) as client:
            try:
                outputs = list(
                    client.table_function(
                        function_name="proj_repro_full_schema",
                        arguments=Arguments(positional=(pa.scalar(5),)),
                        projection_ids=[0],  # single col
                    )
                )
            except Exception as exc:
                # If the framework rejects, error message should be
                # actionable — it should mention projection, not just
                # "different schema".
                msg = str(exc)
                if "different schema" in msg.lower() or "not matching" in msg.lower():
                    pytest.fail(
                        f"projection mismatch surfaced as opaque cast error: {msg}"
                    )
                # Otherwise — a clear projection-related error — that's OK,
                # we just want the framework to be deterministic about it.
                return

            # If no exception, the framework projected server-side. Verify
            # that the resulting table has the expected single column.
            table = pa.Table.from_batches(outputs)
            assert table.num_rows == 5
            assert table.num_columns == 1
            assert table.schema.names == ["topic"]
