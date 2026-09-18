# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for SubstreamPartialSumFunction (parallel streaming finalize, A4).

The Python client fans the input across its connections, each its own substream
of one execution, and finalizes once: ``finish()`` receives every substream's
state and emits one partial row equal to the whole sum. The DuckDB-side fan-out
(many partials re-aggregated by an outer ``SELECT sum()``) is exercised by the
C++ integration test ``table_in_out/parallel_finalize.test`` over both transports.

Run over every client transport because the state is keyed per substream: over
HTTP one process serves every connection of the fan-out, which is where a
per-process key let the connections overwrite each other's state.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from tests.conftest import filter_non_empty, make_schema
from vgi.client import Client
from vgi.client import client as client_module


class TestSubstreamPartialSumFunction:
    """Worker-side contract for substream_partial_sum."""

    def test_partial_sum_single_batch(self, client_transport: Any) -> None:
        """One substream (single client worker) → one partial = the whole sum."""
        schema = make_schema([pa.field("n", pa.int64())])
        batch = pa.RecordBatch.from_pydict({"n": [1, 2, 3, 4, 5]}, schema=schema)
        with client_transport() as client:
            output = list(
                client.table_in_out_function(
                    function_name="substream_partial_sum",
                    schema_path=["main"],
                    input=iter([batch]),
                )
            )
        non_empty = filter_non_empty(output)
        assert len(non_empty) == 1
        assert non_empty[0].to_pydict() == {"n": [15]}

    def test_partial_sum_many_batches(self, client_transport: Any) -> None:
        """Accumulates across many input batches, emits one partial at finalize."""
        schema = make_schema([pa.field("n", pa.int64())])
        batches = [
            pa.RecordBatch.from_pydict({"n": list(range(i * 100, (i + 1) * 100))}, schema=schema) for i in range(20)
        ]
        expected = sum(range(0, 2000))
        with client_transport() as client:
            output = list(
                client.table_in_out_function(
                    function_name="substream_partial_sum",
                    schema_path=["main"],
                    input=iter(batches),
                )
            )
        non_empty = filter_non_empty(output)
        assert len(non_empty) == 1
        assert non_empty[0].to_pydict() == {"n": [expected]}

    def test_partial_sum_empty_input(self, client_transport: Any) -> None:
        """No input rows → the single substream's partial is 0."""
        schema = make_schema([pa.field("n", pa.int64())])
        empty = pa.RecordBatch.from_pydict({"n": []}, schema=schema)
        with client_transport() as client:
            output = list(
                client.table_in_out_function(
                    function_name="substream_partial_sum",
                    schema_path=["main"],
                    input=iter([empty]),
                )
            )
        non_empty = filter_non_empty(output)
        # A single substream with zero rows still finalizes → one 0 partial.
        assert len(non_empty) == 1
        assert non_empty[0].to_pydict() == {"n": [0]}

    def test_a_client_sending_no_substream_id_still_sums_over_subprocess(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An older client sends no ``substream_id``; the worker keys it by process, as before.

        Over subprocess every connection is its own process, so that key still
        separates them — the behavior such a client had before the key moved.
        """
        monkeypatch.setattr(client_module, "_substream_id_for", lambda phase: None)
        schema = make_schema([pa.field("n", pa.int64())])
        batches = [
            pa.RecordBatch.from_pydict({"n": list(range(i * 100, (i + 1) * 100))}, schema=schema) for i in range(20)
        ]
        # A plain subprocess worker whatever transport the suite runs on; four
        # connections are enough to share an execution, where a worker per core
        # would be dozens of processes.
        with Client("vgi-fixture-worker", worker_limit=4) as client:
            output = list(
                client.table_in_out_function(
                    function_name="substream_partial_sum",
                    schema_path=["main"],
                    input=iter(batches),
                )
            )
        assert filter_non_empty(output)[0].to_pydict() == {"n": [sum(range(0, 2000))]}
