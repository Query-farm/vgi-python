# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for SubstreamPartialSumFunction (parallel streaming finalize, A4).

At the single-worker client level there is exactly one substream, so ``finish()``
sees all input and emits one partial row equal to the whole sum. The per-substream
fan-out (many partials re-aggregated by an outer ``SELECT sum()``) is exercised by
the C++ integration test ``table_in_out/parallel_finalize.test`` over both
transports; here we assert the worker-side transform/finish contract in isolation.
"""

from __future__ import annotations

import pyarrow as pa

from tests.conftest import filter_non_empty, make_schema
from vgi.client import Client


class TestSubstreamPartialSumFunction:
    """Worker-side contract for substream_partial_sum."""

    def test_partial_sum_single_batch(self, fixture_worker: str) -> None:
        """One substream (single client worker) → one partial = the whole sum."""
        schema = make_schema([pa.field("n", pa.int64())])
        batch = pa.RecordBatch.from_pydict({"n": [1, 2, 3, 4, 5]}, schema=schema)
        with Client(fixture_worker) as client:
            output = list(
                client.table_in_out_function(
                    function_name="substream_partial_sum",
                    schema_name="main",
                    input=iter([batch]),
                )
            )
        non_empty = filter_non_empty(output)
        assert len(non_empty) == 1
        assert non_empty[0].to_pydict() == {"n": [15]}

    def test_partial_sum_many_batches(self, fixture_worker: str) -> None:
        """Accumulates across many input batches, emits one partial at finalize."""
        schema = make_schema([pa.field("n", pa.int64())])
        batches = [
            pa.RecordBatch.from_pydict({"n": list(range(i * 100, (i + 1) * 100))}, schema=schema) for i in range(20)
        ]
        expected = sum(range(0, 2000))
        with Client(fixture_worker) as client:
            output = list(
                client.table_in_out_function(
                    function_name="substream_partial_sum",
                    schema_name="main",
                    input=iter(batches),
                )
            )
        non_empty = filter_non_empty(output)
        assert len(non_empty) == 1
        assert non_empty[0].to_pydict() == {"n": [expected]}

    def test_partial_sum_empty_input(self, fixture_worker: str) -> None:
        """No input rows → the single substream's partial is 0."""
        schema = make_schema([pa.field("n", pa.int64())])
        empty = pa.RecordBatch.from_pydict({"n": []}, schema=schema)
        with Client(fixture_worker) as client:
            output = list(
                client.table_in_out_function(
                    function_name="substream_partial_sum",
                    schema_name="main",
                    input=iter([empty]),
                )
            )
        non_empty = filter_non_empty(output)
        # A single substream with zero rows still finalizes → one 0 partial.
        assert len(non_empty) == 1
        assert non_empty[0].to_pydict() == {"n": [0]}
