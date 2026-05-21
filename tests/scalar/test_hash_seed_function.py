# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for HashSeedFunction via Client."""

from __future__ import annotations

import pyarrow as pa

from vgi import schema
from vgi.arguments import Arguments
from vgi.client import Client


class TestHashSeedFunction:
    """Tests for HashSeedFunction via Client subprocess."""

    def test_output_length_matches_input(self, fixture_worker: str) -> None:
        """Output row count matches input batch size."""
        s = schema(dummy=pa.int64())
        batch = pa.RecordBatch.from_pydict({"dummy": [1, 2, 3, 4, 5]}, schema=s)

        with Client(fixture_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="hash_seed",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(42),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].num_rows == 5

    def test_values_are_deterministic(self, fixture_worker: str) -> None:
        """Same seed produces same values across calls."""
        s = schema(dummy=pa.int64())
        batch = pa.RecordBatch.from_pydict({"dummy": list(range(10))}, schema=s)

        with Client(fixture_worker) as client:
            first = list(
                client.scalar_function(
                    function_name="hash_seed",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(99),)),
                )
            )
            second = list(
                client.scalar_function(
                    function_name="hash_seed",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(99),)),
                )
            )

        assert first[0].column("result").to_pylist() == second[0].column("result").to_pylist()

    def test_different_seeds_produce_different_results(self, fixture_worker: str) -> None:
        """Different seeds produce different output."""
        s = schema(dummy=pa.int64())
        batch = pa.RecordBatch.from_pydict({"dummy": [1, 2, 3]}, schema=s)

        with Client(fixture_worker) as client:
            first = list(
                client.scalar_function(
                    function_name="hash_seed",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(1),)),
                )
            )
            second = list(
                client.scalar_function(
                    function_name="hash_seed",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(1000),)),
                )
            )

        assert first[0].column("result").to_pylist() != second[0].column("result").to_pylist()

    def test_expected_values(self, fixture_worker: str) -> None:
        """Values equal seed + row_index."""
        s = schema(dummy=pa.int64())
        batch = pa.RecordBatch.from_pydict({"dummy": [1, 2, 3, 4, 5]}, schema=s)

        with Client(fixture_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="hash_seed",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(100),)),
                )
            )

        assert outputs[0].column("result").to_pylist() == [100, 101, 102, 103, 104]

    def test_output_type_is_int64(self, fixture_worker: str) -> None:
        """Result column is int64 type."""
        s = schema(dummy=pa.int64())
        batch = pa.RecordBatch.from_pydict({"dummy": [1]}, schema=s)

        with Client(fixture_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="hash_seed",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(0),)),
                )
            )

        assert outputs[0].schema.field("result").type == pa.int64()
