# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Integration tests for RandomBytesFunction via Client."""

from __future__ import annotations

from typing import cast

import pyarrow as pa

from vgi import schema
from vgi.arguments import Arguments
from vgi.client import Client


class TestRandomBytesFunction:
    """Tests for random_bytes() function behavior."""

    def test_random_bytes_returns_expected_lengths(self, fixture_worker: str) -> None:
        """Each row should contain a blob matching the requested length."""
        s = schema(dummy=pa.int64())
        batch = pa.RecordBatch.from_pydict({"dummy": [1, 2, 3, 4, 5]}, schema=s)

        with Client(fixture_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="random_bytes",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(123), pa.scalar(16))),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].schema.field("result").type == pa.binary()
        values = cast(list[bytes], outputs[0].column("result").to_pylist())
        assert len(values) == 5
        assert all(len(v) == 16 for v in values)

    def test_random_bytes_supports_zero_length(self, fixture_worker: str) -> None:
        """Length=0 should return empty blobs."""
        s = schema(dummy=pa.int64())
        batch = pa.RecordBatch.from_pydict({"dummy": [1, 2, 3]}, schema=s)

        with Client(fixture_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="random_bytes",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(123), pa.scalar(0))),
                )
            )

        assert len(outputs) == 1
        values = cast(list[bytes], outputs[0].column("result").to_pylist())
        assert values == [b"", b"", b""]

    def test_random_bytes_same_seed_same_output(self, fixture_worker: str) -> None:
        """Separate calls with the same seed should return identical blobs."""
        s = schema(dummy=pa.int64())
        batch = pa.RecordBatch.from_pydict({"dummy": list(range(16))}, schema=s)

        with Client(fixture_worker) as client:
            first = list(
                client.scalar_function(
                    function_name="random_bytes",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(999), pa.scalar(16))),
                )
            )
            second = list(
                client.scalar_function(
                    function_name="random_bytes",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(999), pa.scalar(16))),
                )
            )

        first_values = cast(list[bytes], first[0].column("result").to_pylist())
        second_values = cast(list[bytes], second[0].column("result").to_pylist())
        assert first_values == second_values

    def test_random_bytes_different_seed_different_output(self, fixture_worker: str) -> None:
        """Different seeds should produce different output."""
        s = schema(dummy=pa.int64())
        batch = pa.RecordBatch.from_pydict({"dummy": list(range(16))}, schema=s)

        with Client(fixture_worker) as client:
            first = list(
                client.scalar_function(
                    function_name="random_bytes",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(1), pa.scalar(16))),
                )
            )
            second = list(
                client.scalar_function(
                    function_name="random_bytes",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(2), pa.scalar(16))),
                )
            )

        first_values = cast(list[bytes], first[0].column("result").to_pylist())
        second_values = cast(list[bytes], second[0].column("result").to_pylist())
        assert first_values != second_values
