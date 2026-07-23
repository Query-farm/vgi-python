# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for MultiplyFunction via Client."""

from __future__ import annotations

import pyarrow as pa

from vgi import schema
from vgi.arguments import Arguments
from vgi.client import Client


class TestMultiplyFunction:
    """Tests for MultiplyFunction via Client subprocess."""

    def test_multiply_basic(self, fixture_worker: str) -> None:
        """Multiply [10, 20, 30] by factor 2."""
        s = schema(value=pa.int64())
        batch = pa.RecordBatch.from_pydict({"value": [10, 20, 30]}, schema=s)

        with Client(fixture_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="multiply",
                    schema_name="main",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(2),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [20, 40, 60]}

    def test_multiply_by_one(self, fixture_worker: str) -> None:
        """Multiply by 1 returns identity."""
        s = schema(value=pa.int64())
        batch = pa.RecordBatch.from_pydict({"value": [5, 10, 15]}, schema=s)

        with Client(fixture_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="multiply",
                    schema_name="main",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(1),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [5, 10, 15]}

    def test_multiply_empty_batch(self, fixture_worker: str) -> None:
        """Empty input produces empty output."""
        s = schema(value=pa.int64())
        batch = pa.RecordBatch.from_pydict({"value": []}, schema=s)

        with Client(fixture_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="multiply",
                    schema_name="main",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(2),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].num_rows == 0
