"""Tests for BernoulliFunction via Client."""

from __future__ import annotations

import pyarrow as pa

from vgi import schema
from vgi.client import Client


class TestBernoulliFunction:
    """Tests for BernoulliFunction via Client subprocess."""

    def test_bernoulli_basic(self, fixture_worker: str) -> None:
        """Generates booleans with output row count matching input."""
        s = schema(dummy=pa.int64())
        batch = pa.RecordBatch.from_pydict({"dummy": [1, 2, 3, 4, 5]}, schema=s)

        with Client(fixture_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="bernoulli",
                    input=iter([batch]),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].num_rows == 5

    def test_bernoulli_output_type(self, fixture_worker: str) -> None:
        """Result column is boolean type."""
        s = schema(dummy=pa.int64())
        batch = pa.RecordBatch.from_pydict({"dummy": [1, 2, 3]}, schema=s)

        with Client(fixture_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="bernoulli",
                    input=iter([batch]),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].schema.field("result").type == pa.bool_()
        # All values should be boolean
        for val in outputs[0].column("result").to_pylist():
            assert isinstance(val, bool)
