# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for MultiplyBySettingFunction via Client."""

from __future__ import annotations

import pyarrow as pa

from vgi import schema
from vgi.client import Client


class TestMultiplyBySettingFunction:
    """Tests for MultiplyBySettingFunction via Client subprocess."""

    def test_multiply_by_setting_basic(self, fixture_worker: str) -> None:
        """Values [1, 2, 3] with multiplier=5 produces [5, 10, 15]."""
        s = schema(value=pa.int64())
        batch = pa.RecordBatch.from_pydict({"value": [1, 2, 3]}, schema=s)

        with Client(fixture_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="multiply_by_setting",
                    input=iter([batch]),
                    settings={"multiplier": 5},
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [5, 10, 15]}

    def test_multiply_by_setting_one(self, fixture_worker: str) -> None:
        """Multiplier=1 returns identity."""
        s = schema(value=pa.int64())
        batch = pa.RecordBatch.from_pydict({"value": [10, 20, 30]}, schema=s)

        with Client(fixture_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="multiply_by_setting",
                    input=iter([batch]),
                    settings={"multiplier": 1},
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [10, 20, 30]}
