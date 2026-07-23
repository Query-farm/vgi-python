# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for the FilterBySettingFunction (settings-based row filtering)."""

import pyarrow as pa
import pytest

from tests.conftest import filter_non_empty
from vgi.client import Client
from vgi.client.client import ClientError


class TestFilterBySettingFunction:
    """Tests for filter_by_setting function."""

    def test_filter_with_integer_threshold(self, fixture_worker: str) -> None:
        """Rows with value >= threshold should pass through."""
        input_schema = pa.schema([("value", pa.int64())])
        batch = pa.RecordBatch.from_pydict(
            {"value": list(range(10))},
            schema=input_schema,
        )

        with Client(fixture_worker) as client:
            outputs = list(
                client.table_in_out_function(
                    function_name="filter_by_setting",
                    schema_name="main",
                    input=iter([batch]),
                    settings={"threshold": 5},
                )
            )

        result = pa.Table.from_batches(filter_non_empty(outputs))
        assert sorted(result.column("value").to_pylist()) == [5, 6, 7, 8, 9]  # type: ignore[type-var]

    def test_filter_with_zero_threshold(self, fixture_worker: str) -> None:
        """Threshold=0 should let all rows pass."""
        input_schema = pa.schema([("value", pa.int64())])
        batch = pa.RecordBatch.from_pydict(
            {"value": list(range(10))},
            schema=input_schema,
        )

        with Client(fixture_worker) as client:
            outputs = list(
                client.table_in_out_function(
                    function_name="filter_by_setting",
                    schema_name="main",
                    input=iter([batch]),
                    settings={"threshold": 0},
                )
            )

        result = pa.Table.from_batches(filter_non_empty(outputs))
        assert result.num_rows == 10

    def test_missing_required_setting_fails(self, fixture_worker: str) -> None:
        """Missing threshold setting should raise ClientError."""
        input_schema = pa.schema([("value", pa.int64())])
        batch = pa.RecordBatch.from_pydict(
            {"value": [1, 2, 3]},
            schema=input_schema,
        )

        with Client(fixture_worker) as client:
            with pytest.raises(ClientError) as exc_info:
                list(
                    client.table_in_out_function(
                        function_name="filter_by_setting",
                        schema_name="main",
                        input=iter([batch]),
                        # No settings provided
                    )
                )

            assert "threshold" in str(exc_info.value)
