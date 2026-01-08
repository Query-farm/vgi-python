"""Integration tests for Polars scalar functions.

These tests verify that the Polars-based scalar functions work correctly
using ScalarFunctionTestClient.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

# Skip all tests if polars is not installed
polars = pytest.importorskip("polars")

from vgi.arguments import Arguments  # noqa: E402
from vgi.examples.scalar_polars import (  # noqa: E402
    PolarsNormalizeFunction,
    PolarsStringLengthFunction,
    PolarsUpperCaseFunction,
)
from vgi.testing import ScalarFunctionTestClient, batch  # noqa: E402


class TestPolarsUpperCaseFunction:
    """Tests for PolarsUpperCaseFunction."""

    def test_basic_uppercase(self) -> None:
        """Should convert strings to uppercase using Polars."""
        input_batch = batch(name=["alice", "bob", "charlie"])

        with ScalarFunctionTestClient(PolarsUpperCaseFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar("name"),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": ["ALICE", "BOB", "CHARLIE"]}

    def test_preserves_row_count(self) -> None:
        """Output should have same row count as input."""
        input_batch = batch(text=["a", "b", "c", "d", "e"])

        with ScalarFunctionTestClient(PolarsUpperCaseFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar("text"),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].num_rows == input_batch.num_rows

    def test_multiple_batches(self) -> None:
        """Should handle multiple input batches."""
        batch1 = batch(name=["hello"])
        batch2 = batch(name=["world"])

        with ScalarFunctionTestClient(PolarsUpperCaseFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([batch1, batch2]),
                    arguments=Arguments(positional=(pa.scalar("name"),)),
                )
            )

        assert len(outputs) == 2
        assert outputs[0].to_pydict() == {"result": ["HELLO"]}
        assert outputs[1].to_pydict() == {"result": ["WORLD"]}


class TestPolarsStringLengthFunction:
    """Tests for PolarsStringLengthFunction."""

    def test_basic_string_length(self) -> None:
        """Should compute string lengths using Polars."""
        input_batch = batch(text=["hello", "hi", "goodbye"])

        with ScalarFunctionTestClient(PolarsStringLengthFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar("text"),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [5, 2, 7]}

    def test_empty_strings(self) -> None:
        """Should handle empty strings."""
        input_batch = batch(text=["", "a", ""])

        with ScalarFunctionTestClient(PolarsStringLengthFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar("text"),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [0, 1, 0]}

    def test_preserves_row_count(self) -> None:
        """Output should have same row count as input."""
        input_batch = batch(text=["a", "bb", "ccc"])

        with ScalarFunctionTestClient(PolarsStringLengthFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar("text"),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].num_rows == input_batch.num_rows


class TestPolarsNormalizeFunction:
    """Tests for PolarsNormalizeFunction."""

    def test_basic_normalization(self) -> None:
        """Should compute z-score normalization using Polars."""
        # Values with known z-scores: [10, 20, 30, 40, 50]
        # mean = 30, std = 15.81...
        input_batch = batch(value=[10.0, 20.0, 30.0, 40.0, 50.0])

        with ScalarFunctionTestClient(PolarsNormalizeFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar("value"),)),
                )
            )

        assert len(outputs) == 1
        result = outputs[0].to_pydict()["result"]

        # Check that mean of z-scores is approximately 0
        mean_zscore = sum(result) / len(result)
        assert abs(mean_zscore) < 0.0001, (
            f"Mean z-score should be ~0, got {mean_zscore}"
        )  # noqa: E501

        # Check that middle value (30) has z-score ~0
        assert abs(result[2]) < 0.0001, "z-score of middle value should be ~0"

        # Check ordering: lower values have negative z-scores
        assert result[0] < 0, "Lowest value should have negative z-score"
        assert result[4] > 0, "Highest value should have positive z-score"

    def test_preserves_row_count(self) -> None:
        """Output should have same row count as input."""
        input_batch = batch(value=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0])

        with ScalarFunctionTestClient(PolarsNormalizeFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar("value"),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].num_rows == input_batch.num_rows

    def test_output_type_is_float64(self) -> None:
        """Output should be float64 regardless of input numeric type."""
        input_batch = batch(value=[1.0, 2.0, 3.0])

        with ScalarFunctionTestClient(PolarsNormalizeFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar("value"),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].schema.field("result").type == pa.float64()
