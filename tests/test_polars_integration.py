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
    PolarsAddValuesFunction,
    PolarsDoubleFunction,
    PolarsMultiplyFunction,
    PolarsNormalizeFunction,
    PolarsStringLengthFunction,
    PolarsSumValuesFunction,
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
                    arguments=Arguments(),
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
                    arguments=Arguments(),
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
                    arguments=Arguments(),
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
                    arguments=Arguments(),
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
                    arguments=Arguments(),
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
                    arguments=Arguments(),
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
                    arguments=Arguments(),
                )
            )

        assert len(outputs) == 1
        result = outputs[0].to_pydict()["result"]

        # Check that mean of z-scores is approximately 0
        mean_zscore = sum(result) / len(result)
        assert abs(mean_zscore) < 0.0001, f"Mean z-score should be ~0: {mean_zscore}"

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
                    arguments=Arguments(),
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
                    arguments=Arguments(),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].schema.field("result").type == pa.float64()


class TestPolarsAddValuesFunction:
    """Tests for PolarsAddValuesFunction."""

    def test_basic_add(self) -> None:
        """Should add two numeric values together."""
        input_batch = batch(price=[10.0, 20.0, 30.0], tax=[1.0, 2.0, 3.0])

        with ScalarFunctionTestClient(PolarsAddValuesFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [11.0, 22.0, 33.0]}

    def test_preserves_row_count(self) -> None:
        """Output should have same row count as input."""
        input_batch = batch(a=[1.0, 2.0, 3.0], b=[4.0, 5.0, 6.0])

        with ScalarFunctionTestClient(PolarsAddValuesFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].num_rows == input_batch.num_rows


class TestPolarsMultiplyFunction:
    """Tests for PolarsMultiplyFunction."""

    def test_basic_multiply(self) -> None:
        """Should multiply values by a constant factor."""
        input_batch = batch(price=[10.0, 20.0, 30.0])

        with ScalarFunctionTestClient(PolarsMultiplyFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar(2.0),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [20.0, 40.0, 60.0]}

    def test_fractional_factor(self) -> None:
        """Should work with fractional multiplication factors."""
        input_batch = batch(value=[100.0, 200.0, 300.0])

        with ScalarFunctionTestClient(PolarsMultiplyFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(positional=(pa.scalar(0.5),)),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [50.0, 100.0, 150.0]}


class TestPolarsSumValuesFunction:
    """Tests for PolarsSumValuesFunction."""

    def test_sum_two_columns(self) -> None:
        """Should sum two values."""
        input_batch = batch(a=[1.0, 2.0], b=[10.0, 20.0])

        with ScalarFunctionTestClient(PolarsSumValuesFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [11.0, 22.0]}

    def test_sum_three_columns(self) -> None:
        """Should sum three values."""
        input_batch = batch(a=[1.0, 2.0], b=[10.0, 20.0], c=[100.0, 200.0])

        with ScalarFunctionTestClient(PolarsSumValuesFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [111.0, 222.0]}

    def test_preserves_row_count(self) -> None:
        """Output should have same row count as input."""
        input_batch = batch(x=[1.0, 2.0, 3.0, 4.0], y=[5.0, 6.0, 7.0, 8.0])

        with ScalarFunctionTestClient(PolarsSumValuesFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].num_rows == input_batch.num_rows


class TestPolarsDoubleFunction:
    """Tests for PolarsDoubleFunction with dynamic output type."""

    def test_basic_double(self) -> None:
        """Should double numeric values."""
        input_batch = batch(count=[1, 2, 3])

        with ScalarFunctionTestClient(PolarsDoubleFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": [2, 4, 6]}

    def test_preserves_int64_type(self) -> None:
        """Output type should match input type (int64)."""
        input_batch = pa.RecordBatch.from_pydict(
            {"value": pa.array([1, 2, 3], type=pa.int64())}
        )

        with ScalarFunctionTestClient(PolarsDoubleFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].schema.field("result").type == pa.int64()

    def test_preserves_float64_type(self) -> None:
        """Output type should match input type (float64)."""
        input_batch = pa.RecordBatch.from_pydict(
            {"value": pa.array([1.5, 2.5, 3.5], type=pa.float64())}
        )

        with ScalarFunctionTestClient(PolarsDoubleFunction) as client:
            outputs = list(
                client.scalar_function(
                    input=iter([input_batch]),
                    arguments=Arguments(),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].schema.field("result").type == pa.float64()
        assert outputs[0].to_pydict() == {"result": [3.0, 5.0, 7.0]}
