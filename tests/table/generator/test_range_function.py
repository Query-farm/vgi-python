"""Tests for the RangeFunction."""

import pyarrow as pa

from vgi.examples.table import RangeFunction
from vgi.testing import (
    assert_table_function_output,
    batch,
    run_table_function,
)


class TestRangeFunction:
    """Tests for the range function."""

    def test_basic_range(self) -> None:
        """Range should generate integers from start to end."""
        assert_table_function_output(
            RangeFunction,
            args=(0, 5),
            expected=[batch(value=[0, 1, 2, 3, 4])],
        )

    def test_range_with_step(self) -> None:
        """Range with step should skip values."""
        assert_table_function_output(
            RangeFunction,
            args=(0, 10, 2),
            expected=[batch(value=[0, 2, 4, 6, 8])],
        )

    def test_range_non_zero_start(self) -> None:
        """Range with non-zero start."""
        assert_table_function_output(
            RangeFunction,
            args=(5, 10),
            expected=[batch(value=[5, 6, 7, 8, 9])],
        )

    def test_empty_range(self) -> None:
        """Range where end <= start should produce no output."""
        outputs, logs = run_table_function(RangeFunction, args=(10, 5))
        assert len(outputs) == 0

    def test_single_value_range(self) -> None:
        """Range of length 1."""
        assert_table_function_output(
            RangeFunction,
            args=(42, 43),
            expected=[batch(value=[42])],
        )

    def test_large_range_batches(self) -> None:
        """Large ranges should be split into batches."""
        outputs, logs = run_table_function(RangeFunction, args=(0, 2500))

        # Combine all batches
        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 2500

        # Check all values are present
        values = table.column("value").to_pylist()
        assert values == list(range(2500))

    def test_metadata(self) -> None:
        """Range function should have correct metadata."""
        meta = RangeFunction.get_metadata()
        assert meta.name == "range"
        assert meta.max_workers == 1
