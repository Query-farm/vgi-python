"""Tests for the RandomSampleFunction."""

import pyarrow as pa

from vgi.examples.table import RandomSampleFunction
from vgi.testing import run_table_function


class TestRandomSampleFunction:
    """Tests for the random_sample function."""

    def test_generates_correct_count(self) -> None:
        """Random sample should generate exactly the requested number of rows."""
        outputs, logs = run_table_function(RandomSampleFunction, args=(100, 42))

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 100

    def test_schema(self) -> None:
        """Random sample should have id and value columns."""
        outputs, logs = run_table_function(RandomSampleFunction, args=(10, 42))

        assert len(outputs) > 0
        schema = outputs[0].schema
        assert "id" in schema.names
        assert "value" in schema.names
        assert schema.field("id").type == pa.int64()
        assert schema.field("value").type == pa.float64()

    def test_reproducible_with_seed(self) -> None:
        """Same seed should produce same results."""
        outputs1, _ = run_table_function(RandomSampleFunction, args=(100, 42))
        outputs2, _ = run_table_function(RandomSampleFunction, args=(100, 42))

        table1 = pa.Table.from_batches(outputs1)
        table2 = pa.Table.from_batches(outputs2)

        assert table1.equals(table2)

    def test_different_seeds_produce_different_results(self) -> None:
        """Different seeds should produce different results."""
        outputs1, _ = run_table_function(RandomSampleFunction, args=(100, 42))
        outputs2, _ = run_table_function(RandomSampleFunction, args=(100, 43))

        table1 = pa.Table.from_batches(outputs1)
        table2 = pa.Table.from_batches(outputs2)

        # Values should differ (ids will be the same)
        values1 = table1.column("value").to_pylist()
        values2 = table2.column("value").to_pylist()
        assert values1 != values2

    def test_zero_count(self) -> None:
        """Random sample with count=0 should produce no output."""
        outputs, logs = run_table_function(RandomSampleFunction, args=(0, 42))
        assert len(outputs) == 0

    def test_ids_are_sequential(self) -> None:
        """IDs should be sequential starting from 0."""
        outputs, logs = run_table_function(RandomSampleFunction, args=(50, 42))

        table = pa.Table.from_batches(outputs)
        ids = table.column("id").to_pylist()
        assert ids == list(range(50))

    def test_values_in_range(self) -> None:
        """Values should be in [0, 1) range."""
        outputs, logs = run_table_function(RandomSampleFunction, args=(1000, 42))

        table = pa.Table.from_batches(outputs)
        values = table.column("value").to_pylist()

        assert all(0 <= v < 1 for v in values)

    def test_metadata(self) -> None:
        """Random sample function should have correct metadata."""
        meta = RandomSampleFunction.get_metadata()
        assert meta.name == "random_sample"
        # Should not have max_workers limit (parallelizable)
        assert meta.max_workers is None
