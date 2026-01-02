"""Tests for the RandomSampleFunction."""

import pyarrow as pa

from vgi.examples.table import RandomSampleFunction

from .conftest import RunnerWithMode


class TestRandomSampleFunctionInProcess:
    """In-process tests for the random_sample function."""

    def test_metadata(self) -> None:
        """Random sample function should have correct metadata."""
        meta = RandomSampleFunction.get_metadata()
        assert meta.name == "random_sample"
        # Should not have max_workers limit (parallelizable)
        assert meta.max_workers is None


class TestRandomSampleFunctionBothModes:
    """Tests that run both in-process and via Client subprocess."""

    def test_generates_correct_count(
        self, run_table_function_mode: RunnerWithMode
    ) -> None:
        """Random sample should generate exactly the requested number of rows."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(RandomSampleFunction, (100, 42))

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 100

    def test_schema(self, run_table_function_mode: RunnerWithMode) -> None:
        """Random sample should have id and value columns."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(RandomSampleFunction, (10, 42))

        assert len(outputs) > 0
        schema = outputs[0].schema
        assert "id" in schema.names
        assert "value" in schema.names
        assert schema.field("id").type == pa.int64()
        assert schema.field("value").type == pa.float64()

    def test_reproducible_with_seed(
        self, run_table_function_mode: RunnerWithMode
    ) -> None:
        """Same seed should produce same results."""
        runner, mode = run_table_function_mode
        outputs1, _ = runner(RandomSampleFunction, (100, 42))
        outputs2, _ = runner(RandomSampleFunction, (100, 42))

        table1 = pa.Table.from_batches(outputs1)
        table2 = pa.Table.from_batches(outputs2)

        assert table1.equals(table2)

    def test_different_seeds_produce_different_results(
        self, run_table_function_mode: RunnerWithMode
    ) -> None:
        """Different seeds should produce different results."""
        runner, mode = run_table_function_mode
        outputs1, _ = runner(RandomSampleFunction, (100, 42))
        outputs2, _ = runner(RandomSampleFunction, (100, 43))

        table1 = pa.Table.from_batches(outputs1)
        table2 = pa.Table.from_batches(outputs2)

        # Values should differ (ids will be the same)
        values1 = table1.column("value").to_pylist()
        values2 = table2.column("value").to_pylist()
        assert values1 != values2

    def test_zero_count(self, run_table_function_mode: RunnerWithMode) -> None:
        """Random sample with count=0 should produce no output."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(RandomSampleFunction, (0, 42))
        assert len(outputs) == 0

    def test_ids_are_sequential(self, run_table_function_mode: RunnerWithMode) -> None:
        """IDs should be sequential starting from 0."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(RandomSampleFunction, (50, 42))

        table = pa.Table.from_batches(outputs)
        ids = table.column("id").to_pylist()
        assert ids == list(range(50))

    def test_values_in_range(self, run_table_function_mode: RunnerWithMode) -> None:
        """Values should be in [0, 1) range."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(RandomSampleFunction, (1000, 42))

        table = pa.Table.from_batches(outputs)
        values = table.column("value").to_pylist()

        assert all(v is not None and 0 <= v < 1 for v in values)
