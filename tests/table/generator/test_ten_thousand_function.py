"""Tests for the TenThousandFunction."""

import pyarrow as pa

from vgi.examples.table import TenThousandFunction

from .conftest import RunnerWithMode, run_in_process


class TestTenThousandFunctionInProcess:
    """In-process tests for the ten_thousand function."""

    def test_metadata(self) -> None:
        """TenThousand function should have correct metadata."""
        meta = TenThousandFunction.get_metadata()
        assert meta.name == "ten_thousand"
        assert meta.max_workers == 1
        assert "generator" in meta.categories

    def test_output_schema(self) -> None:
        """Output schema should have single int64 column named 'n'."""
        outputs, _ = run_in_process(TenThousandFunction, ())
        assert len(outputs) > 0
        schema = outputs[0].schema
        assert schema.names == ["n"]
        assert schema.field("n").type == pa.int64()


class TestTenThousandFunctionBothModes:
    """Tests that run both in-process and via Client subprocess."""

    def test_generates_ten_thousand_rows(
        self, run_table_function_mode: RunnerWithMode
    ) -> None:
        """TenThousand should generate exactly 10000 rows."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(TenThousandFunction, ())

        table = pa.Table.from_batches(outputs)
        assert table.num_rows == 10000

    def test_generates_correct_values(
        self, run_table_function_mode: RunnerWithMode
    ) -> None:
        """TenThousand should generate integers from 0 to 9999."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(TenThousandFunction, ())

        table = pa.Table.from_batches(outputs)
        values = table.column("n").to_pylist()
        assert values == list(range(10000))

    def test_batching(self, run_table_function_mode: RunnerWithMode) -> None:
        """TenThousand should produce batches of 1000 rows each."""
        runner, mode = run_table_function_mode
        outputs, logs = runner(TenThousandFunction, ())

        # Should produce 10 batches of 1000 rows each
        assert len(outputs) == 10
        for batch in outputs:
            assert batch.num_rows == 1000
