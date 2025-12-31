"""Tests for the GeneratorExceptionFunction."""

import pytest

from vgi.examples.table import GeneratorExceptionFunction
from vgi.testing import FunctionTestClientError, run_table_function


class TestGeneratorExceptionFunction:
    """Tests for the generator_exception function."""

    def test_raises_exception_after_batches(self) -> None:
        """Function should raise exception after specified batches."""
        with pytest.raises(FunctionTestClientError) as exc_info:
            run_table_function(GeneratorExceptionFunction, args=(3,))

        assert "Intentional failure after 3 batches" in str(exc_info.value)

    def test_raises_exception_immediately(self) -> None:
        """Function with fail_after=0 should raise immediately."""
        with pytest.raises(FunctionTestClientError) as exc_info:
            run_table_function(GeneratorExceptionFunction, args=(0,))

        assert "Intentional failure after 0 batches" in str(exc_info.value)

    def test_outputs_batches_before_failure(self) -> None:
        """Function should output batches before failing."""
        # We can't easily test this with the helper since it raises
        # Let's test by catching the exception and checking outputs
        import pyarrow as pa

        from vgi.function import Arguments
        from vgi.testing import TableFunctionTestClient

        with TableFunctionTestClient(GeneratorExceptionFunction) as client:
            outputs: list[pa.RecordBatch] = []
            try:
                for batch in client.table_function(
                    arguments=Arguments(positional=(pa.scalar(3),))
                ):
                    outputs.append(batch)
            except FunctionTestClientError:
                pass

            # Should have 3 batches before failure
            assert len(outputs) == 3
            for i, batch in enumerate(outputs):
                assert batch.column("n").to_pylist() == [i]
