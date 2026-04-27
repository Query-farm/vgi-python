"""Tests for the GeneratorExceptionFunction."""

import pyarrow as pa
import pytest

from vgi.arguments import Arguments
from vgi.client import Client, ClientError


class TestGeneratorExceptionFunctionViaClient:
    """Tests that run via Client subprocess."""

    def test_raises_exception_after_batches(self) -> None:
        """Function should raise exception after specified batches via Client."""
        with (
            Client("vgi-fixture-worker") as client,
            pytest.raises(ClientError) as exc_info,
        ):
            list(
                client.table_function(
                    function_name="generator_exception",
                    arguments=Arguments(positional=(pa.scalar(3),)),
                )
            )

        assert "Intentional failure after 3 batches" in str(exc_info.value)

    def test_raises_exception_immediately(self) -> None:
        """Function with fail_after=0 should raise immediately via Client."""
        with (
            Client("vgi-fixture-worker") as client,
            pytest.raises(ClientError) as exc_info,
        ):
            list(
                client.table_function(
                    function_name="generator_exception",
                    arguments=Arguments(positional=(pa.scalar(0),)),
                )
            )

        assert "Intentional failure after 0 batches" in str(exc_info.value)

    def test_outputs_batches_before_failure(self) -> None:
        """Function should output batches before failing via Client."""
        with Client("vgi-fixture-worker") as client:
            outputs: list[pa.RecordBatch] = []
            try:
                for batch in client.table_function(
                    function_name="generator_exception",
                    arguments=Arguments(positional=(pa.scalar(3),)),
                ):
                    outputs.append(batch)
            except ClientError:
                pass

            # Should have 3 batches before failure
            assert len(outputs) == 3
            for i, batch in enumerate(outputs):
                assert batch.column("n").to_pylist() == [i]
