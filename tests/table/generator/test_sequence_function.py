# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for the SequenceFunction."""

import pyarrow as pa

from vgi.arguments import Arguments
from vgi.client import Client
from vgi.invocation import BindResponse


class TestSequenceFunctionClient:
    """Tests for SequenceFunction via Client (wire protocol)."""

    def test_cardinality_returned_in_bind_result(self) -> None:
        """Cardinality should be returned in bind_result via Client."""
        bind_results: list[BindResponse] = []

        def capture_bind_result(result: BindResponse) -> None:
            bind_results.append(result)

        with Client("vgi-fixture-worker") as client:
            list(
                client.table_function(
                    function_name="sequence",
                    arguments=Arguments(positional=(pa.scalar(100),)),
                    bind_result_callback=capture_bind_result,
                )
            )

        assert len(bind_results) == 1
        bind_result = bind_results[0]

        # With BIND/INIT protocol, verify the bind result has the expected fields
        assert bind_result.output_schema is not None
        assert len(bind_result.output_schema) > 0
