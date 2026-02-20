"""Tests for ConditionalMessageFunction via Client."""

from __future__ import annotations

import pyarrow as pa

from vgi import schema
from vgi.arguments import Arguments
from vgi.client import Client


class TestConditionalMessageFunction:
    """Tests for ConditionalMessageFunction via Client subprocess."""

    def test_conditional_message_basic(self, example_worker: str) -> None:
        """Repeat message 3 times when condition is true, empty otherwise."""
        s = schema(condition=pa.bool_())
        batch = pa.RecordBatch.from_pydict({"condition": [True, False, True]}, schema=s)

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="conditional_message",
                    input=iter([batch]),
                    # ConstParams only: repeat_count=3, message="Hi! "
                    # Param "condition" resolves from batch column 0
                    arguments=Arguments(positional=(pa.scalar(3), pa.scalar("Hi! "))),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": ["Hi! Hi! Hi! ", "", "Hi! Hi! Hi! "]}

    def test_conditional_message_all_false(self, example_worker: str) -> None:
        """All false conditions produce all empty strings."""
        s = schema(condition=pa.bool_())
        batch = pa.RecordBatch.from_pydict({"condition": [False, False, False]}, schema=s)

        with Client(example_worker) as client:
            outputs = list(
                client.scalar_function(
                    function_name="conditional_message",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(2), pa.scalar("X"))),
                )
            )

        assert len(outputs) == 1
        assert outputs[0].to_pydict() == {"result": ["", "", ""]}
