# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Regression tests: calling a function via the wrong RPC method shape.

Found live: a hand-rolled client called ``elevation`` (a blended row-transform /
table-in-out function on a real deployed worker) via ``table_function()``
instead of ``table_in_out_function()``. On that worker's TypeScript SDK this
produced a non-terminating continuation loop rather than an error — both sides
were, independently and correctly per their own local contract, waiting on the
other to stop.

``vgi/worker.py``'s init() dispatch was asymmetric: the ``TableInOutGenerator``
branch already rejected a missing input phase (through incidental, unhelpful
errors — a bare ``AssertionError`` from the default ``on_bind()``, or a
generic ``ValueError`` naming only the phase, not the function or the fix);
the ``TableFunctionGenerator`` branch had no check at all and would silently
ignore a phase/input stream it was never designed to accept. These tests
assert both directions now fail immediately with a clear, actionable message
naming the mismatch -- not a hang, not a bare assert, not a generic error.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from tests.conftest import make_schema
from vgi.arguments import Arguments
from vgi.client import Client
from vgi.client.errors import ClientError


class TestTableFunctionOnTableInOut:
    """`Client.table_function()` called against a function that requires input."""

    def test_classic_table_in_out_function_rejected(self, fixture_worker: str) -> None:
        """`echo` (a classic `TableInOutGenerator`) needs an input stream -- reject, don't hang."""
        with Client(fixture_worker) as client, pytest.raises(ClientError) as exc_info:
            list(
                client.table_function(
                    function_name="echo",
                    schema_name="main",
                    arguments=Arguments(),
                )
            )
        message = str(exc_info.value)
        assert "echo" in message
        assert "table_in_out_function" in message

    def test_blended_row_transform_function_rejected(self, fixture_worker: str) -> None:
        """`row_sum` (a blended `RowTransformFunction`) needs its args as input columns -- reject, don't hang."""
        with Client(fixture_worker) as client, pytest.raises(ClientError) as exc_info:
            list(
                client.table_function(
                    function_name="row_sum",
                    schema_name="main",
                    arguments=Arguments(positional=(pa.scalar(1.0), pa.scalar(2.0))),
                )
            )
        message = str(exc_info.value)
        assert "row_sum" in message
        assert "table_in_out_function" in message


class TestTableInOutFunctionOnPlainTableFunction:
    """`Client.table_in_out_function()` called against a plain producer."""

    def test_plain_table_function_rejected(self, fixture_worker: str) -> None:
        """`sequence` is a plain `TableFunctionGenerator` -- it takes no input stream."""
        input_schema = make_schema([pa.field("n", pa.int64())])
        batch = pa.RecordBatch.from_pydict({"n": [0]}, schema=input_schema)

        with Client(fixture_worker) as client, pytest.raises(ClientError) as exc_info:
            list(
                client.table_in_out_function(
                    function_name="sequence",
                    schema_name="main",
                    input=iter([batch]),
                    arguments=Arguments(positional=(pa.scalar(5),)),
                )
            )
        message = str(exc_info.value)
        assert "sequence" in message
        assert "table_function" in message
