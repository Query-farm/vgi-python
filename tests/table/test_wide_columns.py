# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Tests for the wide-schema benchmark fixture.

Guards the fixture the width benchmarks depend on: if ``wide`` stops
producing the schema it claims, a profile taken against it measures
something other than what it says on the tin.
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from vgi_rpc.rpc import RpcServer

from vgi._test_fixtures.table.wide import DEFAULT_COLUMNS, WideFunction
from vgi.arguments import Arguments
from vgi.protocol import BindRequest, FunctionType, InitRequest, VgiProtocol
from vgi.worker import Worker


class _WideWorker(Worker):
    """Worker exposing only the wide fixture, so it stays off ExampleWorker."""

    functions = [WideFunction]


def _scan(columns: int, count: int, batch_size: int) -> list[pa.RecordBatch]:
    from vgi_rpc.http import http_connect, make_sync_client

    key = b"\x11" * 32
    worker = _WideWorker(quiet=True)
    worker._signing_key = key
    client = make_sync_client(RpcServer(VgiProtocol, worker, enable_describe=False), token_key=key)
    with http_connect(VgiProtocol, client=client, compression_level=None) as proxy:  # type: ignore[type-abstract]
        bind = BindRequest(
            function_name="wide",
            arguments=Arguments(
                positional=(pa.scalar(count),),
                named={"columns": pa.scalar(columns), "batch_size": pa.scalar(batch_size)},
            ),
            function_type=FunctionType.TABLE,
            input_schema=pa.schema([]),
        )
        resp = proxy.bind(request=bind)
        assert len(resp.output_schema) == columns
        stream = proxy.init(
            request=InitRequest(bind_call=bind, output_schema=resp.output_schema, bind_opaque_data=resp.opaque_data)
        )
        try:
            return [ab.batch for ab in stream]
        finally:
            stream.close()


@pytest.mark.parametrize("columns", [1, 12, 2000])
def test_wide_emits_the_requested_width(columns: int) -> None:
    """The bound schema and every emitted batch carry exactly ``columns``."""
    batches = _scan(columns=columns, count=8, batch_size=4)
    assert batches, "expected at least one batch"
    assert sum(b.num_rows for b in batches) == 8
    for batch in batches:
        assert batch.num_columns == columns
        assert batch.schema.names[0] == "c0"
        assert batch.schema.names[-1] == f"c{columns - 1}"


def test_wide_survives_multi_turn_state_round_trip() -> None:
    """Rows stay contiguous across turns, so the cursor round-trips at width.

    Each turn re-serializes state and rebuilds a zero-row sentinel batch of
    the full width; this is the path the width benchmark exercises.
    """
    batches = _scan(columns=DEFAULT_COLUMNS, count=20, batch_size=4)
    values = [v for b in batches for v in b.column(0).to_pylist()]
    assert values == list(range(20))
    assert all(b.num_columns == DEFAULT_COLUMNS for b in batches)


def test_wide_is_not_registered_on_the_example_worker() -> None:
    """Keep it off ExampleWorker: that count is asserted by the C++ suite."""
    from vgi._test_fixtures.worker import ExampleWorker

    assert WideFunction not in ExampleWorker.functions
