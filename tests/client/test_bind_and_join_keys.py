# Copyright 2026 Query Farm LLC - https://query.farm

# ruff: noqa: S101, D101, D102, D103
"""`Client.bind()` and `Client.table_function(join_keys=...)`.

Both were previously unreachable from `Client`:

- Schema discovery for a bare (non-catalog) function name had no
  standalone entry point — only `table_function(bind_result_callback=...)`,
  which only fires as a side effect of a generator's first `next()`, by
  which point `init()`/real execution has already started. `bind()` runs
  only the `bind()` RPC.
- `join_keys` (semi-join pushdown) has been carried on `InitRequest`/
  `TableFunctionPlanRequest` and threaded through worker-side
  `deserialize_filters(..., join_keys=...)` all along
  (`vgi/table_filter_pushdown.py`), but `Client`'s public methods never
  exposed a way to set it on the request.

Exercised against `filter_echo` (`vgi/_test_fixtures/table/filters.py`),
which auto-applies whatever filters it receives and echoes them back as
SQL-like text in its `pushed_filters` column — the same fixture the
`vgi_acero` filter-pushdown spike used, since it proves pushdown is real
(server-side applied), not just correct final rows.
"""

from __future__ import annotations

import json
from typing import Any, cast

import pyarrow as pa
import pytest

from vgi.arguments import Arguments

MAIN = ["main"]


@pytest.fixture
def client(client_transport: Any) -> Any:
    with client_transport() as c:
        yield c


def _join_keys_pushdown_filters_bytes(column_name: str, keys_column: str) -> bytes:
    """Build v2 `pushdown_filters` IPC bytes containing one external IN set.

    The actual key values travel separately as `Client.table_function`'s own
    `join_keys=` argument. V2 resolves authoritative batch/column indexes and
    validates the redundant column name.
    """
    document = {
        "encoding": "vgi.filters.v2",
        "semantics": "vgi.duckdb.standard.v1",
        "kind": "snapshot",
        "predicates": [
            {
                "id": "join:0",
                "revision": 0,
                "mode": "advisory",
                "source": "join",
                "expression": {
                    "node": "in",
                    "expression": {"node": "column_ref", "column_index": 0, "column_name": column_name},
                    "set": {
                        "kind": "external",
                        "batch_index": 0,
                        "column_index": 0,
                        "column_name": keys_column,
                    },
                    "negated": False,
                },
            }
        ],
    }
    spec_field = pa.field("filter_spec", pa.string(), nullable=False)
    schema = pa.schema(
        [spec_field],
        metadata={
            b"vgi_filter_encoding": b"vgi.filters.v2",
            b"vgi_filter_version": b"2",
            b"vgi_evaluation_context": b"vgi.none.v1",
        },
    )
    batch = pa.record_batch({"filter_spec": [json.dumps(document)]}, schema=schema)

    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    return sink.getvalue().to_pybytes()


class TestBind:
    def test_bind_returns_the_bound_schema(self, client: Any) -> None:
        resp = client.bind(
            function_name="filter_echo",
            schema_path=MAIN,
            arguments=Arguments(positional=(pa.scalar(10),)),
        )
        assert resp.output_schema.names == ["n", "s", "pushed_filters"]

    def test_bind_does_not_corrupt_the_connection_for_later_calls(self, client: Any) -> None:
        """A `bind()` call must leave the connection usable for a real scan afterward.

        This is the regression the old workaround (peek one batch via
        `bind_result_callback` then `gen.close()`) could not satisfy — closing
        a generator early desyncs the subprocess's lockstep RPC pipe for the
        rest of that `Client`'s life. `bind()` never opens a stream at all.
        """
        client.bind(
            function_name="filter_echo",
            schema_path=MAIN,
            arguments=Arguments(positional=(pa.scalar(10),)),
        )
        batches = list(
            client.table_function(
                function_name="filter_echo",
                schema_path=MAIN,
                arguments=Arguments(positional=(pa.scalar(5),)),
            )
        )
        table = pa.Table.from_batches(batches)
        assert table.num_rows == 5
        assert sorted(cast("list[int]", table.column("n").to_pylist())) == [0, 1, 2, 3, 4]


class TestJoinKeys:
    def test_join_keys_filters_rows_server_side(self, client: Any) -> None:
        pushdown_filters = _join_keys_pushdown_filters_bytes(column_name="n", keys_column="n")
        join_keys = [pa.record_batch({"n": pa.array([3, 7], type=pa.int64())})]

        batches = list(
            client.table_function(
                function_name="filter_echo",
                schema_path=MAIN,
                arguments=Arguments(positional=(pa.scalar(10),)),
                pushdown_filters=pushdown_filters,
                join_keys=join_keys,
            )
        )
        table = pa.Table.from_batches(batches)

        assert sorted(cast("list[int]", table.column("n").to_pylist())) == [3, 7]
        # auto_apply_filters=True means the worker itself filtered — the
        # echoed pushed_filters text is the proof it's a real server-side
        # InFilter, not us post-filtering client-side.
        for value in cast("list[str]", table.column("pushed_filters").to_pylist()):
            assert "n IN" in value
            assert "3" in value and "7" in value

    def test_no_join_keys_returns_everything(self, client: Any) -> None:
        """Sanity check: omitting join_keys behaves exactly like today (no filter)."""
        batches = list(
            client.table_function(
                function_name="filter_echo",
                schema_path=MAIN,
                arguments=Arguments(positional=(pa.scalar(5),)),
            )
        )
        table = pa.Table.from_batches(batches)
        assert table.num_rows == 5
        assert all(v == "(none)" for v in table.column("pushed_filters").to_pylist())
