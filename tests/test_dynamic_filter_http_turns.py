# Copyright 2026 Query Farm LLC - https://query.farm

"""Dynamic-filter state across HTTP turns: bounded replay, exact reconstruction.

An HTTP producer rebuilds its filters from the call and cursor tokens on every
turn. The cursor used to carry every tick's delta and replay all of them, each
re-validated through the embedded DuckDB evaluator, so turn ``k`` did ``k``
parses: a 100-batch Top-N scan spent ~20 s there. These tests drive the real
``dynamic_filter_echo`` fixture through the real HTTP stack, playing the client
(the requests are hand-built; every response is the worker's), and pin both
halves of the fix: a turn's parse work stays constant as ticks accumulate, and
the state rebuilt from the compacted history is the state the worker had.
"""

from __future__ import annotations

import base64
import contextlib
import json
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pytest
from vgi_rpc.rpc import RpcServer

from vgi import filter_v2
from vgi._test_fixtures.worker import ExampleWorker
from vgi.arguments import Arguments
from vgi.invocation import FunctionType
from vgi.protocol import BindRequest, InitRequest, VgiProtocol

_KEY = b"\x5a" * 32
_METADATA: dict[bytes | str, bytes | str] = {
    b"vgi_filter_encoding": b"vgi.filters.v2",
    b"vgi_filter_version": b"2",
    b"vgi_evaluation_context": b"vgi.none.v1",
}


def _document_batch(document: dict[str, object], *values: int) -> pa.RecordBatch:
    fields: list[pa.Field[Any]] = [pa.field("filter_spec", pa.string(), nullable=False)]
    arrays: list[pa.Array[Any]] = [pa.array([json.dumps(document, separators=(",", ":"))])]
    for index, value in enumerate(values):
        fields.append(pa.field(f"value_{index}", pa.int64()))
        arrays.append(pa.array([value], type=pa.int64()))
    return pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields, metadata=_METADATA))


def _empty_snapshot() -> pa.RecordBatch:
    return _document_batch(
        {"encoding": "vgi.filters.v2", "semantics": "vgi.duckdb.standard.v1", "kind": "snapshot", "predicates": []}
    )


def _upsert(predicate_id: str, revision: int, op: str, value_ref: int) -> dict[str, object]:
    return {
        "operation": "upsert",
        "id": predicate_id,
        "revision": revision,
        "mode": "advisory",
        "source": "top_n",
        "expression": {
            "node": "comparison",
            "op": op,
            "left": {"node": "column_ref", "column_index": 0, "column_name": "n"},
            "right": {"node": "literal", "value_ref": value_ref},
        },
    }


def _remove(predicate_id: str, revision: int) -> dict[str, object]:
    return {"operation": "remove", "id": predicate_id, "revision": revision}


def _tick(updates: list[dict[str, object]], *values: int) -> pa.KeyValueMetadata:
    """Tick metadata carrying one delta, framed the way the C++ client frames it."""
    batch = _document_batch(
        {"encoding": "vgi.filters.v2", "semantics": "vgi.duckdb.standard.v1", "kind": "delta", "updates": updates},
        *values,
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return pa.KeyValueMetadata({b"vgi_pushdown_filters": base64.b64encode(sink.getvalue().to_pybytes())})


@contextlib.contextmanager
def _echo_stream(count: int, batch_size: int) -> Iterator[Any]:
    """Open a ``dynamic_filter_echo`` scan over the in-process HTTP stack."""
    from vgi_rpc.http import http_connect, make_sync_client

    worker = ExampleWorker(quiet=True)
    worker._signing_key = _KEY
    client = make_sync_client(RpcServer(VgiProtocol, worker, enable_describe=False), token_key=_KEY)
    bind = BindRequest(
        function_name="dynamic_filter_echo",
        arguments=Arguments(positional=(pa.scalar(count),), named={"batch_size": pa.scalar(batch_size)}),
        function_type=FunctionType.TABLE,
        input_schema=None,
    )
    with http_connect(VgiProtocol, client=client, compression_level=None) as proxy:  # type: ignore[type-abstract]
        resp = proxy.bind(request=bind)
        stream = proxy.init(
            request=InitRequest(
                bind_call=bind,
                output_schema=resp.output_schema,
                bind_opaque_data=resp.opaque_data,
                pushdown_filters=_empty_snapshot(),
            )
        )
        try:
            yield stream
        finally:
            stream.close()


@pytest.fixture
def parse_count(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Count delta documents the worker parses (a spy: it records, never answers)."""
    calls = [0]
    original = filter_v2._Parser.parse_delta

    def counting(self: Any) -> Any:
        calls[0] += 1
        return original(self)

    monkeypatch.setattr(filter_v2._Parser, "parse_delta", counting)
    return calls


def test_a_turn_parses_a_bounded_number_of_deltas(parse_count: list[int]) -> None:
    """A tightening Top-N bound sends a delta every tick; no turn replays them all.

    Rows descend from 999 in batches of 10 and each tick narrows ``n < bound``,
    so every tick's delta changes the live predicate -- the case the C++ client
    cannot skip. Before compaction turn ``k`` parsed ``k`` deltas.
    """
    ticks = 60
    with _echo_stream(count=1000, batch_size=10) as stream:
        first = stream.tick()  # the init turn's batch; metadata rides continuations only
        assert first.batch.num_rows == 10
        per_turn: list[int] = []
        for revision in range(1, ticks + 1):
            bound = 1000 - 10 * revision  # rows of this tick are [bound - 10, bound - 1]
            before = parse_count[0]
            batch = stream.tick(_tick([_upsert("top_n:0", revision, "lt", 0)], bound)).batch
            per_turn.append(parse_count[0] - before)
            assert batch.column("n").to_pylist() == list(range(bound - 1, bound - 11, -1))
            assert batch.column("pushed_filters")[0].as_py().count("ConstantFilter(n < ") == 1
            assert f"ConstantFilter(n < {bound})" in batch.column("pushed_filters")[0].as_py()

    # One parse for the new delta, one to replay the single delta the cursor keeps.
    assert max(per_turn) <= 2, per_turn


def test_a_resent_revision_is_stale_and_adds_no_replay(parse_count: list[int]) -> None:
    """Resending a revision with another value is a stale no-op: nothing applies, nothing accumulates."""
    with _echo_stream(count=1000, batch_size=10) as stream:
        stream.tick()
        stream.tick(_tick([_upsert("top_n:0", 1, "lt", 0)], 100000))
        before = parse_count[0]
        for _ in range(20):
            batch = stream.tick(_tick([_upsert("top_n:0", 1, "lt", 0)], 5)).batch
            assert batch.num_rows == 10  # n < 5 would have emptied it
            assert "ConstantFilter(n < 100000)" in batch.column("pushed_filters")[0].as_py()
        assert parse_count[0] - before <= 2 * 20


def test_rebuilt_state_keeps_the_order_the_worker_had() -> None:
    """A removed-then-re-added predicate keeps its position across a turn boundary.

    Deltas: {a:1, b:1} -> [a, b]; {remove a:2, b:1} -> [b]; {a:3, b:1} -> [b, a].
    The compacted history is the deltas that first carried a:3 and b:1 -- the
    third and the first -- and replaying those alone yields [a, b]. The turn
    after the third delta has to show the order the third turn itself had.
    """
    with _echo_stream(count=1000, batch_size=10) as stream:
        stream.tick()
        stream.tick(_tick([_upsert("top_n:0", 1, "lt", 0), _upsert("top_n:1", 1, "gt", 1)], 100000, 5))
        stream.tick(_tick([_remove("top_n:0", 2), _upsert("top_n:1", 1, "gt", 0)], 5))
        applied = stream.tick(_tick([_upsert("top_n:0", 3, "lt", 0), _upsert("top_n:1", 1, "gt", 1)], 99999, 5)).batch
        rebuilt = stream.tick().batch  # no delta: filters come purely from the tokens
        assert applied.num_rows == rebuilt.num_rows == 10
        live = applied.column("pushed_filters")[0].as_py()
        assert live.index("ConstantFilter(n > 5)") < live.index("ConstantFilter(n < 99999)"), live
        assert rebuilt.column("pushed_filters")[0].as_py() == live


def test_a_tombstone_survives_compaction() -> None:
    """A removal stays in force after its upsert is compacted away: a stale upsert cannot resurrect it."""
    with _echo_stream(count=1000, batch_size=10) as stream:
        stream.tick()
        stream.tick(_tick([_upsert("top_n:0", 1, "lt", 0)], 100000))
        stream.tick(_tick([_remove("top_n:0", 2)]))
        stream.tick()  # a turn rebuilt from the compacted tokens
        stale = stream.tick(_tick([_upsert("top_n:0", 1, "lt", 0)], 5)).batch
        assert stale.column("pushed_filters")[0].as_py() == "(none)"
        assert stale.num_rows == 10
