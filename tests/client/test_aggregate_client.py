# Copyright 2025, 2026 Query Farm LLC - https://query.farm

# ruff: noqa: S101, D101, D102, D103
"""Tests for the [`Client`][] aggregate driver.

Aggregates are the one function family whose RPCs are all unary — no init
stream, no exchange loop — so the client has to reproduce the drive loop the
C++ hash-aggregate operator runs. These tests exercise that loop against the
real ``vgi-fixture-worker`` over every client transport: the convenience
``aggregate_function`` entry point, the raw session (including ``combine``,
which the convenience path never needs), the optional window RPCs, and the
streaming-partitioned protocol.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from vgi.arguments import Arguments
from vgi.client.client import Client, ClientError

MAIN = "main"


@pytest.fixture
def client(client_transport: Any) -> Any:
    """Yield a started ``Client`` for each transport under test."""
    with client_transport() as started:
        yield started


def _grouped_batches() -> list[pa.RecordBatch]:
    """Two batches whose groups straddle the batch boundary."""
    schema = pa.schema([pa.field("cat", pa.string()), pa.field("value", pa.int64())])
    return [
        pa.RecordBatch.from_pydict({"cat": ["a", "b", "a"], "value": [1, 10, 2]}, schema=schema),
        pa.RecordBatch.from_pydict({"cat": ["b", "a"], "value": [20, 3]}, schema=schema),
    ]


def _values(*rows: int) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict({"value": pa.array(rows, type=pa.int64())})


# ---------------------------------------------------------------------------
# aggregate_function — the convenience driver
# ---------------------------------------------------------------------------


class TestAggregateFunction:
    def test_grouped_sum(self, client: Client) -> None:
        """Groups are keyed client-side and accumulate across batch boundaries."""
        out = client.aggregate_function(
            function_name="vgi_sum",
            schema_name=MAIN,
            input=_grouped_batches(),
            group_by=["cat"],
        )
        assert out.to_pydict() == {"cat": ["a", "b"], "result": [6, 30]}

    def test_groups_are_ordered_first_seen(self, client: Client) -> None:
        """Group ids are minted in first-seen order, so the output row order is too."""
        batch = pa.RecordBatch.from_pydict({"cat": ["z", "m", "a", "m"], "value": [1, 2, 3, 4]})
        out = client.aggregate_function(function_name="vgi_sum", schema_name=MAIN, input=[batch], group_by=["cat"])
        assert out.column("cat").to_pylist() == ["z", "m", "a"]
        assert out.column("result").to_pylist() == [1, 6, 3]

    def test_global_sum(self, client: Client) -> None:
        """No ``group_by`` means one group, the SQL global-aggregate shape."""
        out = client.aggregate_function(
            function_name="vgi_sum",
            schema_name=MAIN,
            input=[_values(1, 2), _values(3, 4)],
        )
        assert out.to_pydict() == {"result": [10]}

    def test_global_over_empty_input_is_one_null_row(self, client: Client) -> None:
        """``SELECT vgi_sum(x) FROM empty_table`` is one NULL row, not zero rows."""
        out = client.aggregate_function(function_name="vgi_sum", schema_name=MAIN, input=[])
        assert out.num_rows == 1
        assert out.column("result").to_pylist() == [None]

    def test_grouped_over_empty_input_is_zero_rows(self, client: Client) -> None:
        """With a GROUP BY there are no groups, so there are no rows."""
        out = client.aggregate_function(
            function_name="vgi_sum",
            schema_name=MAIN,
            input=[],
            group_by=["cat"],
            input_schema=pa.schema([pa.field("value", pa.int64())]),
        )
        assert out.num_rows == 0
        assert out.schema.names == ["cat", "result"]

    def test_empty_batches_are_skipped(self, client: Client) -> None:
        """A zero-row batch contributes nothing but must not break the drive loop."""
        schema = pa.schema([pa.field("value", pa.int64())])
        empty = pa.RecordBatch.from_pydict({"value": pa.array([], type=pa.int64())}, schema=schema)
        out = client.aggregate_function(function_name="vgi_sum", schema_name=MAIN, input=[empty, _values(5), empty])
        assert out.column("result").to_pylist() == [5]

    def test_nullary_aggregate(self, client: Client) -> None:
        """``vgi_count()`` takes no value columns — the row count rides the group ids."""
        batch = pa.RecordBatch.from_pydict({"cat": ["a", "b", "a", "a"]})
        out = client.aggregate_function(function_name="vgi_count", schema_name=MAIN, input=[batch], group_by=["cat"])
        assert out.to_pydict() == {"cat": ["a", "b"], "result": [3, 1]}

    def test_multi_column_aggregate(self, client: Client) -> None:
        """Value columns are passed positionally, in input-batch order."""
        batch = pa.RecordBatch.from_pydict(
            {
                "value": pa.array([1.0, 2.0, 3.0], type=pa.float64()),
                "weight": pa.array([10.0, 100.0, 1000.0], type=pa.float64()),
            }
        )
        out = client.aggregate_function(function_name="vgi_weighted_sum", schema_name=MAIN, input=[batch])
        assert out.column("result").to_pylist() == [pytest.approx(3210.0)]

    def test_varargs_aggregate(self, client: Client) -> None:
        """A varargs aggregate binds against however many value columns arrive."""
        batch = pa.RecordBatch.from_pydict(
            {
                "a": pa.array([1, 2], type=pa.int64()),
                "b": pa.array([10, 20], type=pa.int64()),
                "c": pa.array([100, 200], type=pa.int64()),
            }
        )
        out = client.aggregate_function(function_name="vgi_sum_all", schema_name=MAIN, input=[batch])
        assert out.column("result").to_pylist() == [pytest.approx(333.0)]

    def test_float_output_schema(self, client: Client) -> None:
        """The result column's type comes from the worker's bind, not the input."""
        out = client.aggregate_function(function_name="vgi_avg", schema_name=MAIN, input=[_values(1, 2, 3, 10)])
        assert out.schema.field("result").type == pa.float64()
        assert out.column("result").to_pylist() == [pytest.approx(4.0)]

    def test_const_arguments_reach_finalize(self, client: Client) -> None:
        """``ConstParam`` values ride the bind's ``Arguments`` and are stored per execution."""
        batch = pa.RecordBatch.from_pydict({"value": pa.array([1.0, 2.0, 3.0, 4.0], type=pa.float64())})
        out = client.aggregate_function(
            function_name="vgi_percentile",
            schema_name=MAIN,
            input=[batch],
            arguments=Arguments(positional=(pa.scalar(0.0, type=pa.float64()),)),
        )
        assert out.column("result").to_pylist() == [pytest.approx(1.0)]

    def test_finalize_is_chunked(self, client: Client) -> None:
        """More groups than ``finalize_chunk_size`` means several finalize RPCs."""
        batch = pa.RecordBatch.from_pydict({"cat": ["a", "b", "c", "d", "e"], "value": [1, 2, 3, 4, 5]})
        out = client.aggregate_function(
            function_name="vgi_sum",
            schema_name=MAIN,
            input=[batch],
            group_by=["cat"],
            finalize_chunk_size=2,
        )
        assert out.column("cat").to_pylist() == ["a", "b", "c", "d", "e"]
        assert out.column("result").to_pylist() == [1, 2, 3, 4, 5]

    def test_multi_column_group_by(self, client: Client) -> None:
        """A composite key groups on the tuple of its columns."""
        batch = pa.RecordBatch.from_pydict(
            {
                "region": ["us", "us", "eu", "us"],
                "year": pa.array([2024, 2025, 2024, 2024], type=pa.int64()),
                "value": pa.array([1, 2, 4, 8], type=pa.int64()),
            }
        )
        out = client.aggregate_function(
            function_name="vgi_sum", schema_name=MAIN, input=[batch], group_by=["region", "year"]
        )
        assert out.to_pydict() == {
            "region": ["us", "us", "eu"],
            "year": [2024, 2025, 2024],
            "result": [9, 2, 4],
        }

    def test_null_group_key(self, client: Client) -> None:
        """NULL is a group key like any other, as it is in DuckDB's GROUP BY."""
        batch = pa.RecordBatch.from_pydict({"cat": ["a", None, None], "value": [1, 2, 3]})
        out = client.aggregate_function(function_name="vgi_sum", schema_name=MAIN, input=[batch], group_by=["cat"])
        assert out.to_pydict() == {"cat": ["a", None], "result": [1, 5]}

    def test_missing_group_by_column_is_rejected(self, client: Client) -> None:
        with pytest.raises(ValueError, match="group_by columns not present"):
            client.aggregate_function(
                function_name="vgi_sum",
                schema_name=MAIN,
                input=[_values(1)],
                group_by=["nope"],
            )

    def test_unknown_function_raises_client_error(self, client: Client) -> None:
        with pytest.raises(ClientError, match="no_such_aggregate"):
            client.aggregate_function(function_name="no_such_aggregate", schema_name=MAIN, input=[_values(1)])

    def test_non_aggregate_function_raises_client_error(self, client: Client) -> None:
        """Binding a scalar through the aggregate path is a typed worker error."""
        with pytest.raises(ClientError):
            client.aggregate_function(function_name="upper_case", schema_name=MAIN, input=[_values(1)])


# ---------------------------------------------------------------------------
# aggregate_session — the raw RPC surface
# ---------------------------------------------------------------------------


class TestAggregateSession:
    def test_bind_reports_execution_id_and_output_schema(self, client: Client) -> None:
        with client.aggregate_session(
            function_name="vgi_sum",
            schema_name=MAIN,
            input_schema=pa.schema([pa.field("value", pa.int64())]),
        ) as session:
            assert session.execution_id
            assert session.output_schema.names == ["result"]

    def test_update_then_finalize(self, client: Client) -> None:
        """Caller-allocated group ids accumulate exactly as DuckDB's do."""
        with client.aggregate_session(
            function_name="vgi_sum",
            schema_name=MAIN,
            input_schema=pa.schema([pa.field("value", pa.int64())]),
        ) as session:
            session.update(group_ids=[0, 1, 0], batch=_values(1, 100, 2))
            session.update(group_ids=[1], batch=_values(200))
            assert session.finalize([0, 1]).column("result").to_pylist() == [3, 300]

    def test_finalize_order_follows_requested_group_ids(self, client: Client) -> None:
        with client.aggregate_session(
            function_name="vgi_sum",
            schema_name=MAIN,
            input_schema=pa.schema([pa.field("value", pa.int64())]),
        ) as session:
            session.update(group_ids=[0, 1], batch=_values(7, 9))
            assert session.finalize([1, 0]).column("result").to_pylist() == [9, 7]

    def test_finalize_of_never_updated_group_is_null(self, client: Client) -> None:
        """An absent state finalizes to whatever the function returns for None."""
        with client.aggregate_session(
            function_name="vgi_sum",
            schema_name=MAIN,
            input_schema=pa.schema([pa.field("value", pa.int64())]),
        ) as session:
            assert session.finalize([42]).column("result").to_pylist() == [None]

    def test_combine_merges_source_into_target(self, client: Client) -> None:
        """``combine`` is the thread-local-to-global merge; only the target moves."""
        with client.aggregate_session(
            function_name="vgi_sum",
            schema_name=MAIN,
            input_schema=pa.schema([pa.field("value", pa.int64())]),
        ) as session:
            session.update(group_ids=[0, 0, 1], batch=_values(1, 2, 100))
            session.combine(source_group_ids=[1], target_group_ids=[0])
            assert session.finalize([0, 1]).column("result").to_pylist() == [103, 100]

    def test_update_rejects_mismatched_row_counts(self, client: Client) -> None:
        with (
            client.aggregate_session(
                function_name="vgi_sum",
                schema_name=MAIN,
                input_schema=pa.schema([pa.field("value", pa.int64())]),
            ) as session,
            pytest.raises(ValueError, match="group_ids has 2 entries but batch has 3 rows"),
        ):
            session.update(group_ids=[0, 1], batch=_values(1, 2, 3))

    def test_combine_rejects_mismatched_lengths(self, client: Client) -> None:
        with (
            client.aggregate_session(
                function_name="vgi_sum",
                schema_name=MAIN,
                input_schema=pa.schema([pa.field("value", pa.int64())]),
            ) as session,
            pytest.raises(ValueError, match="source_group_ids has 2 entries"),
        ):
            session.combine(source_group_ids=[0, 1], target_group_ids=[2])

    def test_group_ids_accept_arrow_arrays(self, client: Client) -> None:
        with client.aggregate_session(
            function_name="vgi_sum",
            schema_name=MAIN,
            input_schema=pa.schema([pa.field("value", pa.int64())]),
        ) as session:
            session.update(group_ids=pa.array([5, 5], type=pa.int32()), batch=_values(4, 6))
            assert session.finalize(pa.array([5], type=pa.int64())).column("result").to_pylist() == [10]

    def test_destroy_is_idempotent(self, client: Client) -> None:
        """Teardown is best-effort, so a second destroy must not raise."""
        session = client.aggregate_bind(
            function_name="vgi_sum",
            schema_name=MAIN,
            input_schema=pa.schema([pa.field("value", pa.int64())]),
        )
        session.destroy()
        session.destroy()


# ---------------------------------------------------------------------------
# Optional window RPCs
# ---------------------------------------------------------------------------


class TestAggregateWindow:
    def test_window_over_a_partition(self, client: Client) -> None:
        """One partition shipped once, then queried per output row by frame."""
        with client.aggregate_session(
            function_name="vgi_window_sum",
            schema_name=MAIN,
            input_schema=pa.schema([pa.field("value", pa.int64())]),
        ) as session:
            session.window_init(partition_id=0, partition=_values(1, 2, 3, 4))
            assert session.window(partition_id=0, rid=0, frames=[(0, 1)]).column("result").to_pylist() == [1]
            assert session.window(partition_id=0, rid=3, frames=[(0, 4)]).column("result").to_pylist() == [10]
            assert session.window(partition_id=0, rid=3, frames=[(1, 3)]).column("result").to_pylist() == [5]
            session.window_destroy(0)

    def test_window_batch_computes_many_rows_at_once(self, client: Client) -> None:
        with client.aggregate_session(
            function_name="vgi_window_sum_batch",
            schema_name=MAIN,
            input_schema=pa.schema([pa.field("value", pa.int64())]),
        ) as session:
            session.window_init(partition_id=0, partition=_values(1, 2, 3, 4))
            running = session.window_batch(partition_id=0, row_idx=0, frames=[[(0, end + 1)] for end in range(4)])
            assert running.column("result").to_pylist() == [1, 3, 6, 10]
            session.window_destroy(0)

    def test_window_filter_mask_excludes_rows(self, client: Client) -> None:
        """A ``FILTER (WHERE ...)`` mask arrives as Arrow's packed validity bitmap."""
        with client.aggregate_session(
            function_name="vgi_window_sum",
            schema_name=MAIN,
            input_schema=pa.schema([pa.field("value", pa.int64())]),
        ) as session:
            session.window_init(
                partition_id=0,
                partition=_values(1, 2, 3, 4),
                filter_mask=[True, False, True, False],
                frame_stats=((0, 0), (0, 0)),
                all_valid=[True],
            )
            assert session.window(partition_id=0, rid=3, frames=[(0, 4)]).column("result").to_pylist() == [4]
            session.window_destroy(0)

    def test_several_partitions_are_independent(self, client: Client) -> None:
        with client.aggregate_session(
            function_name="vgi_window_sum",
            schema_name=MAIN,
            input_schema=pa.schema([pa.field("value", pa.int64())]),
        ) as session:
            session.window_init(partition_id=0, partition=_values(1, 1))
            session.window_init(partition_id=1, partition=_values(100, 100))
            assert session.window(partition_id=0, rid=1, frames=[(0, 2)]).column("result").to_pylist() == [2]
            assert session.window(partition_id=1, rid=1, frames=[(0, 2)]).column("result").to_pylist() == [200]
            session.window_destroy(0)
            session.window_destroy(1)

    def test_window_on_an_unknown_partition_raises(self, client: Client) -> None:
        with (
            client.aggregate_session(
                function_name="vgi_window_sum",
                schema_name=MAIN,
                input_schema=pa.schema([pa.field("value", pa.int64())]),
            ) as session,
            pytest.raises(ClientError),
        ):
            session.window(partition_id=99, rid=0, frames=[(0, 1)])


# ---------------------------------------------------------------------------
# Streaming-partitioned aggregates
# ---------------------------------------------------------------------------


class TestAggregateStreaming:
    @staticmethod
    def _schema() -> pa.Schema:
        return pa.schema([pa.field("k", pa.string()), pa.field("ts", pa.int64()), pa.field("v", pa.int64())])

    def test_running_value_per_row(self, client: Client) -> None:
        """One output row per input row: the partition's value at that position."""
        schema = self._schema()
        with client.aggregate_streaming(
            function_name="vgi_streaming_sum",
            schema_name=MAIN,
            input_schema=schema,
            partition_key_count=1,
            order_key_count=1,
        ) as session:
            chunk = pa.RecordBatch.from_pydict(
                {"k": ["a", "b", "a", "b"], "ts": [1, 1, 2, 2], "v": [1, 10, 2, 20]}, schema=schema
            )
            assert session.chunk(chunk).column(0).to_pylist() == [1, 10, 3, 30]

    def test_state_carries_across_chunks(self, client: Client) -> None:
        schema = self._schema()
        with client.aggregate_streaming(
            function_name="vgi_streaming_sum",
            schema_name=MAIN,
            input_schema=schema,
            partition_key_count=1,
            order_key_count=1,
        ) as session:
            first = pa.RecordBatch.from_pydict({"k": ["a"], "ts": [1], "v": [5]}, schema=schema)
            second = pa.RecordBatch.from_pydict({"k": ["a"], "ts": [2], "v": [7]}, schema=schema)
            assert session.chunk(first).column(0).to_pylist() == [5]
            assert session.chunk(second).column(0).to_pylist() == [12]

    def test_output_schema_is_resolved_by_a_probe_bind(self, client: Client) -> None:
        """Omitting ``output_schema`` resolves it exactly as the extension does."""
        with client.aggregate_streaming(
            function_name="vgi_streaming_sum",
            schema_name=MAIN,
            input_schema=self._schema(),
            partition_key_count=1,
            order_key_count=1,
        ) as session:
            assert session.output_schema.names == ["result"]

    def test_key_counts_wider_than_the_schema_are_rejected(self, client: Client) -> None:
        with (
            pytest.raises(ValueError, match="exceeds"),
            client.aggregate_streaming(
                function_name="vgi_streaming_sum",
                schema_name=MAIN,
                input_schema=self._schema(),
                partition_key_count=3,
                order_key_count=2,
            ),
        ):
            pass


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def test_aggregate_on_an_unstarted_client_raises(fixture_worker: str) -> None:
    """Aggregates run on the primary connection, so ``start()`` is required."""
    unstarted = Client(fixture_worker, pool=None)
    with pytest.raises(ClientError, match="Client not started"):
        unstarted.aggregate_function(function_name="vgi_sum", schema_name=MAIN, input=[_values(1)])
