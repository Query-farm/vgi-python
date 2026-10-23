# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Continued output: several replies for one table-in-out input batch.

A function emits ``out.emit(..., more=True)`` and the framework calls its
``resume()`` for each next batch. Each batch is its own reply; the client
fetches the next one with a zero-row ``vgi.status: CONTINUE`` tick.
``blended_paged`` is the fixture: ``blended_explode``'s output, ``page_size``
rows per reply.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
import pytest
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import AnnotatedBatch, OutputCollector

from tests.conftest import make_schema
from vgi.arguments import Arg, Arguments, TableInput
from vgi.cache_control import CacheControl
from vgi.client import Client
from vgi.client.client import ClientError
from vgi.invocation import BindResponse
from vgi.metadata import resolve_metadata
from vgi.protocol import (
    CONTINUE,
    STATUS_KEY,
    _FilteringOutputCollector,
    _TrackingOutputCollector,
)
from vgi.schema_utils import schema
from vgi.table_function import BindParams, ProcessParams
from vgi.table_in_out_function import RowTransformFunction, TableInOutGenerator

_INPUT_SCHEMA = make_schema([pa.field("n", pa.int64())])


def _expected_pairs(counts: list[int | None]) -> list[tuple[int, int]]:
    return [(row, k) for row, n in enumerate(counts) for k in range(n or 0)]


def _run_paged(
    client: Client,
    monkeypatch: pytest.MonkeyPatch,
    batches: list[list[int | None]],
    page_size: int,
) -> tuple[list[tuple[int, int]], list[int]]:
    """Run ``blended_paged``; return (parent_row, value) pairs and replies per input batch."""
    replies: list[int] = []
    original = Client._process_batch_on_worker

    def counting(self: Client, *args: Any, **kwargs: Any) -> Any:
        outputs, parent_rows = original(self, *args, **kwargs)
        replies.append(len(outputs))
        return outputs, parent_rows

    monkeypatch.setattr(Client, "_process_batch_on_worker", counting)
    parent_rows_seen: list[int] = []
    with client:
        results = list(
            client.table_in_out_function(
                function_name="blended_paged",
                schema_path=["main"],
                input=iter([pa.RecordBatch.from_pydict({"n": counts}, schema=_INPUT_SCHEMA) for counts in batches]),
                arguments=Arguments(named={"page_size": pa.scalar(page_size)}),
                parent_row_callback=parent_rows_seen.extend,
            )
        )
    values = [v for batch in results for v in batch.column("i").to_pylist()]
    return list(zip(parent_rows_seen, values, strict=True)), replies


class TestContinuedOutput:
    """A client that accepts more output gets one reply per page."""

    def test_pages_cover_the_output_in_order(
        self, client_transport: Callable[[], Client], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Every transport, including HTTP, where the cursor rides the state token."""
        counts = [0, 1, 3, 2, 0, 4]
        pairs, replies = _run_paged(client_transport(), monkeypatch, [counts], page_size=2)
        assert pairs == _expected_pairs(counts)
        assert replies == [5]  # 10 rows, 2 per reply

    def test_each_input_batch_pages_independently(
        self, client_transport: Callable[[], Client], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A second input batch starts over: provenance indexes its own rows."""
        pairs, replies = _run_paged(client_transport(), monkeypatch, [[3], [1, 2]], page_size=2)
        assert pairs == _expected_pairs([3]) + _expected_pairs([1, 2])
        assert replies == [2, 2]

    @pytest.mark.parametrize(("page_size", "expected_replies"), [(1, 10), (3, 4), (4, 3), (10, 1), (100, 1)])
    def test_page_size_sets_the_reply_count(
        self,
        client_transport: Callable[[], Client],
        monkeypatch: pytest.MonkeyPatch,
        page_size: int,
        expected_replies: int,
    ) -> None:
        """10 output rows: one reply per page_size rows, pages crossing input rows."""
        counts = [0, 1, 3, 2, 0, 4]
        pairs, replies = _run_paged(client_transport(), monkeypatch, [counts], page_size=page_size)
        assert pairs == _expected_pairs(counts)
        assert replies == [expected_replies]

    def test_null_and_trailing_empty_rows_add_no_reply(
        self, fixture_worker: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """NULL counts as 0, and rows with no output after the last page add no empty reply."""
        counts: list[int | None] = [None, 2, 0, 0]
        pairs, replies = _run_paged(Client(fixture_worker), monkeypatch, [counts], page_size=2)
        assert pairs == _expected_pairs(counts)
        assert replies == [1]

    def test_invalid_page_size_is_refused_at_bind(self, fixture_worker: str, monkeypatch: pytest.MonkeyPatch) -> None:
        """page_size := 0 fails at bind, before any exchange."""
        with pytest.raises(ClientError, match="page_size must be at least 1"):
            _run_paged(Client(fixture_worker), monkeypatch, [[3]], page_size=0)

    def test_output_that_fits_one_page_is_one_reply(self, fixture_worker: str, monkeypatch: pytest.MonkeyPatch) -> None:
        """A function that never says more=True answers with a single reply."""
        pairs, replies = _run_paged(Client(fixture_worker), monkeypatch, [[2, 1]], page_size=10)
        assert pairs == _expected_pairs([2, 1])
        assert replies == [1]

    def test_no_output_is_one_empty_reply(self, fixture_worker: str, monkeypatch: pytest.MonkeyPatch) -> None:
        """No output rows at all is one empty reply, not a page followed by an empty one."""
        pairs, replies = _run_paged(Client(fixture_worker), monkeypatch, [[0, 0]], page_size=2)
        assert pairs == []
        assert replies == [1]


def _tick() -> AnnotatedBatch:
    return AnnotatedBatch(
        batch=pa.RecordBatch.from_pydict({"n": []}, schema=_INPUT_SCHEMA),
        custom_metadata=pa.KeyValueMetadata({STATUS_KEY: CONTINUE}),
    )


def _real(*counts: int) -> AnnotatedBatch:
    return AnnotatedBatch(batch=pa.RecordBatch.from_pydict({"n": list(counts)}, schema=_INPUT_SCHEMA))


class TestOutOfTurnInput:
    """While output is pending only a tick is valid, and a tick only then."""

    @staticmethod
    def _send(fixture_worker: str, monkeypatch: pytest.MonkeyPatch, sequence: list[AnnotatedBatch]) -> None:
        """Send ``sequence`` raw on one blended_paged stream (page_size 2); the last must be refused."""

        def send(self: Client, worker: Any, input_batch: pa.RecordBatch, *args: Any, **kwargs: Any) -> Any:
            from vgi_rpc.rpc import RpcError

            try:
                for batch in sequence:
                    worker.stream.exchange(batch)
            except RpcError as e:
                raise ClientError.from_rpc_error(e) from e
            raise AssertionError("the worker accepted out-of-turn input")

        monkeypatch.setattr(Client, "_process_batch_on_worker", send)
        with Client(fixture_worker) as client:
            list(
                client.table_in_out_function(
                    function_name="blended_paged",
                    schema_path=["main"],
                    input=iter([_real(3).batch]),
                )
            )

    def test_tick_without_pending_output_is_refused(self, fixture_worker: str, monkeypatch: pytest.MonkeyPatch) -> None:
        """A tick before any reply said HAVE_MORE_OUTPUT is a protocol error."""
        with pytest.raises(ClientError, match="no output is pending"):
            self._send(fixture_worker, monkeypatch, [_tick()])

    def test_tick_after_the_last_page_is_refused(self, fixture_worker: str, monkeypatch: pytest.MonkeyPatch) -> None:
        """3 rows in pages of 2: the second reply is the last, so a third tick is refused."""
        with pytest.raises(ClientError, match="no output is pending"):
            self._send(fixture_worker, monkeypatch, [_real(3), _tick(), _tick()])

    def test_input_while_output_is_pending_is_refused(
        self, fixture_worker: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Skipping the pending pages with new input would lose them: refused."""
        with pytest.raises(ClientError, match="while output is pending"):
            self._send(fixture_worker, monkeypatch, [_real(3), _real(1)])


@dataclass(slots=True, frozen=True, kw_only=True)
class _NArgs:
    n: Annotated[int, Arg(0)]


@dataclass(slots=True, frozen=True, kw_only=True)
class _TableArgs:
    data: Annotated[TableInput, Arg(0)]


@dataclass(kw_only=True)
class _CursorState(ArrowSerializableDataclass):
    row: int = 0


class TestResumeRegistration:
    """resume() continues from state alone, so it needs a serializable TState."""

    def test_resume_without_state_is_refused(self) -> None:
        """Without a TState the cursor would be lost between HTTP turns."""

        class _Stateless(RowTransformFunction[_NArgs]):
            class Meta:
                name = "stateless_resume"

            @classmethod
            def on_bind(cls, params: BindParams[_NArgs]) -> BindResponse:
                return BindResponse(output_schema=schema({"i": pa.int64()}))

            @classmethod
            def resume(cls, params: ProcessParams[_NArgs], state: None, out: OutputCollector) -> None:
                pass

        with pytest.raises(TypeError, match="serializable TState"):
            resolve_metadata(_Stateless)

    def test_resume_with_max_workers_1_is_refused(self) -> None:
        """max_workers = 1 runs on one shared connection, which cannot continue."""

        class _Serial(RowTransformFunction[_NArgs, _CursorState]):
            class Meta:
                name = "serial_resume"
                max_workers = 1

            @classmethod
            def on_bind(cls, params: BindParams[_NArgs]) -> BindResponse:
                return BindResponse(output_schema=schema({"i": pa.int64()}))

            @classmethod
            def resume(cls, params: ProcessParams[_NArgs], state: _CursorState, out: OutputCollector) -> None:
                pass

        with pytest.raises(TypeError, match="max_workers = 1"):
            resolve_metadata(_Serial)

    def test_resume_with_state_resolves(self) -> None:
        """RowTransformFunction[Args, State] is the supported shape."""

        class _Stateful(RowTransformFunction[_NArgs, _CursorState]):
            class Meta:
                name = "stateful_resume"

            @classmethod
            def on_bind(cls, params: BindParams[_NArgs]) -> BindResponse:
                return BindResponse(output_schema=schema({"i": pa.int64()}))

            @classmethod
            def resume(cls, params: ProcessParams[_NArgs], state: _CursorState, out: OutputCollector) -> None:
                pass

        assert resolve_metadata(_Stateful).input_from_args is True


class _RecordingCollector:
    def __init__(self) -> None:
        self.emits: list[dict[str, Any]] = []

    def emit(self, batch: pa.RecordBatch, **kwargs: Any) -> None:
        self.emits.append({"batch": batch, **kwargs})


class _MaskFilters:
    """Stands in for PushdownFilters: keeps the rows a fixed mask selects."""

    def __init__(self, mask: list[bool]) -> None:
        self._mask = pa.array(mask)

    def __bool__(self) -> bool:
        return True

    def evaluate(self, batch: pa.RecordBatch) -> pa.BooleanArray:
        return self._mask


class TestFilteredProvenance:
    """Auto-applied filters must drop the parent_rows of the rows they drop."""

    def test_parent_rows_follow_the_filter(self) -> None:
        """Filtering rows 1 and 3 drops their provenance; more= passes through."""
        inner = _RecordingCollector()
        out = _FilteringOutputCollector(inner, RowTransformFunction, _MaskFilters([True, False, True, False]))  # type: ignore[arg-type]
        out.emit(pa.record_batch({"i": [10, 11, 12, 13]}), parent_rows=[0, 0, 1, 2], more=True)
        [emit] = inner.emits
        assert emit["batch"].column("i").to_pylist() == [10, 12]
        assert emit["parent_rows"] == [0, 1]
        assert emit["more"] is True


class _FakeOut:
    """The innermost collector: records what reaches the wire."""

    def __init__(self) -> None:
        self.emits: list[tuple[pa.RecordBatch, dict[str, str] | None]] = []
        self.output_schema = pa.schema([pa.field("i", pa.int64())])
        self.finished = False

    def emit(self, batch: pa.RecordBatch, metadata: dict[str, str] | None = None) -> None:
        self.emits.append((batch, metadata))


def _rows(*values: int) -> pa.RecordBatch:
    return pa.record_batch({"i": pa.array(values, type=pa.int64())})


class TestCollectors:
    """The tracking collector's continuation rules."""

    def test_more_sets_have_more_output(self) -> None:
        """more=True is the reply's vgi.status, and the collector remembers it."""
        fake = _FakeOut()
        out = _TrackingOutputCollector(fake, allows_more=True)  # type: ignore[arg-type]
        out.emit(_rows(1), more=True)
        assert out.more is True
        assert fake.emits[0][1] == {"vgi.status": "HAVE_MORE_OUTPUT"}

    def test_more_is_refused_outside_a_table_in_out_exchange(self) -> None:
        """Only a table-in-out exchange is ever ticked, so more=True elsewhere is an error."""
        fake = _FakeOut()
        out = _TrackingOutputCollector(fake)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="only valid in a table-in-out"):
            out.emit(_rows(1), more=True)
        assert fake.emits == []

    def test_cache_control_is_refused_on_a_continuation(self) -> None:
        """A reply to a tick is never an input's first batch, so it can't carry cache_control."""
        out = _TrackingOutputCollector(_FakeOut(), continuation=True)  # type: ignore[arg-type]
        with pytest.raises(RuntimeError, match="only valid on the first emitted batch"):
            out.emit(_rows(1), cache_control=CacheControl(ttl=60))


class TestInputRowsPerCall:
    """Meta.input_rows_per_call: a positive int, on a RowTransformFunction only."""

    def test_declared_value_resolves_and_round_trips(self) -> None:
        """blended_sliced declares 3; it survives the Arrow and dict round-trips."""
        from vgi._test_fixtures.table_in_out import BlendedSlicedFunction
        from vgi.metadata import ResolvedMetadata, arrow_to_metadata, metadata_to_arrow

        m = resolve_metadata(BlendedSlicedFunction)
        assert m.input_rows_per_call == 3
        assert arrow_to_metadata(metadata_to_arrow(m)).input_rows_per_call == 3
        assert ResolvedMetadata.from_dict(m.to_dict()).input_rows_per_call == 3

    def test_default_is_a_whole_chunk(self) -> None:
        """Undeclared means no limit: the client sends a whole chunk per call."""
        from vgi._test_fixtures.table_in_out import BlendedPagedFunction

        assert resolve_metadata(BlendedPagedFunction).input_rows_per_call is None

    @pytest.mark.parametrize("value", [0, -1, True, 2.5, "3"])
    def test_non_positive_or_non_int_is_refused(self, value: object) -> None:
        """Only a positive int is a row count."""

        class _Bad(RowTransformFunction[_NArgs]):
            class Meta:
                name = "bad_rows_per_call"
                input_rows_per_call = value

            @classmethod
            def on_bind(cls, params: BindParams[_NArgs]) -> BindResponse:
                return BindResponse(output_schema=schema({"i": pa.int64()}))

        with pytest.raises(ValueError, match="must be a positive int"):
            resolve_metadata(_Bad)

    def test_refused_outside_a_row_transform_function(self) -> None:
        """A classic table-in-out function receives DuckDB's chunks as they are."""

        class _Classic(TableInOutGenerator[_TableArgs]):
            class Meta:
                name = "classic_rows_per_call"
                input_rows_per_call = 3

        with pytest.raises(TypeError, match="applies only to a RowTransformFunction"):
            resolve_metadata(_Classic)
