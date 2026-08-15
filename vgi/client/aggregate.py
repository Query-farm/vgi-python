# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Aggregate-function driver for the VGI [`Client`][].

The aggregate protocol is **all-unary** — there is no ``init`` stream and no
exchange loop. The DuckDB extension drives it from its hash-aggregate operator;
this module reproduces that drive loop for non-DuckDB callers and for
other-language porters reading the reference client.

Three surfaces, from lowest to highest level:

1. :meth:`AggregateClientMixin.aggregate_session` — a context manager over the
   raw RPCs (``aggregate_bind`` / ``update`` / ``combine`` / ``finalize`` /
   ``destructor``, plus the optional window RPCs). Group ids are yours to
   allocate, exactly as DuckDB allocates them from its state vector. This is
   the surface a port mirrors.
2. :meth:`AggregateClientMixin.aggregate_function` — the convenience driver.
   It hashes the ``group_by`` columns client-side, allocates one group id per
   distinct key in first-seen order (DuckDB's order), pumps every input batch
   through ``update``, then finalizes in chunks and returns one
   ``RecordBatch`` of group keys + aggregate results.
3. :meth:`AggregateClientMixin.aggregate_streaming` — the optional
   streaming-partitioned protocol (``open`` / ``chunk`` / ``close``) used by
   aggregates whose state compresses heavily relative to their input.

Wire shapes mirrored from the C++ extension (``vgi_aggregate_function_impl.cpp``):

* ``aggregate_update`` ships ``__vgi_group_id`` (int64) as the **first** column,
  followed by the aggregate's value columns in declaration order.
* ``aggregate_combine`` ships a two-column
  ``(source_group_id, target_group_id)`` batch.
* ``aggregate_finalize`` / ``aggregate_destructor`` ship a one-column
  ``group_id`` batch.

See Also:
--------
    vgi.protocol.VgiProtocol      — the RPC surface this driver exercises
    vgi.aggregate_function        — the worker-side base class

"""

from __future__ import annotations

import logging
import struct
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from vgi_rpc.rpc import RpcError

from vgi.aggregate_function import GROUP_COLUMN_NAME
from vgi.arguments import Arguments
from vgi.client.errors import ClientError
from vgi.protocol import (
    AggregateBindRequest,
    AggregateCombineRequest,
    AggregateDestructorRequest,
    AggregateFinalizeRequest,
    AggregateStreamingChunkRequest,
    AggregateStreamingCloseRequest,
    AggregateStreamingOpenRequest,
    AggregateUpdateRequest,
    AggregateWindowBatchRequest,
    AggregateWindowDestructorRequest,
    AggregateWindowInitRequest,
    AggregateWindowRequest,
    VgiProtocol,
)

__all__ = [
    "AggregateClientMixin",
    "AggregateSession",
    "AggregateStreamingSession",
]

_logger = logging.getLogger("vgi.client.aggregate")

#: DuckDB's vector size — how many group ids one ``aggregate_finalize`` covers.
DEFAULT_FINALIZE_CHUNK_SIZE = 2048

_MERGE_SCHEMA = pa.schema(
    [
        pa.field("source_group_id", pa.int64(), nullable=False),
        pa.field("target_group_id", pa.int64(), nullable=False),
    ]
)
_GROUP_IDS_SCHEMA = pa.schema([pa.field("group_id", pa.int64(), nullable=False)])

# Frames = [(begin, end), ...]; one pair per subframe of one output row.
type Frames = Sequence[tuple[int, int]]


def _ipc_bytes(batch: pa.RecordBatch) -> bytes:
    """Serialize one `RecordBatch` as a complete IPC stream (schema + data + EOS)."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _read_ipc(data: bytes) -> pa.RecordBatch:
    """Read the first `RecordBatch` out of IPC stream bytes."""
    return pa.ipc.open_stream(data).read_next_batch()


def _int64_array(values: Sequence[int] | pa.Array[Any]) -> pa.Array[Any]:
    """Coerce a group-id sequence to an int64 array."""
    if isinstance(values, pa.Array):
        return values.cast(pa.int64())
    return pa.array(list(values), type=pa.int64())


def pack_filter_mask(mask: Sequence[bool] | pa.BooleanArray | None) -> bytes:
    """Pack a per-row FILTER mask into Arrow's LSB-first validity bitmap.

    ``b""`` means "no ``FILTER (WHERE ...)`` clause" — the worker decodes that
    as all-true, so a caller with no filter should pass ``None``.
    """
    if mask is None:
        return b""
    arr = mask if isinstance(mask, pa.Array) else pa.array(list(mask), type=pa.bool_())
    if arr.null_count:
        raise ValueError("filter mask must not contain nulls")
    buf = arr.buffers()[1]
    return b"" if buf is None else buf.to_pybytes()


def pack_frame_stats(stats: tuple[tuple[int, int], tuple[int, int]] | None) -> bytes:
    """Pack DuckDB's per-partition frame statistics as 4× little-endian int64."""
    if stats is None:
        return b""
    (b0, e0), (b1, e1) = stats
    return struct.pack("<qqqq", b0, e0, b1, e1)


def pack_all_valid(all_valid: Sequence[bool] | None) -> bytes:
    """Pack the per-input-column "column has no nulls" flags, one byte each."""
    if all_valid is None:
        return b""
    return bytes(1 if v else 0 for v in all_valid)


@dataclass(slots=True)
class AggregateSession:
    """A bound aggregate execution — the raw RPC surface, one method per call.

    Obtained from :meth:`AggregateClientMixin.aggregate_session`. Group ids are
    caller-allocated int64s; the worker keys its per-group state on them and
    never invents one. Reuse the same id across ``update`` calls to accumulate
    into one group, exactly as DuckDB reuses the id stamped on a state pointer.

    Attributes:
        execution_id: Worker-minted identifier for this aggregate execution.
            Scopes every piece of worker-side state, including window partitions.
        output_schema: Schema the aggregate's ``finalize`` produces (typically a
            single ``result`` column).
    """

    execution_id: bytes
    output_schema: pa.Schema
    _client: AggregateClientMixin
    _function_name: str
    _schema_name: str | None
    _attach_opaque_data: bytes | None

    # ------------------------------------------------------------------
    # Core aggregate RPCs
    # ------------------------------------------------------------------

    def update(
        self,
        *,
        group_ids: Sequence[int] | pa.Array[Any],
        batch: pa.RecordBatch | None = None,
    ) -> None:
        """Accumulate one chunk of rows into per-group state.

        Args:
            group_ids: One group id per row, parallel to ``batch``'s rows.
            batch: The aggregate's value columns for those rows, in declaration
                order. ``None`` for a nullary aggregate (``vgi_count()``), where
                the row count is carried by ``group_ids`` alone.

        Raises:
            ValueError: If ``batch`` and ``group_ids`` disagree on row count.
            [`ClientError`][]: If the RPC fails.

        """
        gids = _int64_array(group_ids)
        if batch is not None and batch.num_rows != len(gids):
            raise ValueError(f"group_ids has {len(gids)} entries but batch has {batch.num_rows} rows")

        fields: list[pa.Field[Any]] = [pa.field(GROUP_COLUMN_NAME, pa.int64(), nullable=False)]
        columns: list[pa.Array[Any]] = [gids]
        if batch is not None:
            fields.extend(batch.schema)
            columns.extend(batch.columns)
        full = pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))

        self._call(
            "aggregate_update",
            lambda proxy: proxy.aggregate_update(
                request=AggregateUpdateRequest(
                    function_name=self._function_name,
                    execution_id=self.execution_id,
                    input_batch=_ipc_bytes(full),
                    attach_opaque_data=self._attach_opaque_data,
                    schema_name=self._schema_name,
                )
            ),
        )

    def combine(
        self,
        *,
        source_group_ids: Sequence[int] | pa.Array[Any],
        target_group_ids: Sequence[int] | pa.Array[Any],
    ) -> None:
        """Merge each source group's state into the paired target group.

        Mirrors DuckDB's combine step, where thread-local hash tables are merged
        into the global one.

        Args:
            source_group_ids: Groups whose state is merged *from*. Left as-is.
            target_group_ids: Groups merged *into*, parallel to
                ``source_group_ids`` and the same length.

        Raises:
            ValueError: If the two sequences differ in length.
            [`ClientError`][]: If the RPC fails.

        """
        src = _int64_array(source_group_ids)
        tgt = _int64_array(target_group_ids)
        if len(src) != len(tgt):
            raise ValueError(f"source_group_ids has {len(src)} entries but target_group_ids has {len(tgt)}")
        merge = pa.RecordBatch.from_arrays([src, tgt], schema=_MERGE_SCHEMA)

        self._call(
            "aggregate_combine",
            lambda proxy: proxy.aggregate_combine(
                request=AggregateCombineRequest(
                    function_name=self._function_name,
                    execution_id=self.execution_id,
                    merge_batch=_ipc_bytes(merge),
                    attach_opaque_data=self._attach_opaque_data,
                    schema_name=self._schema_name,
                )
            ),
        )

    def finalize(self, group_ids: Sequence[int] | pa.Array[Any]) -> pa.RecordBatch:
        """Produce the result row for each of ``group_ids``, in that order.

        A group id that was never updated finalizes to whatever the function
        returns for an absent state — ``NULL`` for SUM/AVG, ``0`` for COUNT.

        Args:
            group_ids: The groups to produce results for, in output order.

        Returns:
            A `RecordBatch` with :attr:`output_schema` and one row per group id.

        Raises:
            [`ClientError`][]: If the RPC fails.

        """
        gids = _int64_array(group_ids)
        batch = pa.RecordBatch.from_arrays([gids], schema=_GROUP_IDS_SCHEMA)
        response = self._call(
            "aggregate_finalize",
            lambda proxy: proxy.aggregate_finalize(
                request=AggregateFinalizeRequest(
                    function_name=self._function_name,
                    execution_id=self.execution_id,
                    group_ids_batch=_ipc_bytes(batch),
                    output_schema=self.output_schema,
                    attach_opaque_data=self._attach_opaque_data,
                    schema_name=self._schema_name,
                )
            ),
        )
        return _read_ipc(response.result_batch)

    def destroy(self) -> None:
        """Release every piece of worker state for this execution (best-effort).

        Called for you when :meth:`AggregateClientMixin.aggregate_session` exits.
        Like the C++ destructor it never raises: a failure here means leaked
        worker rows, not a wrong answer, and the worker reclaims them on
        execution timeout anyway.
        """
        # The group_ids batch is vestigial on this call — the worker clears the
        # whole execution_id — but the wire field is required, so send one row
        # exactly as the C++ destructor does.
        batch = pa.RecordBatch.from_arrays([pa.array([0], type=pa.int64())], schema=_GROUP_IDS_SCHEMA)
        try:
            self._call(
                "aggregate_destructor",
                lambda proxy: proxy.aggregate_destructor(
                    request=AggregateDestructorRequest(
                        function_name=self._function_name,
                        execution_id=self.execution_id,
                        group_ids_batch=_ipc_bytes(batch),
                        attach_opaque_data=self._attach_opaque_data,
                        schema_name=self._schema_name,
                    )
                ),
            )
        except ClientError:
            _logger.debug("aggregate_destructor failed (ignored)", exc_info=True)

    # ------------------------------------------------------------------
    # Optional window RPCs
    # ------------------------------------------------------------------

    def window_init(
        self,
        *,
        partition_id: int,
        partition: pa.RecordBatch,
        filter_mask: Sequence[bool] | pa.BooleanArray | None = None,
        frame_stats: tuple[tuple[int, int], tuple[int, int]] | None = None,
        all_valid: Sequence[bool] | None = None,
    ) -> None:
        """Ship one window partition to the worker so it can be queried by frame.

        Args:
            partition_id: Caller-allocated id for this partition, scoped to the
                session's ``execution_id``.
            partition: Every input column, every row of the partition, in
                window order.
            filter_mask: Per-row mask from a ``FILTER (WHERE ...)`` clause.
                ``None`` (the default) means no filter.
            frame_stats: DuckDB's per-partition frame bounds, as
                ``((begin_delta, end_delta), (begin_delta, end_delta))``.
                ``None`` sends zeros, which is what a caller with no frame
                statistics should do.
            all_valid: One flag per input column, True when the column has no
                nulls. ``None`` means "assume all valid".

        Raises:
            [`ClientError`][]: If the RPC fails.

        """
        self._call(
            "aggregate_window_init",
            lambda proxy: proxy.aggregate_window_init(
                request=AggregateWindowInitRequest(
                    function_name=self._function_name,
                    execution_id=self.execution_id,
                    partition_id=partition_id,
                    row_count=partition.num_rows,
                    partition_batch=_ipc_bytes(partition),
                    output_schema=self.output_schema,
                    filter_mask=pack_filter_mask(filter_mask),
                    frame_stats=pack_frame_stats(frame_stats),
                    all_valid=pack_all_valid(all_valid),
                    attach_opaque_data=self._attach_opaque_data,
                    schema_name=self._schema_name,
                )
            ),
        )

    def window(self, *, partition_id: int, rid: int, frames: Frames) -> pa.RecordBatch:
        """Compute the aggregate for one output row of a window partition.

        Args:
            partition_id: The partition previously shipped by :meth:`window_init`.
            rid: Row index within the partition of the output row being computed.
            frames: The row's subframes as ``(begin, end)`` half-open offsets
                into the partition. One entry normally; two or three for
                ``EXCLUDE TIES`` / ``EXCLUDE GROUP``.

        Returns:
            A one-row `RecordBatch` with :attr:`output_schema`.

        Raises:
            [`ClientError`][]: If the RPC fails.

        """
        response = self._call(
            "aggregate_window",
            lambda proxy: proxy.aggregate_window(
                request=AggregateWindowRequest(
                    function_name=self._function_name,
                    execution_id=self.execution_id,
                    partition_id=partition_id,
                    rid=rid,
                    frame_starts=[begin for begin, _ in frames],
                    frame_ends=[end for _, end in frames],
                    attach_opaque_data=self._attach_opaque_data,
                    schema_name=self._schema_name,
                )
            ),
        )
        return _read_ipc(response.result_batch)

    def window_batch(self, *, partition_id: int, row_idx: int, frames: Sequence[Frames]) -> pa.RecordBatch:
        """Compute ``len(frames)`` consecutive window output rows in one RPC.

        Args:
            partition_id: The partition previously shipped by :meth:`window_init`.
            row_idx: Partition-relative index of the first output row.
            frames: One subframe list per output row, in row order.

        Returns:
            A `RecordBatch` with :attr:`output_schema` and ``len(frames)`` rows.

        Raises:
            [`ClientError`][]: If the RPC fails.

        """
        flat_starts: list[int] = []
        flat_ends: list[int] = []
        for row_frames in frames:
            for begin, end in row_frames:
                flat_starts.append(begin)
                flat_ends.append(end)
        response = self._call(
            "aggregate_window_batch",
            lambda proxy: proxy.aggregate_window_batch(
                request=AggregateWindowBatchRequest(
                    function_name=self._function_name,
                    execution_id=self.execution_id,
                    partition_id=partition_id,
                    row_idx=row_idx,
                    count=len(frames),
                    frames_per_row=[len(f) for f in frames],
                    frame_starts=flat_starts,
                    frame_ends=flat_ends,
                    attach_opaque_data=self._attach_opaque_data,
                    schema_name=self._schema_name,
                )
            ),
        )
        return _read_ipc(response.result_batch)

    def window_destroy(self, partition_id: int) -> None:
        """Evict one window partition from worker storage (best-effort)."""
        try:
            self._call(
                "aggregate_window_destructor",
                lambda proxy: proxy.aggregate_window_destructor(
                    request=AggregateWindowDestructorRequest(
                        function_name=self._function_name,
                        execution_id=self.execution_id,
                        partition_id=partition_id,
                        attach_opaque_data=self._attach_opaque_data,
                        schema_name=self._schema_name,
                    )
                ),
            )
        except ClientError:
            _logger.debug("aggregate_window_destructor failed (ignored)", exc_info=True)

    # ------------------------------------------------------------------

    def _call(self, name: str, fn: Any) -> Any:
        return self._client._aggregate_rpc(name, fn)


@dataclass(slots=True)
class AggregateStreamingSession:
    """An open streaming-partitioned aggregate session.

    Obtained from :meth:`AggregateClientMixin.aggregate_streaming`. Each
    :meth:`chunk` returns one output row per input row — the aggregate's value
    at that row's position within its partition.

    Attributes:
        execution_id: Worker-minted identifier for this streaming session.
        output_schema: Schema of every batch :meth:`chunk` returns.
    """

    execution_id: bytes
    output_schema: pa.Schema
    _client: AggregateClientMixin
    _function_name: str
    _schema_name: str | None
    _attach_opaque_data: bytes | None

    def chunk(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Process one input chunk and return its per-row output.

        Args:
            batch: One chunk, matching the ``input_schema`` agreed at open
                time: partition-key columns first, then order-key columns,
                then the aggregate's value columns.

        Returns:
            A `RecordBatch` with :attr:`output_schema` and one row per input row.

        Raises:
            [`ClientError`][]: If the RPC fails.

        """
        response = self._client._aggregate_rpc(
            "aggregate_streaming_chunk",
            lambda proxy: proxy.aggregate_streaming_chunk(
                request=AggregateStreamingChunkRequest(
                    function_name=self._function_name,
                    execution_id=self.execution_id,
                    input_batch=_ipc_bytes(batch),
                    attach_opaque_data=self._attach_opaque_data,
                    schema_name=self._schema_name,
                )
            ),
        )
        return _read_ipc(response.result_batch)

    def close(self) -> None:
        """End the session and free its worker-side state (best-effort)."""
        try:
            self._client._aggregate_rpc(
                "aggregate_streaming_close",
                lambda proxy: proxy.aggregate_streaming_close(
                    request=AggregateStreamingCloseRequest(
                        function_name=self._function_name,
                        execution_id=self.execution_id,
                        attach_opaque_data=self._attach_opaque_data,
                        schema_name=self._schema_name,
                    )
                ),
            )
        except ClientError:
            _logger.debug("aggregate_streaming_close failed (ignored)", exc_info=True)


class AggregateClientMixin:
    """Mixin adding aggregate-function invocation to the VGI [`Client`][].

    Every aggregate RPC is unary and runs on the client's primary worker
    connection, so ``start()`` (or the context-manager protocol) must have run
    first — same requirement as ``scalar_function`` / ``table_function``.
    """

    # --- Provided by Client ------------------------------------------------
    _primary: Any
    _attach_opaque_data: bytes | None

    @staticmethod
    def _settings_to_batch(settings: dict[str, Any] | None) -> pa.RecordBatch | None:  # implemented by Client
        raise NotImplementedError

    @staticmethod
    def _secrets_to_batch(secrets: dict[str, Any] | None) -> pa.RecordBatch | None:  # implemented by Client
        raise NotImplementedError

    def _client_error_with_stderr(self, error: ClientError) -> ClientError:  # implemented by Client
        raise NotImplementedError

    # -----------------------------------------------------------------------

    def _aggregate_proxy(self) -> VgiProtocol:
        """The primary worker's typed proxy, or a clear error if not started."""
        if self._primary is None:
            raise ClientError("Client not started. Call start() or use context manager.")
        return self._primary.proxy  # type: ignore[no-any-return]

    def _aggregate_rpc(self, name: str, fn: Any) -> Any:
        """Run one unary aggregate RPC, translating transport errors."""
        proxy = self._aggregate_proxy()
        try:
            return fn(proxy)
        except RpcError as e:
            raise self._client_error_with_stderr(ClientError.from_rpc_error(e)) from e

    # ==================================================================
    # Low-level session
    # ==================================================================

    @contextmanager
    def aggregate_session(
        self,
        *,
        function_name: str,
        schema_name: str,
        input_schema: pa.Schema | None = None,
        arguments: Arguments | None = None,
        settings: dict[str, Any] | None = None,
        secrets: dict[str, Any] | None = None,
    ) -> Iterator[AggregateSession]:
        """Bind an aggregate and yield a session over its raw RPCs.

        The session is destroyed on exit, including on exception, so worker-side
        group state never outlives the ``with`` block.

        Args:
            function_name: Name of the aggregate to bind.
            schema_name: Catalog schema that declares the function. A name is
                unique only within a schema, so this is what identifies the
                implementation.
            input_schema: Schema of the aggregate's value columns, in
                declaration order — **without** the ``__vgi_group_id`` column,
                which the driver prepends per call. ``None`` for a nullary
                aggregate.
            arguments: Constant arguments (``ConstParam`` values) for the
                aggregate. Defaults to empty.
            settings: Optional DuckDB-style settings visible to the function.
            secrets: Optional pre-resolved secret values. Two-phase secret
                resolution is not available on the aggregate path — the worker
                rejects a bind that requests scoped secrets.

        Yields:
            An [`AggregateSession`][] bound to a fresh ``execution_id``.

        Raises:
            [`ClientError`][]: If the client is not started or the bind fails.

        """
        session = self.aggregate_bind(
            function_name=function_name,
            schema_name=schema_name,
            input_schema=input_schema,
            arguments=arguments,
            settings=settings,
            secrets=secrets,
        )
        try:
            yield session
        finally:
            session.destroy()

    def aggregate_bind(
        self,
        *,
        function_name: str,
        schema_name: str,
        input_schema: pa.Schema | None = None,
        arguments: Arguments | None = None,
        settings: dict[str, Any] | None = None,
        secrets: dict[str, Any] | None = None,
    ) -> AggregateSession:
        """Bind an aggregate function without taking responsibility for teardown.

        Prefer :meth:`aggregate_session`, which destroys the session for you.
        This entry point exists for callers that must own the lifetime
        explicitly (e.g. a proxy that hands the ``execution_id`` to another
        process); they must call :meth:`AggregateSession.destroy` themselves.

        Args:
            function_name: Name of the aggregate to bind.
            schema_name: Catalog schema that declares the function.
            input_schema: Schema of the aggregate's value columns, in
                declaration order, without the ``__vgi_group_id`` column.
                ``None`` for a nullary aggregate.
            arguments: Constant arguments for the aggregate. Defaults to empty.
            settings: Optional DuckDB-style settings visible to the function.
            secrets: Optional pre-resolved secret values.

        Returns:
            An [`AggregateSession`][] carrying the worker's ``execution_id``
            and resolved output schema.

        Raises:
            [`ClientError`][]: If the client is not started or the bind fails.

        """
        response = self._aggregate_rpc(
            "aggregate_bind",
            lambda proxy: proxy.aggregate_bind(
                request=AggregateBindRequest(
                    function_name=function_name,
                    arguments=arguments if arguments is not None else Arguments(),
                    input_schema=input_schema,
                    settings=self._settings_to_batch(settings),
                    secrets=self._secrets_to_batch(secrets),
                    attach_opaque_data=self._attach_opaque_data,
                    schema_name=schema_name,
                )
            ),
        )
        return AggregateSession(
            execution_id=response.execution_id,
            output_schema=response.output_schema,
            _client=self,
            _function_name=function_name,
            _schema_name=schema_name,
            _attach_opaque_data=self._attach_opaque_data,
        )

    # ==================================================================
    # High-level driver
    # ==================================================================

    def aggregate_function(
        self,
        *,
        function_name: str,
        schema_name: str,
        input: Iterable[pa.RecordBatch] = (),
        group_by: Sequence[str] = (),
        arguments: Arguments | None = None,
        settings: dict[str, Any] | None = None,
        secrets: dict[str, Any] | None = None,
        input_schema: pa.Schema | None = None,
        finalize_chunk_size: int = DEFAULT_FINALIZE_CHUNK_SIZE,
    ) -> pa.RecordBatch:
        """Run an aggregate over ``input`` and return one row per group.

        This is the ``SELECT <group_by>, agg(...) FROM t GROUP BY <group_by>``
        shape, driven client-side: group ids are allocated per distinct
        ``group_by`` key in first-seen order (DuckDB's hash-aggregate order),
        every batch is pumped through ``aggregate_update``, and the groups are
        finalized in chunks of ``finalize_chunk_size``.

        Every input column that is not named in ``group_by`` is passed to the
        aggregate as a value column, in the order it appears in the batch — so
        order your input columns to match the aggregate's declared parameters.

        Args:
            function_name: Name of the aggregate to invoke.
            schema_name: Catalog schema that declares the function.
            input: Input batches. All must share one schema. May be empty.
            group_by: Column names to group on. Empty (the default) means a
                global aggregate, which returns exactly one row even for empty
                input — matching SQL's ``SELECT agg(x) FROM empty_table``.
            arguments: Constant arguments for the aggregate. Defaults to empty.
            settings: Optional settings visible to the function.
            secrets: Optional pre-resolved secret values.
            input_schema: Value-column schema to bind with when ``input`` yields
                no batches. Ignored once a first batch is seen. Supply it when
                binding a varargs aggregate over empty input, which otherwise
                fails its bind-time arity check.
            finalize_chunk_size: Group ids per ``aggregate_finalize`` call.

        Returns:
            A `RecordBatch` of the ``group_by`` columns followed by the
            aggregate's output columns, one row per group, in group-id order.

        Raises:
            ValueError: If a ``group_by`` column is missing from the input, or
                the input batches disagree on schema.
            [`ClientError`][]: If the client is not started or an RPC fails.

        """
        batches = iter(input)
        first = next(batches, None)

        if first is not None:
            missing = [name for name in group_by if name not in first.schema.names]
            if missing:
                raise ValueError(f"group_by columns not present in input: {missing}")
            value_names = [name for name in first.schema.names if name not in set(group_by)]
            bind_schema: pa.Schema | None = pa.schema([first.schema.field(name) for name in value_names])
        else:
            value_names = []
            bind_schema = input_schema

        # Group key tuple -> group id, in first-seen (DuckDB) order.
        group_ids: dict[tuple[Any, ...], int] = {}
        group_keys: list[tuple[Any, ...]] = []
        if not group_by:
            group_ids[()] = 0
            group_keys.append(())

        key_types: list[pa.DataType] = [first.schema.field(name).type for name in group_by] if first is not None else []

        with self.aggregate_session(
            function_name=function_name,
            schema_name=schema_name,
            input_schema=bind_schema,
            arguments=arguments,
            settings=settings,
            secrets=secrets,
        ) as session:
            batch = first
            while batch is not None:
                if batch.num_rows:
                    if list(batch.schema.names) != (list(first.schema.names) if first is not None else []):
                        raise ValueError("every input batch must share one schema")
                    row_gids = self._assign_group_ids(batch, group_by, group_ids, group_keys)
                    values = batch.select(value_names) if value_names else None
                    session.update(group_ids=row_gids, batch=values)
                batch = next(batches, None)

            if not group_keys:
                results = [
                    pa.RecordBatch.from_arrays(
                        [pa.array([], type=field.type) for field in session.output_schema],
                        schema=session.output_schema,
                    )
                ]
            else:
                results = [
                    session.finalize(range(start, min(start + finalize_chunk_size, len(group_keys))))
                    for start in range(0, len(group_keys), finalize_chunk_size)
                ]

        return _assemble_grouped_result(group_by, key_types, group_keys, results, session.output_schema)

    @staticmethod
    def _assign_group_ids(
        batch: pa.RecordBatch,
        group_by: Sequence[str],
        group_ids: dict[tuple[Any, ...], int],
        group_keys: list[tuple[Any, ...]],
    ) -> list[int]:
        """Map each row of ``batch`` to a group id, minting ids for new keys."""
        if not group_by:
            return [0] * batch.num_rows
        key_columns = [batch.column(name).to_pylist() for name in group_by]
        out: list[int] = []
        for row in range(batch.num_rows):
            key = tuple(column[row] for column in key_columns)
            gid = group_ids.get(key)
            if gid is None:
                gid = len(group_keys)
                group_ids[key] = gid
                group_keys.append(key)
            out.append(gid)
        return out

    # ==================================================================
    # Streaming-partitioned aggregates
    # ==================================================================

    @contextmanager
    def aggregate_streaming(
        self,
        *,
        function_name: str,
        schema_name: str,
        input_schema: pa.Schema,
        partition_key_count: int,
        order_key_count: int = 0,
        arguments: Arguments | None = None,
        settings: dict[str, Any] | None = None,
        secrets: dict[str, Any] | None = None,
        output_schema: pa.Schema | None = None,
    ) -> Iterator[AggregateStreamingSession]:
        """Open a streaming-partitioned aggregate session and yield it.

        The session is closed on exit, including on exception.

        Args:
            function_name: Name of the aggregate to open.
            schema_name: Catalog schema that declares the function.
            input_schema: Schema of every chunk. Column order is fixed by the
                protocol: ``partition_key_count`` partition-key columns first,
                then ``order_key_count`` order-key columns, then the
                aggregate's value columns.
            partition_key_count: How many leading columns are partition keys.
            order_key_count: How many columns after the partition keys are
                order keys. Informational to the worker.
            arguments: Constant arguments for the aggregate. Defaults to empty.
            settings: Optional settings visible to the function.
            secrets: Optional pre-resolved secret values.
            output_schema: The aggregate's output schema. Resolved with a
                throwaway ``aggregate_bind`` over the value columns when
                omitted, which is what the DuckDB extension does.

        Yields:
            An open [`AggregateStreamingSession`][].

        Raises:
            ValueError: If the key counts exceed ``input_schema``'s width.
            [`ClientError`][]: If the client is not started or an RPC fails.

        """
        value_start = partition_key_count + order_key_count
        if value_start > len(input_schema):
            raise ValueError(
                f"partition_key_count + order_key_count ({value_start}) exceeds "
                f"input_schema width ({len(input_schema)})"
            )

        if output_schema is None:
            value_schema = pa.schema(list(input_schema)[value_start:])
            probe = self.aggregate_bind(
                function_name=function_name,
                schema_name=schema_name,
                input_schema=value_schema,
                arguments=arguments,
                settings=settings,
                secrets=secrets,
            )
            output_schema = probe.output_schema
            probe.destroy()

        response = self._aggregate_rpc(
            "aggregate_streaming_open",
            lambda proxy: proxy.aggregate_streaming_open(
                request=AggregateStreamingOpenRequest(
                    function_name=function_name,
                    arguments=arguments if arguments is not None else Arguments(),
                    input_schema=input_schema,
                    partition_key_count=partition_key_count,
                    order_key_count=order_key_count,
                    output_schema=output_schema,
                    settings=self._settings_to_batch(settings),
                    secrets=self._secrets_to_batch(secrets),
                    attach_opaque_data=self._attach_opaque_data,
                    schema_name=schema_name,
                )
            ),
        )
        session = AggregateStreamingSession(
            execution_id=response.execution_id,
            output_schema=output_schema,
            _client=self,
            _function_name=function_name,
            _schema_name=schema_name,
            _attach_opaque_data=self._attach_opaque_data,
        )
        try:
            yield session
        finally:
            session.close()


def _assemble_grouped_result(
    group_by: Sequence[str],
    key_types: Sequence[pa.DataType],
    group_keys: Sequence[tuple[Any, ...]],
    results: Sequence[pa.RecordBatch],
    output_schema: pa.Schema,
) -> pa.RecordBatch:
    """Stitch the finalize chunks back together beside their group-key columns."""
    result_columns: list[pa.Array[Any]] = [
        pa.concat_arrays([batch.column(index).cast(output_schema.field(index).type) for batch in results])
        for index in range(len(output_schema))
    ]

    fields: list[pa.Field[Any]] = []
    columns: list[pa.Array[Any]] = []
    for position, name in enumerate(group_by):
        key_type = key_types[position] if position < len(key_types) else pa.null()
        fields.append(pa.field(name, key_type))
        columns.append(pa.array([key[position] for key in group_keys], type=key_type))
    fields.extend(output_schema)
    columns.extend(result_columns)

    return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))
