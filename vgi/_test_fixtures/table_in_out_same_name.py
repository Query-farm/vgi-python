# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Same-name-in-two-schemas *exchange-mode* fixtures.

The scalar analogue lives in :mod:`vgi._test_fixtures.scalar.same_name` and is
driven by ``scalar/same_name_schemas.test``. This module covers the two
exchange-mode shapes, which reach the worker through **different bind call
sites** in the DuckDB extension than scalars do:

* **table-in-out** (:class:`SameNameMainTransform` / :class:`SameNameDataTransform`)
  — ``VgiTableInOutBind`` builds its bind-time connection directly rather than
  going through ``AcquireAndBindConnection``.
* **table-buffering** (:class:`SameNameMainBuffered` / :class:`SameNameDataBuffered`)
  — shares that same bind site, but its *runtime* connections come from the
  buffering operator's own ``BuildAcquireParams``.

That distinction is the point. The extension originally threaded the owning
schema onto the runtime exchange connections but not onto the bind-time one, so
an exchange-mode call reached the worker with no ``BindRequest.schema_name`` and
could not be resolved when the same name was declared in two schemas. The scalar
fixture could not catch it — scalars bind through an entirely separate call site.

Each class registers under a name shared with its sibling, in the ``main`` and
``data`` schemas of the ``example`` catalog, and tags its rows with its own
schema, so a mis-routed bind reads as the wrong tag rather than a plausible
answer.
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table_in_out import SingleTableArguments
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_buffering_function import (
    TableBufferingFunction,
    TableBufferingParams,
)
from vgi.table_function import BindParams, ProcessParams
from vgi.table_in_out_function import TableInOutGenerator

# Deliberately shared across the two schemas — the collision is the point.
TRANSFORM_NAME = "test_same_name_transform"
BUFFERED_NAME = "test_same_name_buffered"

# The single output column every implementation here emits.
_OUTPUT_SCHEMA = pa.schema([pa.field("tag", pa.string())])


def _tags(schema_name: str, batch: pa.RecordBatch) -> pa.RecordBatch:
    """Render ``<schema_name>:<value>`` for every row of the first input column."""
    return pa.RecordBatch.from_pydict(
        {"tag": [None if v is None else f"{schema_name}:{v}" for v in batch.column(0).to_pylist()]},
        schema=_OUTPUT_SCHEMA,
    )


@dataclass(kw_only=True)
class _DrainState(ArrowSerializableDataclass):
    """Cursor over the buffered state_log; ``after_id`` starts before-first."""

    after_id: int = -1


# ---------------------------------------------------------------------------
# Table-in-out (streaming) pair
# ---------------------------------------------------------------------------


class _SameNameTransform(TableInOutGenerator[SingleTableArguments]):
    """Shared body; each subclass supplies the schema it is declared in."""

    #: Schema this implementation is registered into — the tag it stamps.
    OWNING_SCHEMA: str = ""

    @classmethod
    def on_bind(cls, params: BindParams[SingleTableArguments]) -> BindResponse:
        """Emit a single VARCHAR column regardless of the input schema."""
        return BindResponse(output_schema=_OUTPUT_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[SingleTableArguments],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        """Tag each input row with the owning schema."""
        out.emit(_tags(cls.OWNING_SCHEMA, batch))


class SameNameMainTransform(_SameNameTransform):
    """``test_same_name_transform`` as declared in the ``main`` schema."""

    OWNING_SCHEMA = "main"

    class Meta:
        """Function metadata."""

        name = TRANSFORM_NAME
        description = "Schema-disambiguation probe; the main-schema table-in-out"
        examples = [
            FunctionExample(
                sql=("SELECT * FROM example.main.test_same_name_transform((SELECT 1 AS n))"),
                description="Returns 'main:1'",
            ),
        ]


class SameNameDataTransform(_SameNameTransform):
    """``test_same_name_transform`` as declared in the ``data`` schema."""

    OWNING_SCHEMA = "data"

    class Meta:
        """Function metadata."""

        name = TRANSFORM_NAME
        description = "Schema-disambiguation probe; the data-schema table-in-out"
        examples = [
            FunctionExample(
                sql=("SELECT * FROM example.data.test_same_name_transform((SELECT 1 AS n))"),
                description="Returns 'data:1'",
            ),
        ]


# ---------------------------------------------------------------------------
# Table-buffering pair
# ---------------------------------------------------------------------------


class _SameNameBuffered(TableBufferingFunction[SingleTableArguments, _DrainState]):
    """Shared body; buffers tagged rows in Sink, drains them in Source."""

    #: Schema this implementation is registered into — the tag it stamps.
    OWNING_SCHEMA: str = ""

    @classmethod
    def on_bind(cls, params: BindParams[SingleTableArguments]) -> BindResponse:
        """Emit a single VARCHAR column regardless of the input schema."""
        return BindResponse(output_schema=_OUTPUT_SCHEMA)

    @classmethod
    def process(
        cls,
        batch: pa.RecordBatch,
        params: TableBufferingParams[SingleTableArguments],
    ) -> bytes:
        """Tag the batch in the Sink phase and buffer it for the Source phase.

        Tagging here (rather than in ``finalize``) is deliberate: it proves the
        SINK-side worker resolved the right implementation, which is a distinct
        connection from the one the Source phase acquires.
        """
        sink = pa.BufferOutputStream()
        tagged = _tags(cls.OWNING_SCHEMA, batch)
        with pa.ipc.new_stream(sink, tagged.schema) as writer:
            writer.write_batch(tagged)
        params.storage.state_append(b"buf", b"", sink.getvalue().to_pybytes())
        return params.execution_id

    @classmethod
    def combine(
        cls,
        state_ids: list[bytes],  # noqa: ARG003 - collapse to one finalize stream
        params: TableBufferingParams[SingleTableArguments],
    ) -> list[bytes]:
        """Collapse every Sink bucket into one finalize stream."""
        return [params.execution_id]

    @classmethod
    def initial_finalize_state(
        cls,
        finalize_state_id: bytes,  # noqa: ARG003 - one bucket per execution
        params: TableBufferingParams[SingleTableArguments],  # noqa: ARG003
    ) -> _DrainState:
        """Start the drain cursor before the first buffered batch."""
        return _DrainState(after_id=-1)

    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[SingleTableArguments],
        finalize_state_id: bytes,  # noqa: ARG003
        state: _DrainState,
        out: OutputCollector,
    ) -> None:
        """Emit one buffered batch per tick."""
        rows = params.storage.state_log_scan(
            b"buf",
            b"",
            after_id=state.after_id,
            limit=1,
        )
        if not rows:
            out.finish()
            return
        log_id, value = rows[0]
        out.emit(pa.ipc.open_stream(value).read_next_batch())
        state.after_id = log_id


class SameNameMainBuffered(_SameNameBuffered):
    """``test_same_name_buffered`` as declared in the ``main`` schema."""

    OWNING_SCHEMA = "main"

    class Meta:
        """Function metadata."""

        name = BUFFERED_NAME
        description = "Schema-disambiguation probe; the main-schema buffered function"
        examples = [
            FunctionExample(
                sql=("SELECT * FROM example.main.test_same_name_buffered((SELECT 1 AS n))"),
                description="Returns 'main:1'",
            ),
        ]


class SameNameDataBuffered(_SameNameBuffered):
    """``test_same_name_buffered`` as declared in the ``data`` schema."""

    OWNING_SCHEMA = "data"

    class Meta:
        """Function metadata."""

        name = BUFFERED_NAME
        description = "Schema-disambiguation probe; the data-schema buffered function"
        examples = [
            FunctionExample(
                sql=("SELECT * FROM example.data.test_same_name_buffered((SELECT 1 AS n))"),
                description="Returns 'data:1'",
            ),
        ]
