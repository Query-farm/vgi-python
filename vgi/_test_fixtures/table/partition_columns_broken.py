"""Deliberately-broken PartitionColumns fixtures for v2 contract testing.

Each fixture violates one specific clause of the PartitionColumns contract
documented at ``vgi/_test_fixtures/table/partition_columns.py`` /
``vgi/src/vgi_table_function_impl.cpp::InstallBatch``.

* :class:`BrokenMissingPartitionValuesFunction` — declares
  ``partition_kind = SINGLE_VALUE_PARTITIONS`` and an annotated bind-
  schema field, but bypasses the framework's wrapper validation by
  reaching the inner OutputCollector directly. The C++ extension's
  ``InstallBatch`` catches the missing ``vgi_partition_values#b64``
  metadata.

* :class:`BrokenPartitionMinNeqMaxFunction` — declares
  ``SINGLE_VALUE_PARTITIONS`` but emits a chunk whose partition
  column has multiple distinct values. The framework's auto-extract
  path would catch this client-side, so the fixture supplies an
  explicit ``partition_values={"col": (min, max)}`` with min != max
  to defeat the worker check and reach the C++ defense-in-depth
  validation in ``InstallBatch``. The C++ check is what guarantees
  this fires on release builds where DuckDB's own
  ``BatchedDataCollection::Append`` assertion is compiled out.

* :class:`BrokenPartitionValuesNoAnnotationFunction` — no
  ``vgi.partition_column`` annotation on any bind-schema field and
  ``partition_kind = NOT_PARTITIONED``, but the worker passes
  ``partition_values=`` on ``out.emit`` anyway. The framework
  rejects with RuntimeError at the emit site.

* :class:`BrokenPartitionColumnAbsentFromBatchFunction` — declares
  ``partition_kind`` and annotates a bind-schema field, but the
  worker emits a batch that DOES NOT include that column AND does
  not supply an explicit ``partition_values=`` override. The
  framework's ``_merge_partition_values`` raises RuntimeError at
  the emit site (auto-extract can't find the column).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import _cardinality_from_count
from vgi.arguments import Arg
from vgi.metadata import PartitionKind
from vgi.schema_utils import partition_field
from vgi.table_function import (
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
)


@dataclass(slots=True, frozen=True)
class _BrokenArgs:
    count: Annotated[int, Arg(0, doc="Rows to attempt to emit", ge=1)]


@dataclass(kw_only=True)
class _BrokenState(ArrowSerializableDataclass):
    emitted: bool = False


# =============================================================================
# 1. Missing partition_values metadata (C++ side raises)
# =============================================================================


@bind_fixed_schema
@_cardinality_from_count
class BrokenMissingPartitionValuesFunction(TableFunctionGenerator[_BrokenArgs, _BrokenState]):
    """Opt-in declared, but worker bypasses framework metadata merge."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            partition_field("country", pa.string()),
            pa.field("sales", pa.int64()),
        ]
    )

    class Meta:
        name = "broken_missing_partition_values"
        description = (
            "DELIBERATELY BROKEN: declares partition_kind + partition-annotated "
            "field but emits a data batch without vgi_partition_values#b64 "
            "metadata. C++ extension's contract check raises."
        )
        categories = ["testing", "broken"]
        partition_kind = PartitionKind.SINGLE_VALUE_PARTITIONS

    @classmethod
    def initial_state(cls, params: ProcessParams[_BrokenArgs]) -> _BrokenState:
        return _BrokenState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_BrokenArgs],
        state: _BrokenState,
        out: OutputCollector,
    ) -> None:
        if state.emitted:
            out.finish()
            return
        batch = pa.RecordBatch.from_pydict(
            {"country": ["US"] * params.args.count, "sales": list(range(params.args.count))},
            schema=cls.FIXED_SCHEMA,
        )
        # Reach into the wrapper stack and call the innermost inner
        # directly. This is what makes the fixture "broken": the
        # framework's _merge_partition_values validator never runs, so
        # the data batch has no vgi_partition_values#b64 metadata and
        # the C++ extension's InstallBatch contract check fires.
        # Same pattern as v1's broken_missing_batch_index_tag fixture.
        inner = out
        while hasattr(inner, "_inner"):
            inner = inner._inner
        inner.emit(batch)
        state.emitted = True


# =============================================================================
# 2. SINGLE_VALUE with min != max (C++ defense-in-depth raises)
# =============================================================================


@bind_fixed_schema
@_cardinality_from_count
class BrokenPartitionMinNeqMaxFunction(TableFunctionGenerator[_BrokenArgs, _BrokenState]):
    """SINGLE_VALUE_PARTITIONS but emit min != max via explicit override."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            partition_field("country", pa.string()),
            pa.field("sales", pa.int64()),
        ]
    )

    class Meta:
        name = "broken_partition_min_neq_max"
        description = (
            "DELIBERATELY BROKEN: declares SINGLE_VALUE_PARTITIONS but "
            "supplies an explicit partition_values override with "
            "min != max. The framework's wrapper validation doesn't "
            "compare min vs max for SINGLE_VALUE; the C++ extension's "
            "defense-in-depth check in InstallBatch raises."
        )
        categories = ["testing", "broken"]
        partition_kind = PartitionKind.SINGLE_VALUE_PARTITIONS

    @classmethod
    def initial_state(cls, params: ProcessParams[_BrokenArgs]) -> _BrokenState:
        return _BrokenState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_BrokenArgs],
        state: _BrokenState,
        out: OutputCollector,
    ) -> None:
        if state.emitted:
            out.finish()
            return
        # Single-valued country column at the data level (so the
        # framework's auto-extract WOULD pass), but the explicit
        # override forces min != max — defeats the framework check
        # and reaches C++ defense-in-depth.
        batch = pa.RecordBatch.from_pydict(
            {"country": ["US"] * params.args.count, "sales": list(range(params.args.count))},
            schema=cls.FIXED_SCHEMA,
        )
        out.emit(
            batch,
            partition_values={
                "country": (
                    pa.scalar("US", type=pa.string()),
                    pa.scalar("BR", type=pa.string()),  # max != min — bug
                ),
            },
        )
        state.emitted = True


# =============================================================================
# 3. partition_values kwarg without any annotated field (worker-side raise)
# =============================================================================


@bind_fixed_schema
@_cardinality_from_count
class BrokenPartitionValuesNoAnnotationFunction(TableFunctionGenerator[_BrokenArgs, _BrokenState]):
    """No partition annotation, but worker passes partition_values=."""

    # No partition_field() — bind schema has no partition columns.
    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("country", pa.string()),
            pa.field("sales", pa.int64()),
        ]
    )

    class Meta:
        name = "broken_partition_values_no_annotation"
        description = (
            "DELIBERATELY BROKEN: no field carries vgi.partition_column "
            "metadata (and partition_kind defaults to NOT_PARTITIONED), "
            "but the worker passes partition_values= on out.emit. The "
            "framework rejects with RuntimeError before the wire."
        )
        categories = ["testing", "broken"]
        # No partition_kind setting — defaults to NOT_PARTITIONED.

    @classmethod
    def initial_state(cls, params: ProcessParams[_BrokenArgs]) -> _BrokenState:
        return _BrokenState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_BrokenArgs],
        state: _BrokenState,
        out: OutputCollector,
    ) -> None:
        if state.emitted:
            out.finish()
            return
        batch = pa.RecordBatch.from_pydict(
            {"country": ["US"] * params.args.count, "sales": list(range(params.args.count))},
            schema=cls.FIXED_SCHEMA,
        )
        out.emit(
            batch,
            partition_values={
                "country": (
                    pa.scalar("US", type=pa.string()),
                    pa.scalar("US", type=pa.string()),
                ),
            },
        )
        state.emitted = True


# =============================================================================
# 4. Annotated column missing from batch, no explicit override (worker-side raise)
# =============================================================================


@bind_fixed_schema
@_cardinality_from_count
class BrokenPartitionColumnAbsentFromBatchFunction(TableFunctionGenerator[_BrokenArgs, _BrokenState]):
    """Annotated partition column not in emitted batch, no override."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            partition_field("category", pa.string()),
            pa.field("revenue", pa.int64()),
        ]
    )

    class Meta:
        name = "broken_partition_column_absent_from_batch"
        description = (
            "DELIBERATELY BROKEN: declares partition_kind on "
            "'category' but emits a batch without 'category' AND "
            "doesn't supply an explicit partition_values override. The "
            "framework's auto-extract fails with RuntimeError before "
            "the wire."
        )
        categories = ["testing", "broken"]
        partition_kind = PartitionKind.SINGLE_VALUE_PARTITIONS

    @classmethod
    def initial_state(cls, params: ProcessParams[_BrokenArgs]) -> _BrokenState:
        return _BrokenState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_BrokenArgs],
        state: _BrokenState,
        out: OutputCollector,
    ) -> None:
        if state.emitted:
            out.finish()
            return
        # Emit a batch WITHOUT 'category'. Framework's auto-extract
        # tries to read batch.column('category') and raises.
        batch_schema = pa.schema([pa.field("revenue", pa.int64())])
        batch = pa.RecordBatch.from_pydict(
            {"revenue": list(range(params.args.count))},
            schema=batch_schema,
        )
        out.emit(batch)
        state.emitted = True
