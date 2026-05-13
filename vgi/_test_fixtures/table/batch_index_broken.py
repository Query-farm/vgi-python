"""Deliberately-broken batch_index fixtures for contract-enforcement testing.

These fixtures violate the ``Meta.supports_batch_index = True`` contract in
three different ways so SQL integration tests can assert that the C++
extension's contract checks (in ``InstallBatch``) and the worker library's
``_merge_batch_index`` validator (in ``vgi/protocol.py``) raise typed
errors. None of these is intended for production use.

The shape of the contract is documented at
``vgi-python/vgi/_test_fixtures/table/batch_index.py`` and
``vgi/src/vgi_table_function_impl.cpp::InstallBatch``.

* ``broken_missing_batch_index_tag`` — emits a data batch with NO
  ``vgi_batch_index`` metadata, bypassing the framework wrapper's
  validation by reaching into the inner collector directly. The C++
  extension's ``InstallBatch`` raises IOException "without
  vgi_batch_index metadata" when the function opts in.

* ``broken_non_monotone_batch_index`` — emits batches with strictly
  decreasing partition_ids on the same stream. The C++ extension's
  ``InstallBatch`` raises IOException "decreased from N to M on the
  same stream" — DuckDB's per-thread monotonicity assertion is debug-
  only, so VGI must enforce in release builds.

* ``broken_batch_index_overflow`` — emits a partition_id at 2^60, well
  above DuckDB's ``BATCH_INCREMENT = 10^13`` per-pipeline cap. The
  C++ extension's ``InstallBatch`` raises IOException "exceeds
  DuckDB's per-pipeline cap" — without this, the worker would surface
  an opaque DuckDB InternalException from the pipeline executor.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi._test_fixtures.table._common import _cardinality_from_count
from vgi.arguments import Arg
from vgi.metadata import OrderPreservation
from vgi.schema_utils import schema
from vgi.table_function import (
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
)


@dataclass(slots=True, frozen=True)
class _BrokenArgs:
    count: Annotated[int, Arg(0, doc="Total rows to attempt to generate", ge=1)]


@dataclass(kw_only=True)
class _BrokenState(ArrowSerializableDataclass):
    emitted: bool = False


@bind_fixed_schema
@_cardinality_from_count
class MissingBatchIndexTagFunction(TableFunctionGenerator[_BrokenArgs, _BrokenState]):
    """Opts in to batch_index but emits without a tag. C++ raises."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())

    class Meta:
        name = "broken_missing_batch_index_tag"
        description = (
            "DELIBERATELY BROKEN: declares supports_batch_index=True but "
            "emits a data batch with no vgi_batch_index metadata. C++ "
            "extension's contract check raises."
        )
        categories = ["testing", "broken"]
        preserves_order = OrderPreservation.FIXED_ORDER
        supports_batch_index = True

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
            {"n": list(range(params.args.count))},
            schema=params.output_schema,
        )
        # Reach into the wrapper stack and call the innermost inner directly.
        # This is what makes this fixture "broken": the framework's
        # _merge_batch_index validator never runs, so a data batch with no
        # vgi_batch_index metadata reaches the C++ extension. The walk also
        # exercises the contract that the wire format (not the wrapper
        # layer) is the authoritative check — same defense the worker
        # library provides for stand-alone OutputCollector consumers.
        inner = out
        while hasattr(inner, "_inner"):
            inner = inner._inner
        inner.emit(batch)
        state.emitted = True


@bind_fixed_schema
@_cardinality_from_count
class NonMonotoneBatchIndexFunction(TableFunctionGenerator[_BrokenArgs, _BrokenState]):
    """Emits two batches with strictly decreasing partition_id. C++ raises."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())

    class Meta:
        name = "broken_non_monotone_batch_index"
        description = (
            "DELIBERATELY BROKEN: emits batches with strictly decreasing "
            "partition_id on one stream. C++ extension's monotonicity check "
            "raises (DuckDB's debug-only assertion is not relied upon)."
        )
        categories = ["testing", "broken"]
        preserves_order = OrderPreservation.FIXED_ORDER
        supports_batch_index = True

    @classmethod
    def initial_state(cls, params: ProcessParams[_BrokenArgs]) -> _BrokenState:
        return _BrokenState()

    # Reuse `emitted` to track which of the two batches we've sent.
    @classmethod
    def process(
        cls,
        params: ProcessParams[_BrokenArgs],
        state: _BrokenState,
        out: OutputCollector,
    ) -> None:
        if state.emitted:
            # Second call: emit with a LOWER batch_index than the first.
            batch = pa.RecordBatch.from_pydict(
                {"n": [42]},
                schema=params.output_schema,
            )
            out.emit(batch, batch_index=3)
            out.finish()
            return
        batch = pa.RecordBatch.from_pydict(
            {"n": list(range(params.args.count))},
            schema=params.output_schema,
        )
        out.emit(batch, batch_index=10)
        state.emitted = True


@bind_fixed_schema
@_cardinality_from_count
class BatchIndexOverflowFunction(TableFunctionGenerator[_BrokenArgs, _BrokenState]):
    """Emits a partition_id above the C++ cap. C++ raises."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(n=pa.int64())

    class Meta:
        name = "broken_batch_index_overflow"
        description = (
            "DELIBERATELY BROKEN: emits a batch tagged with a partition_id "
            "well above DuckDB's BATCH_INCREMENT=10^13 per-pipeline cap. "
            "C++ extension rejects at parse time."
        )
        categories = ["testing", "broken"]
        preserves_order = OrderPreservation.FIXED_ORDER
        supports_batch_index = True

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
            {"n": list(range(params.args.count))},
            schema=params.output_schema,
        )
        # 2^60 — far above the 10^13 cap.
        out.emit(batch, batch_index=1 << 60)
        state.emitted = True
