"""Demo of transaction-scoped storage (``BindParams.transaction_storage``).

Backs the ``example.main.tx_cached_value(key, seed)`` function exposed by
``vgi-fixture-worker``. The function uses ``BindParams.transaction_storage``
to cache its ``seed`` argument per ``(transaction_id, key)``:

* First call within a transaction for a given ``key``: stores ``seed`` and
  emits it.
* Subsequent calls within the **same** transaction for the **same** ``key``:
  emit the originally-cached value and **ignore** the new ``seed``.
* New transaction or different ``key``: produces a fresh cached value.
* Without a transaction (``params.transaction_storage is None``): no caching;
  every call emits its own ``seed``.

The resolved value is shipped from ``on_bind`` to ``process`` via
``BindResponse.opaque_data`` so any worker in the pool can produce the same
answer — the value lives in shared storage (sqlite/CF DO/Azure SQL), not in
the bind worker's local memory.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Annotated, ClassVar

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.arguments import Arg
from vgi.invocation import BindResponse, GlobalInitResponse
from vgi.schema_utils import schema
from vgi.table_function import (
    BindParams,
    InitParams,
    ProcessParams,
    TableCardinality,
    TableFunctionGenerator,
)

__all__ = ["TxCachedValueFunction"]


@dataclass(frozen=True)
class TxCachedValueArgs:
    """Arguments for ``tx_cached_value``."""

    key: Annotated[str, Arg(0, doc="Cache key, scoped to the current transaction")]
    seed: Annotated[int, Arg(1, doc="Value to cache on first call; ignored on cache hit")]


@dataclass(kw_only=True)
class _TxCachedValueState(ArrowSerializableDataclass):
    """Mutable per-process state carried into ``process``."""

    # Resolved value (cached or freshly-seeded). Carried from bind via
    # opaque_data so process() doesn't need access to transaction_storage
    # (which is only populated on BindParams).
    value: int
    emitted: bool = False


class TxCachedValueFunction(TableFunctionGenerator[TxCachedValueArgs, _TxCachedValueState]):
    """Returns a single-row table whose value is cached per (transaction, key).

    The cache lives in ``BindParams.transaction_storage`` — a view over
    ``FunctionStorage.transaction_state_*``. On a cache hit the stored value
    is returned; on a miss, the supplied ``seed`` is written to storage and
    returned.

    Without a transaction (``params.transaction_storage is None``) every
    bind acts as a cache miss and emits the caller's ``seed`` verbatim —
    so the same SQL run inside vs. outside a ``BEGIN``/``COMMIT`` block
    visibly differs.
    """

    FunctionArguments = TxCachedValueArgs
    State = _TxCachedValueState

    class Meta:
        """Metadata for tx_cached_value."""

        name = "tx_cached_value"
        description = "Return a value cached per (transaction_id, key) via transaction_storage."
        categories = ["test", "transaction-storage"]
        tags = {"category": "test"}

    OUTPUT_SCHEMA: ClassVar[pa.Schema] = schema({"v": pa.int64()})

    @staticmethod
    def _storage_key(user_key: str) -> bytes:
        """Storage key — namespaced so unrelated demos can share one transaction."""
        return f"vgi-fixture:tx_cached_value:{user_key}".encode()

    @classmethod
    def on_bind(cls, params: BindParams[TxCachedValueArgs]) -> BindResponse:
        """Resolve the value via transaction_storage, ship it via opaque_data."""
        storage = params.transaction_storage
        if storage is not None:
            key = cls._storage_key(params.args.key)
            cached = storage.get_one(key)
            if cached is not None:
                value = struct.unpack(">q", cached)[0]
            else:
                value = params.args.seed
                storage.put_one(key, struct.pack(">q", value))
        else:
            # No transaction → no caching possible. Every call is a fresh
            # bind that uses the caller's seed verbatim.
            value = params.args.seed

        return BindResponse(
            output_schema=cls.OUTPUT_SCHEMA,
            opaque_data=struct.pack(">q", value),
        )

    @classmethod
    def cardinality(cls, params: BindParams[TxCachedValueArgs]) -> TableCardinality:
        """One row, always."""
        del params
        return TableCardinality(estimate=1, max=1)

    @classmethod
    def on_init(cls, params: InitParams[TxCachedValueArgs]) -> GlobalInitResponse:
        """Pass the resolved value through to process().

        ``max_workers=1`` because this function emits exactly one row and
        does no work-queue coordination — running it in parallel would
        cause every secondary worker to re-emit the same row.
        """
        return GlobalInitResponse(
            max_workers=1,
            opaque_data=params.init_call.bind_opaque_data,
        )

    @classmethod
    def initial_state(cls, params: ProcessParams[TxCachedValueArgs]) -> _TxCachedValueState:
        """Decode the opaque_data shipped from on_bind()."""
        assert params.init_response is not None
        opaque = params.init_response.opaque_data
        assert opaque is not None and len(opaque) == 8, (
            "tx_cached_value: bind must populate opaque_data with an 8-byte int"
        )
        return _TxCachedValueState(value=struct.unpack(">q", opaque)[0])

    @classmethod
    def process(
        cls,
        params: ProcessParams[TxCachedValueArgs],
        state: _TxCachedValueState,
        out: OutputCollector,
    ) -> None:
        """Emit the resolved value as a single-row batch, then finish."""
        del params
        if state.emitted:
            out.finish()
            return
        out.emit(pa.RecordBatch.from_pydict({"v": [state.value]}, schema=cls.OUTPUT_SCHEMA))
        state.emitted = True
