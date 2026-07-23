# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Cacheable same-name-in-two-schemas producer fixtures.

The result-cache member of the schema-disambiguation family (see
``scalar/same_name.py``, ``table_in_out_same_name.py``, ``aggregate/same_name.py``).
Those probe *dispatch*; this one probes the *result cache*, a distinct layer.

``test_same_name_cached`` is a one-row producer table function that advertises
``vgi.cache.ttl`` and is registered in BOTH the ``main`` and ``data`` schemas of
the ``example`` catalog. Each schema's implementation emits a single row tagged
with its own schema name.

The result cache keyed on catalog + auth + function name with no schema
dimension, so the two implementations produced byte-identical cache keys and one
schema's memoized rows cross-served the other — the caching-layer twin of the
``(schema, name)`` dispatch bug. The tag makes a cross-serve visible:
``example.data.test_same_name_cached()`` would return a ``main`` row. With the
schema in the key, each schema gets its own entry (so ``vgi_result_cache()``
holds two rows for the one function name) and returns its own tag. Driven by
``cache/same_name_schemas.test``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, cast

import pyarrow as pa
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi.cache_control import CacheControl
from vgi.metadata import FunctionExample
from vgi.protocol import VgiOutputCollector
from vgi.schema_utils import schema
from vgi.table_function import (
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

FUNCTION_NAME = "test_same_name_cached"

# Long enough that the TTL never lapses mid-test.
_TTL_SECONDS = 300


@dataclass(slots=True, frozen=True)
class _CachedArgs:
    """Arguments for the cacheable same-name producer (none)."""


@dataclass(kw_only=True)
class _CachedState(ArrowSerializableDataclass):
    """One-shot emit latch for the single output row."""

    done: bool = False


class _SameNameCached(TableFunctionGenerator[_CachedArgs, _CachedState]):
    """Shared body; each subclass supplies the schema it is declared in."""

    #: Schema this implementation is declared in — the tag it stamps.
    OWNING_SCHEMA: ClassVar[str] = ""

    FunctionArguments = _CachedArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = schema(tag=pa.string())

    @classmethod
    def initial_state(cls, params: ProcessParams[_CachedArgs]) -> _CachedState:
        """Fresh latch per (real, cache-missing) invocation."""
        return _CachedState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[_CachedArgs],
        state: _CachedState,
        out: OutputCollector,
    ) -> None:
        """Emit the single schema-tagged row once, advertising a cache TTL."""
        if state.done:
            out.finish()
            return
        batch = pa.RecordBatch.from_pydict({"tag": [cls.OWNING_SCHEMA]}, schema=params.output_schema)
        cast(VgiOutputCollector, out).emit(batch, cache_control=CacheControl(ttl=_TTL_SECONDS))
        state.done = True


@init_single_worker
@bind_fixed_schema
class SameNameMainCached(_SameNameCached):
    """``test_same_name_cached`` as declared in the ``main`` schema."""

    OWNING_SCHEMA = "main"

    class Meta:
        """Function metadata."""

        name = FUNCTION_NAME
        description = "Schema-disambiguation probe; the main-schema cacheable producer"
        categories = ["generator", "cache", "testing"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM example.main.test_same_name_cached()",
                description="One cacheable row tagged 'main'",
            ),
        ]


@init_single_worker
@bind_fixed_schema
class SameNameDataCached(_SameNameCached):
    """``test_same_name_cached`` as declared in the ``data`` schema."""

    OWNING_SCHEMA = "data"

    class Meta:
        """Function metadata."""

        name = FUNCTION_NAME
        description = "Schema-disambiguation probe; the data-schema cacheable producer"
        categories = ["generator", "cache", "testing"]
        examples = [
            FunctionExample(
                sql="SELECT * FROM example.data.test_same_name_cached()",
                description="One cacheable row tagged 'data'",
            ),
        ]
