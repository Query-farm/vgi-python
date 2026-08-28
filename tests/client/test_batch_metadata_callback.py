# Copyright 2026 Query Farm LLC - https://query.farm

# ruff: noqa: S101, D101, D102, D103
"""`Client.table_function(batch_metadata_callback=...)`.

`_table_function_parallel` previously discarded `AnnotatedBatch.custom_metadata`
unconditionally (`if output.batch.num_rows > 0: output_queue.put(output.batch)`)
and only forwarded the bare `pa.RecordBatch` — so a worker's `vgi.cache.*`
cacheability advertisement (`vgi/cache_control.py`), which rides
`custom_metadata`, was structurally unreachable through the public
`table_function()` generator. `batch_metadata_callback` mirrors the existing
`bind_result_callback` pattern to fix that additively, with no change to the
generator's yield type. Exercised against `cacheable_numbers`
(`vgi/_test_fixtures/table/cache.py`), which advertises `vgi.cache.ttl` on its
first batch.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from vgi.arguments import Arguments

DATA = "data"


@pytest.fixture
def client(client_transport: Any) -> Any:
    with client_transport() as c:
        c.catalog_attach(name="example", data_version_spec=None, implementation_version=None)
        yield c


def test_callback_receives_cache_control_metadata(client: Any) -> None:
    metas: list[pa.KeyValueMetadata | None] = []
    batches = list(
        client.table_function(
            function_name="cacheable_numbers",
            schema_name=DATA,
            arguments=Arguments(named={"n": pa.scalar(5)}),
            batch_metadata_callback=metas.append,
        )
    )
    assert sum(b.num_rows for b in batches) == 5
    assert len(metas) == len(batches)
    assert metas[0] is not None
    assert dict(metas[0])[b"vgi.cache.ttl"] == b"300"


def test_callback_is_optional_and_defaults_to_none(client: Any) -> None:
    """No `batch_metadata_callback` -> unchanged behavior, no crash."""
    batches = list(
        client.table_function(
            function_name="cacheable_numbers",
            schema_name=DATA,
            arguments=Arguments(named={"n": pa.scalar(3)}),
        )
    )
    assert sum(b.num_rows for b in batches) == 3


def test_callback_sees_none_for_a_batch_without_custom_metadata(client: Any) -> None:
    """A function that never advertises cache control still drives the callback once per batch, with `None`."""
    metas: list[pa.KeyValueMetadata | None] = []
    list(
        client.table_function(
            function_name="sequence",
            schema_name="main",
            arguments=Arguments(positional=(pa.scalar(4),)),
            batch_metadata_callback=metas.append,
        )
    )
    assert metas
    assert all(m is None for m in metas)
