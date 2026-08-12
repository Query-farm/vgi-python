# Copyright 2025, 2026 Query Farm LLC - https://query.farm

# ruff: noqa: S101, D102, D103
"""Tests for ``Client.table_column_statistics``.

Workers may inline statistics on ``TableInfo`` or serve them lazily through the per-table
``catalog_table_column_statistics_get`` RPC. This wrapper fetches the lazy form and decodes
it, so a client never has to reach past the public API to reimplement the sparse-union wire
layout. Driven against the real ``vgi-fixture-worker`` over every client transport.
"""

from __future__ import annotations

from typing import Any

import pytest

from vgi.catalog import ColumnStatistics, deserialize_column_statistics


@pytest.fixture
def attached(client_transport: Any) -> Any:
    """Yield ``(client, attach_opaque_data)`` for the example catalog."""
    with client_transport() as client:
        res = client.catalog_attach(name="example", data_version_spec=None, implementation_version=None)
        yield client, res.attach_opaque_data


def test_returns_typed_statistics(attached: Any) -> None:
    client, aod = attached
    stats = client.table_column_statistics(attach_opaque_data=aod, schema_name="data", name="numbers")
    assert stats
    assert all(isinstance(s, ColumnStatistics) for s in stats)


def test_decodes_min_max_for_a_known_table(attached: Any) -> None:
    """`numbers` holds integers 0-99, so its bounds are known exactly."""
    client, aod = attached
    stats = client.table_column_statistics(attach_opaque_data=aod, schema_name="data", name="numbers")
    value = next(s for s in stats if s.column_name == "value")
    assert value.min is not None
    assert value.max is not None
    assert value.min.as_py() == 0
    assert value.max.as_py() == 99


def test_matches_the_inlined_statistics(attached: Any) -> None:
    """The lazy RPC and the copy inlined on TableInfo must agree.

    ``TableInfo.column_statistics`` arrives client-side as the raw wire bytes, so this is
    also the case that motivates a public deserializer: without one, a caller holding an
    inlined blob has no supported way to read it.
    """
    client, aod = attached
    info = client.table_get(attach_opaque_data=aod, schema_name="data", name="numbers")
    assert info is not None
    if not info.column_statistics:
        pytest.skip("worker does not inline statistics for this table")
    fetched = client.table_column_statistics(attach_opaque_data=aod, schema_name="data", name="numbers")
    inlined = {s.column_name: s for s in deserialize_column_statistics(info.column_statistics)}
    assert {s.column_name for s in fetched} == set(inlined)
    for s in fetched:
        other = inlined[s.column_name]
        assert (s.min is None) == (other.min is None)
        if s.min is not None and other.min is not None:
            assert s.min.as_py() == other.min.as_py()
        assert s.has_null == other.has_null


def test_unknown_table_returns_empty_list(attached: Any) -> None:
    """A missing table yields no statistics rather than raising."""
    client, aod = attached
    assert client.table_column_statistics(attach_opaque_data=aod, schema_name="data", name="no_such_table") == []
