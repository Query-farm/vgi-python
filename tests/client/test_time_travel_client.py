# Copyright 2026 Query Farm LLC - https://query.farm

# ruff: noqa: S101, D101, D102, D103
"""`Client.table_get`/`table_function(at_unit=..., at_value=...)`.

Both methods previously dropped `at_unit`/`at_value` on the floor even though
the underlying wire RPCs (`catalog_table_get`, `BindRequest.at_unit`/
`.at_value`) already carried them — `table_scan_function_get` was the only
`Client` method that exposed time travel at all. Two distinct worker
patterns are exercised, both real fixtures, proving this isn't just plumbing
that happens to compile:

* `data.versioned_data` reads its version through a pre-resolved bind
  argument (`table_scan_function_get(at_unit=...)` bakes it into the
  returned `ScanFunctionResult.arguments`) — `table_get(at_unit=...)` is
  what needed fixing here, to see the schema at that version at all.
* `data.tt_pushdown_fn` (`tt_pushdown_scan`) reads `at_unit`/`at_value`
  **directly off the init request** (`ProcessParams.at_unit`/`.at_value`,
  see `vgi/_test_fixtures/table/tt_pushdown.py`) — this is the pattern that
  needed `table_function(at_unit=...)` specifically; no amount of
  pre-resolved arguments would reach it.
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


def _attach_opaque(client: Any) -> Any:
    result = client.catalog_attach(name="example", data_version_spec=None, implementation_version=None)
    return result.attach_opaque_data


def test_table_get_at_unit_returns_the_versioned_schema(client: Any) -> None:
    opaque = _attach_opaque(client)
    v1 = client.table_get(
        attach_opaque_data=opaque, schema_name=DATA, name="versioned_data", at_unit="VERSION", at_value="1"
    )
    live = client.table_get(attach_opaque_data=opaque, schema_name=DATA, name="versioned_data")

    v1_schema = pa.ipc.read_schema(pa.py_buffer(v1.columns))
    live_schema = pa.ipc.read_schema(pa.py_buffer(live.columns))
    assert v1_schema.names == ["id"]  # version 1: (id int64) only
    assert live_schema.names == ["id", "score"]  # current: version 3


def test_table_get_with_no_at_clause_is_unaffected(client: Any) -> None:
    """The new `at_unit`/`at_value` params default to `None` — unchanged behavior without them."""
    info = client.table_get(attach_opaque_data=_attach_opaque(client), schema_name=DATA, name="numbers")
    assert info is not None


def test_table_function_at_unit_reaches_a_worker_that_reads_it_from_init(client: Any) -> None:
    """`tt_pushdown_scan` reads `at_unit`/`at_value` off the init request, not a resolved bind arg."""
    v1_batches = list(
        client.table_function(function_name="tt_pushdown_scan", schema_name=DATA, at_unit="VERSION", at_value="1")
    )
    live_batches = list(client.table_function(function_name="tt_pushdown_scan", schema_name=DATA))

    assert sum(b.num_rows for b in v1_batches) == 5
    assert sum(b.num_rows for b in live_batches) == 10


def test_table_function_with_no_at_clause_is_unaffected(client: Any) -> None:
    """The new `at_unit`/`at_value` params default to `None` — unchanged behavior without them."""
    batches = list(
        client.table_function(
            function_name="sequence", schema_name="main", arguments=Arguments(positional=(pa.scalar(5),))
        )
    )
    assert sum(b.num_rows for b in batches) == 5
