# Copyright 2026 Query Farm LLC - https://query.farm

# ruff: noqa: S101, D101, D102, D103
"""`Client.table_function_plan` + `Client.table_function(split_tokens=...)`.

Splits were previously entirely unreachable from `Client` — no wrapper for
`on_plan()` existed at all, and `table_function()`/`_do_init` had no
`split_tokens` parameter to redeem one, even though the wire protocol
(`TableFunctionPlanRequest`/`PlanResponse`/`InitRequest.split_tokens`) and the
`VgiProtocol.table_function_plan` RPC already supported it — the DuckDB C++
extension drives this today, but the reference Python client never grew the
equivalent. Exercised against `split_sequence` (the split-capable twin of
`sequence`, `vgi/_test_fixtures/table/splits.py`) over every client transport.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from vgi.arguments import Arguments
from vgi.protocol import ScanSplit

MAIN = "main"


@pytest.fixture
def client(client_transport: Any) -> Any:
    with client_transport() as c:
        c.catalog_attach(name="example", data_version_spec=None, implementation_version=None)
        yield c


def test_plan_returns_typed_scan_splits(client: Any) -> None:
    args = Arguments(named={"n": pa.scalar(20), "splits": pa.scalar(4)})
    plan = client.table_function_plan(function_name="split_sequence", schema_name=MAIN, arguments=args)
    assert len(plan.splits) == 4
    for split in plan.splits:
        assert isinstance(split, ScanSplit)
        assert split.token  # framework-stamped, non-empty
        assert split.estimated_rows == 5


def test_redeeming_each_split_in_order_reproduces_the_whole_scan(client: Any) -> None:
    """split_sequence redeemed split-by-split must equal sequence(n)'s output.

    Also threads ``split_execution_id``/``split_init_opaque_data`` from the
    plan on every redemption — this fixture doesn't need cross-split shared
    state to be correct, but this proves the wire path accepts and round-trips
    them without erroring, for workers that do.
    """
    args = Arguments(named={"n": pa.scalar(37), "splits": pa.scalar(5)})
    plan = client.table_function_plan(function_name="split_sequence", schema_name=MAIN, arguments=args)

    all_rows: list[int] = []
    for split in plan.splits:
        batches = list(
            client.table_function(
                function_name="split_sequence",
                schema_name=MAIN,
                arguments=args,
                split_tokens=[split.token],
                split_execution_id=plan.execution_id,
                split_init_opaque_data=plan.init_opaque_data,
            )
        )
        for batch in batches:
            all_rows.extend(batch.column("n").to_pylist())

    assert sorted(all_rows) == list(range(37))


def test_split_zero_yields_no_splits_and_no_rows(client: Any) -> None:
    args = Arguments(named={"n": pa.scalar(10), "splits": pa.scalar(4)})
    plan = client.table_function_plan(function_name="split_zero", schema_name=MAIN, arguments=args)
    assert plan.splits == []


def test_redeeming_a_split_forces_single_worker(client: Any) -> None:
    """`split_tokens` bypasses `_spawn_additional_workers` — no additional worker connections open."""
    args = Arguments(named={"n": pa.scalar(8), "splits": pa.scalar(2)})
    plan = client.table_function_plan(function_name="split_sequence", schema_name=MAIN, arguments=args)
    list(
        client.table_function(
            function_name="split_sequence",
            schema_name=MAIN,
            arguments=args,
            split_tokens=[plan.splits[0].token],
        )
    )
    assert client._additional_workers == []
