# Copyright 2026 Query Farm LLC - https://query.farm

# ruff: noqa: S101, D101, D102, D103
"""`Client.table_scan_branches_get`.

`Client` had no wrapper for `catalog_table_scan_branches_get` at all —
grepped the whole `vgi/client/` package for "branch" and the only hit was an
unrelated code comment ("skip the subprocess branch"). A multi-branch table
was therefore invisible to any non-DuckDB caller, not just unsupported by a
higher-level connector. Exercised against `data.multi_branch_numbers` (two
`sequence(50)` arms — `vgi/_test_fixtures/worker.py`) and, for the legacy
fallback, `data.numbers` (an ordinary single-branch table whose worker only
implements the older `table_scan_function_get`).
"""

from __future__ import annotations

from typing import Any

import pytest

from vgi.catalog import ScanBranch, ScanBranchesResult

DATA = "data"


@pytest.fixture
def client(client_transport: Any) -> Any:
    with client_transport() as c:
        c.catalog_attach(name="example", data_version_spec=None, implementation_version=None)
        yield c


def _attach_opaque(client: Any) -> Any:
    # catalog_attach() was already called by the fixture above (its return
    # value discarded) — re-attach here to get the opaque data this method
    # needs, mirroring how CatalogClientMixin methods are used standalone.
    return client.catalog_attach(name="example", data_version_spec=None, implementation_version=None).attach_opaque_data


def test_multi_branch_table_returns_all_branches(client: Any) -> None:
    result = client.table_scan_branches_get(
        attach_opaque_data=_attach_opaque(client),
        schema_name=DATA,
        name="multi_branch_numbers",
    )
    assert isinstance(result, ScanBranchesResult)
    assert len(result.branches) == 2
    for branch in result.branches:
        assert isinstance(branch, ScanBranch)
        assert branch.function_name == "sequence"
        assert branch.source_table is None  # function branch, not catalog-table


def test_single_branch_table_falls_back_to_legacy_rpc(client: Any) -> None:
    """`data.numbers` only implements `table_scan_function_get` — the fallback wraps it as one branch."""
    result = client.table_scan_branches_get(
        attach_opaque_data=_attach_opaque(client),
        schema_name=DATA,
        name="numbers",
    )
    assert len(result.branches) == 1
    assert (
        result.branches[0].function_name
        == client.table_scan_function_get(
            attach_opaque_data=_attach_opaque(client),
            schema_name=DATA,
            name="numbers",
        ).function_name
    )
