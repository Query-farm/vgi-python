"""Fail when a ``VgiProtocol`` RPC is silently added without Python coverage.

``vgi.client.Client`` is not the user-facing API — real users invoke VGI from
DuckDB via the C++ extension. The Python client is a **protocol conformance
probe**: it gives Python tests a way to drive every RPC the worker implements.
That's what this test guards — when a new ``VgiProtocol`` method lands, the
developer must either wrap it in ``Client`` / ``CatalogClientMixin`` so the
Python test suite can reach it, or explicitly acknowledge via ``NotExposed``
that the RPC is tested elsewhere (C++ sqllogictests, vgi_rpc tests).

Mechanism
---------
1. Reflect over ``VgiProtocol`` to enumerate every public RPC method.
2. Look each one up in ``_RPC_ALLOWLIST``, which maps each RPC to either:
     - A tuple of ``Client`` / ``CatalogClientMixin`` attribute names that invoke
       the RPC. The test verifies the client actually defines those names.
     - ``NotExposed(reason=...)`` — recording where the RPC *is* exercised
       (e.g., "covered by C++ sqllogictests at integration/writable/*.test")
       rather than through the Python client probe.
3. Any RPC missing from the allowlist fails the test with guidance.
"""

from __future__ import annotations

from dataclasses import dataclass

from vgi.client.catalog_mixin import CatalogClientMixin
from vgi.client.client import Client
from vgi.protocol import VgiProtocol


@dataclass(frozen=True)
class NotExposed:
    """Mark an RPC as deliberately unreachable from ``Client``.

    ``reason`` must cite where the RPC *is* tested (C++ sqllogictests path, or
    the vgi_rpc suite) — ``Client`` is a probe, not a user API, so not every
    RPC needs a wrapper, but every skipped RPC needs a pointer to its real
    coverage.
    """

    reason: str


# Mapping from ``VgiProtocol`` method name to either a tuple of client attribute
# names that invoke it, or a ``NotExposed`` reason. Keep alphabetized within each
# section for reviewability.
_RPC_ALLOWLIST: dict[str, tuple[str, ...] | NotExposed] = {
    # ---------- Function invocation ----------
    "bind": ("scalar_function", "table_function", "table_in_out_function"),
    "init": ("scalar_function", "table_function", "table_in_out_function"),
    "table_function_cardinality": NotExposed(
        reason=(
            "DuckDB-only planner hint. Non-DuckDB clients invoke table functions "
            "directly via bind/init; cardinality estimation is a query-planner "
            "concern. Covered by C++ integration/table_function paths."
        )
    ),
    "table_function_statistics": NotExposed(
        reason=(
            "DuckDB-only planner hint. Same reasoning as table_function_cardinality. "
            "Covered by C++ integration/table_function paths."
        )
    ),
    "table_function_dynamic_to_string": NotExposed(
        reason=(
            "DuckDB-only profiler hook. Surfaces user diagnostics under EXPLAIN "
            "ANALYZE Extra Info. Non-DuckDB clients have no equivalent surface. "
            "Covered by tests/test_table_function_dynamic_to_string.py and "
            "C++ integration/table/dynamic_to_string.test."
        )
    ),
    # ---------- Aggregate (all unary) ----------
    "aggregate_bind": NotExposed(
        reason=(
            "DuckDB-only. Canonical client doesn't expose aggregate invocation — "
            "aggregates ship through the C++ extension. Covered by C++ "
            "integration/aggregate/* and tests/test_aggregate_function.py."
        ),
    ),
    "aggregate_update": NotExposed(
        reason=(
            "DuckDB-only. Canonical client doesn't expose aggregate invocation — "
            "aggregates ship through the C++ extension. Covered by C++ "
            "integration/aggregate/* and tests/test_aggregate_function.py."
        ),
    ),
    "aggregate_combine": NotExposed(
        reason=(
            "DuckDB-only. Canonical client doesn't expose aggregate invocation — "
            "aggregates ship through the C++ extension. Covered by C++ "
            "integration/aggregate/* and tests/test_aggregate_function.py."
        ),
    ),
    "aggregate_finalize": NotExposed(
        reason=(
            "DuckDB-only. Canonical client doesn't expose aggregate invocation — "
            "aggregates ship through the C++ extension. Covered by C++ "
            "integration/aggregate/* and tests/test_aggregate_function.py."
        ),
    ),
    "aggregate_destructor": NotExposed(
        reason=(
            "DuckDB-only. Canonical client doesn't expose aggregate invocation — "
            "aggregates ship through the C++ extension. Covered by C++ "
            "integration/aggregate/* and tests/test_aggregate_function.py."
        ),
    ),
    "aggregate_window_init": NotExposed(
        reason=(
            "DuckDB-only. Canonical client doesn't expose aggregate invocation — "
            "aggregates ship through the C++ extension. Covered by C++ "
            "integration/aggregate/* and tests/test_aggregate_function.py."
        ),
    ),
    "aggregate_window": NotExposed(
        reason=(
            "DuckDB-only. Canonical client doesn't expose aggregate invocation — "
            "aggregates ship through the C++ extension. Covered by C++ "
            "integration/aggregate/* and tests/test_aggregate_function.py."
        ),
    ),
    "aggregate_window_destructor": NotExposed(
        reason=(
            "DuckDB-only. Canonical client doesn't expose aggregate invocation — "
            "aggregates ship through the C++ extension. Covered by C++ "
            "integration/aggregate/* and tests/test_aggregate_function.py."
        ),
    ),
    "aggregate_window_batch": NotExposed(
        reason=(
            "DuckDB-only. Canonical client doesn't expose aggregate invocation — "
            "aggregates ship through the C++ extension. Covered by C++ "
            "integration/aggregate/* and tests/test_aggregate_function.py."
        ),
    ),
    # ---------- Catalog lifecycle / transactions ----------
    "catalog_catalogs": ("catalogs",),
    "catalog_attach": ("catalog_attach",),
    "catalog_detach": ("catalog_detach",),
    "catalog_create": ("catalog_create",),
    "catalog_drop": ("catalog_drop",),
    "catalog_version": ("catalog_version",),
    "catalog_transaction_begin": ("catalog_transaction_begin",),
    "catalog_transaction_commit": ("catalog_transaction_commit",),
    "catalog_transaction_rollback": ("catalog_transaction_rollback",),
    # ---------- Catalog schemas ----------
    "catalog_schemas": ("schemas",),
    "catalog_schema_get": ("schema_get",),
    "catalog_schema_create": ("schema_create",),
    "catalog_schema_drop": ("schema_drop",),
    "catalog_schema_contents_tables": ("schema_contents",),
    "catalog_schema_contents_views": ("schema_contents",),
    "catalog_schema_contents_functions": ("schema_contents",),
    "catalog_schema_contents_macros": ("schema_contents",),
    "catalog_schema_contents_indexes": NotExposed(
        reason=(
            "DuckDB-only metadata path. Indexes are catalog-planner territory; "
            "non-DuckDB clients don't surface index listings. Covered by C++ "
            "integration tests."
        )
    ),
    # ---------- Catalog tables ----------
    "catalog_table_get": ("table_get",),
    "catalog_table_create": ("table_create",),
    "catalog_table_drop": ("table_drop",),
    "catalog_table_scan_function_get": ("table_scan_function_get",),
    "catalog_table_column_statistics_get": NotExposed(
        reason=(
            "DuckDB-only planner hint. Column stats feed the query optimizer. "
            "Covered by C++ integration table/stats paths."
        )
    ),
    "catalog_table_insert_function_get": NotExposed(
        reason=(
            "DuckDB-only metadata path. Write functions (INSERT/UPDATE/DELETE) "
            "are resolved by DuckDB, not by non-DuckDB clients. Covered by "
            "C++ test/sql/integration/writable/*.test."
        ),
    ),
    "catalog_table_update_function_get": NotExposed(
        reason=(
            "DuckDB-only metadata path. Write functions (INSERT/UPDATE/DELETE) "
            "are resolved by DuckDB, not by non-DuckDB clients. Covered by "
            "C++ test/sql/integration/writable/*.test."
        ),
    ),
    "catalog_table_delete_function_get": NotExposed(
        reason=(
            "DuckDB-only metadata path. Write functions (INSERT/UPDATE/DELETE) "
            "are resolved by DuckDB, not by non-DuckDB clients. Covered by "
            "C++ test/sql/integration/writable/*.test."
        ),
    ),
    "catalog_table_comment_set": ("table_comment_set",),
    "catalog_table_column_comment_set": NotExposed(
        reason=(
            "DuckDB-only metadata path. Column comments travel with DDL; not a "
            "non-DuckDB-client concern. Add a wrapper here if a use case arises."
        )
    ),
    "catalog_table_rename": ("table_rename",),
    "catalog_table_column_add": ("table_column_add",),
    "catalog_table_column_drop": ("table_column_drop",),
    "catalog_table_column_rename": ("table_column_rename",),
    "catalog_table_column_default_set": ("table_column_default_set",),
    "catalog_table_column_default_drop": ("table_column_default_drop",),
    "catalog_table_column_type_change": ("table_column_type_change",),
    "catalog_table_not_null_set": ("table_not_null_set",),
    "catalog_table_not_null_drop": ("table_not_null_drop",),
    # ---------- Catalog views ----------
    "catalog_view_get": ("view_get",),
    "catalog_view_create": ("view_create",),
    "catalog_view_drop": ("view_drop",),
    "catalog_view_rename": ("view_rename",),
    "catalog_view_comment_set": ("view_comment_set",),
    # ---------- Catalog macros ----------
    "catalog_macro_get": ("macro_get",),
    "catalog_macro_create": ("macro_create",),
    "catalog_macro_drop": ("macro_drop",),
    # ---------- Catalog indexes ----------
    "catalog_index_get": NotExposed(
        reason=("DuckDB-only metadata path. Indexes are catalog-planner territory. Covered by C++ integration tests.")
    ),
    "catalog_index_create": NotExposed(
        reason=("DuckDB-only metadata path. Indexes are catalog-planner territory. Covered by C++ integration tests.")
    ),
    "catalog_index_drop": NotExposed(
        reason=("DuckDB-only metadata path. Indexes are catalog-planner territory. Covered by C++ integration tests.")
    ),
}


def _protocol_rpc_names() -> list[str]:
    """Public method names declared on the ``VgiProtocol`` class body."""
    # ``VgiProtocol`` is a typing.Protocol — its declared methods live on the
    # class __dict__ with plain ``def`` bodies. Filter out dunders.
    return sorted(name for name, value in vars(VgiProtocol).items() if callable(value) and not name.startswith("_"))


def _client_exposes(attr: str) -> bool:
    """Return whether ``Client`` (or its mixin) defines ``attr`` as callable."""
    for owner in (Client, CatalogClientMixin):
        val = getattr(owner, attr, None)
        if callable(val):
            return True
    return False


def test_every_protocol_rpc_is_accounted_for() -> None:
    """Every ``VgiProtocol`` method must be in the allowlist.

    Fails when a new RPC lands without either a wrapper entry or a
    ``NotExposed`` reason.
    """
    rpcs = _protocol_rpc_names()
    missing = [r for r in rpcs if r not in _RPC_ALLOWLIST]
    assert not missing, (
        "VgiProtocol methods with no entry in _RPC_ALLOWLIST:\n  - "
        + "\n  - ".join(missing)
        + "\n\nAdd a tuple of Client wrapper names, or NotExposed(reason=...) "
        "pointing at the plan that tracks closing the gap."
    )


def test_allowlist_has_no_stale_entries() -> None:
    """Catches typos / removed RPCs lingering in the allowlist."""
    rpcs = set(_protocol_rpc_names())
    stale = sorted(k for k in _RPC_ALLOWLIST if k not in rpcs)
    assert not stale, f"_RPC_ALLOWLIST references unknown VgiProtocol methods: {stale}"


def test_allowlist_wrapper_names_resolve_on_client() -> None:
    """Each allowlisted wrapper name must actually exist on ``Client``."""
    broken: list[str] = []
    for rpc, target in _RPC_ALLOWLIST.items():
        if isinstance(target, NotExposed):
            continue
        for attr in target:
            if not _client_exposes(attr):
                broken.append(f"{rpc} -> {attr!r} (not found on Client)")
    assert not broken, "Allowlisted Client wrappers missing:\n  - " + "\n  - ".join(broken)


def test_not_exposed_entries_have_reasons() -> None:
    """Every ``NotExposed`` must cite why the gap exists."""
    bare = [
        rpc for rpc, target in _RPC_ALLOWLIST.items() if isinstance(target, NotExposed) and not target.reason.strip()
    ]
    assert not bare, f"NotExposed entries missing reasons: {bare}"
