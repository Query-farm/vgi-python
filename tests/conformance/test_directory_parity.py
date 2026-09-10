# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Guard against silent drift from the C++ integration suite.

Every subdirectory of ``vgi/test/sql/integration/`` represents a feature area
the C++ client is expected to exercise. For each such area we require a
corresponding ``test_<area>.py`` file under ``tests/conformance/`` so a new
C++ area cannot land without a Python counterpart.

If the C++ checkout is absent the test skips — this is acceptable on CI workers
that only clone the Python repo, but the check is still enforced on developer
machines and on CI jobs that clone both repos.
"""

from __future__ import annotations

import pathlib

import pytest

from tests.conformance.conftest import CPP_INTEGRATION_ROOT

_CONFORMANCE_DIR = pathlib.Path(__file__).parent

# Areas the Python conformance suite intentionally does not mirror. Each entry
# must cite a reason — leaving an empty reason defeats the purpose of the test.
_EXEMPTIONS: dict[str, str] = {
    "catalog": (
        "Multi-branch scan planning (multi_branch_*) is DuckDB-only — the C++ "
        "extension drives catalog_table_scan_branches_get to route scans across "
        "heterogeneous sources, an RPC the Python client deliberately does not "
        "wrap (see NotExposed in test_protocol_inventory.py). Catalog "
        "lifecycle/schema/table surfaces are mirrored by test_attach.py, "
        "test_table.py, test_view.py, and test_macro.py."
    ),
    "cache": (
        "The table-function result cache lives entirely in the C++ extension "
        "(key/eligibility, in-memory + content-addressed disk tiers, serve/"
        "capture, conditional revalidation). The Python worker's only role is "
        "advertising vgi.cache.* metadata + answering 304 not_modified, which is "
        "unit-tested by tests/test_cache_control.py (rendering, the emit merge "
        "path, ProcessParams.if_none_match threading, and the revalidatable "
        "fixture's 304 behavior)."
    ),
    "database_worker": (
        "Resolving database:// package rows, validating artifact digests and "
        "entrypoints, installing archives, and managing runtime leases are "
        "C++-extension responsibilities. The Python SDK supplies workers that "
        "the extension packages and launches, but its Client neither parses "
        "database:// locations nor owns the extension's artifact cache. The "
        "cross-language executable/archive cases remain covered by "
        "vgi/test/sql/integration/database_worker/."
    ),
    "global_functions": (
        "Publishing worker-declared functions into DuckDB's system.main is "
        "entirely a C++-extension concern: the registry, prefix application, "
        "first-attach-wins collision policy, post-DETACH liveness, and the "
        "vgi_global_functions() introspection surface all live in the "
        "extension. The Python worker's only role is advertising "
        "global_functions/global_function_prefix on the attach response, which "
        "tests/catalog/test_declarative.py covers directly (descriptor "
        "validation + the serialized FunctionInfo payload). The observable "
        "behaviour is cross-language by construction and is covered by "
        "vgi/test/sql/integration/global_functions/, which any worker "
        "implementation runs via VGI_TEST_WORKER."
    ),
    "filter_pushdown": (
        "Per-type filter-pushdown coverage is mirrored by tests/test_filter_pushdown.py "
        "and tests/test_filter_pushdown_extension.py, which drive the same predicate "
        "types through the Client's pushdown_filters path."
    ),
    "http": (
        "HTTP transport conformance is mirrored by test_http_client.py, "
        "test_http_external_location.py, test_http_upload_url.py (this directory) "
        "and tests/test_http_demo_storage.py."
    ),
    "launcher": (
        "Worker-launch CLI option parsing/validation is a C++-extension concern "
        "(it spawns the Python worker as a subprocess); the Python client never "
        "exercises the launcher surface."
    ),
    "simple_writable": (
        "INSERT/UPDATE/DELETE/RETURNING write paths are mirrored by "
        "test_writable.py, which drives the same operations against the writable "
        "fixture catalog."
    ),
}


def _cpp_integration_subdirs() -> list[str]:
    root = pathlib.Path(CPP_INTEGRATION_ROOT)
    if not root.is_dir():
        pytest.skip(f"C++ integration tree not present at {root}")
    return sorted(p.name for p in root.iterdir() if p.is_dir())


def test_every_cpp_integration_area_has_python_conformance_file() -> None:
    """Each C++ integration subdir must have a ``test_<name>.py`` sibling here."""
    expected = _cpp_integration_subdirs()
    existing = {p.name for p in _CONFORMANCE_DIR.glob("test_*.py")}

    missing: list[str] = []
    for area in expected:
        if area in _EXEMPTIONS:
            continue
        filename = f"test_{area}.py"
        if filename not in existing:
            missing.append(filename)

    assert not missing, (
        "C++ integration areas without a Python conformance file:\n  - "
        + "\n  - ".join(missing)
        + "\n\nAdd a stub at tests/conformance/<filename> or record an exemption "
        "with a reason in _EXEMPTIONS."
    )


def test_exemptions_are_documented() -> None:
    """Each exemption must have a non-empty reason."""
    blank = [k for k, v in _EXEMPTIONS.items() if not v.strip()]
    assert not blank, f"Exemptions missing reasons: {blank}"
