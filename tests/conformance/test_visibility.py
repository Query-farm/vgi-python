# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance stub for ``vgi/test/sql/integration/visibility/``.

Protocol 3.3 function visibility (``table_only`` scan and write functions) is
an extension-side contract: the C++ suite checks that a ``table_only`` scan
keeps its capabilities while staying out of ``duckdb_functions()``, against
the ``visfix`` fixture and, in ``example_table_only.test``, against this SDK's
standard example worker (``example.contract``, see
``vgi/_test_fixtures/contract.py``). The worker side, listing table-referenced
functions ``table_only``, is unit-tested by
``tests/catalog/test_function_visibility.py``.
"""

from __future__ import annotations

from tests.conformance._stub import skip_area

skip_area(
    "visibility",
    [
        "example_table_only.test",
        "invalid_visibility.test",
        "table_only.test",
    ],
)
