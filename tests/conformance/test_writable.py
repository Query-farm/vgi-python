"""Conformance stub for ``vgi/test/sql/integration/writable/``.

Write support (INSERT/UPDATE/DELETE) RPCs are exposed on ``VgiProtocol`` but
not wrapped by ``Client``. Covered today by the C++ integration suite; a
Python probe wrapper is what would let this area light up here.
"""

from __future__ import annotations

from tests.conformance._stub import skip_area

skip_area(
    "writable",
    [
        "alter.test",
        "cascade.test",
        "comments.test",
        "ctas.test",
        "ddl.test",
        "ddl_constraints.test",
        "delete.test",
        "foreign_key.test",
        "index.test",
        "insert.test",
        "merge.test",
        "products.test",
        "pushdown.test",
        "scale.test",
        "schema.test",
        "transaction.test",
        "update.test",
        "view.test",
    ],
)
