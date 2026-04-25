"""Conformance stub for ``vgi/test/sql/integration/overload/``."""

from __future__ import annotations

from tests.conformance._stub import skip_area

skip_area(
    "overload",
    [
        "scalar_overload.test",
        "scalar_varargs_overload.test",
        "table_overload.test",
        "table_varargs_overload.test",
    ],
)
