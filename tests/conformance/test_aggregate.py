"""Conformance stub for ``vgi/test/sql/integration/aggregate/``.

Python ``Client`` does not currently expose aggregate invocation — aggregates
are driven by the C++ extension via the all-unary RPC protocol. Once a probe
entry point is added, fill this in.
"""

from __future__ import annotations

from tests.conformance._stub import skip_area

skip_area(
    "aggregate",
    [
        "advanced.test",
        "any_type.test",
        "basic.test",
        "const_param.test",
        "dynamic.test",
        "function_registration.test",
        "function_registration_dynamic.test",
        "grouped.test",
        "high_cardinality.test",
        "listagg.test",
        "nest_tensor.test",
        "parallel.test",
        "varargs.test",
        "window.test",
        "window_dynamic.test",
    ],
)
