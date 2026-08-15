# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance stub for ``vgi/test/sql/integration/aggregate/``.

``Client`` now drives the all-unary aggregate protocol
(``Client.aggregate_function`` / ``aggregate_session`` / ``aggregate_streaming``),
and ``tests/client/test_aggregate_client.py`` exercises that surface end to end
against the fixture worker — grouped and global aggregation, ``combine``, the
window RPCs, and the streaming-partitioned protocol.

What is still owed here is the per-``.test`` parity sweep below: driving the
same cases the C++ sqllogictests drive, so a new C++ aggregate capability
cannot land without a Python counterpart.
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
