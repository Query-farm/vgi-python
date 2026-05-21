# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance stub for ``vgi/test/sql/integration/table_in_out/``."""

from __future__ import annotations

from tests.conformance._stub import skip_area

skip_area(
    "table_in_out",
    [
        "buffer_input/*",
        "distributed_sum.test",
        "echo/*",
        "exceptions.test",
        "function_registration.test",
        "logging.test",
        "repeat_inputs/*",
        "sum_all_columns.test",
        "unnest_tensor_rows.test",
    ],
)
