# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance stub for ``vgi/test/sql/integration/scalar/``.

Mirror the scalar-function sqllogictests against the Python ``Client`` probe so
the example worker's scalar surface has Python-side drift detection.
"""

from __future__ import annotations

from tests.conformance._stub import skip_area

skip_area(
    "scalar",
    [
        "add_values.test",
        "binary_packet.test",
        "conditional_message.test",
        "double.test",
        "function_registration.test",
        "geo_centroid.test",
        "geo_distance.test",
        "hash_seed.test",
        "null_handling.test",
        "random_int.test",
        # Covered on the Python side by tests/test_schema_scoped_functions.py.
        "same_name_catalogs.test",
        "same_name_schemas.test",
        "sum_values.test",
        "unnest_tensor.test",
        "upper_case.test",
        "whoami.test",
    ],
)
