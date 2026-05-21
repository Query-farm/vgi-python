# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance stub for ``vgi/test/sql/integration/secret/``."""

from __future__ import annotations

from tests.conformance._stub import skip_area

skip_area(
    "secret",
    [
        "secret_no_secret.test",
        "secret_registration.test",
        "secret_scalar.test",
        "secret_scoped.test",
        "secret_table_function.test",
    ],
)
