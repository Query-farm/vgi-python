# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance stub for ``vgi/test/sql/integration/attach/``."""

from __future__ import annotations

from tests.conformance._stub import skip_area

skip_area(
    "attach",
    [
        "attach_options_echo.test",
        "versioned_tables.test",
        "versioned_tables_http.test",
        "versioned_tables_impl.test",
        "versioned_tables_impl_http.test",
        "versioned_tables_resolved.test",
        "versioned_tables_resolved_http.test",
        "versioned_tables_spec.test",
        "versioned_tables_spec_http.test",
        "versioning.test",
        "versioning_http.test",
    ],
)
