# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance stub for ``vgi/test/sql/integration/copy_from/``."""

from __future__ import annotations

from tests.conformance._stub import skip_area

skip_area(
    "copy_from",
    [
        "basic.test",
        "options.test",
        "secrets.test",
    ],
)
