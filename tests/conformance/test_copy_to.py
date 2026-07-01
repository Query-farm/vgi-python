# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance stub for ``vgi/test/sql/integration/copy_to/``."""

from __future__ import annotations

from tests.conformance._stub import skip_area

skip_area(
    "copy_to",
    [
        "basic.test",
        "failure.test",
        "options.test",
        "ordered.test",
        "parallel.test",
        "secrets.test",
        "tmp_file.test",
        "types.test",
    ],
)
