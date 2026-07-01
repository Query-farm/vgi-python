# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance stub for ``vgi/test/sql/integration/container/``."""

from __future__ import annotations

from tests.conformance._stub import skip_area

skip_area(
    "container",
    [
        "errors.test",
        "shared_http.test",
        "shared_tcp.test",
        "sklearn_smoke.test",
    ],
)
