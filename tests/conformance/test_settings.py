# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance stub for ``vgi/test/sql/integration/settings/``."""

from __future__ import annotations

from tests.conformance._stub import skip_area

skip_area(
    "settings",
    [
        "filter_by_setting.test",
        "multiply_by_setting.test",
        "settings.test",
        "struct_settings.test",
    ],
)
