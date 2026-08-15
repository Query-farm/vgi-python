# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Conformance stub for ``vgi/test/sql/integration/copy_from/``.

``Client.copy_from`` drives the reader path and
``tests/client/test_copy_client.py`` covers it against the fixture worker; the
per-``.test`` parity sweep below is what remains.
"""

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
