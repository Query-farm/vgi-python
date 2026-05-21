# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Helper for conformance-area stub files.

Each ``test_<area>.py`` that hasn't been fleshed out yet uses ``skip_area()``
so the file exists (satisfying ``test_directory_parity``) while clearly
signalling work-in-progress.
"""

from __future__ import annotations

import pytest


def skip_area(area: str, tests: list[str]) -> None:
    """Emit a single ``pytest.skip`` documenting what this stub must cover.

    ``tests`` is the list of C++ sqllogictests at
    ``vgi/test/sql/integration/<area>/`` that this Python stub owes coverage
    for. Listing them in the skip message turns the test output into a
    checklist for whoever picks this up.
    """
    bullet = "\n  - ".join(sorted(tests))
    pytest.skip(
        f"{area!r} conformance tests not yet implemented. Expected Python coverage for:\n  - {bullet}",
        allow_module_level=True,
    )
