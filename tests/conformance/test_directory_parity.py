# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Guard against silent drift from the C++ integration suite.

Every subdirectory of ``vgi/test/sql/integration/`` represents a feature area
the C++ client is expected to exercise. For each such area we require a
corresponding ``test_<area>.py`` file under ``tests/conformance/`` so a new
C++ area cannot land without a Python counterpart.

If the C++ checkout is absent the test skips — this is acceptable on CI workers
that only clone the Python repo, but the check is still enforced on developer
machines and on CI jobs that clone both repos.
"""

from __future__ import annotations

import pathlib

import pytest

from tests.conformance.conftest import CPP_INTEGRATION_ROOT

_CONFORMANCE_DIR = pathlib.Path(__file__).parent

# Areas the Python conformance suite intentionally does not mirror. Each entry
# must cite a reason — leaving an empty reason defeats the purpose of the test.
_EXEMPTIONS: dict[str, str] = {}


def _cpp_integration_subdirs() -> list[str]:
    root = pathlib.Path(CPP_INTEGRATION_ROOT)
    if not root.is_dir():
        pytest.skip(f"C++ integration tree not present at {root}")
    return sorted(p.name for p in root.iterdir() if p.is_dir())


def test_every_cpp_integration_area_has_python_conformance_file() -> None:
    """Each C++ integration subdir must have a ``test_<name>.py`` sibling here."""
    expected = _cpp_integration_subdirs()
    existing = {p.name for p in _CONFORMANCE_DIR.glob("test_*.py")}

    missing: list[str] = []
    for area in expected:
        if area in _EXEMPTIONS:
            continue
        filename = f"test_{area}.py"
        if filename not in existing:
            missing.append(filename)

    assert not missing, (
        "C++ integration areas without a Python conformance file:\n  - "
        + "\n  - ".join(missing)
        + "\n\nAdd a stub at tests/conformance/<filename> or record an exemption "
        "with a reason in _EXEMPTIONS."
    )


def test_exemptions_are_documented() -> None:
    """Each exemption must have a non-empty reason."""
    blank = [k for k, v in _EXEMPTIONS.items() if not v.strip()]
    assert not blank, f"Exemptions missing reasons: {blank}"
