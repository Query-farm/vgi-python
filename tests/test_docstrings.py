# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Docstring-consistency gate.

Runs pydoclint over the ``vgi/`` package as part of the test suite, mirroring
the CI ``Docstring lint (pydoclint)`` step. pydoclint complements ruff's ``D``
rules: ruff checks docstring *shape*, while pydoclint verifies that documented
arguments, return values, yields, and dataclass attributes actually match the
code. Configuration lives in ``[tool.pydoclint]`` in ``pyproject.toml`` — this
test invokes the same CLI, so there is no duplicated rule set.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]


def test_pydoclint_clean() -> None:
    """``vgi/`` must pass the pydoclint docstring gate (config in pyproject.toml)."""
    pydoclint = shutil.which("pydoclint")
    if pydoclint is None:  # pragma: no cover - dev dependency should always be present
        pytest.skip("pydoclint is not installed")

    result = subprocess.run(
        [pydoclint, "vgi/"],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, (
        "pydoclint found docstring violations:\n\n"
        f"{result.stdout}{result.stderr}\n"
        "Fix the docstrings, or run "
        "`uv run pydoclint --generate-baseline=True --baseline=.pydoclint-baseline vgi/` "
        "to defer them (see [tool.pydoclint] in pyproject.toml)."
    )
