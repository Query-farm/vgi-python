# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Docstring-consistency gate.

Runs pydoclint over the ``vgi/`` package as part of the test suite, mirroring
the CI ``Docstring lint (pydoclint)`` step. pydoclint complements ruff's ``D``
rules: ruff checks docstring *shape*, while pydoclint verifies that documented
arguments, return values, yields, and dataclass attributes actually match the
code. Configuration lives in ``[tool.pydoclint]`` in ``pyproject.toml`` — this
test invokes the same CLI, so there is no duplicated rule set.
"""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]

# A real violation line looks like ``    42: DOC101: ...``. If pydoclint exits
# non-zero without emitting any such code, it failed to *run* (e.g. import
# error) rather than finding violations — see the environment note below.
_VIOLATION_RE = re.compile(r"\bDOC\d{3}\b")


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
    output = result.stdout + result.stderr

    if result.returncode == 0:
        return

    # pydoclint depends on ``docstring-parser-fork`` while ``vgi-rpc`` depends on
    # the upstream ``docstring-parser``; both claim the ``docstring_parser``
    # import namespace, so on some interpreter/OS combinations the wrong files
    # win and pydoclint crashes on import. That's a broken tool environment, not
    # a docstring problem — skip rather than fail (the gate still runs on every
    # other matrix cell where the tool is healthy).
    if not _VIOLATION_RE.search(output):  # pragma: no cover - env-dependent
        pytest.skip(f"pydoclint could not run in this environment:\n{output}")

    pytest.fail(
        "pydoclint found docstring violations:\n\n"
        f"{output}\n"
        "Fix the docstrings, or run "
        "`uv run pydoclint --generate-baseline=True --baseline=.pydoclint-baseline vgi/` "
        "to defer them (see [tool.pydoclint] in pyproject.toml)."
    )
