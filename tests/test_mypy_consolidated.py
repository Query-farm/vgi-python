"""Single consolidated mypy run for the project.

Replaces the per-file ``--mypy`` pytest plugin invocation. The plugin
collected one ``::mypy`` item per source file, each spinning up its own
mypy invocation/import phase under pytest-xdist workers and inflating
the overall pytest wall time. Running mypy once here is faster and
gives the same correctness signal.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def test_mypy_vgi_clean() -> None:
    """``mypy vgi/`` returns no errors."""
    mypy = shutil.which("mypy") or "mypy"
    result = subprocess.run(
        [mypy, "vgi/"],
        cwd=_PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        pytest.fail(f"mypy reported errors (exit={result.returncode}):\n{result.stdout}\n{result.stderr}")
