"""Drift + determinism tests for `vgi.codegen.ts_client`.

Enforces that `vgi-typescript/src/generated/vgi-client.ts` in the sibling
repo matches what the generator would emit right now. When the test fails,
the error message prints the regeneration command.

If the `vgi-typescript` repo isn't present next to `vgi-python`, the drift
test is skipped.
"""

from __future__ import annotations

import io
import os
import re
from pathlib import Path

import pytest

from vgi.codegen.ts_client import emit


def _vgi_ts_client_path() -> Path:
    override = os.environ.get("VGI_TS_GENERATED_CLIENT")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "vgi-typescript" / "src" / "generated" / "vgi-client.ts"


_REGEN_HINT = (
    "To regenerate, run:\n"
    "  uv run --project ~/Development/vgi-python vgi-gen-ts-client \\\n"
    "    > ~/Development/vgi-typescript/src/generated/vgi-client.ts"
)


# The generator header includes a git SHA that moves with commits; strip it
# before comparing so the test isn't flaky on uncommitted work. Everything
# below the header banner is compared verbatim.
_HEADER_SHA_RE = re.compile(r"^// vgi-python SHA: [0-9a-f]+\s*$", re.MULTILINE)


def _normalize(text: str) -> str:
    return _HEADER_SHA_RE.sub("// vgi-python SHA: <sha>", text)


def test_generator_is_deterministic() -> None:
    """Two back-to-back runs of the generator must produce byte-identical output."""
    a = io.StringIO()
    emit(a)
    b = io.StringIO()
    emit(b)
    assert a.getvalue() == b.getvalue(), "ts_client generator is non-deterministic"


def test_generator_has_no_duplicate_exports() -> None:
    """Every emitted `export interface`/`export type` name must be unique."""
    buf = io.StringIO()
    emit(buf)
    names = re.findall(r"^export (?:interface|type) (\w+)", buf.getvalue(), re.MULTILINE)
    seen: set[str] = set()
    dupes: list[str] = []
    for n in names:
        if n in seen:
            dupes.append(n)
        seen.add(n)
    assert not dupes, f"generator emitted duplicate TS names: {dupes}"


def test_checked_in_client_matches_generator() -> None:
    """Checked-in vgi-client.ts matches what the generator would emit right now."""
    path = _vgi_ts_client_path()
    if not path.exists():
        pytest.skip(
            f"{path} not found; set VGI_TS_GENERATED_CLIENT or check out vgi-typescript next to vgi-python",
        )

    buf = io.StringIO()
    emit(buf)
    expected = _normalize(buf.getvalue())
    actual = _normalize(path.read_text())

    if expected != actual:
        # Show a compact tail of the first divergence so the failure is readable.
        exp_lines = expected.splitlines()
        act_lines = actual.splitlines()
        first_diff = next(
            (i for i in range(min(len(exp_lines), len(act_lines))) if exp_lines[i] != act_lines[i]),
            min(len(exp_lines), len(act_lines)),
        )
        context = 3
        start = max(0, first_diff - context)
        end = first_diff + context + 1
        exp_slice = "\n".join(f"  {line}" for line in exp_lines[start:end])
        act_slice = "\n".join(f"  {line}" for line in act_lines[start:end])
        raise AssertionError(
            f"checked-in vgi-client.ts differs from generator output "
            f"at line ~{first_diff + 1}.\n\nexpected:\n{exp_slice}\n\nactual:\n{act_slice}\n\n"
            f"{_REGEN_HINT}",
        )
