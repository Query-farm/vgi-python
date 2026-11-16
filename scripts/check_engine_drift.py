# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Fail when vgi-python's engine pins drift from the DuckDB ``vgi`` extension's engine.

vgi-python pins its embedded engine exactly (the ``haybarn``/``duckdb`` extras,
which are also the ``vgi.duckdb.standard.v1`` filter oracle). The extension is
built against the Haybarn tag named in its distribution workflow. When the two
are on different engine lines (major.minor), vgi-python's own suite tests an
engine no deployed extension uses: the 1.5 -> 2.0 move went unnoticed for a
month that way, because every vgi-python test, and every extension test that
launched a vgi-python worker, ran on 1.5.5.

The exact release candidate may differ (the extension builds from a Haybarn tag
that need not have a PyPI wheel), so only the engine line is compared.

Usage: ``python scripts/check_engine_drift.py`` (set ``GITHUB_TOKEN`` to avoid
the anonymous API rate limit; ``VGI_REPO`` / ``VGI_REF`` override the source).
"""

from __future__ import annotations

import json
import os
import re
import sys
import tomllib
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VGI_REPO = os.environ.get("VGI_REPO", "Query-farm/vgi")
WORKFLOW = ".github/workflows/MainDistributionPipeline.yml"
_LINE = re.compile(r"(\d+)\.(\d+)\.")


def _get(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "vgi-python-engine-drift-check"})
    token = os.environ.get("GITHUB_TOKEN")
    if token and url.startswith("https://api.github.com/"):
        request.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 - fixed https URLs
        data: bytes = response.read()
    return data


def extension_engine() -> tuple[str, str]:
    """Return ``(ref, haybarn tag)`` the extension's distribution build uses."""
    ref = os.environ.get("VGI_REF") or json.loads(_get(f"https://api.github.com/repos/{VGI_REPO}"))["default_branch"]
    text = _get(f"https://raw.githubusercontent.com/{VGI_REPO}/{ref}/{WORKFLOW}").decode()
    tags = set(re.findall(r"^\s*duckdb_version:\s*['\"]?([^'\"\s#]+)", text, re.MULTILINE))
    if len(tags) != 1:
        raise SystemExit(f"expected one duckdb_version in {VGI_REPO}@{ref}:{WORKFLOW}, found {sorted(tags) or 'none'}")
    return ref, tags.pop()


def pinned_engines() -> dict[str, str]:
    """Return ``{distribution: exact version}`` from the engine extras."""
    extras = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["optional-dependencies"]
    pins = {}
    for name in ("haybarn", "duckdb"):
        specs = [s.replace(" ", "") for s in extras[name] if re.match(rf"{name}\b", s)]
        match = re.fullmatch(rf"{name}==(\S+)", specs[0]) if len(specs) == 1 else None
        if match is None:
            raise SystemExit(f"the [{name}] extra must pin {name} exactly, got {specs}")
        pins[name] = match.group(1)
    return pins


def engine_line(version: str) -> str:
    """``haybarn-v2.0.0-rc10`` / ``2.0.0rc11`` -> ``2.0``."""
    match = _LINE.search(version)
    if match is None:
        raise SystemExit(f"cannot read an engine version from {version!r}")
    return f"{match.group(1)}.{match.group(2)}"


def main() -> int:
    """Compare the engine lines and report.

    Returns:
        The process exit status (0 when every pin is on the extension's line).
    """
    ref, tag = extension_engine()
    want = engine_line(tag)
    pins = pinned_engines()
    print(f"extension ({VGI_REPO}@{ref}) builds on {tag} -> engine line {want}")
    drift = []
    for name, version in pins.items():
        line = engine_line(version)
        print(f"vgi-python [{name}] pins {name}=={version} -> engine line {line}")
        if line != want:
            drift.append(f"{name}=={version}")
    if drift:
        print(
            f"\nENGINE DRIFT: {', '.join(drift)} is not on the extension's DuckDB {want} line. Move the engine pins "
            "(pyproject extras + dev group), STANDARD_V1_ENGINE_VERSIONS in vgi/filter_v2.py and the oracle in "
            "docs/protocol/vgi-filter-encoding-v2-spec.md section 6 together, then run the suite on the new engine.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
