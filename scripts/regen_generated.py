#!/usr/bin/env python3
"""Regenerate every checked-in generated artifact, atomically.

Run from anywhere:

    uv run --project ~/Development/vgi-python python scripts/regen_generated.py
    uv run --project ~/Development/vgi-python python scripts/regen_generated.py --check

Why this exists rather than a shell one-liner per artifact: the obvious form,
``python -m vgi.codegen.X > path/to/generated.file``, truncates the destination
*before* the generator runs, so any failure -- a syntax error, a missing
sibling checkout, a typo in the module name -- silently destroys the file it
was meant to update, and the loss is invisible until a test or a build fails
somewhere else. Each artifact here is rendered into memory, checked non-empty,
and only then written.

Targets in sibling repos that are not checked out are skipped, not failed:
nobody has all of them.
"""

from __future__ import annotations

import argparse
import importlib
import io
import sys
from pathlib import Path

#: (codegen module, destination relative to the sibling-repo root, repo dir name)
#: ``vgi-java`` lives at ``~/vgi-java`` for some checkouts and beside the other
#: SDKs for others; both are probed.
_TARGETS: list[tuple[str, str, str]] = [
    ("vgi.codegen.cpp_schemas", "src/generated/vgi_protocol_schemas.hpp", "vgi"),
    ("vgi.codegen.cpp_request_builders", "src/generated/vgi_request_builders.hpp", "vgi"),
    ("vgi.codegen.cpp_constants", "src/generated/vgi_protocol_constants.hpp", "vgi"),
    ("vgi.codegen.cpp_protocol_version", "src/generated/vgi_protocol_version.hpp", "vgi"),
    ("vgi.codegen.cpp_protocol_name", "src/generated/vgi_protocol_names.hpp", "vgi"),
    # The secret protocol's three artifacts were checked in but never listed here,
    # so `--check` reported "no drift" over a stale secret header. Listed now: a
    # regen script that silently skips an artifact is worse than no script, because
    # it answers the drift question with false confidence.
    (
        "vgi.codegen.cpp_secret_protocol_version",
        "src/generated/vgi_secret_protocol_version.hpp",
        "vgi",
    ),
    ("vgi.codegen.cpp_secret_schemas", "src/generated/vgi_secret_protocol_schemas.hpp", "vgi"),
    (
        "vgi.codegen.cpp_secret_request_builders",
        "src/generated/vgi_secret_request_builders.hpp",
        "vgi",
    ),
    ("vgi.codegen.go_schemas", "vgi/generated/protocol_schemas.go", "vgi-go"),
    ("vgi.codegen.rust_schemas", "vgi-protocol/src/generated/protocol_schemas.rs", "vgi-rust"),
    ("vgi.codegen.rust_request_builders", "vgi-protocol/src/generated/request_params.rs", "vgi-rust"),
    ("vgi.codegen.ts_schemas", "src/generated/vgi-protocol-schemas.ts", "vgi-typescript"),
    ("vgi.codegen.ts_client", "src/generated/vgi-client.ts", "vgi-typescript"),
    (
        "vgi.codegen.java_schemas",
        "vgi/src/test/java/farm/query/vgi/generated/VgiProtocolSchemas.java",
        "vgi-java",
    ),
]


def _repo_root(name: str) -> Path | None:
    """Locate a sibling checkout, or ``None`` when it is not present."""
    siblings = Path(__file__).resolve().parents[2]
    for candidate in (siblings / name, Path.home() / name):
        if candidate.is_dir():
            return candidate
    return None


def _render(module_name: str) -> str:
    """Render one artifact into memory. Raises if the generator fails."""
    module = importlib.import_module(module_name)
    buf = io.StringIO()
    module.emit(buf)
    text = buf.getvalue()
    if not text.strip():
        raise RuntimeError(f"{module_name}.emit() produced no output")
    return text


def main() -> int:
    """Regenerate (or check) every artifact; return a process exit code."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="report drift without writing anything (exit 1 if any file is stale)",
    )
    args = parser.parse_args()

    stale, wrote, skipped, failed = [], [], [], []

    for module_name, relative, repo in _TARGETS:
        root = _repo_root(repo)
        if root is None:
            skipped.append(f"{repo} (not checked out)")
            continue
        dest = root / relative
        try:
            text = _render(module_name)
        except Exception as exc:  # noqa: BLE001 - report every failure, don't stop
            failed.append(f"{module_name}: {exc}")
            continue

        current = dest.read_text() if dest.exists() else None
        if current == text:
            continue
        if args.check:
            stale.append(str(dest))
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        # Write beside the target, then replace: a crash mid-write leaves the
        # previous version intact rather than a truncated file.
        tmp = dest.with_suffix(dest.suffix + ".tmp")
        tmp.write_text(text)
        tmp.replace(dest)
        wrote.append(str(dest))

    for line in skipped:
        print(f"skip   {line}")
    for line in wrote:
        print(f"wrote  {line}")
    for line in stale:
        print(f"STALE  {line}")
    for line in failed:
        print(f"FAIL   {line}", file=sys.stderr)

    if failed:
        return 2
    if stale:
        print(f"\n{len(stale)} file(s) stale. Run without --check to regenerate.", file=sys.stderr)
        return 1
    if not args.check and not wrote:
        print("everything already up to date")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
