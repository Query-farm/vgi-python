#!/usr/bin/env python3
# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Check the combined contract inventory against the native field audit.

This is a coverage check, not proof of implementation or phase completion.
``--require-decisions`` additionally refuses unresolved scope/field reviews.
The audit checkout is required: an unavailable source cannot count as coverage.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from vgi_rpc.rpc._types import rpc_methods  # type: ignore[attr-defined]

from vgi.protocol import VgiProtocol

_ROOT = Path(__file__).resolve().parents[1]
_MANIFEST = _ROOT / "docs/protocol/duckdb-2.0-manifest.json"
_DISPOSITIONS = {"include", "existing_contract", "existing_contract_plus_engine_port", "engine_only", "defer"}


def audit_rows(source: str) -> list[dict[str, Any]]:
    """Extract every field/hook row, excluding explanatory and result tables."""
    lines = source.splitlines()
    active = False
    section = ""
    rows = []
    for offset, line in enumerate(lines):
        if line.startswith("#"):
            section = line.lstrip("# ").strip()
            if section == "Shared creation fields and object identity":
                active = True
            elif section == "Runtime evidence and limits":
                break
        if not active or not line.startswith("|") or line.startswith("| ---"):
            continue
        if offset + 1 < len(lines) and lines[offset + 1].startswith("| ---"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        rows.append({"source_line": offset + 1, "section": section, "field": cells[0], "audit_evidence": cells[1:]})
    if not rows:
        raise ValueError("No field rows found in the native audit")
    return rows


def plan_scope(source: str) -> dict[str, list[str]]:
    """Read every target and additional candidate directly from the agreed plan."""
    sections = {"Target contract package", "Additional candidates to resolve before freeze"}
    section = ""
    areas = {}
    lines = source.splitlines()
    for index, line in enumerate(lines):
        if line.startswith("#"):
            section = line.lstrip("# ").strip()
        if section not in sections or not line.startswith("|") or line.startswith("| ---"):
            continue
        if index + 1 < len(lines) and lines[index + 1].startswith("| ---"):
            continue
        title, *scope = [cell.strip() for cell in line.strip("|").split("|")]
        if title in areas:
            raise ValueError(f"Duplicate plan area: {title}")
        areas[title] = scope
    if not areas:
        raise ValueError("No contract areas found in the native plan")
    return areas


def check_inventory(manifest: dict[str, Any], native_root: Path) -> list[str]:
    """Return inventory errors; proposed records do not imply implemented RPCs."""
    errors = []
    if manifest.get("format_version") != 1:
        errors.append("unsupported manifest format")
    source_path = native_root / manifest["sources"]["audit_path"]
    if not source_path.is_file():
        return [f"required audit source is missing: {source_path}"]
    source = source_path.read_bytes()
    if hashlib.sha256(source).hexdigest() != manifest["sources"]["audit_sha256"]:
        errors.append("native audit digest changed; reconcile the manifest against the new evidence")
    rows = audit_rows(source.decode())
    fields = manifest["audit_fields"]
    actual = [{key: entry.get(key) for key in row} for entry, row in zip(fields, rows, strict=False)]
    if len(fields) != len(rows) or actual != rows:
        errors.append("audit field inventory differs: every row and its source evidence must be retained in order")
    if len({entry["id"] for entry in fields}) != len(fields):
        errors.append("duplicate audit entry IDs")
    areas = manifest["areas"]
    plan_path = native_root / manifest["sources"]["plan_path"]
    if not plan_path.is_file():
        errors.append(f"required plan source is missing: {plan_path}")
    else:
        expected_scope = plan_scope(plan_path.read_text())
        actual_scope = {area["title"]: area["scope"] for area in areas.values()}
        if len(actual_scope) != len(areas):
            errors.append("duplicate contract area titles")
        for title, scope in expected_scope.items():
            if actual_scope.get(title) != scope:
                errors.append(f"{title}: plan scope is missing or differs; reconcile the agreed requirement")
    methods = set(rpc_methods(VgiProtocol))
    for name, area in areas.items():
        if not area.get("scope") or not area.get("records"):
            errors.append(f"{name}: missing scope or target record inventory")
        missing = set(area["existing_methods"]) - methods
        if missing:
            errors.append(f"{name}: listed existing RPCs do not exist: {sorted(missing)}")
    for entry in fields:
        if entry["area"] not in areas:
            errors.append(f"{entry['id']}: unknown contract area {entry['area']}")
        if entry["disposition"] != "engine_only" and not entry.get("target"):
            errors.append(f"{entry['id']}: no field/record target")
    for proposal in manifest["proposals"]:
        missing = set(proposal["areas"]) - areas.keys()
        if missing:
            errors.append(f"{proposal['source']}: unknown areas {sorted(missing)}")
    return errors


def pending_decisions(manifest: dict[str, Any]) -> list[str]:
    """Identify scope and field reviews still needed for the P0 exit gate."""
    pending = []
    for name, area in manifest["areas"].items():
        if area["disposition"] not in _DISPOSITIONS:
            pending.append(f"{name}: inclusion decision is unresolved")
        if area["disposition"] == "defer" and not (area.get("reason") and area.get("later_release_cost")):
            pending.append(f"{name}: deferral lacks its reason or later protocol-release cost")
        if not area.get("capability_contract") or area["capability_contract"] == "pending_P1":
            pending.append(f"{name}: capability mapping is unresolved")
    for entry in manifest["audit_fields"]:
        if entry["disposition"] not in _DISPOSITIONS or entry["status"] == "pending_contract_review":
            pending.append(f"{entry['id']}: field mapping still requires review")
    return pending


def main() -> int:
    """Validate coverage and optionally require all scope decisions to be settled."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=_MANIFEST)
    parser.add_argument("--native-root", type=Path, default=_ROOT.parent / "vgi")
    parser.add_argument("--require-decisions", action="store_true")
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    errors = check_inventory(manifest, args.native_root)
    if errors:
        for error in errors:
            print(f"FAIL: {error}")
        return 1
    pending = pending_decisions(manifest)
    print(f"Inventory covers {len(manifest['audit_fields'])} audit rows and {len(manifest['areas'])} contract areas.")
    print(f"{len(pending)} scope/capability/field reviews remain; inventory coverage is not phase completion.")
    if args.require_decisions and pending:
        for item in pending:
            print(f"PENDING: {item}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
