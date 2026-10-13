# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""The release inventory must cover the actual audit, not just count its rows."""

import copy
import hashlib
import json
from pathlib import Path

import pytest

from scripts.check_duckdb2_manifest import audit_rows, check_inventory, pending_decisions

_ROOT = Path(__file__).resolve().parents[1]


def test_current_manifest_matches_native_audit() -> None:
    native = _ROOT.parent / "vgi"
    if not native.is_dir():
        pytest.skip("native checkout required for field-audit comparison")
    manifest = json.loads((_ROOT / "docs/protocol/duckdb-2.0-manifest.json").read_text())
    assert check_inventory(manifest, native) == []


@pytest.fixture
def inventory(tmp_path: Path) -> tuple[dict, Path]:
    """A minimal external audit with two distinct requirements."""
    source = (
        "## Shared creation fields and object identity\n\n"
        "| Field | Action |\n| --- | --- |\n| name | preserve path |\n| comment | preserve NULL |\n"
        "## Runtime evidence and limits\n| Probe | Result |\n| --- | --- |\n| sample | pass |\n"
    )
    (tmp_path / "audit.md").write_text(source)
    (tmp_path / "plan.md").write_text(
        "### Target contract package\n"
        "| Area | Scope |\n| --- | --- |\n| Identity | preserve identity |\n"
        "### Additional candidates to resolve before freeze\n"
        "| Candidate | Decision |\n| --- | --- |\n| Queries | review query execution |\n"
    )
    rows = audit_rows(source)
    manifest = {
        "format_version": 1,
        "sources": {
            "audit_path": "audit.md",
            "audit_sha256": hashlib.sha256(source.encode()).hexdigest(),
            "plan_path": "plan.md",
        },
        "areas": {
            "identity": {
                "title": "Identity",
                "scope": ["preserve identity"],
                "records": ["CatalogObjectRef"],
                "existing_methods": ["catalog_attach"],
                "disposition": "include",
                "capability_contract": "existing identity contract",
            },
            "query": {
                "title": "Queries",
                "scope": ["review query execution"],
                "records": ["QueryRequest"],
                "existing_methods": [],
                "disposition": "include",
                "capability_contract": "explicit query profile",
            },
        },
        "audit_fields": [
            dict(row, id=str(index), area="identity", target=row["field"], disposition="include", status="reviewed")
            for index, row in enumerate(rows)
        ],
        "proposals": [],
    }
    return manifest, tmp_path


@pytest.mark.parametrize("change", ["remove", "duplicate", "reorder", "alter_evidence", "nonexistent_method"])
def test_inventory_rejects_lost_or_misrepresented_requirements(inventory: tuple[dict, Path], change: str) -> None:
    manifest, native = inventory
    assert check_inventory(manifest, native) == []
    changed = copy.deepcopy(manifest)
    fields = changed["audit_fields"]
    if change == "remove":
        fields.pop()
    elif change == "duplicate":
        fields[1] = fields[0]
    elif change == "reorder":
        fields.reverse()
    elif change == "alter_evidence":
        fields[0]["audit_evidence"] = ["already implemented"]
    else:
        changed["areas"]["identity"]["existing_methods"] = ["catalog_nonexistent"]
    assert check_inventory(changed, native)


def test_missing_or_changed_audit_is_not_coverage(inventory: tuple[dict, Path]) -> None:
    manifest, native = inventory
    (native / "audit.md").write_text((native / "audit.md").read_text().replace("preserve NULL", "new requirement"))
    assert check_inventory(manifest, native)
    (native / "audit.md").unlink()
    assert "required audit source is missing" in check_inventory(manifest, native)[0]


def test_candidate_without_catalog_fields_cannot_disappear(inventory: tuple[dict, Path]) -> None:
    manifest, native = inventory
    del manifest["areas"]["query"]
    assert any("Queries: plan scope is missing" in error for error in check_inventory(manifest, native))


def test_changed_or_missing_plan_is_not_coverage(inventory: tuple[dict, Path]) -> None:
    manifest, native = inventory
    (native / "plan.md").write_text(
        (native / "plan.md").read_text().replace("preserve identity", "preserve raw identity")
    )
    assert check_inventory(manifest, native)
    (native / "plan.md").unlink()
    assert any("required plan source is missing" in error for error in check_inventory(manifest, native))


def test_unresolved_decision_cannot_be_hidden_by_inventory_coverage(inventory: tuple[dict, Path]) -> None:
    manifest, native = inventory
    manifest["areas"]["identity"]["disposition"] = "decision_required"
    assert check_inventory(manifest, native) == []
    assert pending_decisions(manifest)
    manifest["areas"]["identity"]["disposition"] = "defer"
    assert pending_decisions(manifest)
    manifest["areas"]["identity"].update(reason="missing engine callback", later_release_cost="new RPC and SDK version")
    assert pending_decisions(manifest) == []
