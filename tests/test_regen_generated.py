# Copyright 2025, 2026 Query Farm LLC - https://query.farm
# ruff: noqa: D103

"""Coordinated generation cannot silently omit an unavailable SDK checkout."""

import sys
from pathlib import Path

import pytest

from scripts import regen_generated


@pytest.fixture
def generated_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Use two SDK targets, one present and one missing, without real mutations."""
    monkeypatch.setattr(
        regen_generated,
        "_TARGETS",
        [("present.module", "generated.txt", "present"), ("missing.module", "generated.txt", "missing")],
    )
    monkeypatch.setattr(regen_generated, "_repo_root", lambda name: tmp_path if name == "present" else None)
    monkeypatch.setattr(regen_generated, "_render", lambda name: f"{name}\n")
    return tmp_path


@pytest.mark.parametrize("check", [False, True])
def test_require_all_rejects_missing_checkout_before_writes(
    generated_repo: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], check: bool
) -> None:
    existing = generated_repo / "generated.txt"
    existing.write_text("original\n")
    argv = ["regen_generated.py", "--require-all"] + (["--check"] if check else [])
    monkeypatch.setattr(sys, "argv", argv)
    assert regen_generated.main() == 2
    assert existing.read_text() == "original\n"
    assert "required repository missing is not checked out" in capsys.readouterr().err


def test_local_generation_still_allows_missing_checkouts(
    generated_repo: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(sys, "argv", ["regen_generated.py"])
    assert regen_generated.main() == 0
    assert (generated_repo / "generated.txt").read_text() == "present.module\n"
    assert "skip   missing (not checked out)" in capsys.readouterr().out


def test_only_requires_selected_repository(generated_repo: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["regen_generated.py", "--require-all", "--only", "present"])
    assert regen_generated.main() == 0
    assert (generated_repo / "generated.txt").read_text() == "present.module\n"
    monkeypatch.setattr(sys, "argv", ["regen_generated.py", "--require-all", "--only", "missing"])
    assert regen_generated.main() == 2


def test_require_all_check_rejects_missing_artifact(generated_repo: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["regen_generated.py", "--require-all", "--check", "--only", "present"])
    assert regen_generated.main() == 1
    assert not (generated_repo / "generated.txt").exists()
    (generated_repo / "generated.txt").write_text("present.module\n")
    assert regen_generated.main() == 0
