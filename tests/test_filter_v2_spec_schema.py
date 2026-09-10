# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Validate the language-neutral Filter Encoding v2 structural corpus."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "conformance" / "filter-v2"
SPEC = ROOT / "docs" / "protocol" / "vgi-filter-encoding-v2-spec.md"
JSON_FENCE = re.compile(r"^```json\s*\n(.*?)^```\s*$", re.MULTILINE | re.DOTALL)


def _reject_duplicate_keys(pairs: Iterable[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key!r}")
        result[key] = value
    return result


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(), object_pairs_hook=_reject_duplicate_keys)


SCHEMA = _load_json(CORPUS / "filter-v2.schema.json")
MANIFEST = _load_json(CORPUS / "manifest.json")
VALIDATOR = Draft202012Validator(SCHEMA)
EXPRESSION_VALIDATOR = Draft202012Validator(
    {
        "$schema": SCHEMA["$schema"],
        "$defs": SCHEMA["$defs"],
        "$ref": "#/$defs/expression",
    }
)


def _format_errors(errors: Iterable[ValidationError]) -> str:
    lines = []
    for error in sorted(errors, key=lambda item: tuple(str(part) for part in item.absolute_path)):
        path = ".".join(str(part) for part in error.absolute_path) or "<document>"
        lines.append(f"{path}: {error.message}")
    return "\n".join(lines)


def test_filter_v2_schema_is_valid_draft_2020_12() -> None:
    """The checked-in schema itself must satisfy its declared JSON Schema draft."""
    Draft202012Validator.check_schema(SCHEMA)


def test_filter_v2_manifest_is_complete_and_deterministic() -> None:
    """Every structural vector must appear exactly once in a stable manifest."""
    assert MANIFEST["vgi_protocol_version"] == "2.0.0"
    assert MANIFEST["filter_encoding"] == "vgi.filters.v2"
    assert MANIFEST["schema"] == "filter-v2.schema.json"

    cases = MANIFEST["cases"]
    case_ids = [case["id"] for case in cases]
    documents = [case["document"] for case in cases]
    assert len(case_ids) == len(set(case_ids))
    assert len(documents) == len(set(documents))
    assert {case["expected"] for case in cases} == {"valid", "invalid"}

    listed_paths = {CORPUS / document for document in documents}
    actual_paths = set((CORPUS / "cases").glob("*/*.json"))
    assert listed_paths == actual_paths


@pytest.mark.parametrize("case", MANIFEST["cases"], ids=lambda case: case["id"])
def test_filter_v2_structural_case(case: dict[str, str]) -> None:
    """Each corpus document must produce its declared structural result."""
    document = _load_json(CORPUS / case["document"])
    errors = list(VALIDATOR.iter_errors(document))

    if case["expected"] == "valid":
        assert not errors, _format_errors(errors)
    else:
        assert errors, f"negative case unexpectedly passed: {case['id']}"


def test_normative_filter_v2_json_examples_match_the_schema() -> None:
    """Applicable JSON examples in the normative specification must stay valid."""
    blocks = JSON_FENCE.findall(SPEC.read_text())
    schematic_blocks = [block for block in blocks if "/*" in block]
    assert len(schematic_blocks) == 3
    assert all("Boolean expression" in block for block in schematic_blocks)

    documents = []
    expressions = []
    for block in blocks:
        if block in schematic_blocks:
            continue
        value = json.loads(block, object_pairs_hook=_reject_duplicate_keys)
        if isinstance(value, dict) and value.get("encoding") == "vgi.filters.v2":
            documents.append(value)
        elif isinstance(value, dict) and "node" in value:
            expressions.append(value)

    assert documents
    assert expressions
    for document in documents:
        errors = list(VALIDATOR.iter_errors(document))
        assert not errors, _format_errors(errors)
    for expression in expressions:
        errors = list(EXPRESSION_VALIDATOR.iter_errors(expression))
        assert not errors, _format_errors(errors)
