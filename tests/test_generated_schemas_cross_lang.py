"""Cross-language conformance for the generated protocol schemas.

The three per-language drift tests (``test_generated_cpp_schemas.py``,
``test_generated_go_schemas.py``, ``test_generated_ts_schemas.py``) each assert
that a single generated file matches what *its own* generator would emit now.
That catches intra-language drift but not *cross-language* drift — e.g. a bug
where two generators disagree on a field's nullability or the Go generator
silently drops a new schema that Python added.

This test loads all three generated files, parses each back to
``pa.Schema`` objects, and asserts they agree on:

1. The set of schema names.
2. Each schema's field names, types, and nullability.

Skip semantics: if any sibling repo is missing, the test is skipped rather
than failed — this keeps the check valuable on developer machines that only
have one language tree without noise on CI runners that have all three.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

# Reuse the parsers from the per-language tests rather than re-implementing.
from tests.test_generated_cpp_schemas import _parse_generated_hpp, _vgi_generated_path
from tests.test_generated_go_schemas import _parse_generated_go, _vgi_go_generated_path
from tests.test_generated_ts_schemas import _parse_generated_ts, _vgi_ts_generated_path

LanguageLoader = tuple[str, Path, Any]


def _available_languages() -> list[tuple[str, dict[str, pa.Schema]]]:
    """Collect (language, name→Schema) for every generated file that exists.

    Missing sibling repos are quietly skipped.
    """
    loaders: list[tuple[str, Path, Any]] = [
        ("cpp", _vgi_generated_path(), _parse_generated_hpp),
        ("go", _vgi_go_generated_path(), _parse_generated_go),
        ("ts", _vgi_ts_generated_path(), _parse_generated_ts),
    ]
    out: list[tuple[str, dict[str, pa.Schema]]] = []
    for name, path, parser in loaders:
        if path.exists():
            out.append((name, parser(path.read_text())))
    return out


def test_generators_agree_on_schema_names() -> None:
    """Every language must emit the same set of schema names.

    Drift in this test means one language's generator lost or gained a schema
    that the others do not. Regenerate ALL files and commit together.
    """
    langs = _available_languages()
    if len(langs) < 2:
        pytest.skip(
            "need at least two generated files to cross-check; set VGI_GENERATED_HPP / "
            "VGI_GO_GENERATED_GO / VGI_TS_GENERATED_TS or check out the sibling repos"
        )

    canonical_lang, canonical = langs[0]
    canonical_names = set(canonical)
    for other_lang, other in langs[1:]:
        missing = canonical_names - set(other)
        extra = set(other) - canonical_names
        assert not missing and not extra, (
            f"{other_lang} schemas diverge from {canonical_lang}.\n"
            f"  present in {canonical_lang} but not {other_lang}: {sorted(missing)}\n"
            f"  present in {other_lang} but not {canonical_lang}: {sorted(extra)}\n"
            "Regenerate all three languages and commit together."
        )


def test_generators_agree_on_schema_contents() -> None:
    """For every schema name, all languages agree on field names/types/nullability."""
    langs = _available_languages()
    if len(langs) < 2:
        pytest.skip("need at least two generated files to cross-check")

    canonical_lang, canonical = langs[0]
    mismatches: list[str] = []
    for name, canonical_schema in canonical.items():
        for other_lang, other in langs[1:]:
            if name not in other:
                # Covered by the name-set test; skip here so we only report
                # substantive mismatches.
                continue
            other_schema = other[name]
            if not canonical_schema.equals(other_schema, check_metadata=False):
                mismatches.append(
                    f"  {name}:\n    {canonical_lang}: {canonical_schema}\n    {other_lang}: {other_schema}"
                )
    assert not mismatches, (
        "Cross-language schema content mismatch:\n"
        + "\n".join(mismatches)
        + "\n\nRegenerate all three languages and commit together."
    )


def test_at_least_one_language_has_generated_file() -> None:
    """Sanity check that at least one sibling repo is present.

    When zero are present this test is skipped; when at least one is present
    it passes. The actual cross-language assertions are above and only run
    when at least two are present.
    """
    if not _available_languages():
        pytest.skip("no generated schema files found")
