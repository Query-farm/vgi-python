# Copyright 2026 Query Farm LLC - https://query.farm

"""Execute the language-neutral Arrow IPC Filter v2 corpus."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from vgi.filter_v2_builder import deserialize_filter_batch
from vgi.table_filter_pushdown import FilterDeserializationError, FilterVersionError, deserialize_filters

ROOT = Path(__file__).resolve().parents[1] / "conformance" / "filter-v2"
MANIFEST = json.loads((ROOT / "runtime-manifest.json").read_text())
CASES = MANIFEST["cases"]


def _batch(relative: str) -> pa.RecordBatch:
    return deserialize_filter_batch((ROOT / relative).read_bytes())


def _capabilities(case: dict[str, Any]) -> tuple[tuple[str, str | None], ...]:
    return tuple((profile, fingerprint) for profile, fingerprint in case.get("evaluation_capabilities", []))


def test_runtime_manifest_and_digests_are_complete() -> None:
    """Every referenced IPC stream is present and content-addressed."""
    assert MANIFEST["manifest_version"] == 1
    assert MANIFEST["vgi_protocol_version"] == "2.0.0"
    assert MANIFEST["filter_encoding"] == "vgi.filters.v2"
    assert len(CASES) >= 35
    assert len({case["id"] for case in CASES}) == len(CASES)
    for case in CASES:
        referenced = [case["input"], case["filter"]]
        referenced.extend(case.get("join_keys", []))
        referenced.extend(case.get("deltas", []))
        if "expected" in case:
            referenced.append(case["expected"])
        if "worker_expected" in case:
            referenced.append(case["worker_expected"])
        assert set(referenced) == set(case["sha256"])
        for relative in referenced:
            path = ROOT / relative
            assert path.is_file(), relative
            assert hashlib.sha256(path.read_bytes()).hexdigest() == case["sha256"][relative]


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["id"])
def test_runtime_case(case: dict[str, Any]) -> None:
    """Decode, bind, and evaluate each corpus vector under its declared context."""
    input_batch = _batch(case["input"])
    filter_batch = _batch(case["filter"])
    join_keys = [_batch(path) for path in case.get("join_keys", [])]
    if error := case.get("error"):
        with pytest.raises((FilterDeserializationError, FilterVersionError), match=error):
            deserialize_filters(
                filter_batch,
                output_schema=input_batch.schema,
                join_keys=join_keys,
                evaluation_capabilities=_capabilities(case),
            )
        return
    filters = deserialize_filters(
        filter_batch,
        output_schema=input_batch.schema,
        join_keys=join_keys,
        evaluation_capabilities=_capabilities(case),
    )
    for relative in case.get("deltas", []):
        filters = filters.apply_delta(_batch(relative))
    actual = pa.Table.from_batches([filters.apply(input_batch)])
    expected = pa.Table.from_batches([_batch(case["expected"])])
    assert actual.schema.equals(expected.schema, check_metadata=True)
    assert actual.num_rows == expected.num_rows
    if actual.num_rows == 0:
        return
    actual_batch = actual.combine_chunks().to_batches()[0]
    expected_batch = expected.combine_chunks().to_batches()[0]
    assert actual_batch.serialize().equals(expected_batch.serialize()), (
        f"{actual.to_pylist()} != {expected.to_pylist()}"
    )
